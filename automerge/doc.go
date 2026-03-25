// Package automerge provides a pure Go implementation of the Automerge CRDT library.
//
// Documents can be loaded from and saved to the Automerge binary format,
// which is 100% compatible with the Rust automerge implementation.
package automerge

import (
	"fmt"
	"iter"
	"sort"
	"time"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/storage"
	"github.com/develerltd/go-automerge/internal/types"
)

// Doc represents an automerge document.
type Doc struct {
	opSet   *opset.OpSet
	actors  []ActorId
	heads   []ChangeHash
	changes []*storage.ParsedChange

	// For round-trip save of document chunks
	savedChangeCols columnar.RawColumns

	// For tracking change metadata (from loaded changes or new commits)
	changeRecords []opset.ChangeRecord

	// Change data for sync: raw change chunk bytes indexed by hash
	changeData map[ChangeHash][]byte

	// Transaction state
	actorIdx   uint32
	nextOp     uint64
	seq        uint64
	pendingOps []opset.Op
}

// New creates a new empty document with a random actor ID.
func New() *Doc {
	return NewWithActorId(NewActorId())
}

// NewWithActorId creates a new empty document with the given actor ID.
func NewWithActorId(actor ActorId) *Doc {
	return &Doc{
		opSet:      opset.New(),
		actors:     []ActorId{actor},
		changeData: make(map[ChangeHash][]byte),
		actorIdx:   0,
		nextOp:     1,
		seq:        1,
	}
}

// Load creates a Doc from automerge binary data.
func Load(data []byte) (*Doc, error) {
	loaded, err := storage.Load(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDocument, err)
	}

	doc := &Doc{
		changes:    loaded.Changes,
		changeData: make(map[ChangeHash][]byte),
	}

	if loaded.Document != nil {
		pd := loaded.Document
		doc.actors = make([]ActorId, len(pd.Actors))
		for i, a := range pd.Actors {
			doc.actors[i] = ActorId(a)
		}
		doc.heads = make([]ChangeHash, len(pd.Heads))
		for i, h := range pd.Heads {
			doc.heads[i] = ChangeHash(h)
		}
		os, err := opset.DecodeOps(pd.OpColumns, doc.actors)
		if err != nil {
			return nil, fmt.Errorf("decoding ops: %w", err)
		}
		doc.opSet = os
		doc.savedChangeCols = pd.ChangeColumns
		// No need to rebuild predecessors — columns store Succ natively,
		// and Pred is derived on demand when needed (e.g., Merge).
	} else if len(loaded.Changes) > 0 {
		os, actors, heads, changeRecords, err := buildFromChanges(loaded.Changes)
		if err != nil {
			return nil, fmt.Errorf("building from changes: %w", err)
		}
		doc.opSet = os
		doc.actors = actors
		doc.heads = heads
		doc.changeRecords = changeRecords
		// Store raw change data for sync
		for _, change := range loaded.Changes {
			if len(change.RawData) > 0 {
				h := ChangeHash(change.Hash)
				doc.changeData[h] = change.RawData
			}
		}
	} else {
		doc.opSet = opset.New()
	}

	// Initialize mutation state
	doc.nextOp = doc.opSet.MaxOp + 1
	doc.seq = 1 // will be set properly on commit

	return doc, nil
}

func buildFromChanges(changes []*storage.ParsedChange) (*opset.OpSet, []ActorId, []ChangeHash, []opset.ChangeRecord, error) {
	os := opset.New()

	actorSet := make(map[string]int)
	var actors []ActorId

	ensureActor := func(raw []byte) uint32 {
		key := string(raw)
		if idx, ok := actorSet[key]; ok {
			return uint32(idx)
		}
		idx := len(actors)
		actorSet[key] = idx
		actors = append(actors, ActorId(raw))
		return uint32(idx)
	}

	for _, change := range changes {
		ensureActor(change.Actor)
		for _, other := range change.OtherActors {
			ensureActor(other)
		}
	}

	allHashes := make(map[storage.ChangeHash]bool)
	depHashes := make(map[storage.ChangeHash]bool)
	for _, change := range changes {
		allHashes[change.Hash] = true
		for _, dep := range change.Deps {
			depHashes[dep] = true
		}
	}
	var heads []ChangeHash
	for h := range allHashes {
		if !depHashes[h] {
			heads = append(heads, ChangeHash(h))
		}
	}

	// Build change records for later serialization
	var changeRecords []opset.ChangeRecord
	for _, change := range changes {
		changeActorIdx := ensureActor(change.Actor)
		localActors := make([]uint32, 1+len(change.OtherActors))
		localActors[0] = changeActorIdx
		for i, other := range change.OtherActors {
			localActors[i+1] = ensureActor(other)
		}

		// Compute maxOp for this change
		maxOp := change.StartOp
		changeOps, err := opset.DecodeChangeOps(
			columnar.RawColumns(change.OpColumns),
			types.ActorId(change.Actor),
			nil,
		)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("decoding change ops: %w", err)
		}

		for i := range changeOps {
			op := &changeOps[i]
			op.ID.ActorIdx = mapActor(op.ID.ActorIdx, localActors)
			if !op.Obj.IsRoot() {
				op.Obj.ActorIdx = mapActor(op.Obj.ActorIdx, localActors)
			}
			if op.Key.Kind == opset.KeySeq && !op.Key.ElemID.IsZero() {
				op.Key.ElemID.ActorIdx = mapActor(op.Key.ElemID.ActorIdx, localActors)
			}
			for j := range op.Pred {
				op.Pred[j].ActorIdx = mapActor(op.Pred[j].ActorIdx, localActors)
			}
			if op.ID.Counter > maxOp {
				maxOp = op.ID.Counter
			}
			// Update predecessor ops' successor lists directly via columns
			for _, predId := range op.Pred {
				os.UpdateSucc(predId, op.ID)
			}
			os.AddOp(*op)
		}

		depHashes := make([]types.ChangeHash, len(change.Deps))
		for i, d := range change.Deps {
			depHashes[i] = types.ChangeHash(d)
		}
		changeRecords = append(changeRecords, opset.ChangeRecord{
			Hash:      types.ChangeHash(change.Hash),
			ActorIdx:  changeActorIdx,
			Seq:       change.Seq,
			MaxOp:     maxOp,
			Time:      change.Timestamp,
			Message:   change.Message,
			DepHashes: depHashes,
		})
	}

	// Succ was already built via UpdateSucc during op processing.
	// Sort and reindex to ensure correct column order.
	os.SortAndReindex()
	os.Actors = actors
	return os, actors, heads, changeRecords, nil
}

func mapActor(localIdx uint32, mapping []uint32) uint32 {
	if int(localIdx) < len(mapping) {
		return mapping[localIdx]
	}
	return localIdx
}

// rebuildSuccessors rebuilds Succ from Pred on all ops in the OpSet.
// Used after applying changes where ops have Pred but need Succ.
func rebuildSuccessors(os *opset.OpSet) {
	ops := os.AllOps()
	opset.DeriveSuccFromPred(ops)
	os.RebuildFromOps(ops)
}

// ensureActorIdx ensures the document's actor is in the actors table and sets actorIdx.
func (d *Doc) ensureActorIdx() {
	if len(d.actors) == 0 {
		actor := NewActorId()
		d.actors = append(d.actors, actor)
		d.actorIdx = 0
		return
	}
	// actorIdx should already be set from New/NewWithActorId or we need to add the actor
	// For loaded docs, we add a new actor on first mutation
	if d.actorIdx == 0 && d.nextOp > 1 {
		// Already loaded, need to check if we have an actor set up
		// actorIdx 0 is fine if it was set during load
	}
}

// allocOpId allocates the next operation ID.
func (d *Doc) allocOpId() OpId {
	id := OpId{Counter: d.nextOp, ActorIdx: d.actorIdx}
	d.nextOp++
	return id
}

// applyOp applies an operation to the OpSet and tracks it as pending.
func (d *Doc) applyOp(op opset.Op) {
	// Update predecessors' Succ
	for _, predId := range op.Pred {
		d.opSet.UpdateSucc(predId, op.ID)
	}
	d.opSet.InsertOp(op)
	d.pendingOps = append(d.pendingOps, op)
}

// Put sets a scalar value at a map key.
func (d *Doc) Put(obj ObjId, key string, value ScalarValue) error {
	prop := MapProp(key)

	// Find existing visible ops at this location (these are our predecessors)
	existingOps := d.opSet.GetAll(obj, prop)
	var pred []OpId
	for _, ov := range existingOps {
		pred = append(pred, ov.ID)
	}

	opId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    opset.Key{Kind: opset.KeyMap, MapKey: key},
		Insert: false,
		Action: opset.ActionSet,
		Value:  value,
		Pred:   pred,
	})

	return nil
}

// PutObject creates a new object at a map key and returns its ID.
func (d *Doc) PutObject(obj ObjId, key string, objType ObjType) (ObjId, error) {
	prop := MapProp(key)

	existingOps := d.opSet.GetAll(obj, prop)
	var pred []OpId
	for _, ov := range existingOps {
		pred = append(pred, ov.ID)
	}

	var action opset.Action
	switch objType {
	case ObjTypeMap:
		action = opset.ActionMakeMap
	case ObjTypeList:
		action = opset.ActionMakeList
	case ObjTypeText:
		action = opset.ActionMakeText
	case ObjTypeTable:
		action = opset.ActionMakeTable
	default:
		return ObjId{}, fmt.Errorf("unknown object type: %d", objType)
	}

	opId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    opset.Key{Kind: opset.KeyMap, MapKey: key},
		Insert: false,
		Action: action,
		Pred:   pred,
	})

	return ObjId{OpId: opId}, nil
}

// Delete removes a value at a map key or sequence index.
func (d *Doc) Delete(obj ObjId, prop Prop) error {
	opId := d.allocOpId()

	var key opset.Key
	var pred []OpId

	if prop.Kind == PropKindMap {
		key = opset.Key{Kind: opset.KeyMap, MapKey: prop.MapKey}
		existingOps := d.opSet.GetAll(obj, prop)
		if len(existingOps) == 0 {
			return nil
		}
		for _, ov := range existingOps {
			pred = append(pred, ov.ID)
		}
	} else {
		// For sequence delete, find the visible element at the given index
		elements := d.opSet.VisibleListElements(obj)
		if prop.SeqIndex >= uint64(len(elements)) {
			return fmt.Errorf("index %d out of range (len=%d)", prop.SeqIndex, len(elements))
		}
		elem := elements[prop.SeqIndex]
		key = opset.Key{Kind: opset.KeySeq, ElemID: elem.ElemID}
		pred = []OpId{elem.Op.ID}
	}

	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    key,
		Insert: false,
		Action: opset.ActionDelete,
		Pred:   pred,
	})

	return nil
}

// Insert inserts a scalar value at the given index of a list/text object.
func (d *Doc) Insert(obj ObjId, index uint64, value ScalarValue) error {
	elements := d.opSet.VisibleListElements(obj)
	if index > uint64(len(elements)) {
		return fmt.Errorf("index %d out of range (len=%d)", index, len(elements))
	}

	var elemId types.OpId
	if index > 0 {
		elemId = elements[index-1].ElemID
	}
	// else elemId is zero (HEAD)

	opId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    opset.Key{Kind: opset.KeySeq, ElemID: elemId},
		Insert: true,
		Action: opset.ActionSet,
		Value:  value,
	})

	return nil
}

// InsertObject inserts a new object at the given index of a list.
func (d *Doc) InsertObject(obj ObjId, index uint64, objType ObjType) (ObjId, error) {
	elements := d.opSet.VisibleListElements(obj)
	if index > uint64(len(elements)) {
		return ObjId{}, fmt.Errorf("index %d out of range (len=%d)", index, len(elements))
	}

	var elemId types.OpId
	if index > 0 {
		elemId = elements[index-1].ElemID
	}

	var action opset.Action
	switch objType {
	case ObjTypeMap:
		action = opset.ActionMakeMap
	case ObjTypeList:
		action = opset.ActionMakeList
	case ObjTypeText:
		action = opset.ActionMakeText
	case ObjTypeTable:
		action = opset.ActionMakeTable
	default:
		return ObjId{}, fmt.Errorf("unknown object type: %d", objType)
	}

	opId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    opset.Key{Kind: opset.KeySeq, ElemID: elemId},
		Insert: true,
		Action: action,
	})

	return ObjId{OpId: opId}, nil
}

// Increment increments a counter value at a map key.
func (d *Doc) Increment(obj ObjId, key string, by int64) error {
	prop := MapProp(key)

	existingOps := d.opSet.GetAll(obj, prop)
	if len(existingOps) == 0 {
		return fmt.Errorf("%w: no counter at key %q", ErrNotFound, key)
	}

	var pred []OpId
	for _, ov := range existingOps {
		pred = append(pred, ov.ID)
	}

	opId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:     opId,
		Obj:    obj,
		Key:    opset.Key{Kind: opset.KeyMap, MapKey: key},
		Insert: false,
		Action: opset.ActionIncrement,
		Value:  types.NewInt(by),
		Pred:   pred,
	})

	return nil
}

// SpliceText replaces del characters at pos with text in a text object.
func (d *Doc) SpliceText(obj ObjId, pos, del uint64, text string) error {
	// Delete characters
	for i := uint64(0); i < del; i++ {
		if err := d.Delete(obj, SeqProp(pos)); err != nil {
			return fmt.Errorf("deleting at pos %d: %w", pos, err)
		}
	}

	// Insert characters
	for i, ch := range text {
		if err := d.Insert(obj, pos+uint64(i), types.NewStr(string(ch))); err != nil {
			return fmt.Errorf("inserting at pos %d: %w", pos+uint64(i), err)
		}
	}

	return nil
}

// Commit finalizes the current set of pending operations as a change.
// If there are no pending ops, this is a no-op.
func (d *Doc) Commit(message string, timestamp int64) {
	if len(d.pendingOps) == 0 {
		return
	}

	// Determine startOp and maxOp for this change
	startOp := d.pendingOps[0].ID.Counter
	maxOp := startOp
	for _, op := range d.pendingOps {
		if op.ID.Counter > maxOp {
			maxOp = op.ID.Counter
		}
	}

	// Build the change chunk data to compute the hash
	// Map op actor indices to change-local indices (actor 0 = our actor)
	changeOpCols := opset.EncodeChangeOps(d.pendingOps)

	changeData := storage.AppendChangeChunkData(
		changeHashesToStorage(d.heads),
		[]byte(d.actors[d.actorIdx]),
		d.seq,
		startOp,
		timestamp,
		message,
		nil, // no other actors for now (all ops use our actor)
		changeOpCols,
	)

	hash := storage.ComputeHash(storage.ChunkTypeChange, changeData)

	// Create change record
	record := opset.ChangeRecord{
		Hash:      types.ChangeHash(hash),
		ActorIdx:  d.actorIdx,
		Seq:       d.seq,
		MaxOp:     maxOp,
		Time:      timestamp,
		Message:   message,
		DepHashes: make([]types.ChangeHash, len(d.heads)),
	}
	copy(record.DepHashes, d.heads)

	d.changeRecords = append(d.changeRecords, record)
	// Store raw change data for sync
	changeDataCopy := make([]byte, len(changeData))
	copy(changeDataCopy, changeData)
	if d.changeData == nil {
		d.changeData = make(map[ChangeHash][]byte)
	}
	d.changeData[types.ChangeHash(hash)] = changeDataCopy
	d.heads = []ChangeHash{types.ChangeHash(hash)}
	d.seq++
	d.pendingOps = nil
	d.savedChangeCols = nil // invalidate cached change columns
}

// Save serializes the document to the automerge binary format.
// Any pending operations are committed first.
func (d *Doc) Save() ([]byte, error) {
	// Auto-commit pending ops
	if len(d.pendingOps) > 0 {
		d.Commit("", time.Now().UnixMilli())
	}

	// Export op columns directly from the columnar storage (no encode round-trip)
	opCols := d.opSet.ExportColumns()

	// Build change columns
	var changeCols columnar.RawColumns
	if d.savedChangeCols != nil && len(d.changeRecords) == 0 {
		// Round-trip: reuse original change columns
		changeCols = d.savedChangeCols
	} else {
		// Build from change records
		hashToIdx := make(map[types.ChangeHash]int)
		for i, cr := range d.changeRecords {
			hashToIdx[cr.Hash] = i
		}
		changeCols = opset.EncodeChangeCols(d.changeRecords, hashToIdx)
	}

	// Build actors table as [][]byte
	actorBytes := make([][]byte, len(d.actors))
	for i, a := range d.actors {
		actorBytes[i] = []byte(a)
	}

	// Build heads as storage.ChangeHash
	storageHeads := changeHashesToStorage(d.heads)

	return storage.SaveDocument(actorBytes, storageHeads, changeCols, opCols), nil
}

func changeHashesToStorage(heads []types.ChangeHash) []storage.ChangeHash {
	result := make([]storage.ChangeHash, len(heads))
	for i, h := range heads {
		result[i] = storage.ChangeHash(h)
	}
	return result
}

// Fork creates a copy of this document with a new random actor ID.
func (d *Doc) Fork() *Doc {
	return d.ForkWithActorId(NewActorId())
}

// ForkWithActorId creates a copy of this document with the given actor ID.
func (d *Doc) ForkWithActorId(actor ActorId) *Doc {
	// Auto-commit pending ops first
	if len(d.pendingOps) > 0 {
		d.Commit("", 0)
	}

	newDoc := &Doc{
		opSet:           d.opSet.Clone(),
		actors:          make([]ActorId, len(d.actors)),
		heads:           make([]ChangeHash, len(d.heads)),
		savedChangeCols: d.savedChangeCols,
		changeRecords:   make([]opset.ChangeRecord, len(d.changeRecords)),
		changeData:      make(map[ChangeHash][]byte, len(d.changeData)),
	}
	copy(newDoc.actors, d.actors)
	copy(newDoc.heads, d.heads)
	copy(newDoc.changeRecords, d.changeRecords)
	for k, v := range d.changeData {
		newDoc.changeData[k] = v
	}

	// Add the new actor
	newDoc.actors = append(newDoc.actors, actor)
	newDoc.actorIdx = uint32(len(newDoc.actors) - 1)
	newDoc.nextOp = d.nextOp
	newDoc.seq = 1

	return newDoc
}

// Merge incorporates all changes from other into this document.
func (d *Doc) Merge(other *Doc) error {
	// Auto-commit pending ops on both sides
	if len(d.pendingOps) > 0 {
		d.Commit("", 0)
	}

	// Build actor mapping: other's actor index -> our actor index
	actorMapping := make([]uint32, len(other.actors))
	for i, actor := range other.actors {
		actorMapping[i] = d.ensureActorInTable(actor)
	}

	// Materialize all ops from both sides
	selfOps := d.opSet.AllOps()
	otherAllOps := other.opSet.AllOps()

	// Build set of our existing op IDs (using global actor indices)
	existingSet := make(map[types.OpId]bool, len(selfOps))
	for _, op := range selfOps {
		existingSet[op.ID] = true
	}

	// Collect new ops from other, remapping actor indices
	var newOps []opset.Op
	for _, op := range otherAllOps {
		remapped := remapOp(op, actorMapping)
		if existingSet[remapped.ID] {
			continue
		}
		newOps = append(newOps, remapped)
	}

	if len(newOps) == 0 {
		// Nothing new, but still update heads
		d.mergeHeads(other.heads)
		return nil
	}

	// Derive Pred from Succ on existing ops (columns only store Succ)
	opset.DerivePredFromSucc(selfOps)
	// Derive Pred from Succ on other's ops (before remapping clears info)
	opset.DerivePredFromSucc(otherAllOps)
	// Re-remap the new ops (with Pred now populated)
	newOps = nil
	for _, op := range otherAllOps {
		remapped := remapOp(op, actorMapping)
		if existingSet[remapped.ID] {
			continue
		}
		newOps = append(newOps, remapped)
	}

	// Combine all ops, clear Succ, rebuild from Pred
	allOps := make([]opset.Op, 0, len(selfOps)+len(newOps))
	for _, op := range selfOps {
		op.Succ = nil
		allOps = append(allOps, op)
	}
	for _, op := range newOps {
		op.Succ = nil
		allOps = append(allOps, op)
	}

	// Rebuild Succ from Pred, sort, and rebuild columns
	opset.DeriveSuccFromPred(allOps)
	sort.Slice(allOps, func(i, j int) bool {
		return opset.OpLess(&allOps[i], &allOps[j])
	})
	d.opSet.RebuildFromOps(allOps)

	// Merge heads
	d.mergeHeads(other.heads)

	// Merge change records
	existingHashes := make(map[types.ChangeHash]bool)
	for _, cr := range d.changeRecords {
		existingHashes[cr.Hash] = true
	}
	for _, cr := range other.changeRecords {
		if existingHashes[cr.Hash] {
			continue
		}
		remapped := cr
		remapped.ActorIdx = actorMapping[cr.ActorIdx]
		d.changeRecords = append(d.changeRecords, remapped)
	}

	// Update nextOp
	if d.opSet.MaxOp >= d.nextOp {
		d.nextOp = d.opSet.MaxOp + 1
	}

	// Invalidate saved change columns
	d.savedChangeCols = nil

	return nil
}

// ensureActorInTable adds the actor to the table if not present and returns its index.
func (d *Doc) ensureActorInTable(actor ActorId) uint32 {
	for i, a := range d.actors {
		if a.Compare(actor) == 0 {
			return uint32(i)
		}
	}
	idx := uint32(len(d.actors))
	d.actors = append(d.actors, actor)
	return idx
}

// mergeHeads computes the merged heads: union of both head sets, removing any that are
// known to be ancestors (appear as deps in change records).
func (d *Doc) mergeHeads(otherHeads []ChangeHash) {
	headSet := make(map[ChangeHash]bool)
	for _, h := range d.heads {
		headSet[h] = true
	}
	for _, h := range otherHeads {
		headSet[h] = true
	}

	// Remove heads that are deps of any change record
	depSet := make(map[ChangeHash]bool)
	for _, cr := range d.changeRecords {
		for _, dep := range cr.DepHashes {
			depSet[ChangeHash(dep)] = true
		}
	}
	var merged []ChangeHash
	for h := range headSet {
		if !depSet[h] {
			merged = append(merged, h)
		}
	}
	if len(merged) == 0 {
		// Fallback: keep all heads if we can't determine ancestry
		merged = make([]ChangeHash, 0, len(headSet))
		for h := range headSet {
			merged = append(merged, h)
		}
	}
	d.heads = merged
}

// remapOp creates a copy of an op with actor indices remapped.
func remapOp(op opset.Op, actorMapping []uint32) opset.Op {
	cp := op.Clone()
	cp.ID.ActorIdx = mapActorIdx(cp.ID.ActorIdx, actorMapping)
	if !cp.Obj.IsRoot() {
		cp.Obj.ActorIdx = mapActorIdx(cp.Obj.ActorIdx, actorMapping)
	}
	if cp.Key.Kind == opset.KeySeq && !cp.Key.ElemID.IsZero() {
		cp.Key.ElemID.ActorIdx = mapActorIdx(cp.Key.ElemID.ActorIdx, actorMapping)
	}
	for j := range cp.Pred {
		cp.Pred[j].ActorIdx = mapActorIdx(cp.Pred[j].ActorIdx, actorMapping)
	}
	for j := range cp.Succ {
		cp.Succ[j].ActorIdx = mapActorIdx(cp.Succ[j].ActorIdx, actorMapping)
	}
	return cp
}

func mapActorIdx(idx uint32, mapping []uint32) uint32 {
	if int(idx) < len(mapping) {
		return mapping[idx]
	}
	return idx
}

// Get returns the value at the given property of the given object.
func (d *Doc) Get(obj ObjId, prop Prop) (Value, ExId, error) {
	if prop.Kind == PropKindSeq {
		val, opId, found := d.opSet.ListGet(obj, prop.SeqIndex)
		if !found {
			return Value{}, ExId{}, ErrNotFound
		}
		return val, d.opIdToExId(opId), nil
	}
	val, opId, found := d.opSet.Get(obj, prop)
	if !found {
		return Value{}, ExId{}, ErrNotFound
	}
	return val, d.opIdToExId(opId), nil
}

// GetAll returns all visible values (including conflicts) at the given property.
func (d *Doc) GetAll(obj ObjId, prop Prop) ([]ValueWithId, error) {
	opvals := d.opSet.GetAll(obj, prop)
	results := make([]ValueWithId, len(opvals))
	for i, ov := range opvals {
		results[i] = ValueWithId{
			Value: ov.Value,
			ID:    d.opIdToExId(ov.ID),
		}
	}
	return results, nil
}

// ValueWithId pairs a value with its external ID.
type ValueWithId struct {
	Value Value
	ID    ExId
}

// Keys returns the visible map keys for the given object, sorted alphabetically.
func (d *Doc) Keys(obj ObjId) []string {
	return d.opSet.Keys(obj)
}

// Length returns the number of visible elements in a list/text, or keys in a map.
func (d *Doc) Length(obj ObjId) uint64 {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return 0
	}
	if objType.IsSequence() {
		return d.opSet.ListLen(obj)
	}
	return uint64(len(d.opSet.Keys(obj)))
}

// Text returns the text content of a text object.
func (d *Doc) Text(obj ObjId) (string, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return "", fmt.Errorf("getting object type: %w", err)
	}
	if objType != ObjTypeText {
		return "", fmt.Errorf("%w: expected text, got %s", ErrTypeMismatch, objType)
	}
	return d.opSet.Text(obj), nil
}

// MapRange returns an iterator over the visible key-value pairs of a map object.
func (d *Doc) MapRange(obj ObjId) iter.Seq2[string, Value] {
	return func(yield func(string, Value) bool) {
		for _, key := range d.opSet.Keys(obj) {
			val, _, found := d.opSet.Get(obj, MapProp(key))
			if !found {
				continue
			}
			if !yield(key, val) {
				return
			}
		}
	}
}

// ListItems returns an iterator over the visible index-value pairs of a list/text object.
func (d *Doc) ListItems(obj ObjId) iter.Seq2[uint64, Value] {
	return func(yield func(uint64, Value) bool) {
		elements := d.opSet.VisibleListElements(obj)
		for i, elem := range elements {
			if !yield(uint64(i), elem.Op.ToValue()) {
				return
			}
		}
	}
}

// Splice replaces del elements at pos in a list with the given values.
func (d *Doc) Splice(obj ObjId, pos, del uint64, vals ...ScalarValue) error {
	for i := uint64(0); i < del; i++ {
		if err := d.Delete(obj, SeqProp(pos)); err != nil {
			return fmt.Errorf("deleting at pos %d: %w", pos, err)
		}
	}
	for i, v := range vals {
		if err := d.Insert(obj, pos+uint64(i), v); err != nil {
			return fmt.Errorf("inserting at pos %d: %w", pos+uint64(i), err)
		}
	}
	return nil
}

// Heads returns the current heads of the document.
func (d *Doc) Heads() []ChangeHash { return d.heads }

// Actors returns the actor table.
func (d *Doc) Actors() []ActorId { return d.actors }

// PathElement represents one step in a path from an object to the root.
type PathElement struct {
	ObjId  ObjId
	ObjTyp ObjType
	Prop   Prop
}

// Parents returns the path from the given object to the root.
// The first element is the immediate parent, the last is the root.
func (d *Doc) Parents(obj ObjId) ([]PathElement, error) {
	if obj.IsRoot() {
		return nil, nil
	}

	var path []PathElement
	current := obj
	for !current.IsRoot() {
		info, err := d.opSet.GetParentInfo(current)
		if err != nil {
			return nil, fmt.Errorf("getting parent of %s: %w", current.OpId, err)
		}

		parentType, err := d.opSet.GetObjType(info.Parent)
		if err != nil && !info.Parent.IsRoot() {
			return nil, fmt.Errorf("getting type of %s: %w", info.Parent.OpId, err)
		}
		if info.Parent.IsRoot() {
			parentType = ObjTypeMap
		}

		path = append(path, PathElement{
			ObjId:  info.Parent,
			ObjTyp: parentType,
			Prop:   info.Prop,
		})
		current = info.Parent
	}
	return path, nil
}

func (d *Doc) opIdToExId(id OpId) ExId {
	if id.IsZero() {
		return RootExId()
	}
	var actor ActorId
	if int(id.ActorIdx) < len(d.actors) {
		actor = d.actors[id.ActorIdx]
	}
	return ExId{
		Counter:  id.Counter,
		Actor:    actor,
		ActorIdx: id.ActorIdx,
	}
}
