package opset

import (
	"fmt"
	"sort"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/types"
)

// ListElement pairs a visible op with the elemId of the list element it belongs to.
type ListElement struct {
	ElemID types.OpId
	Op     *Op
}

// ObjInfo stores metadata about an object in the document.
type ObjInfo struct {
	Parent  types.ObjId
	ObjType types.ObjType
}

// OpSet is the core CRDT data structure. It stores all operations in columnar format
// using hexane ColumnData, indexed by object for fast queries.
type OpSet struct {
	Actors []types.ActorId
	MaxOp  uint64

	// cols stores all operations in compressed columnar layout.
	cols *OpColumns

	// objInfo maps object IDs to their metadata
	objInfo map[types.OpId]ObjInfo

	// objIndex maps obj -> row range in the columns for fast lookup
	objIndex map[types.OpId]objRange

	// opIdToRow maps op ID -> row index for O(1) lookup (used by UpdateSucc)
	opIdToRow map[types.OpId]int

	// seqOrder maps (obj, elemID) -> visible index for sequences
	// Built lazily when needed
	seqOrderDirty bool
}

type objRange struct {
	start int
	end   int
}

// New creates an empty OpSet.
func New() *OpSet {
	return &OpSet{
		cols:      NewOpColumns(),
		objInfo:   make(map[types.OpId]ObjInfo),
		objIndex:  make(map[types.OpId]objRange),
		opIdToRow: make(map[types.OpId]int),
	}
}

// AddOp adds an operation to the OpSet. Operations should be added in sorted order
// (by obj, then key, then OpId).
func (os *OpSet) AddOp(op Op) {
	idx := os.cols.Len()
	os.cols.Splice(idx, []Op{op})

	// Track max op
	if op.ID.Counter > os.MaxOp {
		os.MaxOp = op.ID.Counter
	}

	// Update object index
	objKey := op.Obj.OpId
	if r, ok := os.objIndex[objKey]; ok {
		r.end = idx + 1
		os.objIndex[objKey] = r
	} else {
		os.objIndex[objKey] = objRange{start: idx, end: idx + 1}
	}

	// Track object creation
	if op.Action.IsMake() {
		os.objInfo[op.ID] = ObjInfo{
			Parent:  op.Obj,
			ObjType: op.ObjType(),
		}
	}

	os.opIdToRow[op.ID] = idx
	os.seqOrderDirty = true
}

// ParentInfo describes where an object was created.
type ParentInfo struct {
	Parent types.ObjId
	Prop   types.Prop
}

// GetParentInfo returns the parent object and property under which the given object was created.
func (os *OpSet) GetParentInfo(obj types.ObjId) (ParentInfo, error) {
	if obj.IsRoot() {
		return ParentInfo{}, fmt.Errorf("root has no parent")
	}
	info, ok := os.objInfo[obj.OpId]
	if !ok {
		return ParentInfo{}, fmt.Errorf("object %s not found", obj.OpId)
	}

	// Use opIdToRow for O(1) lookup instead of linear scan
	if row, ok := os.opIdToRow[obj.OpId]; ok {
		op := os.cols.Get(row)
		if op != nil {
			var prop types.Prop
			if op.Key.Kind == KeyMap {
				prop = types.MapProp(op.Key.MapKey)
			} else {
				prop = types.SeqProp(0) // index not easily determined here
			}
			return ParentInfo{Parent: info.Parent, Prop: prop}, nil
		}
	}
	return ParentInfo{Parent: info.Parent}, nil
}

// ObjType returns the type of the given object, or an error if not found.
func (os *OpSet) GetObjType(obj types.ObjId) (types.ObjType, error) {
	if obj.IsRoot() {
		return types.ObjTypeMap, nil
	}
	info, ok := os.objInfo[obj.OpId]
	if !ok {
		return 0, fmt.Errorf("object %s not found", obj.OpId)
	}
	return info.ObjType, nil
}

// OpsForObj returns all operations for the given object.
// The returned slice is a materialized copy from the columns.
func (os *OpSet) OpsForObj(obj types.ObjId) []Op {
	r, ok := os.objIndex[obj.OpId]
	if !ok {
		return nil
	}
	return os.cols.MaterializeRange(r.start, r.end)
}

// Get returns the winning visible value at (obj, prop), along with its OpId.
// For conflicts, returns the value with the highest OpId.
func (os *OpSet) Get(obj types.ObjId, prop types.Prop) (types.Value, types.OpId, bool) {
	ops := os.OpsForObj(obj)
	if len(ops) == 0 {
		return types.Value{}, types.OpId{}, false
	}

	var winner *Op
	for i := range ops {
		op := &ops[i]
		if !matchesKey(op, prop) {
			continue
		}
		if !op.IsVisible() {
			continue
		}
		if winner == nil || op.ID.Compare(winner.ID) > 0 {
			winner = op
		}
	}

	if winner == nil {
		return types.Value{}, types.OpId{}, false
	}

	// Counter accumulation: if the winning op is an increment, sum the chain
	if winner.Action == ActionIncrement {
		total := os.accumulateCounter(ops, winner)
		return types.NewScalarValue(types.NewCounter(total)), winner.ID, true
	}

	return winner.ToValue(), winner.ID, true
}

// accumulateCounter computes the counter total by finding all ops for this key:
// the initial counter set op plus all increment ops in the successor chain.
func (os *OpSet) accumulateCounter(ops []Op, tip *Op) int64 {
	var total int64
	for i := range ops {
		op := &ops[i]
		if op.Key != tip.Key {
			continue
		}
		if op.Value.Type() == types.ScalarTypeCounter {
			total += op.Value.Counter()
		} else if op.Action == ActionIncrement {
			total += op.Value.Int()
		}
	}
	return total
}

// GetAll returns all visible values at (obj, prop) — includes conflicts.
func (os *OpSet) GetAll(obj types.ObjId, prop types.Prop) []OpValue {
	ops := os.OpsForObj(obj)
	var results []OpValue

	for i := range ops {
		op := &ops[i]
		if !matchesKey(op, prop) {
			continue
		}
		if !op.IsVisible() {
			continue
		}
		results = append(results, OpValue{
			Value: op.ToValue(),
			ID:    op.ID,
		})
	}

	// Sort by OpId (deterministic conflict resolution order)
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID.Compare(results[j].ID) < 0
	})

	return results
}

// OpValue pairs a value with its operation ID.
type OpValue struct {
	Value types.Value
	ID    types.OpId
}

// Keys returns the visible map keys for the given object, sorted.
func (os *OpSet) Keys(obj types.ObjId) []string {
	ops := os.OpsForObj(obj)
	seen := make(map[string]bool)
	var keys []string

	for i := range ops {
		op := &ops[i]
		if op.Key.Kind != KeyMap {
			continue
		}
		if !op.IsVisible() {
			continue
		}
		if !seen[op.Key.MapKey] {
			seen[op.Key.MapKey] = true
			keys = append(keys, op.Key.MapKey)
		}
	}

	sort.Strings(keys)
	return keys
}

// ListLen returns the number of visible elements in a list/text object.
func (os *OpSet) ListLen(obj types.ObjId) uint64 {
	elements := os.visibleListElements(obj)
	return uint64(len(elements))
}

// ListGet returns the value at the given index in a list/text object.
func (os *OpSet) ListGet(obj types.ObjId, index uint64) (types.Value, types.OpId, bool) {
	elements := os.visibleListElements(obj)
	if int(index) >= len(elements) {
		return types.Value{}, types.OpId{}, false
	}
	op := elements[index]
	return op.ToValue(), op.ID, true
}

// Text returns the text content of a text object.
func (os *OpSet) Text(obj types.ObjId) string {
	elements := os.visibleListElements(obj)
	var result []byte
	for _, op := range elements {
		if op.Value.Type() == types.ScalarTypeString {
			result = append(result, op.Value.Str()...)
		}
	}
	return string(result)
}

// visibleListElements returns visible operations for a sequence object in order.
func (os *OpSet) visibleListElements(obj types.ObjId) []*Op {
	elements := os.VisibleListElements(obj)
	result := make([]*Op, len(elements))
	for i, e := range elements {
		result[i] = e.Op
	}
	return result
}

func matchesKey(op *Op, prop types.Prop) bool {
	switch prop.Kind {
	case types.PropKindMap:
		return op.Key.Kind == KeyMap && op.Key.MapKey == prop.MapKey
	case types.PropKindSeq:
		// For sequence access by index, we need the visible list elements
		// This is handled at a higher level; direct key matching isn't used for sequences
		return false
	default:
		return false
	}
}

// InsertOp adds an operation to the OpSet, maintaining sorted order.
// Uses binary search to find the correct position and column splice for O(log n) insert.
func (os *OpSet) InsertOp(op Op) {
	if op.ID.Counter > os.MaxOp {
		os.MaxOp = op.ID.Counter
	}
	if op.Action.IsMake() {
		os.objInfo[op.ID] = ObjInfo{Parent: op.Obj, ObjType: op.ObjType()}
	}

	// Find correct sorted position
	pos := os.findInsertPos(&op)
	os.cols.Splice(pos, []Op{op})

	// Rebuild indices (positions shifted)
	os.rebuildIndices()
	os.seqOrderDirty = true
}

// findInsertPos finds the correct sorted position for a new op.
func (os *OpSet) findInsertPos(op *Op) int {
	objKey := op.Obj.OpId
	r, ok := os.objIndex[objKey]
	if ok {
		// Binary search within the object's range
		lo, hi := r.start, r.end
		for lo < hi {
			mid := (lo + hi) / 2
			midOp := os.cols.Get(mid)
			if midOp != nil && OpLess(midOp, op) {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return lo
	}

	// New object — binary search on full range to find where this object goes
	lo, hi := 0, os.cols.Len()
	for lo < hi {
		mid := (lo + hi) / 2
		midOp := os.cols.Get(mid)
		if midOp != nil && ObjIdCompare(midOp.Obj, op.Obj) < 0 {
			lo = mid + 1
		} else if midOp != nil && ObjIdCompare(midOp.Obj, op.Obj) == 0 {
			// Same object but not in index (shouldn't happen), compare fully
			if OpLess(midOp, op) {
				lo = mid + 1
			} else {
				hi = mid
			}
		} else {
			hi = mid
		}
	}
	return lo
}

// SortAndReindex sorts all ops and rebuilds the columns and indices.
func (os *OpSet) SortAndReindex() {
	ops := os.cols.ToOps()
	sort.Slice(ops, func(i, j int) bool {
		return OpLess(&ops[i], &ops[j])
	})
	os.RebuildFromOps(ops)
	os.seqOrderDirty = true
}

// RebuildFromOps rebuilds the columns and indices from a slice of ops.
// The ops should already be sorted.
func (os *OpSet) RebuildFromOps(ops []Op) {
	os.cols = NewOpColumns()
	if len(ops) > 0 {
		os.cols.Splice(0, ops)
	}
	os.rebuildIndices()
}

// rebuildIndices rebuilds objIndex and opIdToRow from the columns.
func (os *OpSet) rebuildIndices() {
	os.objIndex = make(map[types.OpId]objRange)
	os.opIdToRow = make(map[types.OpId]int)
	it := os.cols.Iter()
	for i := 0; ; i++ {
		op, ok := it.Next()
		if !ok {
			break
		}
		objKey := op.Obj.OpId
		if r, exists := os.objIndex[objKey]; exists {
			r.end = i + 1
			os.objIndex[objKey] = r
		} else {
			os.objIndex[objKey] = objRange{start: i, end: i + 1}
		}
		os.opIdToRow[op.ID] = i
		if op.Action.IsMake() {
			os.objInfo[op.ID] = ObjInfo{Parent: op.Obj, ObjType: op.ObjType()}
		}
	}
}

// UpdateSucc adds succId to the Succ list of the op with the given predId.
// Uses direct column manipulation for O(log n) performance.
func (os *OpSet) UpdateSucc(predId, succId types.OpId) {
	row, ok := os.opIdToRow[predId]
	if !ok {
		return
	}

	// Get current succ count
	countPtr, cok := os.cols.SuccCount.Get(row)
	if !cok {
		return
	}
	currentCount := uint64(0)
	if countPtr != nil {
		currentCount = *countPtr
	}

	// Compute succ record offset: total succ records before this row
	succOffset := os.cols.SuccCount.GetAcc(row).AsInt()
	insertPos := succOffset + int(currentCount)

	// Insert new succ record into detail columns
	os.cols.SuccActor.Splice(insertPos, 0, []uint64{uint64(succId.ActorIdx)})
	os.cols.SuccCtr.Splice(insertPos, 0, []int64{int64(succId.Counter)})

	// Replace succ count: delete old value, insert new
	newCount := currentCount + 1
	os.cols.SuccCount.Splice(row, 1, []uint64{newCount})
}

// ExportColumns exports the operation columns as RawColumns for serialization.
func (os *OpSet) ExportColumns() columnar.RawColumns {
	return os.cols.Export()
}

// Cols returns the underlying OpColumns (for advanced use).
func (os *OpSet) Cols() *OpColumns {
	return os.cols
}

// VisibleListElements returns visible list elements with their elemIds, in correct
// document order. Uses tree-based traversal: each insert targets a predecessor element,
// and children of the same parent are visited in descending OpId order (later inserts first).
func (os *OpSet) VisibleListElements(obj types.ObjId) []ListElement {
	ops := os.OpsForObj(obj)
	if len(ops) == 0 {
		return nil
	}

	// For each element, track its visible ops
	type elemInfo struct {
		ops []*Op // visible ops at this element position
	}
	elemMap := make(map[types.OpId]*elemInfo)

	// children[parentElemID] = list of child elemIDs (inserts targeting that parent)
	children := make(map[types.OpId][]types.OpId)

	for i := range ops {
		op := &ops[i]
		if op.Insert {
			if elemMap[op.ID] == nil {
				elemMap[op.ID] = &elemInfo{}
			}
			children[op.Key.ElemID] = append(children[op.Key.ElemID], op.ID)
			if op.IsVisible() {
				elemMap[op.ID].ops = append(elemMap[op.ID].ops, op)
			}
		} else if op.Key.Kind == KeySeq {
			entry := elemMap[op.Key.ElemID]
			if entry != nil && op.IsVisible() {
				entry.ops = append(entry.ops, op)
			}
		}
	}

	// Sort each parent's children in descending OpId order
	// (higher counter/later inserts appear first in the list)
	for _, kids := range children {
		sort.Slice(kids, func(i, j int) bool {
			return kids[i].Compare(kids[j]) > 0
		})
	}

	// DFS traversal starting from HEAD (zero OpId)
	var result []ListElement
	var visit func(elemID types.OpId)
	visit = func(elemID types.OpId) {
		// Emit this element if visible (skip HEAD which is zero)
		if !elemID.IsZero() {
			entry := elemMap[elemID]
			if entry != nil {
				var winner *Op
				for _, op := range entry.ops {
					if winner == nil || op.ID.Compare(winner.ID) > 0 {
						winner = op
					}
				}
				if winner != nil && winner.Action != ActionDelete {
					result = append(result, ListElement{ElemID: elemID, Op: winner})
				}
			}
		}
		// Visit children
		for _, childID := range children[elemID] {
			visit(childID)
		}
	}
	visit(types.OpId{})

	return result
}

// Clone creates a deep copy of the OpSet.
func (os *OpSet) Clone() *OpSet {
	// Materialize all ops and rebuild fresh columns
	ops := os.cols.ToOps()

	clone := &OpSet{
		Actors:        make([]types.ActorId, len(os.Actors)),
		MaxOp:         os.MaxOp,
		cols:          NewOpColumns(),
		objInfo:       make(map[types.OpId]ObjInfo, len(os.objInfo)),
		objIndex:      make(map[types.OpId]objRange, len(os.objIndex)),
		opIdToRow:     make(map[types.OpId]int, len(os.opIdToRow)),
		seqOrderDirty: os.seqOrderDirty,
	}

	for i, a := range os.Actors {
		cp := make(types.ActorId, len(a))
		copy(cp, a)
		clone.Actors[i] = cp
	}

	if len(ops) > 0 {
		clone.cols.Splice(0, ops)
	}

	for k, v := range os.objInfo {
		clone.objInfo[k] = v
	}
	for k, v := range os.objIndex {
		clone.objIndex[k] = v
	}
	for k, v := range os.opIdToRow {
		clone.opIdToRow[k] = v
	}

	return clone
}

// Len returns the total number of operations.
func (os *OpSet) Len() int {
	return os.cols.Len()
}

// AllOps materializes and returns all operations. The returned slice is a copy.
func (os *OpSet) AllOps() []Op {
	return os.cols.ToOps()
}
