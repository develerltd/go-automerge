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

// opLoc records where an op lives: which object and key.
// Used by opIndex for O(1) lookup in UpdateSucc.
type opLoc struct {
	Obj types.ObjId
	Key Key
}

// OpSet is the core CRDT data structure. It stores operations in per-object
// B-trees for O(log k) insert/lookup (k = ops in that object).
type OpSet struct {
	Actors []types.ActorId
	MaxOp  uint64

	// trees stores operations per object in B-tree order (by Key, then OpId).
	trees map[types.ObjId]*OpTree

	// opIndex maps every op's ID to its object and key for O(1) lookup.
	opIndex map[types.OpId]opLoc

	// objInfo maps object IDs to their metadata
	objInfo map[types.OpId]ObjInfo
}

// New creates an empty OpSet.
func New() *OpSet {
	return &OpSet{
		trees:   make(map[types.ObjId]*OpTree),
		opIndex: make(map[types.OpId]opLoc),
		objInfo: make(map[types.OpId]ObjInfo),
	}
}

// getOrCreateTree returns the tree for obj, creating it if needed.
func (os *OpSet) getOrCreateTree(obj types.ObjId) *OpTree {
	t := os.trees[obj]
	if t == nil {
		t = NewOpTree()
		os.trees[obj] = t
	}
	return t
}

// insertOp is the shared implementation for adding an op to the tree + index.
func (os *OpSet) insertOp(op Op) {
	if op.ID.Counter > os.MaxOp {
		os.MaxOp = op.ID.Counter
	}
	if op.Action.IsMake() {
		os.objInfo[op.ID] = ObjInfo{Parent: op.Obj, ObjType: op.ObjType()}
	}
	os.getOrCreateTree(op.Obj).Insert(op)
	os.opIndex[op.ID] = opLoc{Obj: op.Obj, Key: op.Key}
}

// AddOp adds an operation to the OpSet.
func (os *OpSet) AddOp(op Op) {
	os.insertOp(op)
}

// InsertOp adds an operation to the OpSet, maintaining sorted order.
func (os *OpSet) InsertOp(op Op) {
	os.insertOp(op)
}

// BulkAddOps adds multiple operations to the OpSet.
func (os *OpSet) BulkAddOps(ops []Op) {
	for i := range ops {
		os.insertOp(ops[i])
	}
}

// SortAndReindex is a no-op for tree-based storage (trees are always sorted).
// Rebuilds opIndex and objInfo.
func (os *OpSet) SortAndReindex() {
	os.rebuildIndex()
}

// RebuildFromOps clears all trees and rebuilds from the given sorted ops.
func (os *OpSet) RebuildFromOps(ops []Op) {
	os.trees = make(map[types.ObjId]*OpTree)
	os.opIndex = make(map[types.OpId]opLoc, len(ops))
	os.objInfo = make(map[types.OpId]ObjInfo)
	for i := range ops {
		os.insertOp(ops[i])
	}
}

// rebuildIndex rebuilds opIndex and objInfo from the trees.
func (os *OpSet) rebuildIndex() {
	os.opIndex = make(map[types.OpId]opLoc)
	os.objInfo = make(map[types.OpId]ObjInfo)
	for obj, tree := range os.trees {
		tree.Ascend(func(op Op) bool {
			os.opIndex[op.ID] = opLoc{Obj: obj, Key: op.Key}
			if op.Action.IsMake() {
				os.objInfo[op.ID] = ObjInfo{Parent: op.Obj, ObjType: op.ObjType()}
			}
			return true
		})
	}
}

// UpdateSucc adds succId to the Succ list of the op with the given predId.
func (os *OpSet) UpdateSucc(predId, succId types.OpId) {
	loc, ok := os.opIndex[predId]
	if !ok {
		return
	}
	tree := os.trees[loc.Obj]
	if tree == nil {
		return
	}
	op, found := tree.Get(loc.Key, predId)
	if !found {
		return
	}
	wasVisible := op.IsVisible()
	// Remove old, add successor, re-insert
	tree.Delete(loc.Key, predId)
	op.Succ = append(op.Succ, succId)
	tree.Insert(op)
	// If the op was visible and is now not, update visibility index
	if wasVisible && !op.IsVisible() && loc.Key.Kind == KeyMap {
		tree.MarkNotVisible(loc.Key.MapKey, predId)
	}
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

	loc, ok := os.opIndex[obj.OpId]
	if ok {
		var prop types.Prop
		if loc.Key.Kind == KeyMap {
			prop = types.MapProp(loc.Key.MapKey)
		} else {
			prop = types.SeqProp(0)
		}
		return ParentInfo{Parent: info.Parent, Prop: prop}, nil
	}
	return ParentInfo{Parent: info.Parent}, nil
}

// GetObjType returns the type of the given object, or an error if not found.
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
func (os *OpSet) OpsForObj(obj types.ObjId) []Op {
	tree := os.trees[obj]
	if tree == nil {
		return nil
	}
	return tree.AllOps()
}

// Get returns the winning visible value at (obj, prop), along with its OpId.
func (os *OpSet) Get(obj types.ObjId, prop types.Prop) (types.Value, types.OpId, bool) {
	tree := os.trees[obj]
	if tree == nil {
		return types.Value{}, types.OpId{}, false
	}

	var visOps []Op
	if prop.Kind == types.PropKindMap {
		visOps = tree.VisibleOpsForMapKey(prop.MapKey)
	} else {
		// For seq props, caller should use ListGet instead
		return types.Value{}, types.OpId{}, false
	}

	if len(visOps) == 0 {
		return types.Value{}, types.OpId{}, false
	}

	var winner *Op
	for i := range visOps {
		op := &visOps[i]
		if winner == nil || op.ID.Compare(winner.ID) > 0 {
			winner = op
		}
	}

	// Counter accumulation: if the winning op is an increment, sum the chain
	if winner.Action == ActionIncrement {
		allOps := tree.OpsForMapKey(prop.MapKey)
		total := accumulateCounter(allOps, winner)
		return types.NewScalarValue(types.NewCounter(total)), winner.ID, true
	}

	return winner.ToValue(), winner.ID, true
}

// accumulateCounter computes the counter total by finding all ops for this key.
func accumulateCounter(ops []Op, tip *Op) int64 {
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

// OpValue pairs a value with its operation ID.
type OpValue struct {
	Value types.Value
	ID    types.OpId
}

// GetAll returns all visible values at (obj, prop) — includes conflicts.
func (os *OpSet) GetAll(obj types.ObjId, prop types.Prop) []OpValue {
	tree := os.trees[obj]
	if tree == nil {
		return nil
	}

	var visOps []Op
	if prop.Kind == types.PropKindMap {
		visOps = tree.VisibleOpsForMapKey(prop.MapKey)
	} else {
		return nil
	}

	var results []OpValue
	for i := range visOps {
		op := &visOps[i]
		results = append(results, OpValue{
			Value: op.ToValue(),
			ID:    op.ID,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].ID.Compare(results[j].ID) < 0
	})
	return results
}

// Keys returns the visible map keys for the given object, sorted.
func (os *OpSet) Keys(obj types.ObjId) []string {
	tree := os.trees[obj]
	if tree == nil {
		return nil
	}

	seen := make(map[string]bool)
	var keys []string
	tree.Ascend(func(op Op) bool {
		if op.Key.Kind != KeyMap {
			return true
		}
		if !op.IsVisible() {
			return true
		}
		if !seen[op.Key.MapKey] {
			seen[op.Key.MapKey] = true
			keys = append(keys, op.Key.MapKey)
		}
		return true
	})

	sort.Strings(keys)
	return keys
}

func matchesKey(op *Op, prop types.Prop) bool {
	switch prop.Kind {
	case types.PropKindMap:
		return op.Key.Kind == KeyMap && op.Key.MapKey == prop.MapKey
	case types.PropKindSeq:
		return false
	default:
		return false
	}
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

// VisibleListElements returns visible list elements with their elemIds, in correct
// document order.
func (os *OpSet) VisibleListElements(obj types.ObjId) []ListElement {
	ops := os.OpsForObj(obj)
	if len(ops) == 0 {
		return nil
	}

	type elemInfo struct {
		ops []*Op
	}
	elemMap := make(map[types.OpId]*elemInfo)
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

	for _, kids := range children {
		sort.Slice(kids, func(i, j int) bool {
			return kids[i].Compare(kids[j]) > 0
		})
	}

	var result []ListElement
	var visit func(elemID types.OpId)
	visit = func(elemID types.OpId) {
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
		for _, childID := range children[elemID] {
			visit(childID)
		}
	}
	visit(types.OpId{})

	return result
}

// AllOpsSorted returns all ops in global sorted order (root first, then
// non-root objects sorted by OpId). Within each object, ops are in tree order.
func (os *OpSet) AllOpsSorted() []Op {
	total := 0
	for _, tree := range os.trees {
		total += tree.Len()
	}
	if total == 0 {
		return nil
	}

	// Collect non-root object IDs and sort them
	var objIds []types.ObjId
	for obj := range os.trees {
		if !obj.IsRoot() {
			objIds = append(objIds, obj)
		}
	}
	sort.Slice(objIds, func(i, j int) bool {
		return ObjIdCompare(objIds[i], objIds[j]) < 0
	})

	ops := make([]Op, 0, total)

	// Root first
	if rootTree := os.trees[types.Root]; rootTree != nil {
		ops = append(ops, rootTree.AllOps()...)
	}

	// Then non-root in OpId order
	for _, obj := range objIds {
		ops = append(ops, os.trees[obj].AllOps()...)
	}

	return ops
}

// ExportColumns exports the operation columns as RawColumns for serialization.
func (os *OpSet) ExportColumns() columnar.RawColumns {
	return EncodeDocOps(os.AllOpsSorted())
}

// Clone creates a deep copy of the OpSet.
func (os *OpSet) Clone() *OpSet {
	clone := &OpSet{
		Actors:  make([]types.ActorId, len(os.Actors)),
		MaxOp:   os.MaxOp,
		trees:   make(map[types.ObjId]*OpTree),
		opIndex: make(map[types.OpId]opLoc, len(os.opIndex)),
		objInfo: make(map[types.OpId]ObjInfo, len(os.objInfo)),
	}

	for i, a := range os.Actors {
		cp := make(types.ActorId, len(a))
		copy(cp, a)
		clone.Actors[i] = cp
	}

	for k, v := range os.objInfo {
		clone.objInfo[k] = v
	}

	// Clone each tree by iterating and cloning ops
	for obj, tree := range os.trees {
		newTree := NewOpTree()
		tree.Ascend(func(op Op) bool {
			newTree.Insert(op.Clone())
			return true
		})
		clone.trees[obj] = newTree
	}

	for k, v := range os.opIndex {
		clone.opIndex[k] = v
	}

	return clone
}

// Len returns the total number of operations.
func (os *OpSet) Len() int {
	total := 0
	for _, tree := range os.trees {
		total += tree.Len()
	}
	return total
}

// AllOps materializes and returns all operations in sorted order.
func (os *OpSet) AllOps() []Op {
	return os.AllOpsSorted()
}
