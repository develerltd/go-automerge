package opset

import (
	"github.com/develerltd/go-automerge/internal/types"
	"github.com/google/btree"
)

const opTreeDegree = 16

// opLess compares ops within a single object: by Key then by OpId.
// This is the within-object portion of OpLess (object comparison is
// implicit from the map key).
func opLess(a, b Op) bool {
	if cmp := KeyCompare(a.Key, b.Key); cmp != 0 {
		return cmp < 0
	}
	return a.ID.Compare(b.ID) < 0
}

// OpTree stores operations for a single object in a B-tree,
// sorted by (Key, OpId). It maintains a visibility index for O(1)
// visible-op queries per map key.
type OpTree struct {
	tree *btree.BTreeG[Op]
	// visibleByKey tracks visible ops per map key. Only tracks map keys;
	// seq visibility is computed from the full op list via tree traversal.
	// Key: mapKey string, Value: set of visible OpIds.
	visibleByKey map[string]map[types.OpId]struct{}
}

// NewOpTree creates an empty OpTree.
func NewOpTree() *OpTree {
	return &OpTree{
		tree:         btree.NewG(opTreeDegree, opLess),
		visibleByKey: make(map[string]map[types.OpId]struct{}),
	}
}

// Insert adds an op to the tree. If an op with the same Key+ID exists,
// it is replaced. Updates the visibility index.
func (t *OpTree) Insert(op Op) {
	t.tree.ReplaceOrInsert(op)
	if op.Key.Kind == KeyMap {
		t.updateVisibility(op)
	}
}

// updateVisibility updates the visibility index for a map key op.
func (t *OpTree) updateVisibility(op Op) {
	key := op.Key.MapKey
	if op.IsVisible() {
		vis := t.visibleByKey[key]
		if vis == nil {
			vis = make(map[types.OpId]struct{})
			t.visibleByKey[key] = vis
		}
		vis[op.ID] = struct{}{}
	} else {
		if vis := t.visibleByKey[key]; vis != nil {
			delete(vis, op.ID)
			if len(vis) == 0 {
				delete(t.visibleByKey, key)
			}
		}
	}
}

// MarkNotVisible removes an op from the visibility index.
// Called when a successor is added to the op.
func (t *OpTree) MarkNotVisible(mapKey string, id types.OpId) {
	if vis := t.visibleByKey[mapKey]; vis != nil {
		delete(vis, id)
		if len(vis) == 0 {
			delete(t.visibleByKey, mapKey)
		}
	}
}

// Delete removes the op matching the given key and ID.
// Returns the removed op and true if found.
func (t *OpTree) Delete(key Key, id types.OpId) (Op, bool) {
	op, ok := t.tree.Delete(Op{Key: key, ID: id})
	if ok && key.Kind == KeyMap {
		if vis := t.visibleByKey[key.MapKey]; vis != nil {
			delete(vis, id)
			if len(vis) == 0 {
				delete(t.visibleByKey, key.MapKey)
			}
		}
	}
	return op, ok
}

// Get retrieves the op matching the given key and ID.
func (t *OpTree) Get(key Key, id types.OpId) (Op, bool) {
	return t.tree.Get(Op{Key: key, ID: id})
}

// Len returns the number of ops in the tree.
func (t *OpTree) Len() int {
	return t.tree.Len()
}

// AllOps returns all ops in sorted order.
func (t *OpTree) AllOps() []Op {
	ops := make([]Op, 0, t.tree.Len())
	t.tree.Ascend(func(op Op) bool {
		ops = append(ops, op)
		return true
	})
	return ops
}

// Ascend iterates all ops in sorted order, calling fn for each.
// Stops if fn returns false.
func (t *OpTree) Ascend(fn func(Op) bool) {
	t.tree.Ascend(fn)
}

// OpsForMapKey returns all ops with the given map key.
func (t *OpTree) OpsForMapKey(mapKey string) []Op {
	pivot := Key{Kind: KeyMap, MapKey: mapKey}
	minOp := Op{Key: pivot, ID: types.OpId{}}
	var ops []Op
	t.tree.AscendGreaterOrEqual(minOp, func(op Op) bool {
		if op.Key.Kind != KeyMap || op.Key.MapKey != mapKey {
			return false
		}
		ops = append(ops, op)
		return true
	})
	return ops
}

// OpsForSeqKey returns all ops targeting the given sequence element ID.
func (t *OpTree) OpsForSeqKey(elemID types.OpId) []Op {
	pivot := Key{Kind: KeySeq, ElemID: elemID}
	minOp := Op{Key: pivot, ID: types.OpId{}}
	var ops []Op
	t.tree.AscendGreaterOrEqual(minOp, func(op Op) bool {
		if op.Key.Kind != KeySeq || op.Key.ElemID != elemID {
			return false
		}
		ops = append(ops, op)
		return true
	})
	return ops
}

// OpsForKey returns all ops matching the given Prop.
func (t *OpTree) OpsForKey(prop types.Prop) []Op {
	if prop.Kind == types.PropKindMap {
		return t.OpsForMapKey(prop.MapKey)
	}
	return t.AllOps()
}

// VisibleOpsForMapKey returns only visible ops for a map key.
// Uses the visibility index for O(1) lookups.
func (t *OpTree) VisibleOpsForMapKey(mapKey string) []Op {
	vis := t.visibleByKey[mapKey]
	if len(vis) == 0 {
		return nil
	}
	ops := make([]Op, 0, len(vis))
	for id := range vis {
		if op, ok := t.tree.Get(Op{Key: Key{Kind: KeyMap, MapKey: mapKey}, ID: id}); ok {
			ops = append(ops, op)
		}
	}
	return ops
}

// VisibleOps returns all visible ops in the tree.
func (t *OpTree) VisibleOps() []Op {
	var ops []Op
	t.tree.Ascend(func(op Op) bool {
		if op.IsVisible() {
			ops = append(ops, op)
		}
		return true
	})
	return ops
}
