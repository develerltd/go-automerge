package opset

import (
	"github.com/develerltd/go-automerge/internal/types"
)

// KeyKind distinguishes map keys from sequence element IDs.
type KeyKind int

const (
	KeyMap KeyKind = iota
	KeySeq
)

// Key represents the key of an operation: either a map key string or a sequence element ID.
type Key struct {
	Kind   KeyKind
	MapKey string
	ElemID types.OpId // for sequences: the element ID (insert target)
}

// Op represents a single operation in the OpSet.
type Op struct {
	ID       types.OpId
	Obj      types.ObjId
	Key      Key
	Insert   bool
	Action   Action
	Value    types.ScalarValue
	Succ     []types.OpId // successors (ops that override this one)
	Pred     []types.OpId // predecessors (ops this one overrides)
	MarkName string           // for Mark operations
	Expand   bool             // for Mark operations

	// Derived fields
	ElemID types.OpId // for insert ops: the element ID this creates (= Op.ID)
}

// IsVisible returns true if this op has no successors (and is not a delete).
func (o *Op) IsVisible() bool {
	return len(o.Succ) == 0 && o.Action != ActionDelete
}

// ObjType returns the ObjType created by a make action, or 0.
func (o *Op) ObjType() types.ObjType {
	switch o.Action {
	case ActionMakeMap:
		return types.ObjTypeMap
	case ActionMakeList:
		return types.ObjTypeList
	case ActionMakeText:
		return types.ObjTypeText
	case ActionMakeTable:
		return types.ObjTypeTable
	default:
		return 0
	}
}

// ToValue returns the Value this operation represents.
func (o *Op) ToValue() types.Value {
	if o.Action.IsMake() {
		return types.NewObjectValue(o.ObjType())
	}
	return types.NewScalarValue(o.Value)
}

// Clone creates a deep copy of the Op.
func (o Op) Clone() Op {
	cp := o
	if len(o.Succ) > 0 {
		cp.Succ = make([]types.OpId, len(o.Succ))
		copy(cp.Succ, o.Succ)
	}
	if len(o.Pred) > 0 {
		cp.Pred = make([]types.OpId, len(o.Pred))
		copy(cp.Pred, o.Pred)
	}
	// ScalarValue bytes need copying
	if len(o.Value.BytesVal) > 0 {
		cp.Value.BytesVal = make([]byte, len(o.Value.BytesVal))
		copy(cp.Value.BytesVal, o.Value.BytesVal)
	}
	return cp
}

// ObjIdCompare compares two ObjIds. Root < non-root; then by OpId.
func ObjIdCompare(a, b types.ObjId) int {
	if a.IsRoot() && b.IsRoot() {
		return 0
	}
	if a.IsRoot() {
		return -1
	}
	if b.IsRoot() {
		return 1
	}
	return a.OpId.Compare(b.OpId)
}

// KeyCompare compares two keys. Map keys < Seq keys; within same kind, by value.
func KeyCompare(a, b Key) int {
	if a.Kind != b.Kind {
		if a.Kind == KeyMap {
			return -1
		}
		return 1
	}
	if a.Kind == KeyMap {
		if a.MapKey < b.MapKey {
			return -1
		}
		if a.MapKey > b.MapKey {
			return 1
		}
		return 0
	}
	// KeySeq: compare by ElemId
	return a.ElemID.Compare(b.ElemID)
}

// OpLess returns true if a should sort before b in the OpSet.
func OpLess(a, b *Op) bool {
	if cmp := ObjIdCompare(a.Obj, b.Obj); cmp != 0 {
		return cmp < 0
	}
	if cmp := KeyCompare(a.Key, b.Key); cmp != 0 {
		return cmp < 0
	}
	return a.ID.Compare(b.ID) < 0
}
