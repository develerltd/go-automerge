package opset

import (
	"sort"

	"github.com/develerltd/go-automerge/internal/types"
)

// isVisibleAt returns true if op is visible at the given clock.
// An op is visible if: the clock covers it, it's not a delete, and none of its
// successors are also covered by the clock.
func isVisibleAt(op *Op, clock Clock) bool {
	if !clock.Covers(op.ID) {
		return false
	}
	if op.Action == ActionDelete {
		return false
	}
	for _, succId := range op.Succ {
		if clock.Covers(succId) {
			return false
		}
	}
	return true
}

// GetAt returns the winning visible value at (obj, prop) at the given clock.
func (os *OpSet) GetAt(obj types.ObjId, prop types.Prop, clock Clock) (types.Value, types.OpId, bool) {
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
		if !isVisibleAt(op, clock) {
			continue
		}
		if winner == nil || op.ID.Compare(winner.ID) > 0 {
			winner = op
		}
	}

	if winner == nil {
		return types.Value{}, types.OpId{}, false
	}
	return winner.ToValue(), winner.ID, true
}

// GetAllAt returns all visible values at (obj, prop) at the given clock.
func (os *OpSet) GetAllAt(obj types.ObjId, prop types.Prop, clock Clock) []OpValue {
	ops := os.OpsForObj(obj)
	var results []OpValue

	for i := range ops {
		op := &ops[i]
		if !matchesKey(op, prop) {
			continue
		}
		if !isVisibleAt(op, clock) {
			continue
		}
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

// KeysAt returns the visible map keys for the given object at the given clock.
func (os *OpSet) KeysAt(obj types.ObjId, clock Clock) []string {
	ops := os.OpsForObj(obj)
	seen := make(map[string]bool)
	var keys []string

	for i := range ops {
		op := &ops[i]
		if op.Key.Kind != KeyMap {
			continue
		}
		if !isVisibleAt(op, clock) {
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

// VisibleListElementsAt returns visible list elements at the given clock.
func (os *OpSet) VisibleListElementsAt(obj types.ObjId, clock Clock) []ListElement {
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
		if !clock.Covers(op.ID) {
			continue
		}
		if op.Insert {
			if elemMap[op.ID] == nil {
				elemMap[op.ID] = &elemInfo{}
			}
			children[op.Key.ElemID] = append(children[op.Key.ElemID], op.ID)
			if isVisibleAt(op, clock) {
				elemMap[op.ID].ops = append(elemMap[op.ID].ops, op)
			}
		} else if op.Key.Kind == KeySeq {
			entry := elemMap[op.Key.ElemID]
			if entry != nil && isVisibleAt(op, clock) {
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

// TextAt returns the text content of a text object at the given clock.
func (os *OpSet) TextAt(obj types.ObjId, clock Clock) string {
	elements := os.VisibleListElementsAt(obj, clock)
	var result []byte
	for _, elem := range elements {
		if elem.Op.Value.Type() == types.ScalarTypeString {
			result = append(result, elem.Op.Value.Str()...)
		}
	}
	return string(result)
}

// ListLenAt returns the number of visible elements in a list/text at the given clock.
func (os *OpSet) ListLenAt(obj types.ObjId, clock Clock) uint64 {
	return uint64(len(os.VisibleListElementsAt(obj, clock)))
}
