package opset

import (
	"sort"

	"github.com/develerltd/go-automerge/internal/types"
)

// Mark represents a rich text annotation on a range of a text object.
type Mark struct {
	Start uint64
	End   uint64
	Name  string
	Value types.ScalarValue
}

// Marks returns all marks on the given text object.
func (os *OpSet) Marks(obj types.ObjId) []Mark {
	return os.marksFor(obj, nil)
}

// MarksAt returns all marks on the given text object at the given clock.
func (os *OpSet) MarksAt(obj types.ObjId, clock Clock) []Mark {
	return os.marksFor(obj, &clock)
}

// marksFor collects mark operations and converts them to mark ranges.
// If clock is nil, uses current visibility; otherwise uses clock-based visibility.
func (os *OpSet) marksFor(obj types.ObjId, clock *Clock) []Mark {
	ops := os.OpsForObj(obj)
	if len(ops) == 0 {
		return nil
	}

	// Get the visible list elements to map elemIDs to positions
	var elements []ListElement
	if clock != nil {
		elements = os.VisibleListElementsAt(obj, *clock)
	} else {
		elements = os.VisibleListElements(obj)
	}

	elemToIdx := make(map[types.OpId]uint64)
	for i, e := range elements {
		elemToIdx[e.ElemID] = uint64(i)
	}

	// Collect mark ops
	type markOp struct {
		elemID types.OpId
		name   string
		value  types.ScalarValue
		opId   types.OpId
		expand bool
	}

	var markOps []markOp
	for i := range ops {
		op := &ops[i]
		if op.Action != ActionMark {
			continue
		}
		if clock != nil && !clock.Covers(op.ID) {
			continue
		}
		// Check visibility: a mark op is superseded if it has a successor in scope
		superseded := false
		for _, succId := range op.Succ {
			if clock == nil || clock.Covers(succId) {
				superseded = true
				break
			}
		}
		if superseded {
			continue
		}
		markOps = append(markOps, markOp{
			elemID: op.Key.ElemID,
			name:   op.MarkName,
			value:  op.Value,
			opId:   op.ID,
			expand: op.Expand,
		})
	}

	if len(markOps) == 0 {
		return nil
	}

	// Group mark ops by name, ordered by position
	sort.Slice(markOps, func(i, j int) bool {
		if markOps[i].name != markOps[j].name {
			return markOps[i].name < markOps[j].name
		}
		return markOps[i].opId.Counter < markOps[j].opId.Counter
	})

	// Pair consecutive mark ops (begin, end) into Mark ranges
	var marks []Mark
	for i := 0; i+1 < len(markOps); i += 2 {
		begin := markOps[i]
		end := markOps[i+1]

		// Convert elemIDs to positions
		var startIdx uint64
		if begin.elemID.IsZero() {
			startIdx = 0
		} else if idx, ok := elemToIdx[begin.elemID]; ok {
			startIdx = idx + 1 // mark starts after the referenced element
		}

		var endIdx uint64
		if idx, ok := elemToIdx[end.elemID]; ok {
			endIdx = idx + 1 // mark ends after the referenced element
		} else {
			endIdx = uint64(len(elements))
		}

		if startIdx < endIdx {
			marks = append(marks, Mark{
				Start: startIdx,
				End:   endIdx,
				Name:  begin.name,
				Value: begin.value,
			})
		}
	}

	return marks
}
