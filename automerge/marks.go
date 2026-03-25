package automerge

import (
	"fmt"
	"sort"

	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/types"
)

// ExpandMark determines how a mark expands when text is inserted at its boundaries.
type ExpandMark int

const (
	ExpandBefore ExpandMark = iota // Expand when text inserted before the mark
	ExpandAfter                    // Expand when text inserted after the mark
	ExpandBoth                     // Expand in both directions
	ExpandNone                     // Never expand
)

// Mark represents a rich text annotation on a range of a text object.
type Mark struct {
	Start uint64
	End   uint64
	Name  string
	Value ScalarValue
}

// MarkSet represents the set of active marks at a given position.
type MarkSet struct {
	marks map[string]ScalarValue
}

// NewMarkSet creates an empty MarkSet.
func NewMarkSet() *MarkSet {
	return &MarkSet{marks: make(map[string]ScalarValue)}
}

// Get returns the value of a mark by name.
func (ms *MarkSet) Get(name string) (ScalarValue, bool) {
	v, ok := ms.marks[name]
	return v, ok
}

// Len returns the number of marks in the set.
func (ms *MarkSet) Len() int {
	if ms == nil {
		return 0
	}
	return len(ms.marks)
}

// Range iterates over all marks in the set in sorted order.
func (ms *MarkSet) Range(fn func(name string, value ScalarValue) bool) {
	if ms == nil {
		return
	}
	keys := make([]string, 0, len(ms.marks))
	for k := range ms.marks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !fn(k, ms.marks[k]) {
			return
		}
	}
}

// MarkOp creates a mark operation on a range of a text object.
func (d *Doc) Mark(obj ObjId, start, end uint64, expand ExpandMark, name string, value ScalarValue) error {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return fmt.Errorf("getting object type: %w", err)
	}
	if objType != ObjTypeText {
		return fmt.Errorf("%w: mark requires text object", ErrTypeMismatch)
	}

	elements := d.opSet.VisibleListElements(obj)
	if start > uint64(len(elements)) || end > uint64(len(elements)) {
		return fmt.Errorf("mark range [%d, %d) out of bounds (len=%d)", start, end, len(elements))
	}
	if start >= end {
		return fmt.Errorf("mark range [%d, %d) is empty or inverted", start, end)
	}

	// Find the element IDs at start and end positions
	var startElemID, endElemID types.OpId
	if start > 0 {
		startElemID = elements[start-1].ElemID
	}
	// endElemID is the elemID of the last element in the range
	if end > 0 {
		endElemID = elements[end-1].ElemID
	}

	// Determine expand flag for the mark begin op
	expandBegin := expand == ExpandBefore || expand == ExpandBoth
	expandEnd := expand == ExpandAfter || expand == ExpandBoth

	// Create MarkBegin op
	beginOpId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:       beginOpId,
		Obj:      obj,
		Key:      opset.Key{Kind: opset.KeySeq, ElemID: startElemID},
		Insert:   false,
		Action:   opset.ActionMark,
		Value:    value,
		MarkName: name,
		Expand:   expandBegin,
	})

	// Create MarkEnd op (references the begin op)
	endOpId := d.allocOpId()
	d.applyOp(opset.Op{
		ID:       endOpId,
		Obj:      obj,
		Key:      opset.Key{Kind: opset.KeySeq, ElemID: endElemID},
		Insert:   false,
		Action:   opset.ActionMark,
		MarkName: name,
		Expand:   expandEnd,
	})

	return nil
}

// Marks returns all marks on the given text object.
func (d *Doc) Marks(obj ObjId) ([]Mark, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return nil, fmt.Errorf("getting object type: %w", err)
	}
	if objType != ObjTypeText {
		return nil, fmt.Errorf("%w: marks requires text object", ErrTypeMismatch)
	}
	return convertMarks(d.opSet.Marks(obj)), nil
}

// MarksAt returns all marks on the given text object at the specified point in history.
func (d *Doc) MarksAt(obj ObjId, heads []ChangeHash) ([]Mark, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return nil, fmt.Errorf("getting object type: %w", err)
	}
	if objType != ObjTypeText {
		return nil, fmt.Errorf("%w: marks requires text object", ErrTypeMismatch)
	}
	clock := d.clockAt(heads)
	return convertMarks(d.opSet.MarksAt(obj, clock)), nil
}

// MarksAtPosition returns the set of active marks at a specific position in the text.
func (d *Doc) MarksAtPosition(obj ObjId, index uint64) (*MarkSet, error) {
	marks, err := d.Marks(obj)
	if err != nil {
		return nil, err
	}
	ms := NewMarkSet()
	for _, m := range marks {
		if index >= m.Start && index < m.End {
			ms.marks[m.Name] = m.Value
		}
	}
	return ms, nil
}

func convertMarks(opsetMarks []opset.Mark) []Mark {
	if len(opsetMarks) == 0 {
		return nil
	}
	marks := make([]Mark, len(opsetMarks))
	for i, m := range opsetMarks {
		marks[i] = Mark{
			Start: m.Start,
			End:   m.End,
			Name:  m.Name,
			Value: m.Value,
		}
	}
	return marks
}
