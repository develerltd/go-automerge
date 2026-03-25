package automerge

import (
	"fmt"
	"iter"

	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/types"
)

// clockAt converts a set of heads (ChangeHash) into a Clock that represents
// the document state at that point in history. The clock contains per-actor
// max op counters derived by traversing the change graph backwards from heads.
func (d *Doc) clockAt(heads []ChangeHash) opset.Clock {
	if len(heads) == 0 {
		return opset.NewClock(nil)
	}

	// Build a hash->record index
	recordMap := make(map[types.ChangeHash]*opset.ChangeRecord, len(d.changeRecords))
	for i := range d.changeRecords {
		recordMap[d.changeRecords[i].Hash] = &d.changeRecords[i]
	}

	// BFS from heads backwards through deps to collect all reachable changes
	visited := make(map[types.ChangeHash]bool)
	queue := make([]types.ChangeHash, len(heads))
	copy(queue, heads)

	// Per-actor max op counter
	maxCounters := make(map[uint32]uint64)

	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]

		if visited[h] {
			continue
		}
		visited[h] = true

		cr, ok := recordMap[h]
		if !ok {
			continue
		}

		if cr.MaxOp > maxCounters[cr.ActorIdx] {
			maxCounters[cr.ActorIdx] = cr.MaxOp
		}

		for _, dep := range cr.DepHashes {
			if !visited[dep] {
				queue = append(queue, dep)
			}
		}
	}

	// Convert to indexed slice
	var maxIdx uint32
	for idx := range maxCounters {
		if idx > maxIdx {
			maxIdx = idx
		}
	}

	counters := make([]uint64, maxIdx+1)
	for idx, val := range maxCounters {
		counters[idx] = val
	}

	return opset.NewClock(counters)
}

// GetAt returns the value at the given property at the specified point in history.
func (d *Doc) GetAt(obj ObjId, prop Prop, heads []ChangeHash) (Value, ExId, error) {
	clock := d.clockAt(heads)
	val, opId, found := d.opSet.GetAt(obj, prop, clock)
	if !found {
		return Value{}, ExId{}, ErrNotFound
	}
	return val, d.opIdToExId(opId), nil
}

// GetAllAt returns all visible values (including conflicts) at the given property
// at the specified point in history.
func (d *Doc) GetAllAt(obj ObjId, prop Prop, heads []ChangeHash) ([]ValueWithId, error) {
	clock := d.clockAt(heads)
	opvals := d.opSet.GetAllAt(obj, prop, clock)
	results := make([]ValueWithId, len(opvals))
	for i, ov := range opvals {
		results[i] = ValueWithId{
			Value: ov.Value,
			ID:    d.opIdToExId(ov.ID),
		}
	}
	return results, nil
}

// KeysAt returns the visible map keys at the specified point in history.
func (d *Doc) KeysAt(obj ObjId, heads []ChangeHash) []string {
	clock := d.clockAt(heads)
	return d.opSet.KeysAt(obj, clock)
}

// LengthAt returns the number of visible elements at the specified point in history.
func (d *Doc) LengthAt(obj ObjId, heads []ChangeHash) uint64 {
	clock := d.clockAt(heads)
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return 0
	}
	if objType.IsSequence() {
		return d.opSet.ListLenAt(obj, clock)
	}
	return uint64(len(d.opSet.KeysAt(obj, clock)))
}

// TextAt returns the text content of a text object at the specified point in history.
func (d *Doc) TextAt(obj ObjId, heads []ChangeHash) (string, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return "", fmt.Errorf("getting object type: %w", err)
	}
	if objType != ObjTypeText {
		return "", fmt.Errorf("%w: expected text, got %s", ErrTypeMismatch, objType)
	}
	clock := d.clockAt(heads)
	return d.opSet.TextAt(obj, clock), nil
}

// MapRangeAt returns an iterator over visible key-value pairs at the specified point in history.
func (d *Doc) MapRangeAt(obj ObjId, heads []ChangeHash) iter.Seq2[string, Value] {
	clock := d.clockAt(heads)
	return func(yield func(string, Value) bool) {
		for _, key := range d.opSet.KeysAt(obj, clock) {
			val, _, found := d.opSet.GetAt(obj, MapProp(key), clock)
			if !found {
				continue
			}
			if !yield(key, val) {
				return
			}
		}
	}
}

// ListItemsAt returns an iterator over visible list items at the specified point in history.
func (d *Doc) ListItemsAt(obj ObjId, heads []ChangeHash) iter.Seq2[uint64, Value] {
	clock := d.clockAt(heads)
	return func(yield func(uint64, Value) bool) {
		elements := d.opSet.VisibleListElementsAt(obj, clock)
		for i, elem := range elements {
			if !yield(uint64(i), elem.Op.ToValue()) {
				return
			}
		}
	}
}
