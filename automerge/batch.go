package automerge

import (
	"fmt"
	"sort"

	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/types"
)

// valueToOpAction returns the opset action and optional scalar value for a HydrateValue.
// For object types, it also returns the ObjType.
func valueToOpAction(v HydrateValue) (objType *ObjType, action opset.Action, value ScalarValue) {
	switch v.Kind {
	case HydrateMap:
		ot := ObjTypeMap
		return &ot, opset.ActionMakeMap, ScalarValue{}
	case HydrateList:
		ot := ObjTypeList
		return &ot, opset.ActionMakeList, ScalarValue{}
	case HydrateText:
		ot := ObjTypeText
		return &ot, opset.ActionMakeText, ScalarValue{}
	default:
		return nil, opset.ActionSet, v.Scalar
	}
}

// batchEntry is a queued item for BFS traversal of nested hydrate values.
type batchEntry struct {
	obj   ObjId
	value *HydrateValue
}

// batchBFS performs a BFS traversal of a nested HydrateValue, generating ops
// for all descendant objects. The root op is NOT included (caller handles it).
// Returns the generated ops (unsorted).
func batchBFS(d *Doc, queue []batchEntry) []opset.Op {
	var ops []opset.Op

	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]

		switch entry.value.Kind {
		case HydrateMap:
			// Sort keys for deterministic ordering
			keys := make([]string, 0, len(entry.value.MapEntries))
			for k := range entry.value.MapEntries {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, key := range keys {
				child := entry.value.MapEntries[key]
				childObjType, action, scalar := valueToOpAction(child)

				opId := d.allocOpId()
				op := opset.Op{
					ID:     opId,
					Obj:    entry.obj,
					Key:    opset.Key{Kind: opset.KeyMap, MapKey: key},
					Insert: false,
					Action: action,
					Value:  scalar,
				}
				ops = append(ops, op)
				d.pendingOps = append(d.pendingOps, op)

				if childObjType != nil {
					childObj := ObjId{OpId: opId}
					queue = append(queue, batchEntry{obj: childObj, value: &child})
				}
			}

		case HydrateList:
			var elemId types.OpId // HEAD (zero value)
			for _, child := range entry.value.ListItems {
				childObjType, action, scalar := valueToOpAction(child)

				opId := d.allocOpId()
				op := opset.Op{
					ID:     opId,
					Obj:    entry.obj,
					Key:    opset.Key{Kind: opset.KeySeq, ElemID: elemId},
					Insert: true,
					Action: action,
					Value:  scalar,
				}
				ops = append(ops, op)
				d.pendingOps = append(d.pendingOps, op)

				elemId = opId // next insert targets this op

				if childObjType != nil {
					childObj := ObjId{OpId: opId}
					queue = append(queue, batchEntry{obj: childObj, value: &child})
				}
			}

		case HydrateText:
			var elemId types.OpId // HEAD
			for _, ch := range entry.value.Text {
				opId := d.allocOpId()
				op := opset.Op{
					ID:     opId,
					Obj:    entry.obj,
					Key:    opset.Key{Kind: opset.KeySeq, ElemID: elemId},
					Insert: true,
					Action: opset.ActionSet,
					Value:  types.NewStr(string(ch)),
				}
				ops = append(ops, op)
				d.pendingOps = append(d.pendingOps, op)

				elemId = opId
			}
		}
	}

	return ops
}

// BatchCreateObject creates a new nested object tree at the given property
// using batch insertion for efficiency. For map properties, set insert=false.
// For list properties with insert=true, the value is inserted at the index;
// with insert=false, it replaces the existing element.
//
// The value must be a map, list, or text HydrateValue (not a scalar).
func (d *Doc) BatchCreateObject(obj ObjId, prop Prop, value HydrateValue, insert bool) (ObjId, error) {
	objType, ok := value.ObjType()
	if !ok {
		return ObjId{}, fmt.Errorf("BatchCreateObject requires a map, list, or text value, not scalar")
	}

	var action opset.Action
	switch objType {
	case ObjTypeMap:
		action = opset.ActionMakeMap
	case ObjTypeList:
		action = opset.ActionMakeList
	case ObjTypeText:
		action = opset.ActionMakeText
	}

	// Create the root object op (using existing single-op path for correct predecessor handling)
	var rootOpId OpId
	if insert && prop.Kind == PropKindSeq {
		// Insert into list
		elements := d.opSet.VisibleListElements(obj)
		if prop.SeqIndex > uint64(len(elements)) {
			return ObjId{}, fmt.Errorf("index %d out of range (len=%d)", prop.SeqIndex, len(elements))
		}
		var elemId types.OpId
		if prop.SeqIndex > 0 {
			elemId = elements[prop.SeqIndex-1].ElemID
		}
		rootOpId = d.allocOpId()
		d.applyOp(opset.Op{
			ID:     rootOpId,
			Obj:    obj,
			Key:    opset.Key{Kind: opset.KeySeq, ElemID: elemId},
			Insert: true,
			Action: action,
		})
	} else if prop.Kind == PropKindMap {
		// Put into map
		existingOps := d.opSet.GetAll(obj, prop)
		var pred []OpId
		for _, ov := range existingOps {
			pred = append(pred, ov.ID)
		}
		rootOpId = d.allocOpId()
		d.applyOp(opset.Op{
			ID:     rootOpId,
			Obj:    obj,
			Key:    opset.Key{Kind: opset.KeyMap, MapKey: prop.MapKey},
			Insert: false,
			Action: action,
			Pred:   pred,
		})
	} else {
		// Seq without insert (replace) - need predecessors
		existingOps := d.opSet.GetAll(obj, prop)
		if len(existingOps) == 0 {
			return ObjId{}, fmt.Errorf("%w: nothing at index %d", ErrNotFound, prop.SeqIndex)
		}
		var pred []OpId
		for _, ov := range existingOps {
			pred = append(pred, ov.ID)
		}
		elements := d.opSet.VisibleListElements(obj)
		if prop.SeqIndex >= uint64(len(elements)) {
			return ObjId{}, fmt.Errorf("index %d out of range (len=%d)", prop.SeqIndex, len(elements))
		}
		elem := elements[prop.SeqIndex]
		rootOpId = d.allocOpId()
		d.applyOp(opset.Op{
			ID:     rootOpId,
			Obj:    obj,
			Key:    opset.Key{Kind: opset.KeySeq, ElemID: elem.ElemID},
			Insert: false,
			Action: action,
			Pred:   pred,
		})
	}

	rootObj := ObjId{OpId: rootOpId}

	// BFS all descendants and bulk-insert
	queue := []batchEntry{{obj: rootObj, value: &value}}
	descendantOps := batchBFS(d, queue)
	if len(descendantOps) > 0 {
		// Update predecessors' Succ for descendant ops (none have predecessors)
		d.opSet.BulkAddOps(descendantOps)
	}

	return rootObj, nil
}

// InitFromHydrate initializes the root map of the document from a map of
// HydrateValues. Existing keys not present in the map are left unchanged.
func (d *Doc) InitFromHydrate(value map[string]HydrateValue) error {
	// Sort keys for deterministic ordering
	keys := make([]string, 0, len(value))
	for k := range value {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var allOps []opset.Op
	var queue []batchEntry

	for _, key := range keys {
		child := value[key]
		_, action, scalar := valueToOpAction(child)

		// Find existing ops at this key for predecessors
		prop := MapProp(key)
		existingOps := d.opSet.GetAll(Root, prop)
		var pred []OpId
		for _, ov := range existingOps {
			pred = append(pred, ov.ID)
		}

		opId := d.allocOpId()
		op := opset.Op{
			ID:     opId,
			Obj:    Root,
			Key:    opset.Key{Kind: opset.KeyMap, MapKey: key},
			Insert: false,
			Action: action,
			Value:  scalar,
			Pred:   pred,
		}
		allOps = append(allOps, op)
		d.pendingOps = append(d.pendingOps, op)

		if childObjType, ok := child.ObjType(); ok {
			_ = childObjType
			childObj := ObjId{OpId: opId}
			queue = append(queue, batchEntry{obj: childObj, value: &child})
		}
	}

	// BFS descendants
	descendantOps := batchBFS(d, queue)
	allOps = append(allOps, descendantOps...)

	if len(allOps) > 0 {
		// Update predecessors' Succ for root-level ops
		for _, op := range allOps {
			for _, predId := range op.Pred {
				d.opSet.UpdateSucc(predId, op.ID)
			}
		}
		d.opSet.BulkAddOps(allOps)
	}

	return nil
}

// SpliceValues replaces del elements at pos in a list with the given values.
// Unlike Splice, values can be nested objects (maps, lists, text) in addition
// to scalars. Nested objects are inserted using batch operations for efficiency.
func (d *Doc) SpliceValues(obj ObjId, pos, del uint64, vals ...HydrateValue) error {
	// Delete elements
	for i := uint64(0); i < del; i++ {
		if err := d.Delete(obj, SeqProp(pos)); err != nil {
			return fmt.Errorf("deleting at pos %d: %w", pos, err)
		}
	}

	// Insert values
	for i, v := range vals {
		idx := pos + uint64(i)
		if v.Kind == HydrateScalar {
			if err := d.Insert(obj, idx, v.Scalar); err != nil {
				return fmt.Errorf("inserting scalar at pos %d: %w", idx, err)
			}
		} else {
			if _, err := d.BatchCreateObject(obj, SeqProp(idx), v, true); err != nil {
				return fmt.Errorf("inserting object at pos %d: %w", idx, err)
			}
		}
	}

	return nil
}
