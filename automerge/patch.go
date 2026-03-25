package automerge

import (
	"github.com/develerltd/go-automerge/internal/opset"
)

// PatchAction describes the type of change in a patch.
type PatchAction int

const (
	PatchPutMap PatchAction = iota
	PatchPutSeq
	PatchInsert
	PatchSpliceText
	PatchDeleteMap
	PatchDeleteSeq
	PatchIncrement
	PatchMark
	PatchFlagConflict
)

// Patch represents a single change to the document that can be used for
// incremental UI updates.
type Patch struct {
	Obj    ObjId
	Path   []PathElement
	Action PatchAction

	// PutMap/DeleteMap
	Key string

	// PutSeq/Insert/DeleteSeq/SpliceText
	Index  uint64
	Length uint64 // for DeleteSeq

	// PutMap/PutSeq/Insert
	Value    Value
	ObjID    ExId
	Conflict bool

	// SpliceText
	Text string

	// Increment
	Prop      Prop
	IncrValue int64

	// Mark
	Marks []Mark
}

// PatchLog accumulates patches during document operations.
type PatchLog struct {
	patches []Patch
	active  bool
}

// NewPatchLog creates a new active patch log.
func NewPatchLog() *PatchLog {
	return &PatchLog{active: true}
}

// Patches returns the accumulated patches.
func (pl *PatchLog) Patches() []Patch {
	if pl == nil {
		return nil
	}
	return pl.patches
}

// Clear resets the patch log.
func (pl *PatchLog) Clear() {
	if pl != nil {
		pl.patches = nil
	}
}

// Diff computes the patches needed to transform the document from oldHeads to newHeads.
func (d *Doc) Diff(oldHeads, newHeads []ChangeHash) []Patch {
	oldClock := d.clockAt(oldHeads)
	newClock := d.clockAt(newHeads)

	var patches []Patch

	// Walk all objects and compare state at oldClock vs newClock
	d.diffObject(Root, oldClock, newClock, nil, &patches)

	return patches
}

func (d *Doc) diffObject(obj ObjId, oldClock, newClock opset.Clock, path []PathElement, patches *[]Patch) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil && !obj.IsRoot() {
		return
	}
	if obj.IsRoot() {
		objType = ObjTypeMap
	}

	if objType.IsSequence() {
		d.diffSequence(obj, objType, oldClock, newClock, path, patches)
	} else {
		d.diffMap(obj, oldClock, newClock, path, patches)
	}
}

func (d *Doc) diffMap(obj ObjId, oldClock, newClock opset.Clock, path []PathElement, patches *[]Patch) {
	// Get keys visible at both clocks
	oldKeys := make(map[string]bool)
	for _, k := range d.opSet.KeysAt(obj, oldClock) {
		oldKeys[k] = true
	}
	newKeys := make(map[string]bool)
	for _, k := range d.opSet.KeysAt(obj, newClock) {
		newKeys[k] = true
	}

	// Deleted keys
	for k := range oldKeys {
		if !newKeys[k] {
			*patches = append(*patches, Patch{
				Obj:    obj,
				Path:   copyPath(path),
				Action: PatchDeleteMap,
				Key:    k,
			})
		}
	}

	// Added or changed keys
	for k := range newKeys {
		prop := MapProp(k)
		newVal, newId, newFound := d.opSet.GetAt(obj, prop, newClock)
		if !newFound {
			continue
		}

		if oldKeys[k] {
			oldVal, _, oldFound := d.opSet.GetAt(obj, prop, oldClock)
			if oldFound && valuesEqual(oldVal, newVal) {
				// Check for sub-object changes
				if newVal.IsObject {
					childObj := ObjId{OpId: newId}
					childPath := append(copyPath(path), PathElement{ObjId: obj, ObjTyp: ObjTypeMap, Prop: prop})
					d.diffObject(childObj, oldClock, newClock, childPath, patches)
				}
				continue
			}
		}

		*patches = append(*patches, Patch{
			Obj:    obj,
			Path:   copyPath(path),
			Action: PatchPutMap,
			Key:    k,
			Value:  newVal,
			ObjID:  d.opIdToExId(newId),
		})

		// Recurse into new sub-objects
		if newVal.IsObject {
			childObj := ObjId{OpId: newId}
			childPath := append(copyPath(path), PathElement{ObjId: obj, ObjTyp: ObjTypeMap, Prop: prop})
			d.diffObject(childObj, oldClock, newClock, childPath, patches)
		}
	}
}

func (d *Doc) diffSequence(obj ObjId, objType ObjType, oldClock, newClock opset.Clock, path []PathElement, patches *[]Patch) {
	oldElems := d.opSet.VisibleListElementsAt(obj, oldClock)
	newElems := d.opSet.VisibleListElementsAt(obj, newClock)

	// Simple diff: report deletions and insertions
	oldSet := make(map[OpId]int, len(oldElems))
	for i, e := range oldElems {
		oldSet[e.ElemID] = i
	}
	newSet := make(map[OpId]int, len(newElems))
	for i, e := range newElems {
		newSet[e.ElemID] = i
	}

	// Deletions (in old but not in new)
	deleteCount := uint64(0)
	for _, e := range oldElems {
		if _, ok := newSet[e.ElemID]; !ok {
			deleteCount++
		}
	}
	if deleteCount > 0 {
		*patches = append(*patches, Patch{
			Obj:    obj,
			Path:   copyPath(path),
			Action: PatchDeleteSeq,
			Index:  0,
			Length: deleteCount,
		})
	}

	// Insertions (in new but not in old)
	if objType == ObjTypeText {
		var text []byte
		insertStart := uint64(0)
		hasInsert := false
		for i, e := range newElems {
			if _, ok := oldSet[e.ElemID]; !ok {
				if !hasInsert {
					insertStart = uint64(i)
					hasInsert = true
				}
				if e.Op.Value.Type() == ScalarTypeString {
					text = append(text, e.Op.Value.Str()...)
				}
			}
		}
		if hasInsert {
			*patches = append(*patches, Patch{
				Obj:    obj,
				Path:   copyPath(path),
				Action: PatchSpliceText,
				Index:  insertStart,
				Text:   string(text),
			})
		}
	} else {
		for i, e := range newElems {
			if _, ok := oldSet[e.ElemID]; !ok {
				*patches = append(*patches, Patch{
					Obj:    obj,
					Path:   copyPath(path),
					Action: PatchInsert,
					Index:  uint64(i),
					Value:  e.Op.ToValue(),
					ObjID:  d.opIdToExId(e.Op.ID),
				})
			}
		}
	}
}

func valuesEqual(a, b Value) bool {
	if a.IsObject != b.IsObject {
		return false
	}
	if a.IsObject {
		return a.ObjType == b.ObjType
	}
	return a.Scalar.Equal(b.Scalar)
}

func copyPath(path []PathElement) []PathElement {
	if path == nil {
		return nil
	}
	cp := make([]PathElement, len(path))
	copy(cp, path)
	return cp
}
