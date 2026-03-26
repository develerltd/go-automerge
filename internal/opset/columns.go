package opset

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/hexane"
	"github.com/develerltd/go-automerge/internal/types"
)

// OpColumns stores operations in columnar layout using hexane ColumnData.
//
// This is the hexane-backed equivalent of the flat []Op storage. Each column stores
// one field across all operations, enabling compression (RLE, delta, boolean) and
// O(log n) positional operations via the SpanTree.
//
// Columns follow the Automerge binary format: actor indices are RLE-encoded uint64,
// counters are delta-encoded int64, etc. Nullable columns use nil for absent values
// (e.g., obj_actor/obj_ctr are nil for root objects).
//
// Grouped columns (succ, value) use a count+items pattern: succ_count holds per-op
// counts, and succ_actor/succ_ctr hold the flattened successor data. The count column's
// accumulator gives the total length of the detail columns.
type OpColumns struct {
	IDActor   *hexane.ColumnData[uint64] // RLE — actor index of op ID
	IDCtr     *hexane.ColumnData[int64]  // Delta — counter of op ID
	ObjActor  *hexane.ColumnData[uint64] // RLE — actor index of object (nil = root)
	ObjCtr    *hexane.ColumnData[uint64] // RLE — counter of object (nil = root)
	KeyActor  *hexane.ColumnData[uint64] // RLE — actor index of key (nil = map key or HEAD)
	KeyCtr    *hexane.ColumnData[int64]  // Delta — counter of key (nil = map key or HEAD)
	KeyStr    *hexane.ColumnData[string] // RLE — string key (nil = seq key)
	Insert    *hexane.ColumnData[bool]   // Boolean — insert flag
	Action    *hexane.ColumnData[uint64] // RLE — action type
	ValueMeta *hexane.ColumnData[uint64] // RLE (ValueMetaPacker) — value metadata; acc = total value bytes
	Value     *hexane.ColumnData[[]byte] // Raw — raw value byte data
	SuccCount *hexane.ColumnData[uint64] // RLE — number of successors per op; acc = total succ records
	SuccActor *hexane.ColumnData[uint64] // RLE — successor actor indices (flattened)
	SuccCtr   *hexane.ColumnData[int64]  // Delta — successor counters (flattened)
	MarkName  *hexane.ColumnData[string] // RLE — mark name
	Expand    *hexane.ColumnData[bool]   // Boolean — mark expand flag
}

// NewOpColumns creates an empty OpColumns with all columns initialized.
func NewOpColumns() *OpColumns {
	return &OpColumns{
		IDActor:   hexane.NewUIntColumn(),
		IDCtr:     hexane.NewDeltaColumn(),
		ObjActor:  hexane.NewUIntColumn(),
		ObjCtr:    hexane.NewUIntColumn(),
		KeyActor:  hexane.NewUIntColumn(),
		KeyCtr:    hexane.NewDeltaColumn(),
		KeyStr:    hexane.NewStrColumn(),
		Insert:    hexane.NewBoolColumn(),
		Action:    hexane.NewUIntColumn(),
		ValueMeta: hexane.NewValueMetaColumn(),
		Value:     hexane.NewRawColumn(),
		SuccCount: hexane.NewUIntColumn(),
		SuccActor: hexane.NewUIntColumn(),
		SuccCtr:   hexane.NewDeltaColumn(),
		MarkName:  hexane.NewStrColumn(),
		Expand:    hexane.NewBoolColumn(),
	}
}

// LoadOpColumns loads operation columns from raw columnar data (document format with successors).
func LoadOpColumns(cols columnar.RawColumns) (*OpColumns, error) {
	// Load IDActor first to determine the op count.
	idActorData := cols.FindData(columnar.OpColActor)
	idActor, err := loadUIntCol(idActorData)
	if err != nil {
		return nil, fmt.Errorf("loading id_actor: %w", err)
	}
	opLen := idActor.Len()

	// Load remaining main columns (one row per op).
	idCtr, err := loadDeltaColLen(cols.FindData(columnar.OpColCounter), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading id_ctr: %w", err)
	}
	objActor, err := loadUIntColLen(cols.FindData(columnar.OpColObjActor), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading obj_actor: %w", err)
	}
	objCtr, err := loadUIntColLen(cols.FindData(columnar.OpColObjCtr), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading obj_ctr: %w", err)
	}
	keyActor, err := loadUIntColLen(cols.FindData(columnar.OpColKeyActor), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading key_actor: %w", err)
	}
	keyCtr, err := loadDeltaColLen(cols.FindData(columnar.OpColKeyCtr), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading key_ctr: %w", err)
	}
	keyStr, err := loadStrColLen(cols.FindData(columnar.OpColKeyStr), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading key_str: %w", err)
	}
	insert, err := loadBoolColLen(cols.FindData(columnar.OpColInsert), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading insert: %w", err)
	}
	action, err := loadUIntColLen(cols.FindData(columnar.OpColAction), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading action: %w", err)
	}

	// Mark columns.
	markName, err := loadStrColLen(cols.FindData(columnar.OpColMarkName), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading mark_name: %w", err)
	}
	expand, err := loadBoolColLen(cols.FindData(columnar.OpColMarkExpand), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading expand: %w", err)
	}

	// Grouped: value — value_meta.Acc() = total raw value bytes.
	valueMeta, err := loadValueMetaColLen(cols.FindData(columnar.OpColValueMeta), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading value_meta: %w", err)
	}
	valueLen := valueMeta.Acc().AsInt()
	value, err := loadRawColLen(cols.FindData(columnar.OpColValue), valueLen)
	if err != nil {
		return nil, fmt.Errorf("loading value: %w", err)
	}

	// Grouped: successors — succ_count.Acc() = total successor records.
	succCount, err := loadUIntColLen(cols.FindData(columnar.OpColSuccGroup), opLen)
	if err != nil {
		return nil, fmt.Errorf("loading succ_count: %w", err)
	}
	succLen := succCount.Acc().AsInt()
	succActor, err := loadUIntColLen(cols.FindData(columnar.OpColSuccActor), succLen)
	if err != nil {
		return nil, fmt.Errorf("loading succ_actor: %w", err)
	}
	succCtr, err := loadDeltaColLen(cols.FindData(columnar.OpColSuccCtr), succLen)
	if err != nil {
		return nil, fmt.Errorf("loading succ_ctr: %w", err)
	}

	return &OpColumns{
		IDActor:   idActor,
		IDCtr:     idCtr,
		ObjActor:  objActor,
		ObjCtr:    objCtr,
		KeyActor:  keyActor,
		KeyCtr:    keyCtr,
		KeyStr:    keyStr,
		Insert:    insert,
		Action:    action,
		ValueMeta: valueMeta,
		Value:     value,
		SuccCount: succCount,
		SuccActor: succActor,
		SuccCtr:   succCtr,
		MarkName:  markName,
		Expand:    expand,
	}, nil
}

// Len returns the number of operations.
func (c *OpColumns) Len() int {
	return c.IDActor.Len()
}

// IsEmpty returns true if no operations are stored.
func (c *OpColumns) IsEmpty() bool {
	return c.IDActor.Len() == 0
}

// Export serializes the columns to document-format RawColumns (with successors).
func (c *OpColumns) Export() columnar.RawColumns {
	var cols columnar.RawColumns

	addCol := func(spec columnar.ColumnSpec, data []byte) {
		if len(data) > 0 {
			cols = append(cols, columnar.RawColumn{Spec: spec, Data: data})
		}
	}

	// Export in sorted spec order.
	// ObjActor uses SaveTo (not SaveToUnlessEmpty) because actor index 0 is a valid
	// value that must be preserved; IsEmpty(0)==true would incorrectly drop the column.
	addCol(columnar.OpColObjActor, c.ObjActor.SaveTo(nil))
	addCol(columnar.OpColObjCtr, c.ObjCtr.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColKeyActor, c.KeyActor.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColKeyCtr, c.KeyCtr.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColKeyStr, c.KeyStr.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColActor, c.IDActor.SaveTo(nil))
	addCol(columnar.OpColCounter, c.IDCtr.SaveTo(nil))

	// Insert is always exported, even if all false.
	insertData := c.Insert.SaveTo(nil)
	if len(insertData) > 0 {
		cols = append(cols, columnar.RawColumn{Spec: columnar.OpColInsert, Data: insertData})
	}

	addCol(columnar.OpColAction, c.Action.SaveTo(nil))
	addCol(columnar.OpColValueMeta, c.ValueMeta.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColValue, c.Value.SaveTo(nil))
	addCol(columnar.OpColSuccGroup, c.SuccCount.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColSuccActor, c.SuccActor.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColSuccCtr, c.SuccCtr.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColMarkExpand, c.Expand.SaveToUnlessEmpty(nil))
	addCol(columnar.OpColMarkName, c.MarkName.SaveToUnlessEmpty(nil))

	return cols
}

// SpliceOne inserts a single operation at the given position.
// This is a fast path that avoids allocating intermediate pointer slices,
// calling column.Splice directly with non-nullable values where possible.
func (c *OpColumns) SpliceOne(pos int, op *Op) {
	// IDActor, IDCtr — always non-null
	c.IDActor.Splice(pos, 0, []uint64{uint64(op.ID.ActorIdx)})
	c.IDCtr.Splice(pos, 0, []int64{int64(op.ID.Counter)})

	// Obj — null for root
	if op.Obj.IsRoot() {
		c.ObjActor.SpliceNullable(pos, 0, []*uint64{nil})
		c.ObjCtr.SpliceNullable(pos, 0, []*uint64{nil})
	} else {
		c.ObjActor.Splice(pos, 0, []uint64{uint64(op.Obj.ActorIdx)})
		c.ObjCtr.Splice(pos, 0, []uint64{op.Obj.Counter})
	}

	// Key
	if op.Key.Kind == KeyMap {
		c.KeyActor.SpliceNullable(pos, 0, []*uint64{nil})
		c.KeyCtr.SpliceNullable(pos, 0, []*int64{nil})
		c.KeyStr.Splice(pos, 0, []string{op.Key.MapKey})
	} else {
		c.KeyStr.SpliceNullable(pos, 0, []*string{nil})
		if op.Key.ElemID.IsZero() {
			c.KeyActor.SpliceNullable(pos, 0, []*uint64{nil})
			c.KeyCtr.SpliceNullable(pos, 0, []*int64{nil})
		} else {
			c.KeyActor.Splice(pos, 0, []uint64{uint64(op.Key.ElemID.ActorIdx)})
			c.KeyCtr.Splice(pos, 0, []int64{int64(op.Key.ElemID.Counter)})
		}
	}

	c.Insert.Splice(pos, 0, []bool{op.Insert})
	c.Action.Splice(pos, 0, []uint64{uint64(op.Action)})

	// Value: compute byte offset BEFORE splicing metadata
	meta := encodeValueMeta(op.Value)
	valuePos := c.ValueMeta.GetAcc(pos).AsInt()
	c.ValueMeta.Splice(pos, 0, []uint64{meta})
	vb := encodeValueBytes(op.Value)
	if len(vb) > 0 {
		rawItems := make([][]byte, len(vb))
		for i, b := range vb {
			rawItems[i] = []byte{b}
		}
		c.Value.Splice(valuePos, 0, rawItems)
	}

	// Succ: compute record offset BEFORE splicing count
	sc := uint64(len(op.Succ))
	succPos := c.SuccCount.GetAcc(pos).AsInt()
	c.SuccCount.Splice(pos, 0, []uint64{sc})
	if len(op.Succ) > 0 {
		actors := make([]uint64, len(op.Succ))
		ctrs := make([]int64, len(op.Succ))
		for i, s := range op.Succ {
			actors[i] = uint64(s.ActorIdx)
			ctrs[i] = int64(s.Counter)
		}
		c.SuccActor.Splice(succPos, 0, actors)
		c.SuccCtr.Splice(succPos, 0, ctrs)
	}

	// Marks
	if op.MarkName != "" {
		c.MarkName.Splice(pos, 0, []string{op.MarkName})
	} else {
		c.MarkName.SpliceNullable(pos, 0, []*string{nil})
	}
	c.Expand.Splice(pos, 0, []bool{op.Expand})
}

// Splice inserts operations at the given position. Returns the number of ops inserted.
// This handles grouped columns (value, succ) correctly by computing byte/record
// offsets from accumulator values.
func (c *OpColumns) Splice(pos int, ops []Op) int {
	if len(ops) == 0 {
		return 0
	}

	// Build column value slices from ops.
	n := len(ops)
	idActors := make([]*uint64, n)
	idCtrs := make([]*int64, n)
	objActors := make([]*uint64, n)
	objCtrs := make([]*uint64, n)
	keyActors := make([]*uint64, n)
	keyCtrs := make([]*int64, n)
	keyStrs := make([]*string, n)
	inserts := make([]*bool, n)
	actions := make([]*uint64, n)
	valueMetas := make([]*uint64, n)
	succCounts := make([]*uint64, n)
	markNames := make([]*string, n)
	expands := make([]*bool, n)

	// Collect flattened successor and value data.
	var succActors []*uint64
	var succCtrs []*int64
	var valueBytes [][]byte

	for i := range ops {
		op := &ops[i]

		idActor := uint64(op.ID.ActorIdx)
		idActors[i] = &idActor
		idCtr := int64(op.ID.Counter)
		idCtrs[i] = &idCtr

		// Object (nil = root)
		if op.Obj.IsRoot() {
			objActors[i] = nil
			objCtrs[i] = nil
		} else {
			oa := uint64(op.Obj.ActorIdx)
			objActors[i] = &oa
			oc := op.Obj.Counter
			objCtrs[i] = &oc
		}

		// Key
		if op.Key.Kind == KeyMap {
			keyActors[i] = nil
			keyCtrs[i] = nil
			s := op.Key.MapKey
			keyStrs[i] = &s
		} else {
			keyStrs[i] = nil
			if op.Key.ElemID.IsZero() {
				// HEAD
				keyActors[i] = nil
				keyCtrs[i] = nil
			} else {
				ka := uint64(op.Key.ElemID.ActorIdx)
				keyActors[i] = &ka
				kc := int64(op.Key.ElemID.Counter)
				keyCtrs[i] = &kc
			}
		}

		inserts[i] = &op.Insert
		act := uint64(op.Action)
		actions[i] = &act

		// Value metadata and raw bytes.
		meta := encodeValueMeta(op.Value)
		valueMetas[i] = &meta
		valueBytes = append(valueBytes, encodeValueBytes(op.Value))

		// Successors
		sc := uint64(len(op.Succ))
		succCounts[i] = &sc
		for _, s := range op.Succ {
			sa := uint64(s.ActorIdx)
			succActors = append(succActors, &sa)
			sctr := int64(s.Counter)
			succCtrs = append(succCtrs, &sctr)
		}

		// Marks
		if op.MarkName != "" {
			mn := op.MarkName
			markNames[i] = &mn
		}
		expands[i] = &op.Expand
	}

	// Splice main columns (one row per op).
	c.IDActor.SpliceNullable(pos, 0, idActors)
	c.IDCtr.SpliceNullable(pos, 0, idCtrs)
	c.ObjActor.SpliceNullable(pos, 0, objActors)
	c.ObjCtr.SpliceNullable(pos, 0, objCtrs)
	c.KeyActor.SpliceNullable(pos, 0, keyActors)
	c.KeyCtr.SpliceNullable(pos, 0, keyCtrs)
	c.KeyStr.SpliceNullable(pos, 0, keyStrs)
	c.Insert.SpliceNullable(pos, 0, inserts)
	c.Action.SpliceNullable(pos, 0, actions)
	c.Expand.SpliceNullable(pos, 0, expands)
	c.MarkName.SpliceNullable(pos, 0, markNames)

	// Grouped: value — compute byte offset BEFORE splicing value_meta.
	valuePos := c.ValueMeta.GetAcc(pos).AsInt()
	c.ValueMeta.SpliceNullable(pos, 0, valueMetas)
	// Flatten value bytes into individual raw byte items.
	var rawItems [][]byte
	for _, vb := range valueBytes {
		for _, b := range vb {
			rawItems = append(rawItems, []byte{b})
		}
	}
	if len(rawItems) > 0 {
		c.Value.Splice(valuePos, 0, rawItems)
	}

	// Grouped: succ — compute record offset BEFORE splicing succ_count.
	succPos := c.SuccCount.GetAcc(pos).AsInt()
	c.SuccCount.SpliceNullable(pos, 0, succCounts)
	if len(succActors) > 0 {
		c.SuccActor.SpliceNullable(succPos, 0, succActors)
		c.SuccCtr.SpliceNullable(succPos, 0, succCtrs)
	}

	return n
}

// Delete removes count operations starting at pos.
// Handles grouped columns (value, succ) by computing byte/record
// offsets from accumulator values before deleting.
func (c *OpColumns) Delete(pos, count int) {
	if count == 0 {
		return
	}

	// Grouped: value — compute byte range BEFORE deleting value_meta.
	valueStart := c.ValueMeta.GetAcc(pos).AsInt()
	valueEnd := c.ValueMeta.GetAcc(pos + count).AsInt()
	valueDel := valueEnd - valueStart

	// Grouped: succ — compute record range BEFORE deleting succ_count.
	succStart := c.SuccCount.GetAcc(pos).AsInt()
	succEnd := c.SuccCount.GetAcc(pos + count).AsInt()
	succDel := succEnd - succStart

	// Delete main columns.
	c.IDActor.Splice(pos, count, nil)
	c.IDCtr.Splice(pos, count, nil)
	c.ObjActor.Splice(pos, count, nil)
	c.ObjCtr.Splice(pos, count, nil)
	c.KeyActor.Splice(pos, count, nil)
	c.KeyCtr.Splice(pos, count, nil)
	c.KeyStr.Splice(pos, count, nil)
	c.Insert.Splice(pos, count, nil)
	c.Action.Splice(pos, count, nil)
	c.ValueMeta.Splice(pos, count, nil)
	c.Expand.Splice(pos, count, nil)
	c.MarkName.Splice(pos, count, nil)
	c.SuccCount.Splice(pos, count, nil)

	// Delete grouped value bytes.
	if valueDel > 0 {
		c.Value.Splice(valueStart, valueDel, nil)
	}

	// Delete grouped succ records.
	if succDel > 0 {
		c.SuccActor.Splice(succStart, succDel, nil)
		c.SuccCtr.Splice(succStart, succDel, nil)
	}
}

// RewriteWithNewActor increments actor indices >= idx in all actor columns.
// Called when a new actor is inserted into the actors list.
func (c *OpColumns) RewriteWithNewActor(idx uint32) {
	bumpActor := func(v *uint64) *uint64 {
		if v == nil {
			return nil
		}
		if uint32(*v) >= idx {
			bumped := *v + 1
			return &bumped
		}
		return v
	}
	c.IDActor.Remap(bumpActor)
	c.ObjActor.Remap(bumpActor)
	c.KeyActor.Remap(bumpActor)
	c.SuccActor.Remap(bumpActor)
}

// RewriteWithoutActor decrements actor indices > idx in all actor columns.
// Panics if idx is found (the actor must not be present).
func (c *OpColumns) RewriteWithoutActor(idx uint32) {
	unbumpActor := func(v *uint64) *uint64 {
		if v == nil {
			return nil
		}
		if uint32(*v) == idx {
			panic(fmt.Sprintf("RewriteWithoutActor: actor index %d is present", idx))
		}
		if uint32(*v) > idx {
			unbumped := *v - 1
			return &unbumped
		}
		return v
	}
	c.IDActor.Remap(unbumpActor)
	c.ObjActor.Remap(unbumpActor)
	c.KeyActor.Remap(unbumpActor)
	c.SuccActor.Remap(unbumpActor)
}

// --- Column loading helpers ---

func loadUIntCol(data []byte) (*hexane.ColumnData[uint64], error) {
	if len(data) == 0 {
		return hexane.NewUIntColumn(), nil
	}
	return hexane.LoadColumnData(hexane.UIntCursorOps(), data)
}

func loadUIntColLen(data []byte, length int) (*hexane.ColumnData[uint64], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.UIntCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.UIntCursorOps(), data, length)
}

func loadDeltaColLen(data []byte, length int) (*hexane.ColumnData[int64], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.DeltaCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.DeltaCursorOps(), data, length)
}

func loadBoolColLen(data []byte, length int) (*hexane.ColumnData[bool], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.BoolCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.BoolCursorOps(), data, length)
}

func loadStrColLen(data []byte, length int) (*hexane.ColumnData[string], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.StrCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.StrCursorOps(), data, length)
}

func loadValueMetaColLen(data []byte, length int) (*hexane.ColumnData[uint64], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.ValueMetaCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.ValueMetaCursorOps(), data, length)
}

func loadRawColLen(data []byte, length int) (*hexane.ColumnData[[]byte], error) {
	if len(data) == 0 {
		return hexane.InitEmptyColumnData(hexane.RawCursorOps(), length), nil
	}
	return hexane.LoadColumnDataUnlessEmpty(hexane.RawCursorOps(), data, length)
}

// encodeValueBytes returns the raw bytes for a scalar value (used during splice).
func encodeValueBytes(v types.ScalarValue) []byte {
	return appendValueData(nil, v)
}
