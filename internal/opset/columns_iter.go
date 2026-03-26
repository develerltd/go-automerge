package opset

import (
	"encoding/binary"
	"math"

	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/hexane"
	"github.com/develerltd/go-automerge/internal/types"
)

// OpIter iterates over OpColumns, materializing Op structs one row at a time.
//
// For grouped columns (value, succ), the iterator tracks separate positions:
// value bytes are read from the Value column based on metadata byte lengths,
// and successor records are read from SuccActor/SuccCtr based on succ counts.
type OpIter struct {
	idActor   *hexane.ColumnDataIter[uint64]
	idCtr     *hexane.ColumnDataIter[int64]
	objActor  *hexane.ColumnDataIter[uint64]
	objCtr    *hexane.ColumnDataIter[uint64]
	keyActor  *hexane.ColumnDataIter[uint64]
	keyCtr    *hexane.ColumnDataIter[int64]
	keyStr    *hexane.ColumnDataIter[string]
	insert    *hexane.ColumnDataIter[bool]
	action    *hexane.ColumnDataIter[uint64]
	valueMeta *hexane.ColumnDataIter[uint64]
	value     *hexane.ColumnDataIter[[]byte]
	succCount *hexane.ColumnDataIter[uint64]
	succActor *hexane.ColumnDataIter[uint64]
	succCtr   *hexane.ColumnDataIter[int64]
	markName  *hexane.ColumnDataIter[string]
	expand    *hexane.ColumnDataIter[bool]
	pos       int
}

// Iter returns an iterator over all operations in the columns.
func (c *OpColumns) Iter() *OpIter {
	return c.IterRange(0, c.Len())
}

// IterRange returns an iterator over operations in [start, end).
func (c *OpColumns) IterRange(start, end int) *OpIter {
	// For grouped columns, compute offset ranges from accumulators.
	valueStart := c.ValueMeta.GetAcc(start).AsInt()
	valueEnd := c.ValueMeta.GetAcc(end).AsInt()
	succStart := c.SuccCount.GetAcc(start).AsInt()
	succEnd := c.SuccCount.GetAcc(end).AsInt()

	return &OpIter{
		idActor:   c.IDActor.IterRange(start, end),
		idCtr:     c.IDCtr.IterRange(start, end),
		objActor:  c.ObjActor.IterRange(start, end),
		objCtr:    c.ObjCtr.IterRange(start, end),
		keyActor:  c.KeyActor.IterRange(start, end),
		keyCtr:    c.KeyCtr.IterRange(start, end),
		keyStr:    c.KeyStr.IterRange(start, end),
		insert:    c.Insert.IterRange(start, end),
		action:    c.Action.IterRange(start, end),
		valueMeta: c.ValueMeta.IterRange(start, end),
		value:     c.Value.IterRange(valueStart, valueEnd),
		succCount: c.SuccCount.IterRange(start, end),
		succActor: c.SuccActor.IterRange(succStart, succEnd),
		succCtr:   c.SuccCtr.IterRange(succStart, succEnd),
		markName:  c.MarkName.IterRange(start, end),
		expand:    c.Expand.IterRange(start, end),
		pos:       start,
	}
}

// Pos returns the current position of the iterator.
func (it *OpIter) Pos() int { return it.pos }

// Next materializes the next operation from the column iterators.
// Returns nil, false when exhausted.
func (it *OpIter) Next() (*Op, bool) {
	actionVal, actionOk := it.action.Next()
	if !actionOk {
		return nil, false
	}

	idActorVal, _ := it.idActor.Next()
	idCtrVal, _ := it.idCtr.Next()
	objActorVal, _ := it.objActor.Next()
	objCtrVal, _ := it.objCtr.Next()
	keyActorVal, _ := it.keyActor.Next()
	keyCtrVal, _ := it.keyCtr.Next()
	keyStrVal, _ := it.keyStr.Next()
	insertVal, _ := it.insert.Next()
	valueMetaVal, _ := it.valueMeta.Next()
	succCountVal, _ := it.succCount.Next()
	markNameVal, _ := it.markName.Next()
	expandVal, _ := it.expand.Next()

	op := &Op{}

	// Op ID
	if idActorVal != nil && idCtrVal != nil {
		op.ID = types.OpId{Counter: uint64(*idCtrVal), ActorIdx: uint32(*idActorVal)}
	}

	// Object (nil = root)
	if objActorVal == nil && objCtrVal == nil {
		op.Obj = types.Root
	} else {
		var actor uint32
		var ctr uint64
		if objActorVal != nil {
			actor = uint32(*objActorVal)
		}
		if objCtrVal != nil {
			ctr = *objCtrVal
		}
		op.Obj = types.ObjId{OpId: types.OpId{Counter: ctr, ActorIdx: actor}}
	}

	// Key
	if keyStrVal != nil && *keyStrVal != "" {
		op.Key = Key{Kind: KeyMap, MapKey: *keyStrVal}
	} else if keyActorVal != nil || keyCtrVal != nil {
		var actor uint32
		var ctr uint64
		if keyActorVal != nil {
			actor = uint32(*keyActorVal)
		}
		if keyCtrVal != nil {
			ctr = uint64(*keyCtrVal)
		}
		op.Key = Key{Kind: KeySeq, ElemID: types.OpId{Counter: ctr, ActorIdx: actor}}
	} else {
		// Both null: HEAD
		op.Key = Key{Kind: KeySeq, ElemID: types.Head.OpId}
	}

	// Insert
	if insertVal != nil {
		op.Insert = *insertVal
	}

	// Action
	if actionVal != nil {
		op.Action = Action(*actionVal)
	}

	// Value: read metadata, then consume the right number of raw bytes.
	if valueMetaVal != nil {
		meta := *valueMetaVal
		byteLen := int(meta >> 4)
		var rawBytes []byte
		if byteLen > 0 {
			rawBytes = make([]byte, 0, byteLen)
			for j := 0; j < byteLen; j++ {
				b, ok := it.value.Next()
				if ok && b != nil {
					rawBytes = append(rawBytes, (*b)...)
				}
			}
		}
		op.Value = decodeValueFromMeta(meta, rawBytes)
	}

	// Successors: read count, then that many (actor, ctr) pairs.
	if succCountVal != nil {
		count := int(*succCountVal)
		if count > 0 {
			op.Succ = make([]types.OpId, count)
			for j := 0; j < count; j++ {
				sa, _ := it.succActor.Next()
				sc, _ := it.succCtr.Next()
				if sa != nil {
					op.Succ[j].ActorIdx = uint32(*sa)
				}
				if sc != nil {
					op.Succ[j].Counter = uint64(*sc)
				}
			}
		}
	}

	// Mark info
	if markNameVal != nil {
		op.MarkName = *markNameVal
	}
	if expandVal != nil {
		op.Expand = *expandVal
	}

	it.pos++
	return op, true
}

// Get materializes a single operation at the given index.
// Uses per-column Get() calls with DecodeAt fast paths instead of creating
// a full IterRange with 16 column iterators + slab decoding.
func (c *OpColumns) Get(index int) *Op {
	if index < 0 || index >= c.Len() {
		return nil
	}

	op := &Op{}

	// Op ID
	if v, ok := c.IDActor.Get(index); ok && v != nil {
		op.ID.ActorIdx = uint32(*v)
	}
	if v, ok := c.IDCtr.Get(index); ok && v != nil {
		op.ID.Counter = uint64(*v)
	}

	// Object (nil = root)
	objActorVal, _ := c.ObjActor.Get(index)
	objCtrVal, _ := c.ObjCtr.Get(index)
	if objActorVal == nil && objCtrVal == nil {
		op.Obj = types.Root
	} else {
		var actor uint32
		var ctr uint64
		if objActorVal != nil {
			actor = uint32(*objActorVal)
		}
		if objCtrVal != nil {
			ctr = *objCtrVal
		}
		op.Obj = types.ObjId{OpId: types.OpId{Counter: ctr, ActorIdx: actor}}
	}

	// Key
	keyStrVal, _ := c.KeyStr.Get(index)
	keyActorVal, _ := c.KeyActor.Get(index)
	keyCtrVal, _ := c.KeyCtr.Get(index)
	if keyStrVal != nil && *keyStrVal != "" {
		op.Key = Key{Kind: KeyMap, MapKey: *keyStrVal}
	} else if keyActorVal != nil || keyCtrVal != nil {
		var actor uint32
		var ctr uint64
		if keyActorVal != nil {
			actor = uint32(*keyActorVal)
		}
		if keyCtrVal != nil {
			ctr = uint64(*keyCtrVal)
		}
		op.Key = Key{Kind: KeySeq, ElemID: types.OpId{Counter: ctr, ActorIdx: actor}}
	} else {
		op.Key = Key{Kind: KeySeq, ElemID: types.Head.OpId}
	}

	// Insert
	if v, ok := c.Insert.Get(index); ok && v != nil {
		op.Insert = *v
	}

	// Action
	if v, ok := c.Action.Get(index); ok && v != nil {
		op.Action = Action(*v)
	}

	// Value: use GetWithAcc for byte offset, then read value bytes
	valueMeta, valueAcc, vok := c.ValueMeta.GetWithAcc(index)
	if vok && valueMeta != nil {
		meta := *valueMeta
		byteLen := int(meta >> 4)
		var rawBytes []byte
		if byteLen > 0 {
			valueOffset := valueAcc.AsInt()
			rawBytes = make([]byte, 0, byteLen)
			for j := 0; j < byteLen; j++ {
				b, ok := c.Value.Get(valueOffset + j)
				if ok && b != nil {
					rawBytes = append(rawBytes, (*b)...)
				}
			}
		}
		op.Value = decodeValueFromMeta(meta, rawBytes)
	}

	// Successors: use GetWithAcc for record offset
	succCount, succAcc, sok := c.SuccCount.GetWithAcc(index)
	if sok && succCount != nil && *succCount > 0 {
		count := int(*succCount)
		succOffset := succAcc.AsInt()
		op.Succ = make([]types.OpId, count)
		for j := 0; j < count; j++ {
			sa, _ := c.SuccActor.Get(succOffset + j)
			sc, _ := c.SuccCtr.Get(succOffset + j)
			if sa != nil {
				op.Succ[j].ActorIdx = uint32(*sa)
			}
			if sc != nil {
				op.Succ[j].Counter = uint64(*sc)
			}
		}
	}

	// Mark info
	if v, ok := c.MarkName.Get(index); ok && v != nil {
		op.MarkName = *v
	}
	if v, ok := c.Expand.Get(index); ok && v != nil {
		op.Expand = *v
	}

	return op
}

// MaterializeRange materializes operations in [start, end) into an Op slice.
func (c *OpColumns) MaterializeRange(start, end int) []Op {
	if start >= end {
		return nil
	}
	ops := make([]Op, 0, end-start)
	it := c.IterRange(start, end)
	for {
		op, ok := it.Next()
		if !ok {
			break
		}
		ops = append(ops, *op)
	}
	return ops
}

// VisibleOpsInRange returns only visible ops in [start, end) by scanning
// SuccCount using run-based iteration to skip large blocks of non-visible ops,
// then materializing only those with SuccCount==0 and Action!=Delete.
//
// For the common case where most ops are superseded (e.g. repeated updates to
// the same key), this is O(num_runs) instead of O(range_size).
func (c *OpColumns) VisibleOpsInRange(start, end int) []Op {
	if start >= end {
		return nil
	}

	// Scan SuccCount using NextRun() to skip entire runs of non-visible ops.
	// For repeatedPut(n), SuccCount has ~1 run of (n-1) ones and 1 zero,
	// so this completes in O(1) instead of O(n).
	succIt := c.SuccCount.IterRange(start, end)
	var candidatePositions []int
	pos := start
	for {
		run := succIt.NextRun()
		if run == nil {
			break
		}
		if run.Value == nil || *run.Value == 0 {
			// SuccCount == 0: these positions are visibility candidates
			for i := 0; i < run.Count; i++ {
				candidatePositions = append(candidatePositions, pos+i)
			}
		}
		pos += run.Count
	}

	if len(candidatePositions) == 0 {
		return nil
	}

	// Filter by Action != Delete, then materialize only visible ops
	ops := make([]Op, 0, len(candidatePositions))
	for _, p := range candidatePositions {
		act, _ := c.Action.Get(p)
		if act != nil && Action(*act) == ActionDelete {
			continue
		}
		op := c.Get(p)
		if op != nil {
			ops = append(ops, *op)
		}
	}
	return ops
}

// ToOps materializes all operations from OpColumns into an Op slice.
func (c *OpColumns) ToOps() []Op {
	ops := make([]Op, 0, c.Len())
	it := c.Iter()
	for {
		op, ok := it.Next()
		if !ok {
			break
		}
		ops = append(ops, *op)
	}
	return ops
}

// decodeValueFromMeta reconstructs a ScalarValue from a value metadata uint64
// and the raw value bytes.
func decodeValueFromMeta(meta uint64, data []byte) types.ScalarValue {
	valType := meta & 0x0f

	switch valType {
	case 0: // null
		return types.NewNull()
	case 1: // false
		return types.NewBool(false)
	case 2: // true
		return types.NewBool(true)
	case 3: // uint
		if len(data) == 0 {
			return types.NewUint(0)
		}
		v, _, _ := encoding.ReadULEB128(data, 0)
		return types.NewUint(v)
	case 4: // int
		if len(data) == 0 {
			return types.NewInt(0)
		}
		v, _, _ := encoding.ReadSLEB128(data, 0)
		return types.NewInt(v)
	case 5: // float64
		if len(data) < 8 {
			return types.NewFloat64(0)
		}
		bits := binary.LittleEndian.Uint64(data[:8])
		return types.NewFloat64(math.Float64frombits(bits))
	case 6: // string
		return types.NewStr(string(data))
	case 7: // bytes
		cp := make([]byte, len(data))
		copy(cp, data)
		return types.NewBytes(cp)
	case 8: // counter
		if len(data) == 0 {
			return types.NewCounter(0)
		}
		v, _, _ := encoding.ReadSLEB128(data, 0)
		return types.NewCounter(v)
	case 9: // timestamp
		if len(data) == 0 {
			return types.NewTimestamp(0)
		}
		v, _, _ := encoding.ReadSLEB128(data, 0)
		return types.NewTimestamp(v)
	default:
		cp := make([]byte, len(data))
		copy(cp, data)
		return types.NewUnknownScalar(uint8(valType), cp)
	}
}
