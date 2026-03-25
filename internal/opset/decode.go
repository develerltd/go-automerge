package opset

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/types"
)

// DecodeOps decodes operations from document-format columnar data.
// In document chunks, ops have successors (succ) instead of predecessors (pred).
// Uses LoadOpColumns to load directly into columnar storage, avoiding per-op decode/encode.
func DecodeOps(cols columnar.RawColumns, actors []types.ActorId) (*OpSet, error) {
	opCols, err := LoadOpColumns(cols)
	if err != nil {
		return nil, fmt.Errorf("loading op columns: %w", err)
	}

	os := &OpSet{
		Actors:    actors,
		cols:      opCols,
		objInfo:   make(map[types.OpId]ObjInfo),
		objIndex:  make(map[types.OpId]objRange),
		opIdToRow: make(map[types.OpId]int),
	}

	// Build indices by iterating the columns once
	it := opCols.Iter()
	for i := 0; ; i++ {
		op, ok := it.Next()
		if !ok {
			break
		}

		if op.ID.Counter > os.MaxOp {
			os.MaxOp = op.ID.Counter
		}

		objKey := op.Obj.OpId
		if r, exists := os.objIndex[objKey]; exists {
			r.end = i + 1
			os.objIndex[objKey] = r
		} else {
			os.objIndex[objKey] = objRange{start: i, end: i + 1}
		}

		if op.Action.IsMake() {
			os.objInfo[op.ID] = ObjInfo{Parent: op.Obj, ObjType: op.ObjType()}
		}

		os.opIdToRow[op.ID] = i
	}

	return os, nil
}

// DecodeChangeOps decodes operations from change-format columnar data.
// In change chunks, ops have predecessors (pred) instead of successors (succ).
// Actor index 0 = change actor, 1+ = other actors.
func DecodeChangeOps(cols columnar.RawColumns, actor types.ActorId, otherActors []types.ActorId) ([]Op, error) {
	objActorData := cols.FindData(columnar.OpColObjActor)
	objCtrData := cols.FindData(columnar.OpColObjCtr)
	keyActorData := cols.FindData(columnar.OpColKeyActor)
	keyCtrData := cols.FindData(columnar.OpColKeyCtr)
	keyStrData := cols.FindData(columnar.OpColKeyStr)
	actorData := cols.FindData(columnar.OpColActor)
	counterData := cols.FindData(columnar.OpColCounter)
	insertData := cols.FindData(columnar.OpColInsert)
	actionData := cols.FindData(columnar.OpColAction)
	valueMetaData := cols.FindData(columnar.OpColValueMeta)
	valueData := cols.FindData(columnar.OpColValue)
	predGroupData := cols.FindData(columnar.OpColPredGroup)
	predActorData := cols.FindData(columnar.OpColPredActor)
	predCtrData := cols.FindData(columnar.OpColPredCtr)

	objActorDec := encoding.NewRLEDecoderUint64(objActorData)
	objCtrDec := encoding.NewRLEDecoderUint64(objCtrData)
	keyActorDec := encoding.NewRLEDecoderUint64(keyActorData)
	keyCtrDec := encoding.NewDeltaDecoder(keyCtrData)
	keyStrDec := encoding.NewRLEDecoderString(keyStrData)
	actorDec := encoding.NewRLEDecoderUint64(actorData)
	counterDec := encoding.NewDeltaDecoder(counterData)
	insertDec := encoding.NewBooleanDecoder(insertData)
	actionDec := encoding.NewRLEDecoderUint64(actionData)
	valueMetaDec := encoding.NewRLEDecoderUint64(valueMetaData)
	valueReader := encoding.NewReader(valueData)
	predGroupDec := encoding.NewRLEDecoderUint64(predGroupData)
	predActorDec := encoding.NewRLEDecoderUint64(predActorData)
	predCtrDec := encoding.NewDeltaDecoder(predCtrData)

	_ = actor
	_ = otherActors

	var ops []Op

	for !actionDec.Done() {
		actionVal, _, err := actionDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding action: %w", err)
		}

		objActor, objActorNull, err := objActorDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding obj actor: %w", err)
		}
		objCtr, objCtrNull, err := objCtrDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding obj counter: %w", err)
		}

		var obj types.ObjId
		if objActorNull && objCtrNull {
			obj = types.Root
		} else {
			obj = types.ObjId{OpId: types.OpId{Counter: objCtr, ActorIdx: uint32(objActor)}}
		}

		keyActor, keyActorNull, err := keyActorDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding key actor: %w", err)
		}
		keyCtr, keyCtrNull, err := keyCtrDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding key counter: %w", err)
		}
		keyStr, keyStrNull, err := keyStrDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding key string: %w", err)
		}

		var key Key
		if !keyStrNull && keyStr != "" {
			key = Key{Kind: KeyMap, MapKey: keyStr}
		} else if !keyActorNull || !keyCtrNull {
			key = Key{Kind: KeySeq, ElemID: types.OpId{Counter: uint64(keyCtr), ActorIdx: uint32(keyActor)}}
		} else {
			key = Key{Kind: KeySeq, ElemID: types.Head.OpId}
		}

		opActor, _, err := actorDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding op actor: %w", err)
		}
		opCounter, _, err := counterDec.Next()
		if err != nil {
			return nil, fmt.Errorf("decoding op counter: %w", err)
		}
		opId := types.OpId{Counter: uint64(opCounter), ActorIdx: uint32(opActor)}

		insert := false
		if !insertDec.Done() {
			insert, err = insertDec.Next()
			if err != nil {
				return nil, fmt.Errorf("decoding insert: %w", err)
			}
		}

		value, err := decodeValue(valueMetaDec, valueReader)
		if err != nil {
			return nil, fmt.Errorf("decoding value: %w", err)
		}

		var pred []types.OpId
		if !predGroupDec.Done() {
			predCount, _, err := predGroupDec.Next()
			if err != nil {
				return nil, fmt.Errorf("decoding pred group: %w", err)
			}
			if predCount > 0 {
				pred = make([]types.OpId, predCount)
				for j := range pred {
					pa, _, err := predActorDec.Next()
					if err != nil {
						return nil, fmt.Errorf("decoding pred actor: %w", err)
					}
					pc, _, err := predCtrDec.Next()
					if err != nil {
						return nil, fmt.Errorf("decoding pred counter: %w", err)
					}
					pred[j] = types.OpId{Counter: uint64(pc), ActorIdx: uint32(pa)}
				}
			}
		}

		ops = append(ops, Op{
			ID:     opId,
			Obj:    obj,
			Key:    key,
			Insert: insert,
			Action: Action(actionVal),
			Value:  value,
			Pred:   pred,
		})
	}

	return ops, nil
}

// decodeValue reads a value from the value metadata and value data columns.
func decodeValue(metaDec *encoding.RLEDecoder[uint64], valueReader *encoding.Reader) (types.ScalarValue, error) {
	metaVal, metaNull, err := metaDec.Next()
	if err != nil {
		return types.NewNull(), fmt.Errorf("reading value meta: %w", err)
	}
	if metaNull {
		return types.NewNull(), nil
	}

	valType := metaVal & 0x0f
	valLen := metaVal >> 4

	switch valType {
	case 0: // null
		return types.NewNull(), nil
	case 1: // false
		return types.NewBool(false), nil
	case 2: // true
		return types.NewBool(true), nil
	case 3: // uint
		if valLen == 0 {
			return types.NewUint(0), nil
		}
		v, err := valueReader.ReadULEB128()
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading uint value: %w", err)
		}
		return types.NewUint(v), nil
	case 4: // int
		if valLen == 0 {
			return types.NewInt(0), nil
		}
		v, err := valueReader.ReadSLEB128()
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading int value: %w", err)
		}
		return types.NewInt(v), nil
	case 5: // float64
		v, err := valueReader.ReadFloat64()
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading float64 value: %w", err)
		}
		return types.NewFloat64(v), nil
	case 6: // string
		if valLen == 0 {
			return types.NewStr(""), nil
		}
		b, err := valueReader.ReadBytes(int(valLen))
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading string value: %w", err)
		}
		return types.NewStr(string(b)), nil
	case 7: // bytes
		if valLen == 0 {
			return types.NewBytes(nil), nil
		}
		b, err := valueReader.ReadBytes(int(valLen))
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading bytes value: %w", err)
		}
		return types.NewBytes(b), nil
	case 8: // counter
		if valLen == 0 {
			return types.NewCounter(0), nil
		}
		v, err := valueReader.ReadSLEB128()
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading counter value: %w", err)
		}
		return types.NewCounter(v), nil
	case 9: // timestamp
		if valLen == 0 {
			return types.NewTimestamp(0), nil
		}
		v, err := valueReader.ReadSLEB128()
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading timestamp value: %w", err)
		}
		return types.NewTimestamp(v), nil
	default:
		// Unknown type — preserve for forward compatibility
		if valLen == 0 {
			return types.NewUnknownScalar(uint8(valType), nil), nil
		}
		b, err := valueReader.ReadBytes(int(valLen))
		if err != nil {
			return types.NewNull(), fmt.Errorf("reading unknown value type %d: %w", valType, err)
		}
		return types.NewUnknownScalar(uint8(valType), b), nil
	}
}

// encodeValueMeta returns the value metadata uint for a scalar value.
func encodeValueMeta(v types.ScalarValue) uint64 {
	switch v.Type() {
	case types.ScalarTypeNull:
		return 0
	case types.ScalarTypeFalse:
		return 1
	case types.ScalarTypeTrue:
		return 2
	case types.ScalarTypeUint:
		size := encoding.ULEBSize(v.Uint())
		return uint64(size)<<4 | 3
	case types.ScalarTypeInt:
		size := encoding.SLEBSize(v.Int())
		return uint64(size)<<4 | 4
	case types.ScalarTypeFloat64:
		return 8<<4 | 5 // always 8 bytes
	case types.ScalarTypeString:
		return uint64(len(v.Str()))<<4 | 6
	case types.ScalarTypeBytes:
		return uint64(len(v.Bytes()))<<4 | 7
	case types.ScalarTypeCounter:
		size := encoding.SLEBSize(v.Counter())
		return uint64(size)<<4 | 8
	case types.ScalarTypeTimestamp:
		size := encoding.SLEBSize(v.Timestamp())
		return uint64(size)<<4 | 9
	default:
		return 0
	}
}

// appendValueData appends the raw value data for a scalar value.
func appendValueData(dst []byte, v types.ScalarValue) []byte {
	switch v.Type() {
	case types.ScalarTypeNull, types.ScalarTypeFalse, types.ScalarTypeTrue:
		return dst
	case types.ScalarTypeUint:
		return encoding.AppendULEB128(dst, v.Uint())
	case types.ScalarTypeInt:
		return encoding.AppendSLEB128(dst, v.Int())
	case types.ScalarTypeFloat64:
		return encoding.AppendFloat64(dst, v.Float64())
	case types.ScalarTypeString:
		return append(dst, v.Str()...)
	case types.ScalarTypeBytes:
		return append(dst, v.Bytes()...)
	case types.ScalarTypeCounter:
		return encoding.AppendSLEB128(dst, v.Counter())
	case types.ScalarTypeTimestamp:
		return encoding.AppendSLEB128(dst, v.Timestamp())
	default:
		return append(dst, v.Bytes()...)
	}
}

