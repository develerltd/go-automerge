package opset

import (
	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/types"
)

// EncodeDocOps encodes operations into document-format columnar data (with successors).
func EncodeDocOps(ops []Op) columnar.RawColumns {
	if len(ops) == 0 {
		return nil
	}

	objActorEnc := encoding.NewRLEEncoderUint64()
	objCtrEnc := encoding.NewRLEEncoderUint64()
	keyActorEnc := encoding.NewRLEEncoderUint64()
	keyCtrEnc := encoding.NewDeltaEncoder()
	keyStrEnc := encoding.NewRLEEncoderString()
	actorEnc := encoding.NewRLEEncoderUint64()
	counterEnc := encoding.NewDeltaEncoder()
	insertEnc := encoding.NewMaybeBooleanEncoder()
	actionEnc := encoding.NewRLEEncoderUint64()
	valueMetaEnc := encoding.NewRLEEncoderUint64()
	var valueData []byte
	succGroupEnc := encoding.NewRLEEncoderUint64()
	succActorEnc := encoding.NewRLEEncoderUint64()
	succCtrEnc := encoding.NewDeltaEncoder()

	for i := range ops {
		op := &ops[i]

		// Object
		if op.Obj.IsRoot() {
			objActorEnc.AppendNull()
			objCtrEnc.AppendNull()
		} else {
			objActorEnc.AppendValue(uint64(op.Obj.ActorIdx))
			objCtrEnc.AppendValue(op.Obj.Counter)
		}

		// Key
		if op.Key.Kind == KeyMap {
			keyActorEnc.AppendNull()
			keyCtrEnc.AppendNull()
			keyStrEnc.AppendValue(op.Key.MapKey)
		} else {
			if op.Key.ElemID.IsZero() {
				// HEAD
				keyActorEnc.AppendNull()
				keyCtrEnc.AppendNull()
			} else {
				keyActorEnc.AppendValue(uint64(op.Key.ElemID.ActorIdx))
				keyCtrEnc.AppendValue(int64(op.Key.ElemID.Counter))
			}
			keyStrEnc.AppendNull()
		}

		// Op ID
		actorEnc.AppendValue(uint64(op.ID.ActorIdx))
		counterEnc.AppendValue(int64(op.ID.Counter))

		// Insert
		insertEnc.Append(op.Insert)

		// Action
		actionEnc.AppendValue(uint64(op.Action))

		// Value
		valueMetaEnc.AppendValue(encodeValueMeta(op.Value))
		valueData = appendValueData(valueData, op.Value)

		// Successors
		succGroupEnc.AppendValue(uint64(len(op.Succ)))
		for _, s := range op.Succ {
			succActorEnc.AppendValue(uint64(s.ActorIdx))
			succCtrEnc.AppendValue(int64(s.Counter))
		}
	}

	return buildOpColumns(
		objActorEnc, objCtrEnc,
		keyActorEnc, keyCtrEnc, keyStrEnc,
		actorEnc, counterEnc,
		insertEnc,
		actionEnc,
		valueMetaEnc, valueData,
		succGroupEnc, succActorEnc, succCtrEnc,
		nil, nil, nil, // no pred columns in document format
	)
}

// EncodeChangeOps encodes operations into change-format columnar data (with predecessors).
func EncodeChangeOps(ops []Op) columnar.RawColumns {
	if len(ops) == 0 {
		return nil
	}

	objActorEnc := encoding.NewRLEEncoderUint64()
	objCtrEnc := encoding.NewRLEEncoderUint64()
	keyActorEnc := encoding.NewRLEEncoderUint64()
	keyCtrEnc := encoding.NewDeltaEncoder()
	keyStrEnc := encoding.NewRLEEncoderString()
	actorEnc := encoding.NewRLEEncoderUint64()
	counterEnc := encoding.NewDeltaEncoder()
	insertEnc := encoding.NewMaybeBooleanEncoder()
	actionEnc := encoding.NewRLEEncoderUint64()
	valueMetaEnc := encoding.NewRLEEncoderUint64()
	var valueData []byte
	predGroupEnc := encoding.NewRLEEncoderUint64()
	predActorEnc := encoding.NewRLEEncoderUint64()
	predCtrEnc := encoding.NewDeltaEncoder()

	for i := range ops {
		op := &ops[i]

		// Object
		if op.Obj.IsRoot() {
			objActorEnc.AppendNull()
			objCtrEnc.AppendNull()
		} else {
			objActorEnc.AppendValue(uint64(op.Obj.ActorIdx))
			objCtrEnc.AppendValue(op.Obj.Counter)
		}

		// Key
		if op.Key.Kind == KeyMap {
			keyActorEnc.AppendNull()
			keyCtrEnc.AppendNull()
			keyStrEnc.AppendValue(op.Key.MapKey)
		} else {
			if op.Key.ElemID.IsZero() {
				keyActorEnc.AppendNull()
				keyCtrEnc.AppendNull()
			} else {
				keyActorEnc.AppendValue(uint64(op.Key.ElemID.ActorIdx))
				keyCtrEnc.AppendValue(int64(op.Key.ElemID.Counter))
			}
			keyStrEnc.AppendNull()
		}

		// Op ID
		actorEnc.AppendValue(uint64(op.ID.ActorIdx))
		counterEnc.AppendValue(int64(op.ID.Counter))

		// Insert
		insertEnc.Append(op.Insert)

		// Action
		actionEnc.AppendValue(uint64(op.Action))

		// Value
		valueMetaEnc.AppendValue(encodeValueMeta(op.Value))
		valueData = appendValueData(valueData, op.Value)

		// Predecessors
		predGroupEnc.AppendValue(uint64(len(op.Pred)))
		for _, p := range op.Pred {
			predActorEnc.AppendValue(uint64(p.ActorIdx))
			predCtrEnc.AppendValue(int64(p.Counter))
		}
	}

	return buildOpColumns(
		objActorEnc, objCtrEnc,
		keyActorEnc, keyCtrEnc, keyStrEnc,
		actorEnc, counterEnc,
		insertEnc,
		actionEnc,
		valueMetaEnc, valueData,
		nil, nil, nil, // no succ columns in change format
		predGroupEnc, predActorEnc, predCtrEnc,
	)
}

func buildOpColumns(
	objActorEnc, objCtrEnc *encoding.RLEEncoder[uint64],
	keyActorEnc *encoding.RLEEncoder[uint64],
	keyCtrEnc *encoding.DeltaEncoder,
	keyStrEnc *encoding.RLEEncoder[string],
	actorEnc *encoding.RLEEncoder[uint64],
	counterEnc *encoding.DeltaEncoder,
	insertEnc *encoding.MaybeBooleanEncoder,
	actionEnc *encoding.RLEEncoder[uint64],
	valueMetaEnc *encoding.RLEEncoder[uint64],
	valueData []byte,
	succGroupEnc, succActorEnc *encoding.RLEEncoder[uint64],
	succCtrEnc *encoding.DeltaEncoder,
	predGroupEnc, predActorEnc *encoding.RLEEncoder[uint64],
	predCtrEnc *encoding.DeltaEncoder,
) columnar.RawColumns {
	var cols columnar.RawColumns

	addCol := func(spec columnar.ColumnSpec, data []byte) {
		if len(data) > 0 {
			cols = append(cols, columnar.RawColumn{Spec: spec, Data: data})
		}
	}

	addCol(columnar.OpColObjActor, objActorEnc.Finish())
	addCol(columnar.OpColObjCtr, objCtrEnc.Finish())
	addCol(columnar.OpColKeyActor, keyActorEnc.Finish())
	addCol(columnar.OpColKeyCtr, keyCtrEnc.Finish())
	addCol(columnar.OpColKeyStr, keyStrEnc.Finish())
	addCol(columnar.OpColActor, actorEnc.Finish())
	addCol(columnar.OpColCounter, counterEnc.Finish())
	addCol(columnar.OpColInsert, insertEnc.Finish())
	addCol(columnar.OpColAction, actionEnc.Finish())
	addCol(columnar.OpColValueMeta, valueMetaEnc.Finish())
	addCol(columnar.OpColValue, valueData)

	if predGroupEnc != nil {
		addCol(columnar.OpColPredGroup, predGroupEnc.Finish())
		addCol(columnar.OpColPredActor, predActorEnc.Finish())
		addCol(columnar.OpColPredCtr, predCtrEnc.Finish())
	}

	if succGroupEnc != nil {
		addCol(columnar.OpColSuccGroup, succGroupEnc.Finish())
		addCol(columnar.OpColSuccActor, succActorEnc.Finish())
		addCol(columnar.OpColSuccCtr, succCtrEnc.Finish())
	}

	return cols
}

// ChangeRecord holds metadata about a single change for encoding into document format.
type ChangeRecord struct {
	Hash      types.ChangeHash
	ActorIdx  uint32
	Seq       uint64
	MaxOp     uint64
	Time      int64
	Message   string
	DepHashes []types.ChangeHash
	Extra     []byte
}

// EncodeChangeCols encodes change records into columnar format for document chunks.
// hashToIdx maps change hashes to their index in the sorted changes list (for deps_index).
func EncodeChangeCols(changes []ChangeRecord, hashToIdx map[types.ChangeHash]int) columnar.RawColumns {
	if len(changes) == 0 {
		return nil
	}

	actorEnc := encoding.NewRLEEncoderUint64()
	seqEnc := encoding.NewDeltaEncoder()
	maxOpEnc := encoding.NewDeltaEncoder()
	timeEnc := encoding.NewDeltaEncoder()
	messageEnc := encoding.NewRLEEncoderString()
	depsGroupEnc := encoding.NewRLEEncoderUint64()
	depsIdxEnc := encoding.NewDeltaEncoder()
	extraMetaEnc := encoding.NewRLEEncoderUint64()
	// extra value data is empty for now

	for _, c := range changes {
		actorEnc.AppendValue(uint64(c.ActorIdx))
		seqEnc.AppendValue(int64(c.Seq))
		maxOpEnc.AppendValue(int64(c.MaxOp))
		timeEnc.AppendValue(c.Time)
		if c.Message == "" {
			messageEnc.AppendNull()
		} else {
			messageEnc.AppendValue(c.Message)
		}
		depsGroupEnc.AppendValue(uint64(len(c.DepHashes)))
		for _, dep := range c.DepHashes {
			if idx, ok := hashToIdx[dep]; ok {
				depsIdxEnc.AppendValue(int64(idx))
			}
		}
		extraMetaEnc.AppendNull()
	}

	var cols columnar.RawColumns
	addCol := func(spec columnar.ColumnSpec, data []byte) {
		if len(data) > 0 {
			cols = append(cols, columnar.RawColumn{Spec: spec, Data: data})
		}
	}

	addCol(columnar.ChgColActor, actorEnc.Finish())
	addCol(columnar.ChgColSeq, seqEnc.Finish())
	addCol(columnar.ChgColMaxOp, maxOpEnc.Finish())
	addCol(columnar.ChgColTime, timeEnc.Finish())
	addCol(columnar.ChgColMessage, messageEnc.Finish())
	addCol(columnar.ChgColDepsGroup, depsGroupEnc.Finish())
	addCol(columnar.ChgColDepsIdx, depsIdxEnc.Finish())
	addCol(columnar.ChgColExtraMeta, extraMetaEnc.Finish())

	return cols
}
