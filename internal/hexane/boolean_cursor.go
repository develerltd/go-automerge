package hexane

import (
	"github.com/develerltd/go-automerge/internal/encoding"
)

// boolCursorState tracks the decode state for a boolean cursor within a slab.
type boolCursorState struct {
	value      bool
	index      int
	offset     int
	acc        Acc
	lastOffset int
}

// boolTryNext decodes the next boolean run from slab data.
// Boolean encoding uses alternating run lengths starting with false.
func boolTryNext(state *boolCursorState, data []byte) (*Run[bool], error) {
	if state.offset >= len(data) {
		return nil, nil
	}

	count, off, err := encoding.ReadULEB128(data, state.offset)
	if err != nil {
		return nil, err
	}

	value := state.value
	state.value = !value
	state.lastOffset = state.offset
	state.offset = off
	state.index += int(count)

	if value {
		state.acc.AddAssign(AccFrom(count))
	}

	return &Run[bool]{Count: int(count), Value: &value}, nil
}

// boolNext decodes the next non-empty boolean run.
func boolNext(state *boolCursorState, data []byte) (*Run[bool], error) {
	for {
		run, err := boolTryNext(state, data)
		if err != nil || run == nil {
			return nil, err
		}
		if run.Count > 0 {
			return run, nil
		}
	}
}

// boolLoad parses raw boolean-encoded bytes into slabs.
func boolLoad(data []byte, slabSize int) ([]Slab, int, error) {
	var state boolCursorState
	var lastState boolCursorState
	var lastCopy boolCursorState // tracks the cursor state at the last copy/slab-break point
	writer := NewSlabWriter[bool](BoolPacker{}, slabSize, true)

	for {
		run, err := boolTryNext(&state, data)
		if err != nil {
			return nil, 0, err
		}
		if run == nil {
			break
		}

		if state.offset-lastCopy.offset >= slabSize {
			// For boolean, break on false boundaries to keep alternating pattern clean
			if !state.value { // just read a true run, so current state.value is now false
				boolCopyRange(data, writer, lastCopy.offset, state.offset, lastCopy.index, state.index, lastCopy.acc, state.acc)
				writer.ManualSlabBreak()
				lastCopy = state
			} else {
				boolCopyRange(data, writer, lastCopy.offset, lastState.offset, lastCopy.index, lastState.index, lastCopy.acc, lastState.acc)
				writer.ManualSlabBreak()
				lastCopy = lastState
			}
		}
		lastState = state
	}

	// Copy remaining
	boolCopyRange(data, writer, lastCopy.offset, state.offset, lastCopy.index, state.index, lastCopy.acc, state.acc)

	slabs := writer.Finish()
	return slabs, state.index, nil
}

func boolCopyRange(data []byte, writer *SlabWriter[bool], from, to, startIdx, endIdx int, startAcc, endAcc Acc) {
	if from >= to {
		return
	}
	writer.Copy(data, from, to, 0, endIdx-startIdx, endAcc.Sub(startAcc), nil)
}

// boolSplice performs a splice within a boolean-encoded slab.
func boolSplice(slab *Slab, index, del int, values []bool, slabSize int) SpliceResult {
	// Decode all values
	decoded := boolDecodeAll(slab)

	overflow := 0
	actualDel := del
	if index+del > len(decoded) {
		overflow = index + del - len(decoded)
		actualDel = len(decoded) - index
	}

	// Compute deleted acc
	var delAcc Acc
	for i := index; i < index+actualDel; i++ {
		if decoded[i] {
			delAcc.AddAssign(AccFrom(1))
		}
	}

	// Apply splice
	newLen := len(decoded) - actualDel + len(values)
	result := make([]bool, 0, newLen)
	result = append(result, decoded[:index]...)
	result = append(result, values...)
	result = append(result, decoded[index+actualDel:]...)

	// Re-encode
	state := &BoolState{}
	writer := NewSlabWriter[bool](BoolPacker{}, slabSize, false)
	for i := range result {
		state.AppendChunk(writer, Run[bool]{Count: 1, Value: &result[i]})
	}
	state.Flush(writer)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}

	return SpliceResult{
		Add:      len(values),
		Del:      actualDel,
		Overflow: overflow,
		Group:    delAcc,
		Slabs:    slabs,
	}
}

// boolDecodeAll decodes all boolean values from a slab.
func boolDecodeAll(slab *Slab) []bool {
	var state boolCursorState
	data := slab.Bytes()
	result := make([]bool, 0, slab.Len())
	for {
		run, err := boolNext(&state, data)
		if err != nil || run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			if run.Value != nil {
				result = append(result, *run.Value)
			} else {
				result = append(result, false)
			}
		}
	}
	return result
}

// boolEncode encodes boolean values to bytes.
func boolEncode(values []bool) []byte {
	if len(values) == 0 {
		return nil
	}
	state := &BoolState{}
	writer := NewSlabWriter[bool](BoolPacker{}, 1<<30, true)
	for i := range values {
		state.AppendChunk(writer, Run[bool]{Count: 1, Value: &values[i]})
	}
	state.Flush(writer)
	return writer.Write(nil)
}

// boolInitEmpty creates an empty boolean slab of given length (all false).
func boolInitEmpty(length int) Slab {
	if length == 0 {
		return Slab{}
	}
	writer := NewSlabWriter[bool](BoolPacker{}, 1<<30, false)
	writer.FlushBoolRun(length, false)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		return Slab{}
	}
	return slabs[0]
}
