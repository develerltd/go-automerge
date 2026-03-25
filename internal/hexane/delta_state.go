package hexane

// DeltaEncState is the encoder state machine for delta cursors.
// It wraps an RleState[int64] and tracks the absolute value to compute deltas.
type DeltaEncState struct {
	Abs int64
	Rle *RleState[int64]
}

// NewDeltaEncState creates a new delta encoder state.
func NewDeltaEncState() *DeltaEncState {
	return &DeltaEncState{
		Rle: NewRleState[int64](func(a, b int64) bool { return a == b }),
	}
}

// IsEmpty implements EncoderState[int64].
func (s *DeltaEncState) IsEmpty() bool {
	return s.Rle.IsEmpty()
}

// Append implements EncoderState[int64].
// Converts absolute value to delta before passing to RLE state.
func (s *DeltaEncState) Append(sw *SlabWriter[int64], value *int64) int {
	if value == nil {
		return s.AppendChunk(sw, Run[int64]{Count: 1, Value: nil})
	}
	delta := *value - s.Abs
	return s.AppendChunk(sw, Run[int64]{Count: 1, Value: &delta})
}

// AppendChunk implements EncoderState[int64].
// For delta encoding, the run values are already deltas.
func (s *DeltaEncState) AppendChunk(sw *SlabWriter[int64], run Run[int64]) int {
	s.Abs += RunDelta(&run)
	return s.Rle.AppendChunk(sw, run)
}

// Flush implements EncoderState[int64].
func (s *DeltaEncState) Flush(sw *SlabWriter[int64]) {
	s.Rle.Flush(sw)
}
