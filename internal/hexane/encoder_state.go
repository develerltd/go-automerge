package hexane

// EncoderState is the state machine interface for encoding column values.
// Each cursor type provides its own implementation:
//   - *RleState[T] for RLE cursors (uint64, int64, string, []byte)
//   - *BoolState for boolean cursors
//   - *DeltaEncState for delta cursors
//   - *RawEncState for raw byte cursors
type EncoderState[T any] interface {
	IsEmpty() bool
	Append(sw *SlabWriter[T], value *T) int
	AppendChunk(sw *SlabWriter[T], run Run[T]) int
	Flush(sw *SlabWriter[T])
}
