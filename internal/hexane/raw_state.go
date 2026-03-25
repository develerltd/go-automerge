package hexane

// RawEncState is a no-op encoder state for raw byte cursors.
// Raw data is written directly without RLE encoding.
type RawEncState struct{}

// IsEmpty implements EncoderState[[]byte].
func (s *RawEncState) IsEmpty() bool { return true }

// Append implements EncoderState[[]byte].
func (s *RawEncState) Append(sw *SlabWriter[[]byte], value *[]byte) int {
	if value != nil {
		sw.FlushBytes(*value)
		return len(*value)
	}
	return 0
}

// AppendChunk implements EncoderState[[]byte].
func (s *RawEncState) AppendChunk(sw *SlabWriter[[]byte], run Run[[]byte]) int {
	total := 0
	for i := 0; i < run.Count; i++ {
		if run.Value != nil {
			sw.FlushBytes(*run.Value)
			total += len(*run.Value)
		}
	}
	return total
}

// Flush implements EncoderState[[]byte].
func (s *RawEncState) Flush(_ *SlabWriter[[]byte]) {}
