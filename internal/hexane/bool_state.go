package hexane

// BoolState is the encoder state machine for boolean cursors.
// Boolean encoding uses alternating run lengths: false count, true count, false count, ...
type BoolState struct {
	Value   bool
	Count   int
	Flushed bool
}

// IsEmpty implements EncoderState[bool].
func (s *BoolState) IsEmpty() bool {
	return !s.Value || s.Count == 0
}

// Append implements EncoderState[bool].
func (s *BoolState) Append(sw *SlabWriter[bool], value *bool) int {
	v := false
	if value != nil {
		v = *value
	}
	return s.AppendChunk(sw, Run[bool]{Count: 1, Value: &v})
}

// AppendChunk implements EncoderState[bool].
func (s *BoolState) AppendChunk(sw *SlabWriter[bool], run Run[bool]) int {
	item := false
	if run.Value != nil {
		item = *run.Value
	}
	if s.Value == item {
		s.Count += run.Count
	} else {
		if s.Count > 0 || !s.Flushed {
			sw.FlushBoolRun(s.Count, s.Value)
			s.Flushed = true
		}
		s.Value = item
		s.Count = run.Count
	}
	return run.Count
}

// Flush implements EncoderState[bool].
func (s *BoolState) Flush(sw *SlabWriter[bool]) {
	sw.FlushBoolRun(s.Count, s.Value)
	s.Count = 0
	s.Value = false
	s.Flushed = true
}
