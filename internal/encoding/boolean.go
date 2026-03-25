package encoding

// BooleanEncoder encodes booleans as alternating run lengths starting with false.
// Counts are encoded as uLEB128 unsigned integers.
type BooleanEncoder struct {
	buf   []byte
	last  bool
	count int
}

// NewBooleanEncoder creates a new boolean encoder.
func NewBooleanEncoder() *BooleanEncoder {
	return &BooleanEncoder{last: false}
}

// Append appends a boolean value.
func (e *BooleanEncoder) Append(value bool) {
	if value == e.last {
		e.count++
	} else {
		e.buf = AppendULEB128(e.buf, uint64(e.count))
		e.last = value
		e.count = 1
	}
}

// Finish flushes the encoder and returns the encoded bytes.
func (e *BooleanEncoder) Finish() []byte {
	if e.count > 0 {
		e.buf = AppendULEB128(e.buf, uint64(e.count))
	}
	return e.buf
}

// MaybeBooleanEncoder is like BooleanEncoder but returns empty output if all values are false.
type MaybeBooleanEncoder struct {
	enc      *BooleanEncoder
	allFalse bool
}

// NewMaybeBooleanEncoder creates a new maybe-boolean encoder.
func NewMaybeBooleanEncoder() *MaybeBooleanEncoder {
	return &MaybeBooleanEncoder{
		enc:      NewBooleanEncoder(),
		allFalse: true,
	}
}

// Append appends a boolean value.
func (e *MaybeBooleanEncoder) Append(value bool) {
	if value {
		e.allFalse = false
	}
	e.enc.Append(value)
}

// Finish flushes the encoder and returns the encoded bytes.
// Returns nil if all values were false.
func (e *MaybeBooleanEncoder) Finish() []byte {
	if e.allFalse {
		return nil
	}
	return e.enc.Finish()
}

// BooleanDecoder decodes alternating run-length encoded booleans.
type BooleanDecoder struct {
	data      []byte
	offset    int
	lastValue bool
	count     int
}

// NewBooleanDecoder creates a new boolean decoder.
func NewBooleanDecoder(data []byte) *BooleanDecoder {
	return &BooleanDecoder{
		data:      data,
		lastValue: true, // starts at true so first toggle gives false
	}
}

// Done returns true when the decoder has consumed all data and has no remaining count.
func (d *BooleanDecoder) Done() bool {
	return d.offset >= len(d.data) && d.count == 0
}

// Next returns the next boolean value. Returns false when exhausted.
func (d *BooleanDecoder) Next() (bool, error) {
	for d.count == 0 {
		if d.offset >= len(d.data) {
			return false, nil // exhausted = implicit false
		}
		count, newOff, err := ReadULEB128(d.data, d.offset)
		if err != nil {
			return false, err
		}
		d.offset = newOff
		d.count = int(count)
		d.lastValue = !d.lastValue
	}
	d.count--
	return d.lastValue, nil
}
