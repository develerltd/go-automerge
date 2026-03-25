package encoding

// DeltaEncoder encodes integers as deltas from the previous value, then RLE-compresses the deltas.
// The initial absolute value is 0.
type DeltaEncoder struct {
	rle           *RLEEncoder[int64]
	absoluteValue int64
}

// NewDeltaEncoder creates a new delta encoder.
func NewDeltaEncoder() *DeltaEncoder {
	return &DeltaEncoder{
		rle: NewRLEEncoderInt64(),
	}
}

// AppendValue appends an absolute value, encoding it as a delta from the previous value.
func (e *DeltaEncoder) AppendValue(value int64) {
	delta := value - e.absoluteValue
	e.rle.AppendValue(delta)
	e.absoluteValue = value
}

// AppendNull appends a null value.
func (e *DeltaEncoder) AppendNull() {
	e.rle.AppendNull()
}

// Append appends a value that may be null.
func (e *DeltaEncoder) Append(value int64, isNull bool) {
	if isNull {
		e.AppendNull()
	} else {
		e.AppendValue(value)
	}
}

// Finish flushes the encoder and returns the encoded bytes.
func (e *DeltaEncoder) Finish() []byte {
	return e.rle.Finish()
}

// DeltaDecoder decodes delta-then-RLE-encoded integers.
type DeltaDecoder struct {
	rle         *RLEDecoder[int64]
	absoluteVal int64
}

// NewDeltaDecoder creates a new delta decoder.
func NewDeltaDecoder(data []byte) *DeltaDecoder {
	return &DeltaDecoder{
		rle: NewRLEDecoderInt64(data),
	}
}

// Done returns true when the decoder has consumed all data.
func (d *DeltaDecoder) Done() bool {
	return d.rle.Done()
}

// Next returns the next absolute value. Returns (value, false, nil) for a non-null value,
// (0, true, nil) for null, and (0, false, err) on error.
func (d *DeltaDecoder) Next() (int64, bool, error) {
	delta, isNull, err := d.rle.Next()
	if err != nil {
		return 0, false, err
	}
	if isNull {
		return 0, true, nil
	}
	d.absoluteVal += delta
	return d.absoluteVal, false, nil
}
