package encoding

import (
	"encoding/binary"
	"math"
)

// Reader is a byte slice reader with an offset cursor for efficient sequential reads.
type Reader struct {
	Data   []byte
	Offset int
}

// NewReader creates a new reader over the given data.
func NewReader(data []byte) *Reader {
	return &Reader{Data: data}
}

// Done returns true when all data has been consumed.
func (r *Reader) Done() bool {
	return r.Offset >= len(r.Data)
}

// Remaining returns the number of bytes remaining.
func (r *Reader) Remaining() int {
	return len(r.Data) - r.Offset
}

// ReadByte reads a single byte.
func (r *Reader) ReadByte() (byte, error) {
	if r.Offset >= len(r.Data) {
		return 0, ErrUnexpectedEOF
	}
	b := r.Data[r.Offset]
	r.Offset++
	return b, nil
}

// ReadBytes reads n bytes and returns a slice (not a copy).
func (r *Reader) ReadBytes(n int) ([]byte, error) {
	end := r.Offset + n
	if end > len(r.Data) {
		return nil, ErrUnexpectedEOF
	}
	result := r.Data[r.Offset:end]
	r.Offset = end
	return result, nil
}

// ReadULEB128 reads an unsigned LEB128 value.
func (r *Reader) ReadULEB128() (uint64, error) {
	val, newOff, err := ReadULEB128(r.Data, r.Offset)
	if err != nil {
		return 0, err
	}
	r.Offset = newOff
	return val, nil
}

// ReadSLEB128 reads a signed LEB128 value.
func (r *Reader) ReadSLEB128() (int64, error) {
	val, newOff, err := ReadSLEB128(r.Data, r.Offset)
	if err != nil {
		return 0, err
	}
	r.Offset = newOff
	return val, nil
}

// ReadFloat64 reads a little-endian IEEE 754 float64.
func (r *Reader) ReadFloat64() (float64, error) {
	if r.Offset+8 > len(r.Data) {
		return 0, ErrUnexpectedEOF
	}
	bits := binary.LittleEndian.Uint64(r.Data[r.Offset : r.Offset+8])
	r.Offset += 8
	return math.Float64frombits(bits), nil
}

// ReadString reads a length-prefixed UTF-8 string.
func (r *Reader) ReadString() (string, error) {
	val, newOff, err := ReadString(r.Data, r.Offset)
	if err != nil {
		return "", err
	}
	r.Offset = newOff
	return val, nil
}

// ReadLenPrefixedBytes reads length-prefixed bytes (uLEB128 length + raw bytes).
func (r *Reader) ReadLenPrefixedBytes() ([]byte, error) {
	length, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	return r.ReadBytes(int(length))
}

// AppendFloat64 appends a little-endian IEEE 754 float64 to dst.
func AppendFloat64(dst []byte, v float64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
	return append(dst, buf[:]...)
}
