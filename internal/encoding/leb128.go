package encoding

import (
	"errors"
	"fmt"
)

const continuationBit byte = 0x80

var (
	ErrUnexpectedEOF   = errors.New("unexpected end of data")
	ErrOverflow        = errors.New("LEB128 value overflows uint64/int64")
	ErrOverlongEncoding = errors.New("overlong LEB128 encoding")
)

// AppendULEB128 appends the unsigned LEB128 encoding of v to dst.
func AppendULEB128(dst []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= continuationBit
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}

// AppendSLEB128 appends the signed LEB128 encoding of v to dst.
func AppendSLEB128(dst []byte, v int64) []byte {
	for {
		b := byte(v & 0x7f)
		v >>= 6
		done := v == 0 || v == -1
		if done {
			b &= ^continuationBit
		} else {
			v >>= 1
			b |= continuationBit
		}
		dst = append(dst, b)
		if done {
			return dst
		}
	}
}

// ReadULEB128 reads an unsigned LEB128 value from data starting at offset.
// Returns the value and the new offset after reading.
func ReadULEB128(data []byte, offset int) (uint64, int, error) {
	var result uint64
	var shift uint
	start := offset
	for {
		if offset >= len(data) {
			return 0, start, ErrUnexpectedEOF
		}
		b := data[offset]
		offset++

		if shift >= 64 && b > 0 {
			return 0, start, ErrOverflow
		}

		result |= uint64(b&0x7f) << shift

		if b&continuationBit == 0 {
			// Check for overlong encoding: if more than 1 byte was used,
			// the last byte must be non-zero (i.e., must contribute bits).
			bytesRead := offset - start
			if bytesRead > 1 && b == 0 {
				return 0, start, fmt.Errorf("%w: trailing zero byte in unsigned LEB128", ErrOverlongEncoding)
			}
			// Also check that high bits don't exceed uint64 range
			if shift >= 63 && b > 1 {
				return 0, start, ErrOverflow
			}
			return result, offset, nil
		}
		shift += 7
	}
}

// ReadSLEB128 reads a signed LEB128 value from data starting at offset.
// Returns the value and the new offset after reading.
func ReadSLEB128(data []byte, offset int) (int64, int, error) {
	var result int64
	var shift uint
	start := offset
	for {
		if offset >= len(data) {
			return 0, start, ErrUnexpectedEOF
		}
		b := data[offset]
		offset++

		result |= int64(b&0x7f) << shift
		shift += 7

		if b&continuationBit == 0 {
			// Sign extend if the sign bit is set
			if shift < 64 && b&0x40 != 0 {
				result |= -(1 << shift)
			}
			return result, offset, nil
		}
	}
}

// ULEBSize returns the number of bytes needed to encode v as unsigned LEB128.
func ULEBSize(v uint64) int {
	if v == 0 {
		return 1
	}
	n := 0
	for v > 0 {
		v >>= 7
		n++
	}
	return n
}

// SLEBSize returns the number of bytes needed to encode v as signed LEB128.
func SLEBSize(v int64) int {
	n := 0
	for {
		b := byte(v & 0x7f)
		v >>= 7
		n++
		if (v == 0 && b&0x40 == 0) || (v == -1 && b&0x40 != 0) {
			return n
		}
	}
}
