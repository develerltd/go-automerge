package hexane

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/encoding"
)

// PackError represents errors during column data encoding/decoding.
type PackError struct {
	msg string
}

func (e *PackError) Error() string { return e.msg }

// NewPackError creates a new PackError.
func NewPackError(msg string) *PackError {
	return &PackError{msg: msg}
}

// Packer defines how a type is encoded/decoded in hexane's columnar format.
//
// Implemented for the primitive types used by the built-in cursors:
// uint64, int64, bool, string, []byte.
type Packer[T any] interface {
	// Pack appends the encoded form of item to out and returns the extended slice.
	Pack(item T, out []byte) []byte

	// Unpack decodes a value from data, returning the value and bytes consumed.
	Unpack(data []byte) (T, int, error)

	// Width returns the number of bytes Pack will write for item.
	Width(item T) int

	// ItemAgg returns the per-item Agg for this value.
	ItemAgg(item T) Agg

	// Abs returns the absolute value for delta encoding. Only meaningful for int64.
	Abs(item T) int64

	// IsEmpty returns true if the value represents "nothing" (zero, false, empty string, etc.).
	IsEmpty(item T) bool
}

// UInt64Packer implements Packer[uint64] using unsigned LEB128.
type UInt64Packer struct{}

func (UInt64Packer) Pack(item uint64, out []byte) []byte {
	return encoding.AppendULEB128(out, item)
}

func (UInt64Packer) Unpack(data []byte) (uint64, int, error) {
	val, off, err := encoding.ReadULEB128(data, 0)
	if err != nil {
		return 0, 0, err
	}
	return val, off, nil
}

func (UInt64Packer) Width(item uint64) int  { return encoding.ULEBSize(item) }
func (UInt64Packer) ItemAgg(item uint64) Agg { return AggFromUint64(item) }
func (UInt64Packer) Abs(_ uint64) int64      { return 0 }
func (UInt64Packer) IsEmpty(item uint64) bool { return item == 0 }

// Int64Packer implements Packer[int64] using signed LEB128.
type Int64Packer struct{}

func (Int64Packer) Pack(item int64, out []byte) []byte {
	return encoding.AppendSLEB128(out, item)
}

func (Int64Packer) Unpack(data []byte) (int64, int, error) {
	val, off, err := encoding.ReadSLEB128(data, 0)
	if err != nil {
		return 0, 0, err
	}
	return val, off, nil
}

func (Int64Packer) Width(item int64) int  { return encoding.SLEBSize(item) }
func (Int64Packer) ItemAgg(item int64) Agg { return AggFromInt64(item) }
func (Int64Packer) Abs(item int64) int64   { return item }
func (Int64Packer) IsEmpty(item int64) bool { return item == 0 }

// BoolPacker implements Packer[bool].
// Note: bools are not individually packed in hexane — they use alternating run-length encoding.
// Pack/Unpack/Width panic; only ItemAgg and IsEmpty are meaningful.
type BoolPacker struct{}

func (BoolPacker) Pack(_ bool, _ []byte) []byte {
	panic("hexane: bool values use alternating run encoding, not individual packing")
}

func (BoolPacker) Unpack(_ []byte) (bool, int, error) {
	panic("hexane: bool values use alternating run encoding, not individual packing")
}

func (BoolPacker) Width(_ bool) int { panic("hexane: bool values have no individual width") }

func (BoolPacker) ItemAgg(item bool) Agg {
	if item {
		return AggFrom(1)
	}
	return Agg{}
}

func (BoolPacker) Abs(_ bool) int64     { return 0 }
func (BoolPacker) IsEmpty(item bool) bool { return !item }

// StrPacker implements Packer[string] using length-prefixed encoding.
type StrPacker struct{}

func (StrPacker) Pack(item string, out []byte) []byte {
	out = encoding.AppendULEB128(out, uint64(len(item)))
	return append(out, item...)
}

func (StrPacker) Unpack(data []byte) (string, int, error) {
	length, off, err := encoding.ReadULEB128(data, 0)
	if err != nil {
		return "", 0, err
	}
	end := off + int(length)
	if end > len(data) {
		return "", 0, fmt.Errorf("string data truncated: need %d bytes at offset %d, have %d", length, off, len(data)-off)
	}
	return string(data[off:end]), end, nil
}

func (StrPacker) Width(item string) int {
	return encoding.ULEBSize(uint64(len(item))) + len(item)
}

func (StrPacker) ItemAgg(_ string) Agg  { return Agg{} }
func (StrPacker) Abs(_ string) int64     { return 0 }
func (StrPacker) IsEmpty(item string) bool { return item == "" }

// BytesPacker implements Packer[[]byte] using length-prefixed encoding.
type BytesPacker struct{}

func (BytesPacker) Pack(item []byte, out []byte) []byte {
	out = encoding.AppendULEB128(out, uint64(len(item)))
	return append(out, item...)
}

func (BytesPacker) Unpack(data []byte) ([]byte, int, error) {
	length, off, err := encoding.ReadULEB128(data, 0)
	if err != nil {
		return nil, 0, err
	}
	end := off + int(length)
	if end > len(data) {
		return nil, 0, fmt.Errorf("bytes data truncated: need %d bytes at offset %d, have %d", length, off, len(data)-off)
	}
	result := make([]byte, length)
	copy(result, data[off:end])
	return result, end, nil
}

func (BytesPacker) Width(item []byte) int {
	return encoding.ULEBSize(uint64(len(item))) + len(item)
}

func (BytesPacker) ItemAgg(_ []byte) Agg   { return Agg{} }
func (BytesPacker) Abs(_ []byte) int64      { return 0 }
func (BytesPacker) IsEmpty(item []byte) bool { return len(item) == 0 }

// ValueMetaPacker implements Packer[uint64] for value metadata columns.
//
// Identical to UInt64Packer except ItemAgg returns val >> 4 (the byte length portion
// of the metadata), so the column accumulator tracks total raw value bytes.
type ValueMetaPacker struct{}

func (ValueMetaPacker) Pack(item uint64, out []byte) []byte {
	return encoding.AppendULEB128(out, item)
}

func (ValueMetaPacker) Unpack(data []byte) (uint64, int, error) {
	val, off, err := encoding.ReadULEB128(data, 0)
	if err != nil {
		return 0, 0, err
	}
	return val, off, nil
}

func (ValueMetaPacker) Width(item uint64) int   { return encoding.ULEBSize(item) }
func (ValueMetaPacker) ItemAgg(item uint64) Agg  { return AggFrom(uint32(item >> 4)) }
func (ValueMetaPacker) Abs(_ uint64) int64       { return 0 }
func (ValueMetaPacker) IsEmpty(item uint64) bool { return item == 0 }
