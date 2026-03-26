package hexane

import "bytes"

// CursorOps is a function table that bundles all cursor-type-specific behavior.
//
// Each cursor type (RLE, Boolean, Delta, Raw) provides a CursorOps[T] constructor.
// ColumnData[T] stores a CursorOps[T] and delegates all encoding/decoding operations to it.
type CursorOps[T any] struct {
	// Load parses raw encoded bytes into slabs and returns the total item count.
	Load func(data []byte) ([]Slab, int, error)

	// Splice modifies a slab at the given index: removes del items and inserts values.
	Splice func(slab *Slab, index, del int, values []T) SpliceResult

	// InitEmpty creates a slab representing 'length' null/empty items.
	InitEmpty func(length int) Slab

	// ComputeMinMax sets the min/max metadata on each slab.
	ComputeMinMax func(slabs []Slab)

	// Encode encodes values to raw bytes (single contiguous encoding).
	Encode func(values []T, out []byte) []byte

	// NewEncoder creates a streaming encoder for this cursor type.
	NewEncoder func(singleSlab bool) *Encoder[T]

	// DecodeAll decodes all values from a slab.
	// Returns *T pointers (nil for null values).
	DecodeAll func(slab *Slab) []*T

	// DecodeFirst decodes only the first value from a slab.
	// Returns nil if the slab is empty or the first value is null.
	// Much cheaper than DecodeAll when only the first value is needed.
	DecodeFirst func(slab *Slab) *T

	// DecodeAt decodes the value at a specific offset within a slab.
	// Walks runs until the target offset without allocating decode buffers.
	// Returns nil if the offset is out of range or the value is null.
	DecodeAt func(slab *Slab, offset int) *T

	// IsAllNull checks if a slab contains only null values.
	// Returns false if not implemented or if the slab has non-null values.
	IsAllNull func(slab *Slab) bool

	// EqualValue compares two value pointers for equality.
	// Both may be nil (null values). Two nils are equal.
	EqualValue func(a, b *T) bool

	// CompareValue compares two value pointers for ordering.
	// Returns -1 if a < b, 0 if a == b, 1 if a > b.
	// nil (null) is less than any non-nil value. Two nils are equal.
	// May be nil for types that don't support ordering (e.g. strings, bytes).
	CompareValue func(a, b *T) int

	// SlabSize is the target byte size for slab splitting.
	SlabSize int

	// Packer for the item type.
	Packer Packer[T]
}

// Standard slab sizes matching the Rust hexane implementation.
const (
	RleSlabSize     = 64   // for uint64, int64 RLE cursors
	StrSlabSize     = 128  // for string, []byte RLE cursors
	BoolSlabSize    = 64   // for boolean cursors
	DeltaSlabSize   = 64   // for delta cursors
	RawSlabSize     = 4096 // for raw byte cursors
)

// UIntCursorOps returns CursorOps for unsigned 64-bit integer columns (RLE encoded).
func UIntCursorOps() CursorOps[uint64] {
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	return CursorOps[uint64]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rleLoad(data, RleSlabSize, packer)
		},
		Splice: func(slab *Slab, index, del int, values []uint64) SpliceResult {
			return rleSplice(slab, index, del, values, RleSlabSize, packer, eq)
		},
		InitEmpty: func(length int) Slab {
			return rleInitEmpty[uint64](length, packer)
		},
		ComputeMinMax: func(slabs []Slab) {
			rleComputeMinMax(slabs, packer)
		},
		Encode: func(values []uint64, out []byte) []byte {
			encoded := rleEncode(values, packer, eq)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[uint64] {
			return NewEncoder[uint64](NewRleState[uint64](eq), packer, RleSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*uint64 {
			return rleDecodeAll(slab, packer)
		},
		DecodeFirst: func(slab *Slab) *uint64 {
			return rleDecodeFirst(slab, packer)
		},
		DecodeAt: func(slab *Slab, offset int) *uint64 {
			return rleDecodeAt(slab, offset, packer)
		},
		IsAllNull: rleIsAllNull,
		EqualValue: func(a, b *uint64) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		CompareValue: func(a, b *uint64) int {
			if a == nil && b == nil {
				return 0
			}
			if a == nil {
				return -1
			}
			if b == nil {
				return 1
			}
			if *a < *b {
				return -1
			}
			if *a > *b {
				return 1
			}
			return 0
		},
		SlabSize: RleSlabSize,
		Packer:   packer,
	}
}

// IntCursorOps returns CursorOps for signed 64-bit integer columns (RLE encoded).
func IntCursorOps() CursorOps[int64] {
	packer := Int64Packer{}
	eq := func(a, b int64) bool { return a == b }
	return CursorOps[int64]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rleLoad(data, RleSlabSize, packer)
		},
		Splice: func(slab *Slab, index, del int, values []int64) SpliceResult {
			return rleSplice(slab, index, del, values, RleSlabSize, packer, eq)
		},
		InitEmpty: func(length int) Slab {
			return rleInitEmpty[int64](length, packer)
		},
		ComputeMinMax: func(slabs []Slab) {
			rleComputeMinMax(slabs, packer)
		},
		Encode: func(values []int64, out []byte) []byte {
			encoded := rleEncode(values, packer, eq)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[int64] {
			return NewEncoder[int64](NewRleState[int64](eq), packer, RleSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*int64 {
			return rleDecodeAll(slab, packer)
		},
		DecodeFirst: func(slab *Slab) *int64 {
			return rleDecodeFirst(slab, packer)
		},
		DecodeAt: func(slab *Slab, offset int) *int64 {
			return rleDecodeAt(slab, offset, packer)
		},
		IsAllNull: rleIsAllNull,
		EqualValue: func(a, b *int64) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		CompareValue: func(a, b *int64) int {
			if a == nil && b == nil {
				return 0
			}
			if a == nil {
				return -1
			}
			if b == nil {
				return 1
			}
			if *a < *b {
				return -1
			}
			if *a > *b {
				return 1
			}
			return 0
		},
		SlabSize: RleSlabSize,
		Packer:   packer,
	}
}

// DeltaCursorOps returns CursorOps for delta-encoded signed 64-bit integer columns.
func DeltaCursorOps() CursorOps[int64] {
	packer := Int64Packer{}
	return CursorOps[int64]{
		Load: func(data []byte) ([]Slab, int, error) {
			return deltaLoad(data, DeltaSlabSize)
		},
		Splice: func(slab *Slab, index, del int, values []int64) SpliceResult {
			return deltaSplice(slab, index, del, values, DeltaSlabSize)
		},
		InitEmpty: func(length int) Slab {
			return deltaInitEmpty(length)
		},
		ComputeMinMax: func(slabs []Slab) {
			deltaComputeMinMax(slabs)
		},
		Encode: func(values []int64, out []byte) []byte {
			encoded := deltaEncode(values)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[int64] {
			return NewEncoder[int64](NewDeltaEncState(), packer, DeltaSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*int64 {
			return deltaDecodeAllAbsolute(slab)
		},
		DecodeFirst: func(slab *Slab) *int64 {
			return deltaDecodeFirst(slab)
		},
		DecodeAt: func(slab *Slab, offset int) *int64 {
			return deltaDecodeAt(slab, offset)
		},
		EqualValue: func(a, b *int64) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		CompareValue: func(a, b *int64) int {
			if a == nil && b == nil {
				return 0
			}
			if a == nil {
				return -1
			}
			if b == nil {
				return 1
			}
			if *a < *b {
				return -1
			}
			if *a > *b {
				return 1
			}
			return 0
		},
		SlabSize: DeltaSlabSize,
		Packer:   packer,
	}
}

// BoolCursorOps returns CursorOps for boolean columns (alternating run-length encoded).
func BoolCursorOps() CursorOps[bool] {
	packer := BoolPacker{}
	return CursorOps[bool]{
		Load: func(data []byte) ([]Slab, int, error) {
			return boolLoad(data, BoolSlabSize)
		},
		Splice: func(slab *Slab, index, del int, values []bool) SpliceResult {
			return boolSplice(slab, index, del, values, BoolSlabSize)
		},
		InitEmpty: func(length int) Slab {
			return boolInitEmpty(length)
		},
		ComputeMinMax: func(_ []Slab) {
			// Boolean cursors don't track min/max
		},
		Encode: func(values []bool, out []byte) []byte {
			encoded := boolEncode(values)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[bool] {
			return NewEncoder[bool](&BoolState{}, packer, BoolSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*bool {
			decoded := boolDecodeAll(slab)
			result := make([]*bool, len(decoded))
			for i := range decoded {
				v := decoded[i]
				result[i] = &v
			}
			return result
		},
		DecodeFirst: func(slab *Slab) *bool {
			return boolDecodeAt(slab, 0)
		},
		DecodeAt: func(slab *Slab, offset int) *bool {
			return boolDecodeAt(slab, offset)
		},
		EqualValue: func(a, b *bool) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		SlabSize: BoolSlabSize,
		Packer:   packer,
	}
}

// StrCursorOps returns CursorOps for string columns (RLE encoded).
func StrCursorOps() CursorOps[string] {
	packer := StrPacker{}
	eq := func(a, b string) bool { return a == b }
	return CursorOps[string]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rleLoad(data, StrSlabSize, packer)
		},
		Splice: func(slab *Slab, index, del int, values []string) SpliceResult {
			return rleSplice(slab, index, del, values, StrSlabSize, packer, eq)
		},
		InitEmpty: func(length int) Slab {
			return rleInitEmpty[string](length, packer)
		},
		ComputeMinMax: func(slabs []Slab) {
			rleComputeMinMax(slabs, packer)
		},
		Encode: func(values []string, out []byte) []byte {
			encoded := rleEncode(values, packer, eq)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[string] {
			return NewEncoder[string](NewRleState[string](eq), packer, StrSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*string {
			return rleDecodeAll(slab, packer)
		},
		DecodeFirst: func(slab *Slab) *string {
			return rleDecodeFirst(slab, packer)
		},
		DecodeAt: func(slab *Slab, offset int) *string {
			return rleDecodeAt(slab, offset, packer)
		},
		IsAllNull: rleIsAllNull,
		EqualValue: func(a, b *string) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		SlabSize: StrSlabSize,
		Packer:   packer,
	}
}

// BytesCursorOps returns CursorOps for byte slice columns (RLE encoded).
func BytesCursorOps() CursorOps[[]byte] {
	packer := BytesPacker{}
	eq := func(a, b []byte) bool { return bytes.Equal(a, b) }
	return CursorOps[[]byte]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rleLoad(data, StrSlabSize, packer)
		},
		Splice: func(slab *Slab, index, del int, values [][]byte) SpliceResult {
			return rleSplice(slab, index, del, values, StrSlabSize, packer, eq)
		},
		InitEmpty: func(length int) Slab {
			return rleInitEmpty[[]byte](length, packer)
		},
		ComputeMinMax: func(slabs []Slab) {
			rleComputeMinMax(slabs, packer)
		},
		Encode: func(values [][]byte, out []byte) []byte {
			encoded := rleEncode(values, packer, eq)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[[]byte] {
			return NewEncoder[[]byte](NewRleState[[]byte](eq), packer, StrSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*[]byte {
			return rleDecodeAll(slab, packer)
		},
		EqualValue: func(a, b *[]byte) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return bytes.Equal(*a, *b)
		},
		SlabSize: StrSlabSize,
		Packer:   packer,
	}
}

// ValueMetaCursorOps returns CursorOps for value metadata columns (RLE encoded).
// Same as UIntCursorOps but ItemAgg extracts byte length (val >> 4), so the
// accumulator tracks total raw value bytes rather than summing raw metadata values.
func ValueMetaCursorOps() CursorOps[uint64] {
	packer := ValueMetaPacker{}
	eq := func(a, b uint64) bool { return a == b }
	return CursorOps[uint64]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rleLoad(data, RleSlabSize, packer)
		},
		Splice: func(slab *Slab, index, del int, values []uint64) SpliceResult {
			return rleSplice(slab, index, del, values, RleSlabSize, packer, eq)
		},
		InitEmpty: func(length int) Slab {
			return rleInitEmpty[uint64](length, packer)
		},
		ComputeMinMax: func(slabs []Slab) {
			rleComputeMinMax(slabs, packer)
		},
		Encode: func(values []uint64, out []byte) []byte {
			encoded := rleEncode(values, packer, eq)
			return append(out, encoded...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[uint64] {
			return NewEncoder[uint64](NewRleState[uint64](eq), packer, RleSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*uint64 {
			return rleDecodeAll(slab, packer)
		},
		EqualValue: func(a, b *uint64) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return *a == *b
		},
		SlabSize: RleSlabSize,
		Packer:   packer,
	}
}

// RawCursorOps returns CursorOps for raw byte columns (no compression).
func RawCursorOps() CursorOps[[]byte] {
	packer := BytesPacker{}
	return CursorOps[[]byte]{
		Load: func(data []byte) ([]Slab, int, error) {
			return rawLoad(data)
		},
		Splice: func(slab *Slab, index, del int, values [][]byte) SpliceResult {
			return rawSplice(slab, index, del, values, RawSlabSize)
		},
		InitEmpty: func(_ int) Slab {
			return rawInitEmpty(0)
		},
		ComputeMinMax: func(_ []Slab) {},
		Encode: func(values [][]byte, out []byte) []byte {
			return append(out, rawEncode(values)...)
		},
		NewEncoder: func(singleSlab bool) *Encoder[[]byte] {
			return NewEncoder[[]byte](&RawEncState{}, packer, RawSlabSize, singleSlab)
		},
		DecodeAll: func(slab *Slab) []*[]byte {
			data := slab.Bytes()
			result := make([]*[]byte, len(data))
			for i := range data {
				b := []byte{data[i]}
				result[i] = &b
			}
			return result
		},
		DecodeAt: func(slab *Slab, offset int) *[]byte {
			data := slab.Bytes()
			if offset < 0 || offset >= len(data) {
				return nil
			}
			b := []byte{data[offset]}
			return &b
		},
		EqualValue: func(a, b *[]byte) bool {
			if a == nil && b == nil {
				return true
			}
			if a == nil || b == nil {
				return false
			}
			return bytes.Equal(*a, *b)
		},
		SlabSize: RawSlabSize,
		Packer:   packer,
	}
}
