package columnar

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sort"

	"github.com/develerltd/go-automerge/internal/encoding"
)

// RawColumn represents a parsed column: its specification and the raw byte data.
type RawColumn struct {
	Spec ColumnSpec
	Data []byte // the raw (possibly decompressed) column data
}

// RawColumns is a slice of RawColumn values.
type RawColumns []RawColumn

// Find returns the raw column matching the given spec (ignoring the deflate bit), or nil.
func (rc RawColumns) Find(spec ColumnSpec) *RawColumn {
	target := spec.Normalized()
	for i := range rc {
		if rc[i].Spec.Normalized() == target {
			return &rc[i]
		}
	}
	return nil
}

// FindData returns the data for the column matching spec, or nil if not found.
func (rc RawColumns) FindData(spec ColumnSpec) []byte {
	col := rc.Find(spec)
	if col == nil {
		return nil
	}
	return col.Data
}

// ParseRawColumns parses column metadata and data from the given reader.
// Format: uLEB128 column_count, then column_count pairs of (ColumnSpec as uLEB128, column_length as uLEB128),
// then the column data bytes in sequence.
func ParseRawColumns(r *encoding.Reader) (RawColumns, error) {
	count, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading column count: %w", err)
	}

	type colMeta struct {
		spec   ColumnSpec
		length int
	}

	metas := make([]colMeta, count)
	for i := range metas {
		specVal, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading column %d spec: %w", i, err)
		}
		length, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading column %d length: %w", i, err)
		}
		metas[i] = colMeta{
			spec:   ColumnSpec(specVal),
			length: int(length),
		}
	}

	// Now read the column data
	columns := make(RawColumns, len(metas))
	for i, m := range metas {
		data, err := r.ReadBytes(m.length)
		if err != nil {
			return nil, fmt.Errorf("reading column %d data (%s, %d bytes): %w", i, m.spec, m.length, err)
		}

		// Decompress if deflated
		if m.spec.Deflate() {
			decompressed, err := inflateData(data)
			if err != nil {
				return nil, fmt.Errorf("decompressing column %d (%s): %w", i, m.spec, err)
			}
			data = decompressed
		}

		columns[i] = RawColumn{
			Spec: m.spec,
			Data: data,
		}
	}

	return columns, nil
}

// ColumnMeta holds parsed column metadata (spec + length) without the data.
type ColumnMeta struct {
	Spec   ColumnSpec
	Length int
}

// TotalDataLen returns the sum of all column data lengths.
func TotalDataLen(metas []ColumnMeta) int {
	total := 0
	for _, m := range metas {
		total += m.Length
	}
	return total
}

// ParseColumnMeta parses just the column metadata (count + spec/length pairs) without reading data.
func ParseColumnMeta(r *encoding.Reader) ([]ColumnMeta, error) {
	count, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading column count: %w", err)
	}
	metas := make([]ColumnMeta, count)
	for i := range metas {
		specVal, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading column %d spec: %w", i, err)
		}
		length, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading column %d length: %w", i, err)
		}
		metas[i] = ColumnMeta{
			Spec:   ColumnSpec(specVal),
			Length: int(length),
		}
	}
	return metas, nil
}

// ReadColumnsData reads column data from the reader using the given metadata,
// and returns populated RawColumns. Handles decompression of deflated columns.
func ReadColumnsData(r *encoding.Reader, metas []ColumnMeta) (RawColumns, error) {
	columns := make(RawColumns, len(metas))
	for i, m := range metas {
		data, err := r.ReadBytes(m.Length)
		if err != nil {
			return nil, fmt.Errorf("reading column %d data (%s, %d bytes): %w", i, m.Spec, m.Length, err)
		}
		if m.Spec.Deflate() {
			decompressed, err := inflateData(data)
			if err != nil {
				return nil, fmt.Errorf("decompressing column %d (%s): %w", i, m.Spec, err)
			}
			data = decompressed
		}
		columns[i] = RawColumn{
			Spec: m.Spec,
			Data: data,
		}
	}
	return columns, nil
}

// AppendRawColumns appends the encoded column metadata and data to dst.
// Columns are sorted by normalized spec value before writing.
func AppendRawColumns(dst []byte, columns RawColumns) []byte {
	// Sort by normalized spec
	sorted := make(RawColumns, len(columns))
	copy(sorted, columns)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Spec.Normalized() < sorted[j].Spec.Normalized()
	})

	// Write count
	dst = encoding.AppendULEB128(dst, uint64(len(sorted)))

	// Write metadata (spec, length) pairs
	for _, col := range sorted {
		dst = encoding.AppendULEB128(dst, uint64(col.Spec.Raw()))
		dst = encoding.AppendULEB128(dst, uint64(len(col.Data)))
	}

	// Write data
	for _, col := range sorted {
		dst = append(dst, col.Data...)
	}

	return dst
}

// SortedColumns returns a copy of columns sorted by normalized spec.
func SortedColumns(columns RawColumns) RawColumns {
	sorted := make(RawColumns, len(columns))
	copy(sorted, columns)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Spec.Normalized() < sorted[j].Spec.Normalized()
	})
	return sorted
}

// AppendColumnMeta appends column metadata (count + spec/length pairs) to dst.
// Columns are sorted by normalized spec.
func AppendColumnMeta(dst []byte, columns RawColumns) []byte {
	sorted := SortedColumns(columns)
	dst = encoding.AppendULEB128(dst, uint64(len(sorted)))
	for _, col := range sorted {
		dst = encoding.AppendULEB128(dst, uint64(col.Spec.Raw()))
		dst = encoding.AppendULEB128(dst, uint64(len(col.Data)))
	}
	return dst
}

// AppendColumnData appends column data (in sorted order) to dst.
func AppendColumnData(dst []byte, columns RawColumns) []byte {
	sorted := SortedColumns(columns)
	for _, col := range sorted {
		dst = append(dst, col.Data...)
	}
	return dst
}

func inflateData(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	result, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return result, nil
}
