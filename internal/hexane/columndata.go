package hexane

import "iter"

// ColumnData is a compressed, mutable column of optional typed values.
//
// It stores a sequence of *T values (nil for nulls) using cursor-type-specific encoding
// (RLE, Delta, Boolean, or Raw). Data is held internally in a SpanTree of Slabs;
// modifications re-encode only the affected slab, leaving the rest untouched.
//
// Construct with NewUIntColumn, NewIntColumn, NewDeltaColumn, NewBoolColumn,
// NewStrColumn, NewBytesColumn, or NewRawColumn.
type ColumnData[T any] struct {
	len     int
	slabs   *SpanTree[Slab, SlabWeight]
	ops     CursorOps[T]
	counter int
}

// newColumnData creates a ColumnData with the given ops and an empty slab.
func newColumnData[T any](ops CursorOps[T]) *ColumnData[T] {
	slabs := NewSpanTree[Slab, SlabWeight](SlabWeighter())
	slabs.Push(Slab{})
	return &ColumnData[T]{ops: ops, slabs: slabs}
}

// NewUIntColumn creates a new empty uint64 column (RLE encoded).
func NewUIntColumn() *ColumnData[uint64] { return newColumnData(UIntCursorOps()) }

// NewIntColumn creates a new empty int64 column (RLE encoded).
func NewIntColumn() *ColumnData[int64] { return newColumnData(IntCursorOps()) }

// NewDeltaColumn creates a new empty int64 column (delta encoded).
func NewDeltaColumn() *ColumnData[int64] { return newColumnData(DeltaCursorOps()) }

// NewBoolColumn creates a new empty bool column (alternating run-length encoded).
func NewBoolColumn() *ColumnData[bool] { return newColumnData(BoolCursorOps()) }

// NewStrColumn creates a new empty string column (RLE encoded).
func NewStrColumn() *ColumnData[string] { return newColumnData(StrCursorOps()) }

// NewBytesColumn creates a new empty byte slice column (RLE encoded).
func NewBytesColumn() *ColumnData[[]byte] { return newColumnData(BytesCursorOps()) }

// NewValueMetaColumn creates a new empty uint64 column for value metadata.
// The accumulator tracks total value byte length (val >> 4) rather than raw values.
func NewValueMetaColumn() *ColumnData[uint64] { return newColumnData(ValueMetaCursorOps()) }

// NewRawColumn creates a new empty raw byte column (no compression).
func NewRawColumn() *ColumnData[[]byte] { return newColumnData(RawCursorOps()) }

// Init creates a ColumnData from pre-built slabs and known length.
func InitColumnData[T any](ops CursorOps[T], length int, slabs []Slab) *ColumnData[T] {
	tree := LoadSpanTree[Slab, SlabWeight](SlabWeighter(), slabs)
	return &ColumnData[T]{
		len:   length,
		slabs: tree,
		ops:   ops,
	}
}

// InitEmpty creates a ColumnData of the given length with all null/empty values.
func InitEmptyColumnData[T any](ops CursorOps[T], length int) *ColumnData[T] {
	newSlab := ops.InitEmpty(length)
	slabs := NewSpanTree[Slab, SlabWeight](SlabWeighter())
	slabs.Push(newSlab)
	return &ColumnData[T]{
		len:   length,
		slabs: slabs,
		ops:   ops,
	}
}

// --- Basic properties ---

// Len returns the number of items (including nulls).
func (c *ColumnData[T]) Len() int { return c.len }

// ByteLen returns the total number of encoded bytes across all slabs.
func (c *ColumnData[T]) ByteLen() int {
	total := 0
	it := c.slabs.Iter()
	for s := it.Next(); s != nil; s = it.Next() {
		total += s.ByteLen()
	}
	return total
}

// IsEmpty returns true if every item is null (or false for booleans).
// An empty column (len == 0) is also considered empty.
func (c *ColumnData[T]) IsEmpty() bool {
	if c.len == 0 {
		return true
	}
	// Check if the first run covers all items and is empty
	it := c.slabs.Iter()
	for s := it.Next(); s != nil; s = it.Next() {
		decoded := c.ops.DecodeAll(s)
		for _, v := range decoded {
			if v != nil && !c.ops.Packer.IsEmpty(*v) {
				return false
			}
		}
	}
	return true
}

// Acc returns the total accumulated Acc for the entire column.
func (c *ColumnData[T]) Acc() Acc {
	w := c.slabs.Weight()
	if w == nil {
		return Acc{}
	}
	return w.Acc
}

// NumSlabs returns the number of slabs (for testing/debugging).
func (c *ColumnData[T]) NumSlabs() int { return c.slabs.Len() }

// Ops returns the cursor operations for this column.
func (c *ColumnData[T]) Ops() CursorOps[T] { return c.ops }

// Counter returns the mutation counter (for iterator invalidation).
func (c *ColumnData[T]) Counter() int { return c.counter }

// Slabs returns the underlying SpanTree (for advanced use).
func (c *ColumnData[T]) Slabs() *SpanTree[Slab, SlabWeight] { return c.slabs }

// --- Reading data ---

// Get returns the value at index, or nil if out of bounds or null.
// The outer bool indicates whether the index was valid.
func (c *ColumnData[T]) Get(index int) (*T, bool) {
	if index < 0 || index >= c.len {
		return nil, false
	}
	it := c.IterRange(index, index+1)
	val, ok := it.Next()
	if !ok {
		return nil, false
	}
	return val, true
}

// GetAcc returns the cumulative Acc for all items before index.
func (c *ColumnData[T]) GetAcc(index int) Acc {
	if index <= 0 || c.len == 0 {
		return Acc{}
	}
	if index >= c.len {
		return c.Acc()
	}
	it := c.IterRange(index, index+1)
	return it.CalculateAcc()
}

// GetWithAcc returns the value at index together with the Acc immediately before it.
// Returns nil, Acc{}, false if out of bounds.
func (c *ColumnData[T]) GetWithAcc(index int) (*T, Acc, bool) {
	if index < 0 || index >= c.len {
		return nil, Acc{}, false
	}
	it := c.IterRange(index, index+1)
	acc := it.CalculateAcc()
	val, ok := it.Next()
	if !ok {
		return nil, acc, false
	}
	return val, acc, true
}

// --- Mutation ---

// Splice removes del items starting at index and inserts values in their place.
// Returns the accumulated Acc of the original values at the splice position.
// Panics if index > Len().
func (c *ColumnData[T]) Splice(index, del int, values []T) Acc {
	if index > c.len {
		panic("ColumnData.Splice: index out of bounds")
	}
	if len(values) == 0 && del == 0 {
		return Acc{}
	}

	// Find target slab
	cursor := c.slabs.GetWhereOrLast(func(accW, nextW SlabWeight) bool {
		return index < accW.Pos+nextW.Pos
	})
	if cursor == nil {
		panic("ColumnData.Splice: no slabs")
	}

	acc := cursor.Weight.Acc
	subindex := index - cursor.Weight.Pos

	// Splice within slab
	result := c.ops.Splice(cursor.Element, subindex, del, values)

	acc.AddAssign(result.Group)
	c.ops.ComputeMinMax(result.Slabs)
	c.len = c.len + result.Add - result.Del

	// Replace old slab with new slabs in tree
	postSlabIndex := cursor.Index + len(result.Slabs)
	c.slabs.Splice(cursor.Index, cursor.Index+1, result.Slabs)
	c.counter++

	// Handle overflow deletion cascading into subsequent slabs
	for result.Overflow > 0 {
		postSlab := c.slabs.Get(postSlabIndex)
		if postSlab == nil {
			break
		}
		if postSlab.Len() <= result.Overflow {
			result.Overflow -= postSlab.Len()
			c.len -= postSlab.Len()
			c.slabs.Remove(postSlabIndex)
		} else {
			r := c.ops.Splice(postSlab, 0, result.Overflow, nil)
			c.len -= r.Del
			c.ops.ComputeMinMax(r.Slabs)
			c.slabs.Splice(postSlabIndex, postSlabIndex+1, r.Slabs)
			break
		}
	}

	// Ensure we always have at least one slab
	if c.slabs.IsEmpty() {
		c.slabs.Push(Slab{})
	}

	return acc
}

// Push appends a single value to the end of the column.
func (c *ColumnData[T]) Push(value T) Acc {
	return c.Splice(c.len, 0, []T{value})
}

// PushNull appends a null value to the end of the column.
// Implemented via splice with empty values and a dummy to increment length.
func (c *ColumnData[T]) PushNull() {
	// For null, we splice with del=0 and no values, but we need to extend.
	// The simplest approach: use the encoder to add a null.
	enc := c.ops.NewEncoder(false)
	enc.AppendNull()
	enc.Flush()
	slabs := enc.IntoSlabs(c.ops.ComputeMinMax)
	if len(slabs) > 0 {
		// Merge with last slab by splicing at end
		c.len++
		c.counter++
		// Append the null slab
		for _, s := range slabs {
			c.slabs.Push(s)
		}
	}
}

// Extend appends multiple values to the end of the column.
func (c *ColumnData[T]) Extend(values []T) Acc {
	return c.Splice(c.len, 0, values)
}

// SpliceNullable removes del items starting at index and inserts nullable values
// (nil = null) in their place. Returns the accumulated Acc of the deleted items.
//
// This is slower than Splice (O(n) full re-encode) but handles null values correctly.
func (c *ColumnData[T]) SpliceNullable(index, del int, values []*T) Acc {
	if index > c.len {
		panic("ColumnData.SpliceNullable: index out of bounds")
	}

	// Check for nulls — fast path if none
	hasNulls := false
	for _, v := range values {
		if v == nil {
			hasNulls = true
			break
		}
	}
	if !hasNulls {
		nonNull := make([]T, len(values))
		for i, v := range values {
			nonNull[i] = *v
		}
		return c.Splice(index, del, nonNull)
	}

	// Compute acc of deleted items
	var deletedAcc Acc
	if del > 0 && index < c.len {
		packer := c.ops.Packer
		end := index + del
		if end > c.len {
			end = c.len
		}
		it := c.IterRange(index, end)
		for {
			v, ok := it.Next()
			if !ok {
				break
			}
			if v != nil {
				deletedAcc.AddAssignAgg(packer.ItemAgg(*v))
			}
		}
	}

	// Decode all, splice, re-encode
	all := c.ToSlice()
	end := index + del
	if end > len(all) {
		end = len(all)
	}

	newAll := make([]*T, 0, len(all)-del+len(values))
	newAll = append(newAll, all[:index]...)
	newAll = append(newAll, values...)
	newAll = append(newAll, all[end:]...)

	// Re-encode
	enc := c.ops.NewEncoder(false)
	for _, v := range newAll {
		enc.Append(v)
	}
	enc.Flush()
	slabs := enc.IntoSlabs(c.ops.ComputeMinMax)
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}
	c.slabs = LoadSpanTree[Slab, SlabWeight](SlabWeighter(), slabs)
	c.len = len(newAll)
	c.counter++

	return deletedAcc
}

// FillIfEmpty fills the column with len null values if currently empty.
// Returns true if filled, false if the column already had items.
func (c *ColumnData[T]) FillIfEmpty(length int) bool {
	if c.len == 0 && length > 0 {
		newSlab := c.ops.InitEmpty(length)
		c.slabs = NewSpanTree[Slab, SlabWeight](SlabWeighter())
		c.slabs.Push(newSlab)
		c.len = length
		c.counter++
		return true
	}
	return false
}

// --- Serialization ---

// Save serializes the column to a new byte slice.
func (c *ColumnData[T]) Save() []byte {
	return c.SaveTo(nil)
}

// SaveTo appends the serialized column to out and returns the extended slice.
// If the column has zero length, nothing is written.
func (c *ColumnData[T]) SaveTo(out []byte) []byte {
	if c.len == 0 {
		return out
	}

	if c.slabs.Len() == 1 {
		slab := c.slabs.Get(0)
		if slab != nil && slab.ByteLen() > 0 {
			return append(out, slab.Bytes()...)
		}
		// Empty slab with items (all nulls) — need to encode
		enc := c.ops.NewEncoder(true)
		for i := 0; i < c.len; i++ {
			enc.AppendNull()
		}
		return enc.SaveTo(out)
	}

	// Multiple slabs: decode all, re-encode into single stream
	enc := c.ops.NewEncoder(true)
	it := c.slabs.Iter()
	for s := it.Next(); s != nil; s = it.Next() {
		decoded := c.ops.DecodeAll(s)
		for _, v := range decoded {
			enc.Append(v)
		}
	}
	return enc.SaveTo(out)
}

// SaveToUnlessEmpty is like SaveTo but writes nothing if IsEmpty() returns true.
func (c *ColumnData[T]) SaveToUnlessEmpty(out []byte) []byte {
	if c.IsEmpty() {
		return out
	}
	return c.SaveTo(out)
}

// Load deserializes a column from bytes.
func LoadColumnData[T any](ops CursorOps[T], data []byte) (*ColumnData[T], error) {
	slabs, length, err := ops.Load(data)
	if err != nil {
		return nil, err
	}
	if len(slabs) == 0 {
		return newColumnData(ops), nil
	}
	ops.ComputeMinMax(slabs)
	tree := LoadSpanTree[Slab, SlabWeight](SlabWeighter(), slabs)
	return &ColumnData[T]{
		len:   length,
		slabs: tree,
		ops:   ops,
	}, nil
}

// LoadUnlessEmpty deserializes, or returns a column of length nulls if data is empty.
func LoadColumnDataUnlessEmpty[T any](ops CursorOps[T], data []byte, length int) (*ColumnData[T], error) {
	if len(data) == 0 {
		return InitEmptyColumnData(ops, length), nil
	}
	col, err := LoadColumnData(ops, data)
	if err != nil {
		return nil, err
	}
	if col.len != length {
		return nil, NewPackError("column length mismatch")
	}
	return col, nil
}

// --- Conversion ---

// ToSlice decodes all values into a slice. Nil entries represent nulls.
func (c *ColumnData[T]) ToSlice() []*T {
	result := make([]*T, 0, c.len)
	it := c.slabs.Iter()
	for s := it.Next(); s != nil; s = it.Next() {
		result = append(result, c.ops.DecodeAll(s)...)
	}
	return result
}

// All returns an iter.Seq that yields all values (nil for nulls).
func (c *ColumnData[T]) All() iter.Seq[*T] {
	return func(yield func(*T) bool) {
		it := c.Iter()
		for {
			val, ok := it.Next()
			if !ok {
				return
			}
			if !yield(val) {
				return
			}
		}
	}
}

// --- Iteration ---

// Iter returns a forward iterator over all items in the column.
func (c *ColumnData[T]) Iter() *ColumnDataIter[T] {
	return c.newIter(0, c.len)
}

// IterRange returns an iterator over items in [start, end), clamped to column length.
func (c *ColumnData[T]) IterRange(start, end int) *ColumnDataIter[T] {
	if start > c.len {
		start = c.len
	}
	if end > c.len {
		end = c.len
	}
	if start > end {
		start = end
	}
	return c.newIter(start, end)
}

// --- Search ---

// FindByValue returns indices of items whose Agg value equals the given agg.
// Uses slab-level min/max metadata to skip non-matching slabs.
func (c *ColumnData[T]) FindByValue(agg Agg) iter.Seq[int] {
	return func(yield func(int) bool) {
		fnIter := c.slabs.IterWhere(func(accW, nextW SlabWeight) bool {
			return agg.IsSome() && !agg.Less(nextW.Min) && !nextW.Max.Less(agg)
		})
		for cursor := fnIter.Next(); cursor != nil; cursor = fnIter.Next() {
			pos := cursor.Weight.Pos
			decoded := c.ops.DecodeAll(cursor.Element)
			for i, v := range decoded {
				if v != nil {
					itemAgg := c.ops.Packer.ItemAgg(*v)
					if itemAgg.Equal(agg) {
						if !yield(pos + i) {
							return
						}
					}
				}
			}
		}
	}
}

// FindByRange returns indices of items whose Agg value falls within [start, end).
// Uses slab-level min/max metadata to skip non-matching slabs.
func (c *ColumnData[T]) FindByRange(start, end int) iter.Seq[int] {
	startAgg := AggFrom(uint32(start))
	endAgg := AggFrom(uint32(end))
	return func(yield func(int) bool) {
		fnIter := c.slabs.IterWhere(func(accW, nextW SlabWeight) bool {
			// Slab intersects [start, end) if slab.max >= start && slab.min < end
			return !nextW.Max.Less(startAgg) && nextW.Min.Less(endAgg)
		})
		for cursor := fnIter.Next(); cursor != nil; cursor = fnIter.Next() {
			pos := cursor.Weight.Pos
			decoded := c.ops.DecodeAll(cursor.Element)
			for i, v := range decoded {
				if v != nil {
					itemAgg := c.ops.Packer.ItemAgg(*v)
					if !itemAgg.Less(startAgg) && itemAgg.Less(endAgg) {
						if !yield(pos + i) {
							return
						}
					}
				}
			}
		}
	}
}

// --- Remap ---

// Remap replaces the column with a re-encoded version where every item has been
// transformed by f. f receives nil for null items and may return nil for null output.
func (c *ColumnData[T]) Remap(f func(*T) *T) {
	enc := c.ops.NewEncoder(false)
	it := c.slabs.Iter()
	for s := it.Next(); s != nil; s = it.Next() {
		decoded := c.ops.DecodeAll(s)
		for _, v := range decoded {
			enc.Append(f(v))
		}
	}
	enc.Flush()
	slabs := enc.IntoSlabs(c.ops.ComputeMinMax)
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}
	c.slabs = LoadSpanTree[Slab, SlabWeight](SlabWeighter(), slabs)
	c.counter++
}

// --- Equality ---

// Equal returns true if two columns contain the same values.
func (c *ColumnData[T]) Equal(other *ColumnData[T]) bool {
	if c.len != other.len {
		return false
	}
	it1 := c.Iter()
	it2 := other.Iter()
	for {
		v1, ok1 := it1.Next()
		v2, ok2 := it2.Next()
		if !ok1 && !ok2 {
			return true
		}
		if ok1 != ok2 {
			return false
		}
		if !c.ops.EqualValue(v1, v2) {
			return false
		}
	}
}
