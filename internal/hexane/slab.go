package hexane

// Slab is an immutable chunk of encoded column data.
//
// Each slab holds RLE/Delta/Boolean encoded bytes for a contiguous range of items.
// Slabs are the leaf elements stored in a SpanTree within ColumnData.
type Slab struct {
	data   []byte // encoded bytes
	length int    // number of items
	acc    Acc    // cumulative aggregate of all items
	min    Agg    // minimum value in slab (for filtering)
	max    Agg    // maximum value in slab (for filtering)
	abs    int64  // absolute value (delta encoding base)
}

// NewSlab creates a slab from encoded bytes.
func NewSlab(data []byte, length int, acc Acc, abs int64) Slab {
	return Slab{
		data:   data,
		length: length,
		acc:    acc,
		abs:    abs,
	}
}

// SetMinMax updates the min/max metadata.
func (s *Slab) SetMinMax(min, max Agg) {
	s.min = min
	s.max = max
}

// Bytes returns the encoded byte slice.
func (s *Slab) Bytes() []byte { return s.data }

// ByteLen returns the number of encoded bytes.
func (s *Slab) ByteLen() int { return len(s.data) }

// Len returns the number of items in this slab.
func (s *Slab) Len() int { return s.length }

// IsEmpty returns true if the slab has no items.
func (s *Slab) IsEmpty() bool { return s.length == 0 }

// Acc returns the cumulative aggregate.
func (s *Slab) Acc() Acc { return s.acc }

// Min returns the minimum value metadata.
func (s *Slab) Min() Agg { return s.min }

// Max returns the maximum value metadata.
func (s *Slab) Max() Agg { return s.max }

// Abs returns the absolute value (delta encoding base).
func (s *Slab) Abs() int64 { return s.abs }

// SlabWeight is the SpanWeight type for Slab elements in a SpanTree.
// It tracks cumulative item count, accumulator, and min/max bounds.
type SlabWeight struct {
	Pos int // cumulative item count
	Acc Acc // cumulative accumulator
	Min Agg // minimum value across all slabs
	Max Agg // maximum value across all slabs
}

// SlabWeighter returns a Weighter[Slab, SlabWeight] for use with SpanTree.
func SlabWeighter() Weighter[Slab, SlabWeight] {
	return Weighter[Slab, SlabWeight]{
		Zero: SlabWeight{},
		Alloc: func(s *Slab) SlabWeight {
			return SlabWeight{
				Pos: s.Len(),
				Acc: s.Acc(),
				Min: s.Min(),
				Max: s.Max(),
			}
		},
		And: func(a, b SlabWeight) SlabWeight {
			return SlabWeight{
				Pos: a.Pos + b.Pos,
				Acc: a.Acc.Add(b.Acc),
				Max: a.Max.Maximize(b.Max),
				Min: a.Min.Minimize(b.Min),
			}
		},
		Union: func(a *SlabWeight, b SlabWeight) {
			a.Pos += b.Pos
			a.Acc.AddAssign(b.Acc)
			a.Max = a.Max.Maximize(b.Max)
			a.Min = a.Min.Minimize(b.Min)
		},
		MaybeSub: func(a *SlabWeight, b SlabWeight) bool {
			// Can only fast-subtract if the removed slab's min/max don't
			// affect the overall min/max (i.e., the remaining tree still
			// owns the extreme values).
			maxOk := b.Max.IsNone() || a.Max.Greater(b.Max)
			minOk := b.Min.IsNone() || (a.Min.IsSome() && a.Min.Less(b.Min))
			if maxOk && minOk {
				a.Pos -= b.Pos
				a.Acc.SubAssign(b.Acc)
				return true
			}
			return false
		},
	}
}
