package hexane

import (
	"math/rand"
	"testing"
)

// intWeighter treats int elements as their own weight (sum-based).
func intWeighter() Weighter[int, int] {
	return Weighter[int, int]{
		Zero:  0,
		Alloc: func(s *int) int { return *s },
		And:   func(a, b int) int { return a + b },
		Union: func(a *int, b int) { *a += b },
		MaybeSub: func(a *int, b int) bool {
			*a -= b
			return true
		},
	}
}

func TestSpanTreePush(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	for i := 0; i < 100; i++ {
		tree.Push(i)
	}
	if tree.Len() != 100 {
		t.Errorf("Len = %d", tree.Len())
	}
	for i := 0; i < 100; i++ {
		got := tree.Get(i)
		if got == nil || *got != i {
			t.Fatalf("Get(%d) = %v", i, got)
		}
	}
}

func TestSpanTreeInsert(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	var ref []int

	for i := 0; i < 200; i++ {
		idx := i % 3
		if idx > len(ref) {
			idx = len(ref)
		}
		tree.Insert(idx, i)

		// Reference: insert into slice
		ref = append(ref, 0)
		copy(ref[idx+1:], ref[idx:])
		ref[idx] = i

		got := tree.ToSlice()
		if len(got) != len(ref) {
			t.Fatalf("length mismatch at i=%d: %d vs %d", i, len(got), len(ref))
		}
		for j := range ref {
			if got[j] != ref[j] {
				t.Fatalf("mismatch at i=%d j=%d: %d vs %d", i, j, got[j], ref[j])
			}
		}
	}
}

func TestSpanTreeRemove(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	var ref []int
	for i := 0; i < 100; i++ {
		tree.Push(i)
		ref = append(ref, i)
	}

	rng := rand.New(rand.NewSource(42))
	for len(ref) > 0 {
		idx := rng.Intn(len(ref))
		treeVal := tree.Remove(idx)
		refVal := ref[idx]
		ref = append(ref[:idx], ref[idx+1:]...)

		if treeVal != refVal {
			t.Fatalf("removed value mismatch: %d vs %d", treeVal, refVal)
		}

		got := tree.ToSlice()
		if len(got) != len(ref) {
			t.Fatalf("length mismatch: %d vs %d", len(got), len(ref))
		}
		for j := range ref {
			if got[j] != ref[j] {
				t.Fatalf("mismatch at j=%d: %d vs %d", j, got[j], ref[j])
			}
		}
	}
}

func TestSpanTreeReplace(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	for i := 0; i < 50; i++ {
		tree.Push(i * 10)
	}

	old := tree.Replace(5, 999)
	if old != 50 {
		t.Errorf("Replace returned %d, expected 50", old)
	}
	if v := tree.Get(5); v == nil || *v != 999 {
		t.Errorf("Get(5) after replace = %v", v)
	}
	if tree.Len() != 50 {
		t.Errorf("Len after replace = %d", tree.Len())
	}
}

func TestSpanTreeSplice(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	var ref []int

	// Insert initial values
	tree.Splice(0, 0, []int{1, 2, 3})
	ref = append(ref, 1, 2, 3)
	assertSliceEqual(t, tree.ToSlice(), ref)

	// Replace middle
	tree.Splice(1, 2, []int{7})
	ref = []int{1, 7, 3}
	assertSliceEqual(t, tree.ToSlice(), ref)

	// Replace 1 and insert more
	tree.Splice(1, 2, []int{10, 11, 12, 13, 14})
	ref = []int{1, 10, 11, 12, 13, 14, 3}
	assertSliceEqual(t, tree.ToSlice(), ref)

	// Replace range
	tree.Splice(2, 5, []int{50, 60, 70})
	ref = []int{1, 10, 50, 60, 70, 14, 3}
	assertSliceEqual(t, tree.ToSlice(), ref)
}

func TestSpanTreeWeight(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	tree.Push(10)
	tree.Push(20)
	tree.Push(30)

	w := tree.Weight()
	if w == nil || *w != 60 {
		t.Errorf("Weight = %v, expected 60", w)
	}

	tree.Remove(1)
	w = tree.Weight()
	if w == nil || *w != 40 {
		t.Errorf("Weight after remove = %v, expected 40", w)
	}
}

func TestSpanTreeGetWhere(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	tree.Push(10)
	tree.Push(20)
	tree.Push(30)
	tree.Push(40)
	tree.Push(50)

	// Find first element where cumulative > 0
	c := tree.GetWhere(func(acc, next int) bool { return 0 < acc+next })
	if c == nil || c.Index != 0 || *c.Element != 10 {
		t.Errorf("GetWhere(>0) = %v", c)
	}

	// Find first element where cumulative > 10
	c = tree.GetWhere(func(acc, next int) bool { return 10 < acc+next })
	if c == nil || c.Index != 1 || c.Weight != 10 || *c.Element != 20 {
		t.Errorf("GetWhere(>10) = %+v", c)
	}

	// Find first element where cumulative > 30
	c = tree.GetWhere(func(acc, next int) bool { return 30 < acc+next })
	if c == nil || c.Index != 2 || c.Weight != 30 || *c.Element != 30 {
		t.Errorf("GetWhere(>30) = %+v", c)
	}

	// Find first element where cumulative > 60
	c = tree.GetWhere(func(acc, next int) bool { return 60 < acc+next })
	if c == nil || c.Index != 3 || c.Weight != 60 || *c.Element != 40 {
		t.Errorf("GetWhere(>60) = %+v", c)
	}

	// Beyond total weight — nothing matches
	c = tree.GetWhere(func(acc, next int) bool { return 200 < acc+next })
	if c != nil {
		t.Errorf("GetWhere(>200) should be nil, got %+v", c)
	}
}

func TestSpanTreeGetWhereOrLast(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	tree.Push(10)
	tree.Push(20)
	tree.Push(30)

	c := tree.GetWhereOrLast(func(acc, next int) bool { return 999 < acc+next })
	if c == nil || c.Index != 2 || *c.Element != 30 {
		t.Errorf("GetWhereOrLast(>999) should return last, got %+v", c)
	}
}

func TestSpanTreeGetCursor(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	tree.Push(10)
	tree.Push(20)
	tree.Push(30)

	c := tree.GetCursor(0)
	if c == nil || c.Index != 0 || c.Weight != 0 || *c.Element != 10 {
		t.Errorf("GetCursor(0) = %+v", c)
	}

	c = tree.GetCursor(1)
	if c == nil || c.Index != 1 || c.Weight != 10 || *c.Element != 20 {
		t.Errorf("GetCursor(1) = %+v", c)
	}

	c = tree.GetCursor(2)
	if c == nil || c.Index != 2 || c.Weight != 30 || *c.Element != 30 {
		t.Errorf("GetCursor(2) = %+v", c)
	}
}

func TestSpanTreeIter(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	for i := 1; i <= 5; i++ {
		tree.Push(i * 10)
	}

	iter := tree.Iter()
	var vals []int
	for {
		v := iter.Next()
		if v == nil {
			break
		}
		vals = append(vals, *v)
	}
	expected := []int{10, 20, 30, 40, 50}
	assertSliceEqual(t, vals, expected)
}

func TestSpanTreeLargeInsertRemove(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	var ref []int

	rng := rand.New(rand.NewSource(123))
	n := 1000

	// Insert at random positions
	for i := 0; i < n; i++ {
		idx := 0
		if len(ref) > 0 {
			idx = rng.Intn(len(ref) + 1)
		}
		tree.Insert(idx, i)
		ref = append(ref, 0)
		copy(ref[idx+1:], ref[idx:])
		ref[idx] = i
	}

	got := tree.ToSlice()
	assertSliceEqual(t, got, ref)

	// Remove half at random
	for i := 0; i < n/2; i++ {
		idx := rng.Intn(len(ref))
		treeVal := tree.Remove(idx)
		refVal := ref[idx]
		ref = append(ref[:idx], ref[idx+1:]...)
		if treeVal != refVal {
			t.Fatalf("remove mismatch at step %d: %d vs %d", i, treeVal, refVal)
		}
	}

	got = tree.ToSlice()
	assertSliceEqual(t, got, ref)
}

func TestSpanTreeWeightCorrectness(t *testing.T) {
	tree := NewSpanTree[int, int](intWeighter())
	rng := rand.New(rand.NewSource(99))
	var ref []int

	for i := 0; i < 500; i++ {
		val := rng.Intn(100) + 1
		idx := 0
		if len(ref) > 0 {
			idx = rng.Intn(len(ref) + 1)
		}
		tree.Insert(idx, val)
		ref = append(ref, 0)
		copy(ref[idx+1:], ref[idx:])
		ref[idx] = val

		expectedWeight := 0
		for _, v := range ref {
			expectedWeight += v
		}
		w := tree.Weight()
		if w == nil || *w != expectedWeight {
			t.Fatalf("weight mismatch at step %d: got %v, expected %d", i, w, expectedWeight)
		}
	}
}

func TestSpanTreeUnitWeight(t *testing.T) {
	tree := NewSpanTree[string, struct{}](UnitWeighter[string]())
	tree.Push("hello")
	tree.Push("world")
	if tree.Len() != 2 {
		t.Errorf("Len = %d", tree.Len())
	}
	if v := tree.Get(0); v == nil || *v != "hello" {
		t.Errorf("Get(0) = %v", v)
	}
	tree.Remove(0)
	if v := tree.Get(0); v == nil || *v != "world" {
		t.Errorf("Get(0) after remove = %v", v)
	}
}

func assertSliceEqual[T comparable](t *testing.T, got, expected []T) {
	t.Helper()
	if len(got) != len(expected) {
		t.Fatalf("length mismatch: got %d, expected %d", len(got), len(expected))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Fatalf("mismatch at index %d: got %v, expected %v", i, got[i], expected[i])
		}
	}
}
