package hexane

import (
	"testing"
)

// --- Helper functions ---

func assertSlice[T comparable](t *testing.T, got []*T, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if want[i] == nil {
			if got[i] != nil {
				t.Errorf("[%d] got %v, want nil", i, *got[i])
			}
		} else {
			wantVal := want[i].(T)
			if got[i] == nil {
				t.Errorf("[%d] got nil, want %v", i, wantVal)
			} else if *got[i] != wantVal {
				t.Errorf("[%d] got %v, want %v", i, *got[i], wantVal)
			}
		}
	}
}

// --- ColumnData constructors ---

func TestNewColumns(t *testing.T) {
	uintCol := NewUIntColumn()
	if uintCol.Len() != 0 {
		t.Errorf("new uint column len: got %d, want 0", uintCol.Len())
	}

	intCol := NewIntColumn()
	if intCol.Len() != 0 {
		t.Errorf("new int column len: got %d, want 0", intCol.Len())
	}

	deltaCol := NewDeltaColumn()
	if deltaCol.Len() != 0 {
		t.Errorf("new delta column len: got %d, want 0", deltaCol.Len())
	}

	boolCol := NewBoolColumn()
	if boolCol.Len() != 0 {
		t.Errorf("new bool column len: got %d, want 0", boolCol.Len())
	}

	strCol := NewStrColumn()
	if strCol.Len() != 0 {
		t.Errorf("new str column len: got %d, want 0", strCol.Len())
	}
}

// --- Push and Get ---

func TestUIntPushGet(t *testing.T) {
	col := NewUIntColumn()
	col.Push(10)
	col.Push(20)
	col.Push(30)

	if col.Len() != 3 {
		t.Fatalf("len: got %d, want 3", col.Len())
	}

	v, ok := col.Get(0)
	if !ok || v == nil || *v != 10 {
		t.Errorf("Get(0): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(1)
	if !ok || v == nil || *v != 20 {
		t.Errorf("Get(1): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(2)
	if !ok || v == nil || *v != 30 {
		t.Errorf("Get(2): got %v, ok=%v", v, ok)
	}

	// Out of bounds
	_, ok = col.Get(3)
	if ok {
		t.Error("Get(3) should be out of bounds")
	}
	_, ok = col.Get(-1)
	if ok {
		t.Error("Get(-1) should be out of bounds")
	}
}

func TestBoolPushGet(t *testing.T) {
	col := NewBoolColumn()
	col.Push(true)
	col.Push(false)
	col.Push(true)

	if col.Len() != 3 {
		t.Fatalf("len: got %d, want 3", col.Len())
	}

	v, ok := col.Get(0)
	if !ok || v == nil || *v != true {
		t.Errorf("Get(0): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(1)
	if !ok || v == nil || *v != false {
		t.Errorf("Get(1): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(2)
	if !ok || v == nil || *v != true {
		t.Errorf("Get(2): got %v, ok=%v", v, ok)
	}
}

func TestDeltaPushGet(t *testing.T) {
	col := NewDeltaColumn()
	col.Push(int64(100))
	col.Push(int64(200))
	col.Push(int64(300))

	if col.Len() != 3 {
		t.Fatalf("len: got %d, want 3", col.Len())
	}

	v, ok := col.Get(0)
	if !ok || v == nil || *v != 100 {
		t.Errorf("Get(0): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(1)
	if !ok || v == nil || *v != 200 {
		t.Errorf("Get(1): got %v, ok=%v", v, ok)
	}
	v, ok = col.Get(2)
	if !ok || v == nil || *v != 300 {
		t.Errorf("Get(2): got %v, ok=%v", v, ok)
	}
}

func TestStrPushGet(t *testing.T) {
	col := NewStrColumn()
	col.Push("hello")
	col.Push("world")

	if col.Len() != 2 {
		t.Fatalf("len: got %d, want 2", col.Len())
	}

	v, ok := col.Get(0)
	if !ok || v == nil || *v != "hello" {
		t.Errorf("Get(0): got %v", v)
	}
	v, ok = col.Get(1)
	if !ok || v == nil || *v != "world" {
		t.Errorf("Get(1): got %v", v)
	}
}

// --- Splice ---

func TestSpliceInsert(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	// Insert 10, 20 at index 2
	col.Splice(2, 0, []uint64{10, 20})

	got := col.ToSlice()
	want := []any{uint64(1), uint64(2), uint64(10), uint64(20), uint64(3), uint64(4), uint64(5)}
	assertSlice(t, got, want)
}

func TestSpliceDelete(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	// Delete 2 items at index 1
	col.Splice(1, 2, nil)

	got := col.ToSlice()
	want := []any{uint64(1), uint64(4), uint64(5)}
	assertSlice(t, got, want)
}

func TestSpliceReplace(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	// Replace items 1-2 with 10, 20, 30
	col.Splice(1, 2, []uint64{10, 20, 30})

	got := col.ToSlice()
	want := []any{uint64(1), uint64(10), uint64(20), uint64(30), uint64(4), uint64(5)}
	assertSlice(t, got, want)
}

func TestSpliceAtEnd(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2})
	col.Splice(2, 0, []uint64{3, 4})

	got := col.ToSlice()
	want := []any{uint64(1), uint64(2), uint64(3), uint64(4)}
	assertSlice(t, got, want)
}

func TestSpliceAtBeginning(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{3, 4})
	col.Splice(0, 0, []uint64{1, 2})

	got := col.ToSlice()
	want := []any{uint64(1), uint64(2), uint64(3), uint64(4)}
	assertSlice(t, got, want)
}

func TestSpliceDeleteAll(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})
	col.Splice(0, 3, nil)

	if col.Len() != 0 {
		t.Errorf("len after delete all: got %d, want 0", col.Len())
	}
}

func TestSpliceBool(t *testing.T) {
	col := NewBoolColumn()
	col.Extend([]bool{false, false, false, false})
	col.Splice(1, 2, []bool{true, true, true})

	got := col.ToSlice()
	want := []any{false, true, true, true, false}
	assertSlice(t, got, want)
}

func TestSpliceDelta(t *testing.T) {
	col := NewDeltaColumn()
	col.Extend([]int64{10, 20, 30, 40, 50})

	// Insert 25 at index 2
	col.Splice(2, 0, []int64{25})

	got := col.ToSlice()
	want := []any{int64(10), int64(20), int64(25), int64(30), int64(40), int64(50)}
	assertSlice(t, got, want)
}

// --- Extend ---

func TestExtend(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})
	col.Extend([]uint64{4, 5})

	if col.Len() != 5 {
		t.Fatalf("len: got %d, want 5", col.Len())
	}

	got := col.ToSlice()
	want := []any{uint64(1), uint64(2), uint64(3), uint64(4), uint64(5)}
	assertSlice(t, got, want)
}

// --- IsEmpty ---

func TestIsEmpty(t *testing.T) {
	col := NewUIntColumn()
	if !col.IsEmpty() {
		t.Error("new column should be empty")
	}

	col.Push(0) // zero uint is "empty" per Packer.IsEmpty
	if !col.IsEmpty() {
		t.Error("column with only zero should be empty")
	}

	col.Push(1)
	if col.IsEmpty() {
		t.Error("column with non-zero should not be empty")
	}
}

func TestBoolIsEmpty(t *testing.T) {
	col := NewBoolColumn()
	col.Extend([]bool{false, false, false})
	if !col.IsEmpty() {
		t.Error("all-false bool column should be empty")
	}

	col.Splice(1, 0, []bool{true})
	if col.IsEmpty() {
		t.Error("bool column with true should not be empty")
	}
}

// --- Acc ---

func TestAcc(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})
	acc := col.Acc()
	// For uint, Agg is the value itself (truncated to uint32), so Acc = sum
	// But actually AggFromUint64(0) is None, so 0 contributes nothing
	// AggFromUint64(1) = Agg{1, true}, AggFromUint64(2) = Agg{2, true}, AggFromUint64(3) = Agg{3, true}
	// Acc = 1 + 2 + 3 = 6
	if acc.Val() != 6 {
		t.Errorf("Acc: got %d, want 6", acc.Val())
	}
}

func TestBoolAcc(t *testing.T) {
	col := NewBoolColumn()
	col.Extend([]bool{true, false, true, true})
	acc := col.Acc()
	// For bool, Acc = count of true values = 3
	if acc.Val() != 3 {
		t.Errorf("Bool Acc: got %d, want 3", acc.Val())
	}
}

// --- GetAcc ---

func TestGetAcc(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4})

	acc0 := col.GetAcc(0)
	if acc0.Val() != 0 {
		t.Errorf("GetAcc(0): got %d, want 0", acc0.Val())
	}

	acc1 := col.GetAcc(1)
	if acc1.Val() != 1 {
		t.Errorf("GetAcc(1): got %d, want 1", acc1.Val())
	}

	acc2 := col.GetAcc(2)
	if acc2.Val() != 3 {
		t.Errorf("GetAcc(2): got %d, want 3", acc2.Val())
	}

	acc3 := col.GetAcc(3)
	if acc3.Val() != 6 {
		t.Errorf("GetAcc(3): got %d, want 6", acc3.Val())
	}
}

// --- GetWithAcc ---

func TestGetWithAcc(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30})

	val, acc, ok := col.GetWithAcc(1)
	if !ok || val == nil || *val != 20 {
		t.Errorf("GetWithAcc(1): val=%v, ok=%v", val, ok)
	}
	if acc.Val() != 10 {
		t.Errorf("GetWithAcc(1) acc: got %d, want 10", acc.Val())
	}
}

// --- Iterator ---

func TestIterAll(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	var result []uint64
	it := col.Iter()
	for {
		v, ok := it.Next()
		if !ok {
			break
		}
		if v != nil {
			result = append(result, *v)
		}
	}

	if len(result) != 5 {
		t.Fatalf("iter count: got %d, want 5", len(result))
	}
	for i, want := range []uint64{1, 2, 3, 4, 5} {
		if result[i] != want {
			t.Errorf("[%d] got %d, want %d", i, result[i], want)
		}
	}
}

func TestIterRange(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30, 40, 50})

	it := col.IterRange(1, 4)
	var result []uint64
	for {
		v, ok := it.Next()
		if !ok {
			break
		}
		if v != nil {
			result = append(result, *v)
		}
	}

	if len(result) != 3 {
		t.Fatalf("iter range count: got %d, want 3", len(result))
	}
	for i, want := range []uint64{20, 30, 40} {
		if result[i] != want {
			t.Errorf("[%d] got %d, want %d", i, result[i], want)
		}
	}
}

func TestIterPos(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})

	it := col.Iter()
	if it.Pos() != 0 {
		t.Errorf("initial pos: got %d, want 0", it.Pos())
	}

	it.Next()
	if it.Pos() != 1 {
		t.Errorf("after 1 Next: got %d, want 1", it.Pos())
	}

	it.Next()
	it.Next()
	if it.Pos() != 3 {
		t.Errorf("after 3 Next: got %d, want 3", it.Pos())
	}
}

// --- NextRun ---

func TestNextRun(t *testing.T) {
	col := NewUIntColumn()
	// RLE should create runs of equal values
	col.Extend([]uint64{1, 1, 1, 2, 2, 3})

	it := col.Iter()

	run := it.NextRun()
	if run == nil || run.Count != 3 || run.Value == nil || *run.Value != 1 {
		t.Errorf("run 1: got %+v", run)
	}

	run = it.NextRun()
	if run == nil || run.Count != 2 || run.Value == nil || *run.Value != 2 {
		t.Errorf("run 2: got %+v", run)
	}

	run = it.NextRun()
	if run == nil || run.Count != 1 || run.Value == nil || *run.Value != 3 {
		t.Errorf("run 3: got %+v", run)
	}

	run = it.NextRun()
	if run != nil {
		t.Errorf("expected nil after exhaustion, got %+v", run)
	}
}

// --- AdvanceBy ---

func TestAdvanceBy(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30, 40, 50})

	it := col.Iter()
	it.AdvanceBy(3)

	if it.Pos() != 3 {
		t.Errorf("pos after advance: got %d, want 3", it.Pos())
	}

	v, ok := it.Next()
	if !ok || v == nil || *v != 40 {
		t.Errorf("after advance: got %v, ok=%v", v, ok)
	}
}

func TestAdvanceTo(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30, 40, 50})

	it := col.Iter()
	it.AdvanceTo(4)

	v, ok := it.Next()
	if !ok || v == nil || *v != 50 {
		t.Errorf("after advance to 4: got %v", v)
	}
}

// --- CalculateAcc ---

func TestIterCalculateAcc(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4})

	it := col.Iter()

	acc := it.CalculateAcc()
	if acc.Val() != 0 {
		t.Errorf("acc at start: got %d, want 0", acc.Val())
	}

	it.Next() // consume 1
	acc = it.CalculateAcc()
	if acc.Val() != 1 {
		t.Errorf("acc after 1: got %d, want 1", acc.Val())
	}

	it.Next() // consume 2
	acc = it.CalculateAcc()
	if acc.Val() != 3 {
		t.Errorf("acc after 2: got %d, want 3", acc.Val())
	}
}

// --- SaveTo / Load ---

func TestSaveLoadUInt(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	data := col.Save()
	if len(data) == 0 {
		t.Fatal("save produced empty data")
	}

	col2, err := LoadColumnData(UIntCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	if col2.Len() != 5 {
		t.Fatalf("loaded len: got %d, want 5", col2.Len())
	}

	got := col2.ToSlice()
	want := []any{uint64(1), uint64(2), uint64(3), uint64(4), uint64(5)}
	assertSlice(t, got, want)
}

func TestSaveLoadBool(t *testing.T) {
	col := NewBoolColumn()
	col.Extend([]bool{true, false, true, false, true})

	data := col.Save()

	col2, err := LoadColumnData(BoolCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	got := col2.ToSlice()
	want := []any{true, false, true, false, true}
	assertSlice(t, got, want)
}

func TestSaveLoadDelta(t *testing.T) {
	col := NewDeltaColumn()
	col.Extend([]int64{10, 20, 30, 40, 50})

	data := col.Save()

	col2, err := LoadColumnData(DeltaCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	got := col2.ToSlice()
	want := []any{int64(10), int64(20), int64(30), int64(40), int64(50)}
	assertSlice(t, got, want)
}

func TestSaveLoadStr(t *testing.T) {
	col := NewStrColumn()
	col.Extend([]string{"hello", "world", "foo"})

	data := col.Save()

	col2, err := LoadColumnData(StrCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	got := col2.ToSlice()
	want := []any{"hello", "world", "foo"}
	assertSlice(t, got, want)
}

func TestSaveLoadEmpty(t *testing.T) {
	col := NewUIntColumn()
	data := col.Save()
	if len(data) != 0 {
		t.Errorf("empty column save should be empty, got %d bytes", len(data))
	}
}

func TestLoadUnlessEmpty(t *testing.T) {
	// Empty data with specified length → null column
	col, err := LoadColumnDataUnlessEmpty(UIntCursorOps(), nil, 5)
	if err != nil {
		t.Fatalf("LoadUnlessEmpty error: %v", err)
	}
	if col.Len() != 5 {
		t.Errorf("len: got %d, want 5", col.Len())
	}
	// All values should be nil (null)
	for i := 0; i < 5; i++ {
		v, ok := col.Get(i)
		if !ok {
			t.Errorf("Get(%d) failed", i)
		}
		if v != nil {
			t.Errorf("Get(%d) should be nil, got %v", i, *v)
		}
	}
}

// --- InitEmpty ---

func TestInitEmpty(t *testing.T) {
	col := InitEmptyColumnData(UIntCursorOps(), 10)
	if col.Len() != 10 {
		t.Fatalf("len: got %d, want 10", col.Len())
	}
	for i := 0; i < 10; i++ {
		v, ok := col.Get(i)
		if !ok {
			t.Errorf("Get(%d) failed", i)
		}
		if v != nil {
			t.Errorf("Get(%d) should be nil", i)
		}
	}
}

// --- FillIfEmpty ---

func TestFillIfEmpty(t *testing.T) {
	col := NewUIntColumn()
	ok := col.FillIfEmpty(5)
	if !ok {
		t.Error("FillIfEmpty should return true on empty column")
	}
	if col.Len() != 5 {
		t.Errorf("len: got %d, want 5", col.Len())
	}

	ok = col.FillIfEmpty(10)
	if ok {
		t.Error("FillIfEmpty should return false on non-empty column")
	}
	if col.Len() != 5 {
		t.Errorf("len unchanged: got %d, want 5", col.Len())
	}
}

// --- ByteLen ---

func TestByteLen(t *testing.T) {
	col := NewUIntColumn()
	if col.ByteLen() != 0 {
		t.Errorf("empty ByteLen: got %d, want 0", col.ByteLen())
	}

	col.Extend([]uint64{1, 2, 3})
	bl := col.ByteLen()
	if bl <= 0 {
		t.Errorf("non-empty ByteLen should be > 0, got %d", bl)
	}
}

// --- Equal ---

func TestEqual(t *testing.T) {
	col1 := NewUIntColumn()
	col1.Extend([]uint64{1, 2, 3})

	col2 := NewUIntColumn()
	col2.Extend([]uint64{1, 2, 3})

	if !col1.Equal(col2) {
		t.Error("identical columns should be equal")
	}

	col3 := NewUIntColumn()
	col3.Extend([]uint64{1, 2, 4})

	if col1.Equal(col3) {
		t.Error("different columns should not be equal")
	}
}

// --- All (iter.Seq) ---

func TestAll(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30})

	var result []uint64
	for v := range col.All() {
		if v != nil {
			result = append(result, *v)
		}
	}

	if len(result) != 3 {
		t.Fatalf("All count: got %d, want 3", len(result))
	}
}

// --- FindByValue ---

func TestFindByValue(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 2, 5})

	var found []int
	for idx := range col.FindByValue(AggFromUint64(2)) {
		found = append(found, idx)
	}

	if len(found) != 2 || found[0] != 1 || found[1] != 3 {
		t.Errorf("FindByValue(2): got %v, want [1, 3]", found)
	}
}

func TestFindByValueNotFound(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})

	var found []int
	for idx := range col.FindByValue(AggFromUint64(99)) {
		found = append(found, idx)
	}

	if len(found) != 0 {
		t.Errorf("FindByValue(99): got %v, want []", found)
	}
}

// --- ColGroupIter ---

func TestColGroupIter(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30})

	gi := col.Iter().WithAcc()

	item, ok := gi.Next()
	if !ok || item == nil {
		t.Fatal("expected first item")
	}
	if item.Pos != 0 || item.Acc.Val() != 0 {
		t.Errorf("first: pos=%d, acc=%d", item.Pos, item.Acc.Val())
	}
	if item.Item == nil || *item.Item != 10 {
		t.Errorf("first value: got %v", item.Item)
	}

	item, ok = gi.Next()
	if !ok || item == nil {
		t.Fatal("expected second item")
	}
	if item.Pos != 1 || item.Acc.Val() != 10 {
		t.Errorf("second: pos=%d, acc=%d", item.Pos, item.Acc.Val())
	}

	item, ok = gi.Next()
	if !ok || item == nil {
		t.Fatal("expected third item")
	}
	if item.Pos != 2 || item.Acc.Val() != 30 {
		t.Errorf("third: pos=%d, acc=%d", item.Pos, item.Acc.Val())
	}
}

// --- ColAccIter ---

func TestColAccIter(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})

	ai := col.Iter().AsAcc()

	acc, ok := ai.Next()
	if !ok {
		t.Fatal("expected first")
	}
	if acc.Val() != 1 {
		t.Errorf("acc after first: got %d, want 1", acc.Val())
	}

	acc, ok = ai.Next()
	if !ok {
		t.Fatal("expected second")
	}
	if acc.Val() != 3 {
		t.Errorf("acc after second: got %d, want 3", acc.Val())
	}
}

// --- Suspend / Resume ---

func TestSuspendResume(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30, 40, 50})

	it := col.Iter()
	it.Next() // 10
	it.Next() // 20

	state := it.Suspend()

	it2, err := ResumeColumnDataIter(col, state)
	if err != nil {
		t.Fatalf("resume error: %v", err)
	}

	v, ok := it2.Next()
	if !ok || v == nil || *v != 30 {
		t.Errorf("resumed next: got %v", v)
	}
}

func TestResumeAfterMutation(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3})

	it := col.Iter()
	state := it.Suspend()

	col.Push(4) // mutate

	_, err := ResumeColumnDataIter(col, state)
	if err == nil {
		t.Error("resume after mutation should fail")
	}
}

// --- ShiftNext ---

func TestShiftNext(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{10, 20, 30, 40, 50})

	it := col.Iter()
	v, ok := it.ShiftNext(2, 4)
	if !ok || v == nil || *v != 30 {
		t.Errorf("ShiftNext(2,4): got %v", v)
	}
	if it.Pos() != 3 {
		t.Errorf("pos after ShiftNext: got %d, want 3", it.Pos())
	}
	if it.EndPos() != 4 {
		t.Errorf("EndPos: got %d, want 4", it.EndPos())
	}
}

// --- AdvanceAccBy ---

func TestAdvanceAccBy(t *testing.T) {
	col := NewBoolColumn()
	col.Extend([]bool{false, true, false, true, true, false})
	// Acc for bools = count of true values
	// Items: f(0), t(1), f(0), t(1), t(1), f(0)
	// Cumulative acc: 0, 0, 1, 1, 2, 3, 3

	it := col.Iter()
	consumed := it.AdvanceAccBy(AccFrom(2))
	// Need to advance until acc >= 2
	// After items 0,1,2,3 → acc = 0+1+0+1 = 2 ≥ 2
	if consumed != 4 {
		t.Errorf("AdvanceAccBy(2): consumed %d, want 4", consumed)
	}
	if it.Pos() != 4 {
		t.Errorf("pos: got %d, want 4", it.Pos())
	}
}

// --- Remap ---

func TestRemap(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})

	col.Remap(func(v *uint64) *uint64 {
		if v == nil {
			return nil
		}
		doubled := *v * 2
		return &doubled
	})

	got := col.ToSlice()
	want := []any{uint64(2), uint64(4), uint64(6), uint64(8), uint64(10)}
	assertSlice(t, got, want)
}

// --- Large splice test ---

func TestLargeSplice(t *testing.T) {
	col := NewUIntColumn()

	// Build up column
	values := make([]uint64, 100)
	for i := range values {
		values[i] = uint64(i)
	}
	col.Extend(values)

	if col.Len() != 100 {
		t.Fatalf("len: got %d, want 100", col.Len())
	}

	// Splice in the middle
	col.Splice(50, 10, []uint64{999, 888, 777})

	if col.Len() != 93 {
		t.Fatalf("len after splice: got %d, want 93", col.Len())
	}

	v, _ := col.Get(50)
	if v == nil || *v != 999 {
		t.Errorf("Get(50): got %v, want 999", v)
	}
	v, _ = col.Get(52)
	if v == nil || *v != 777 {
		t.Errorf("Get(52): got %v, want 777", v)
	}
	v, _ = col.Get(53)
	if v == nil || *v != 60 {
		t.Errorf("Get(53): got %v, want 60", v)
	}
}

// --- Save/Load roundtrip after mutations ---

func TestSaveLoadAfterSplice(t *testing.T) {
	col := NewUIntColumn()
	col.Extend([]uint64{1, 2, 3, 4, 5})
	col.Splice(2, 1, []uint64{10, 20})

	data := col.Save()
	col2, err := LoadColumnData(UIntCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	if !col.Equal(col2) {
		t.Error("loaded column should equal original after splice")
	}
}

// --- Monotonic delta test ---

func TestDeltaMonotonic(t *testing.T) {
	col := NewDeltaColumn()
	for i := 1; i <= 20; i++ {
		col.Push(int64(i))
	}

	// Save and reload
	data := col.Save()
	col2, err := LoadColumnData(DeltaCursorOps(), data)
	if err != nil {
		t.Fatalf("load error: %v", err)
	}

	for i := 0; i < 20; i++ {
		v, ok := col2.Get(i)
		if !ok || v == nil || *v != int64(i+1) {
			t.Errorf("Get(%d): got %v, want %d", i, v, i+1)
		}
	}
}

// --- Iterator with empty column ---

func TestIterEmpty(t *testing.T) {
	col := NewUIntColumn()
	it := col.Iter()
	_, ok := it.Next()
	if ok {
		t.Error("empty column iter should return false")
	}
}

// --- NumSlabs ---

func TestNumSlabs(t *testing.T) {
	col := NewUIntColumn()
	if col.NumSlabs() != 1 {
		t.Errorf("new column slabs: got %d, want 1", col.NumSlabs())
	}

	col.Extend([]uint64{1, 2, 3})
	if col.NumSlabs() < 1 {
		t.Error("column should have at least 1 slab")
	}
}
