package hexane

import (
	"testing"
)

// --- RLE Encode/Decode Tests ---

func TestRleUIntEncodeDecodeRoundTrip(t *testing.T) {
	values := []uint64{1, 2, 3, 3, 3, 4, 5, 5}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)
	if len(encoded) == 0 {
		t.Fatal("encoded should not be empty")
	}

	// Decode
	var state rleCursorState
	var decoded []uint64
	for {
		run, err := rleNext(&state, encoded, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, values)
}

func TestRleIntEncodeDecodeRoundTrip(t *testing.T) {
	values := []int64{-5, -5, -5, 0, 1, 2, 3}
	packer := Int64Packer{}
	eq := func(a, b int64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)

	var state rleCursorState
	var decoded []int64
	for {
		run, err := rleNext(&state, encoded, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, values)
}

func TestRleStrEncodeDecodeRoundTrip(t *testing.T) {
	values := []string{"hello", "hello", "world", "foo", "bar"}
	packer := StrPacker{}
	eq := func(a, b string) bool { return a == b }

	encoded := rleEncode(values, packer, eq)

	var state rleCursorState
	var decoded []string
	for {
		run, err := rleNext(&state, encoded, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, values)
}

func TestRleNullRuns(t *testing.T) {
	// Encode: 3 nulls, then value 42, then 2 nulls
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	state := NewRleState[uint64](eq)
	writer := NewSlabWriter[uint64](packer, 1<<30, true)

	state.Append(writer, nil)
	state.Append(writer, nil)
	state.Append(writer, nil)
	v42 := uint64(42)
	state.Append(writer, &v42)
	state.Append(writer, nil)
	state.Append(writer, nil)
	state.Flush(writer)

	encoded := writer.Write(nil)

	// Decode
	var cursor rleCursorState
	type item struct {
		isNull bool
		value  uint64
	}
	var items []item
	for {
		run, err := rleNext(&cursor, encoded, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			if run.Value == nil {
				items = append(items, item{isNull: true})
			} else {
				items = append(items, item{value: *run.Value})
			}
		}
	}

	if len(items) != 6 {
		t.Fatalf("expected 6 items, got %d", len(items))
	}
	for i := 0; i < 3; i++ {
		if !items[i].isNull {
			t.Errorf("item %d should be null", i)
		}
	}
	if items[3].isNull || items[3].value != 42 {
		t.Errorf("item 3 should be 42, got %+v", items[3])
	}
	for i := 4; i < 6; i++ {
		if !items[i].isNull {
			t.Errorf("item %d should be null", i)
		}
	}
}

func TestRleLitRun(t *testing.T) {
	// All different values → should produce a literal run
	values := []uint64{1, 2, 3, 4, 5}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)

	var state rleCursorState
	var decoded []uint64
	for {
		run, err := rleNext(&state, encoded, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, values)
}

func TestRleLoad(t *testing.T) {
	values := []uint64{1, 1, 1, 2, 3, 4, 5, 6, 9, 9}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)
	slabs, length, err := rleLoad(encoded, 1024, packer)
	if err != nil {
		t.Fatal(err)
	}
	if length != 10 {
		t.Errorf("length = %d, expected 10", length)
	}

	// Decode all slabs and verify
	var decoded []uint64
	for _, slab := range slabs {
		var state rleCursorState
		data := slab.Bytes()
		for {
			run, errR := rleNext(&state, data, packer)
			if errR != nil {
				t.Fatal(errR)
			}
			if run == nil {
				break
			}
			for i := 0; i < run.Count; i++ {
				if run.Value != nil {
					decoded = append(decoded, *run.Value)
				}
			}
		}
	}
	assertSliceEqual(t, decoded, values)

	// Verify AccForRun
	_ = eq // suppress unused
}

func TestRleComputeMinMax(t *testing.T) {
	values := []uint64{5, 10, 3, 8}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	encoded := rleEncode(values, packer, eq)
	slabs, _, err := rleLoad(encoded, 1024, packer)
	if err != nil {
		t.Fatal(err)
	}
	rleComputeMinMax(slabs, packer)
	if len(slabs) == 0 {
		t.Fatal("no slabs")
	}
	if slabs[0].Min() != AggFrom(3) {
		t.Errorf("min = %v, expected 3", slabs[0].Min())
	}
	if slabs[0].Max() != AggFrom(10) {
		t.Errorf("max = %v, expected 10", slabs[0].Max())
	}
}

// --- Boolean Encode/Decode Tests ---

func TestBoolEncodeDecodeRoundTrip(t *testing.T) {
	values := []bool{false, false, true, true, true, false, true}
	encoded := boolEncode(values)

	var state boolCursorState
	var decoded []bool
	for {
		run, err := boolNext(&state, encoded)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, values)
}

func TestBoolAllFalse(t *testing.T) {
	values := []bool{false, false, false}
	encoded := boolEncode(values)
	decoded := boolDecodeAll(&Slab{data: encoded, length: 3})
	assertSliceEqual(t, decoded, values)
}

func TestBoolAllTrue(t *testing.T) {
	values := []bool{true, true, true}
	encoded := boolEncode(values)
	decoded := boolDecodeAll(&Slab{data: encoded, length: 3})
	assertSliceEqual(t, decoded, values)
}

func TestBoolEmpty(t *testing.T) {
	encoded := boolEncode(nil)
	if len(encoded) != 0 {
		t.Errorf("empty bool encode should produce no bytes, got %d", len(encoded))
	}
}

// --- Delta Encode/Decode Tests ---

func TestDeltaEncodeDecodeRoundTrip(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5}
	encoded := deltaEncode(values)

	// Decode
	slab := NewSlab(encoded, len(values), Acc{}, 0)
	decoded := deltaDecodeAllAbsolute(&slab)

	if len(decoded) != len(values) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(values))
	}
	for i := range values {
		if decoded[i] == nil {
			t.Fatalf("nil at index %d", i)
		}
		if *decoded[i] != values[i] {
			t.Errorf("index %d: got %d, expected %d", i, *decoded[i], values[i])
		}
	}
}

func TestDeltaConstantValues(t *testing.T) {
	values := []int64{5, 5, 5, 5, 5}
	encoded := deltaEncode(values)

	slab := NewSlab(encoded, len(values), Acc{}, 0)
	decoded := deltaDecodeAllAbsolute(&slab)

	if len(decoded) != len(values) {
		t.Fatalf("length mismatch: %d vs %d", len(decoded), len(values))
	}
	for i := range values {
		if *decoded[i] != values[i] {
			t.Errorf("index %d: got %d, expected %d", i, *decoded[i], values[i])
		}
	}
}

func TestDeltaDecreasing(t *testing.T) {
	values := []int64{10, 8, 6, 4, 2}
	encoded := deltaEncode(values)

	slab := NewSlab(encoded, len(values), Acc{}, 0)
	decoded := deltaDecodeAllAbsolute(&slab)

	for i := range values {
		if *decoded[i] != values[i] {
			t.Errorf("index %d: got %d, expected %d", i, *decoded[i], values[i])
		}
	}
}

func TestDeltaMixed(t *testing.T) {
	values := []int64{1, 10, 2, 11, 4, 27, 19, 3}
	encoded := deltaEncode(values)

	slab := NewSlab(encoded, len(values), Acc{}, 0)
	decoded := deltaDecodeAllAbsolute(&slab)

	for i := range values {
		if decoded[i] == nil || *decoded[i] != values[i] {
			t.Errorf("index %d: got %v, expected %d", i, decoded[i], values[i])
		}
	}
}

// --- Raw Encode/Decode Tests ---

func TestRawLoadRoundTrip(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	slabs, length, err := rawLoad(data)
	if err != nil {
		t.Fatal(err)
	}
	if length != 5 {
		t.Errorf("length = %d, expected 5", length)
	}
	if len(slabs) != 1 {
		t.Fatalf("expected 1 slab, got %d", len(slabs))
	}
	assertSliceEqual(t, slabs[0].Bytes(), data)
}

func TestRawEmpty(t *testing.T) {
	slabs, length, err := rawLoad(nil)
	if err != nil {
		t.Fatal(err)
	}
	if length != 0 || len(slabs) != 0 {
		t.Errorf("expected empty, got length=%d slabs=%d", length, len(slabs))
	}
}

// --- Encoder Tests ---

func TestEncoderUIntRoundTrip(t *testing.T) {
	ops := UIntCursorOps()
	enc := ops.NewEncoder(true)
	values := []uint64{1, 1, 1, 2, 3, 4, 5, 5}
	for i := range values {
		enc.AppendValue(values[i])
	}
	if enc.Len != 8 {
		t.Errorf("Len = %d, expected 8", enc.Len)
	}

	encoded := enc.SaveTo(nil)

	// Decode back
	var state rleCursorState
	var decoded []uint64
	for {
		run, err := rleNext(&state, encoded, UInt64Packer{})
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}
	assertSliceEqual(t, decoded, values)
}

func TestEncoderBoolRoundTrip(t *testing.T) {
	ops := BoolCursorOps()
	enc := ops.NewEncoder(true)
	values := []bool{false, false, true, true, false}
	for i := range values {
		enc.AppendValue(values[i])
	}

	encoded := enc.SaveTo(nil)
	slab := Slab{data: encoded, length: len(values)}
	decoded := boolDecodeAll(&slab)
	assertSliceEqual(t, decoded, values)
}

func TestEncoderDeltaRoundTrip(t *testing.T) {
	ops := DeltaCursorOps()
	enc := ops.NewEncoder(true)
	values := []int64{1, 2, 3, 10, 20}
	for i := range values {
		enc.AppendValue(values[i])
	}

	encoded := enc.SaveTo(nil)
	slab := NewSlab(encoded, len(values), Acc{}, 0)
	decoded := deltaDecodeAllAbsolute(&slab)

	for i := range values {
		if decoded[i] == nil || *decoded[i] != values[i] {
			t.Errorf("index %d: got %v, expected %d", i, decoded[i], values[i])
		}
	}
}

// --- CursorOps Tests ---

func TestCursorOpsUIntLoadEncode(t *testing.T) {
	ops := UIntCursorOps()
	values := []uint64{10, 20, 30, 30, 30, 40}

	encoded := ops.Encode(values, nil)
	slabs, length, err := ops.Load(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if length != 6 {
		t.Errorf("length = %d", length)
	}

	// Decode all values from slabs
	var decoded []uint64
	for _, s := range slabs {
		ptrs := ops.DecodeAll(&s)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	assertSliceEqual(t, decoded, values)
}

func TestCursorOpsStrLoadEncode(t *testing.T) {
	ops := StrCursorOps()
	values := []string{"alpha", "alpha", "beta", "gamma"}

	encoded := ops.Encode(values, nil)
	slabs, length, err := ops.Load(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if length != 4 {
		t.Errorf("length = %d", length)
	}

	var decoded []string
	for _, s := range slabs {
		ptrs := ops.DecodeAll(&s)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	assertSliceEqual(t, decoded, values)
}

func TestCursorOpsBoolLoadEncode(t *testing.T) {
	ops := BoolCursorOps()
	values := []bool{true, true, false, false, true}

	encoded := ops.Encode(values, nil)
	slabs, length, err := ops.Load(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if length != 5 {
		t.Errorf("length = %d", length)
	}

	var decoded []bool
	for _, s := range slabs {
		ptrs := ops.DecodeAll(&s)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	assertSliceEqual(t, decoded, values)
}

func TestCursorOpsDeltaLoadEncode(t *testing.T) {
	ops := DeltaCursorOps()
	values := []int64{1, 2, 3, 4, 5}

	encoded := ops.Encode(values, nil)
	slabs, length, err := ops.Load(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if length != 5 {
		t.Errorf("length = %d", length)
	}

	var decoded []int64
	for _, s := range slabs {
		ptrs := ops.DecodeAll(&s)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	assertSliceEqual(t, decoded, values)
}

func TestCursorOpsRawLoadEncode(t *testing.T) {
	ops := RawCursorOps()
	values := [][]byte{{1, 2, 3}, {4, 5, 6}}

	encoded := ops.Encode(values, nil)
	if len(encoded) != 6 {
		t.Errorf("expected 6 bytes, got %d", len(encoded))
	}

	slabs, length, err := ops.Load(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if length != 6 {
		t.Errorf("length = %d", length)
	}
	_ = slabs
}

// --- Splice Tests ---

func TestRleSpliceInsert(t *testing.T) {
	values := []uint64{1, 2, 3, 4, 5}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)
	slab := NewSlab(encoded, 5, Acc{}, 0)

	result := rleSplice(&slab, 2, 0, []uint64{10, 11}, 1024, packer, eq)
	if result.Add != 2 || result.Del != 0 {
		t.Errorf("add=%d del=%d", result.Add, result.Del)
	}

	// Decode result
	var decoded []uint64
	for _, s := range result.Slabs {
		ptrs := rleDecodeAll(&s, packer)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	expected := []uint64{1, 2, 10, 11, 3, 4, 5}
	assertSliceEqual(t, decoded, expected)
}

func TestRleSpliceDelete(t *testing.T) {
	values := []uint64{1, 2, 3, 4, 5}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)
	slab := NewSlab(encoded, 5, Acc{}, 0)

	result := rleSplice(&slab, 1, 2, nil, 1024, packer, eq)
	if result.Add != 0 || result.Del != 2 {
		t.Errorf("add=%d del=%d", result.Add, result.Del)
	}

	var decoded []uint64
	for _, s := range result.Slabs {
		ptrs := rleDecodeAll(&s, packer)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	expected := []uint64{1, 4, 5}
	assertSliceEqual(t, decoded, expected)
}

func TestBoolSpliceInsert(t *testing.T) {
	values := []bool{true, true, false, false}
	encoded := boolEncode(values)
	slab := Slab{data: encoded, length: 4}

	result := boolSplice(&slab, 2, 0, []bool{true}, 1024)
	if result.Add != 1 || result.Del != 0 {
		t.Errorf("add=%d del=%d", result.Add, result.Del)
	}

	var decoded []bool
	for _, s := range result.Slabs {
		decoded = append(decoded, boolDecodeAll(&s)...)
	}
	expected := []bool{true, true, true, false, false}
	assertSliceEqual(t, decoded, expected)
}

func TestDeltaSpliceInsert(t *testing.T) {
	values := []int64{1, 2, 3, 4, 5}
	encoded := deltaEncode(values)
	slab := NewSlab(encoded, 5, Acc{}, 0)

	result := deltaSplice(&slab, 2, 0, []int64{10}, DeltaSlabSize)
	if result.Add != 1 || result.Del != 0 {
		t.Errorf("add=%d del=%d", result.Add, result.Del)
	}

	var decoded []int64
	for _, s := range result.Slabs {
		ptrs := deltaDecodeAllAbsolute(&s)
		for _, p := range ptrs {
			if p != nil {
				decoded = append(decoded, *p)
			}
		}
	}
	expected := []int64{1, 2, 10, 3, 4, 5}
	assertSliceEqual(t, decoded, expected)
}

// --- InitEmpty Tests ---

func TestRleInitEmpty(t *testing.T) {
	slab := rleInitEmpty[uint64](5, UInt64Packer{})
	if slab.Len() != 5 {
		t.Errorf("Len = %d, expected 5", slab.Len())
	}

	ptrs := rleDecodeAll(&slab, UInt64Packer{})
	if len(ptrs) != 5 {
		t.Fatalf("decoded %d items, expected 5", len(ptrs))
	}
	for i, p := range ptrs {
		if p != nil {
			t.Errorf("index %d should be nil, got %d", i, *p)
		}
	}
}

func TestBoolInitEmpty(t *testing.T) {
	slab := boolInitEmpty(3)
	if slab.Len() != 3 {
		t.Errorf("Len = %d, expected 3", slab.Len())
	}
	decoded := boolDecodeAll(&slab)
	for i, v := range decoded {
		if v {
			t.Errorf("index %d should be false", i)
		}
	}
}

// --- Empty data tests ---

func TestRleLoadEmpty(t *testing.T) {
	packer := UInt64Packer{}
	slabs, length, err := rleLoad(nil, 64, packer)
	if err != nil {
		t.Fatal(err)
	}
	if length != 0 || len(slabs) != 0 {
		t.Errorf("expected empty, got length=%d slabs=%d", length, len(slabs))
	}
}

func TestBoolLoadEmpty(t *testing.T) {
	slabs, length, err := boolLoad(nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	if length != 0 {
		t.Errorf("length = %d", length)
	}
	_ = slabs
}

func TestDeltaLoadEmpty(t *testing.T) {
	slabs, length, err := deltaLoad(nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	if length != 0 || len(slabs) != 0 {
		t.Errorf("expected empty, got length=%d slabs=%d", length, len(slabs))
	}
}

// --- Wire format compatibility tests ---

func TestRleWireFormatValueRun(t *testing.T) {
	// 3 repetitions of value 10: SLEB(3) ULEB(10) = [3, 10]
	packer := UInt64Packer{}
	var state rleCursorState
	data := []byte{3, 10}

	run, err := rleNext(&state, data, packer)
	if err != nil {
		t.Fatal(err)
	}
	if run == nil || run.Count != 3 || *run.Value != 10 {
		t.Errorf("expected run(3, 10), got %+v", run)
	}
}

func TestRleWireFormatNullRun(t *testing.T) {
	// 5 nulls: 0 ULEB(5) = [0, 5]
	packer := UInt64Packer{}
	var state rleCursorState
	data := []byte{0, 5}

	run, err := rleNext(&state, data, packer)
	if err != nil {
		t.Fatal(err)
	}
	if run == nil || run.Count != 5 || run.Value != nil {
		t.Errorf("expected null run(5), got %+v", run)
	}
}

func TestRleWireFormatLitRun(t *testing.T) {
	// Lit run of 3 values [1,2,3]: SLEB(-3) ULEB(1) ULEB(2) ULEB(3) = [0x7D, 1, 2, 3]
	packer := UInt64Packer{}
	var state rleCursorState
	data := []byte{0x7D, 1, 2, 3} // sLEB(-3) = 0x7D

	var decoded []uint64
	for {
		run, err := rleNext(&state, data, packer)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	assertSliceEqual(t, decoded, []uint64{1, 2, 3})
}

func TestBoolWireFormat(t *testing.T) {
	// false×2, true×3: [2, 3]
	var state boolCursorState
	data := []byte{2, 3}

	var decoded []bool
	for {
		run, err := boolNext(&state, data)
		if err != nil {
			t.Fatal(err)
		}
		if run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			decoded = append(decoded, *run.Value)
		}
	}

	expected := []bool{false, false, true, true, true}
	assertSliceEqual(t, decoded, expected)
}

// --- Cross-validate with Rust byte output ---

func TestRleSaveMatchesRust(t *testing.T) {
	// From Rust test: col2.save() for [1,1,2,3,4,5,6,7,8,9,10,10]
	// Expected: vec![2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10]
	values := []uint64{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }

	encoded := rleEncode(values, packer, eq)
	expected := []byte{2, 1, 120, 2, 3, 4, 5, 6, 7, 8, 9, 2, 10}
	assertSliceEqual(t, encoded, expected)
}

func TestDeltaSaveMatchesRust(t *testing.T) {
	// From Rust test: [1,2,4,6,9,12,16,20,25,30,36,42,49,56,64,72,81,90]
	// Expected: vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
	values := []int64{1, 2, 4, 6, 9, 12, 16, 20, 25, 30, 36, 42, 49, 56, 64, 72, 81, 90}
	encoded := deltaEncode(values)
	expected := []byte{2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9}
	assertSliceEqual(t, encoded, expected)
}

func TestBoolSaveMatchesRust(t *testing.T) {
	// From Rust test: alternating single bools → [0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
	values := []bool{true, false, true, false, true, false, true, false, true, false}
	encoded := boolEncode(values)
	expected := []byte{0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	assertSliceEqual(t, encoded, expected)
}

func TestRleSaveAllSameMatchesRust(t *testing.T) {
	// From Rust test: [1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9]
	// Expected: vec![2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9]
	values := []uint64{1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	encoded := rleEncode(values, packer, eq)
	expected := []byte{2, 1, 2, 2, 2, 3, 2, 4, 2, 5, 2, 6, 2, 7, 2, 8, 2, 9}
	assertSliceEqual(t, encoded, expected)
}

func TestRleSaveLitRunCappedByRuns(t *testing.T) {
	// From Rust test: [1,2,3,4,4,5,5,6,7,8,9,10,11,11]
	// Expected: vec![125, 1, 2, 3, 2, 4, 2, 5, 123, 6, 7, 8, 9, 10, 2, 11]
	values := []uint64{1, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 11}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	encoded := rleEncode(values, packer, eq)
	expected := []byte{125, 1, 2, 3, 2, 4, 2, 5, 123, 6, 7, 8, 9, 10, 2, 11}
	assertSliceEqual(t, encoded, expected)
}

func TestRleSaveAllLitRun(t *testing.T) {
	// From Rust test: [1,2,3,4,5,6,7,8,9,10,11,12]
	// Expected: vec![116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
	values := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	packer := UInt64Packer{}
	eq := func(a, b uint64) bool { return a == b }
	encoded := rleEncode(values, packer, eq)
	expected := []byte{116, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	assertSliceEqual(t, encoded, expected)
}

func TestBoolSaveAlternatingGroups(t *testing.T) {
	// From Rust test: false×3,true×3,false×3,true×3,... (12 groups)
	// Expected: vec![3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
	var values []bool
	for g := 0; g < 12; g++ {
		v := g%2 == 1
		for i := 0; i < 3; i++ {
			values = append(values, v)
		}
	}
	encoded := boolEncode(values)
	expected := []byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
	assertSliceEqual(t, encoded, expected)
}
