package automerge

import (
	"fmt"
	"math/rand"
	"testing"
)

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// --- Map operation builders (matching Rust map.rs) ---

func repeatedPut(n int) *Doc {
	doc := New()
	for i := 0; i < n; i++ {
		doc.Put(Root, "0", NewUint(uint64(i)))
	}
	doc.Commit("", 0)
	return doc
}

func repeatedIncrement(n int) *Doc {
	doc := New()
	doc.Put(Root, "counter", NewCounter(0))
	for i := 0; i < n; i++ {
		doc.Increment(Root, "counter", 1)
	}
	doc.Commit("", 0)
	return doc
}

func increasingPut(n int) *Doc {
	doc := New()
	for i := 0; i < n; i++ {
		doc.Put(Root, fmt.Sprintf("%d", i), NewUint(uint64(i)))
	}
	doc.Commit("", 0)
	return doc
}

func decreasingPut(n int) *Doc {
	doc := New()
	for i := n - 1; i >= 0; i-- {
		doc.Put(Root, fmt.Sprintf("%d", i), NewUint(uint64(i)))
	}
	doc.Commit("", 0)
	return doc
}

// --- Document pattern builders (matching Rust load_save.rs) ---

func bigPasteDoc(n int) *Doc {
	doc := New()
	doc.Put(Root, "content", NewStr(randomString(n)))
	doc.Commit("", 0)
	return doc
}

func poorlySimulatedTypingDoc(n int) *Doc {
	doc := New()
	obj, _ := doc.PutObject(Root, "content", ObjTypeText)
	doc.Commit("", 0)

	for i := 0; i < n; i++ {
		doc.SpliceText(obj, uint64(i), 0, randomString(1))
		doc.Commit("", 0)
	}
	return doc
}

func mapsInMapsDoc(n int) *Doc {
	doc := New()
	obj := Root
	for i := 0; i < n; i++ {
		newObj, _ := doc.PutObject(obj, fmt.Sprintf("%d", i), ObjTypeMap)
		obj = newObj
	}
	doc.Commit("", 0)
	return doc
}

func deepHistoryDoc(n int) *Doc {
	doc := New()
	for i := 0; i < n; i++ {
		doc.Put(Root, "x", NewStr(fmt.Sprintf("%d", i)))
		doc.Put(Root, "y", NewStr(fmt.Sprintf("%d", i)))
		doc.Commit("", 0)
	}
	return doc
}

// --- Map benchmarks ---

func BenchmarkMapRepeatedPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				repeatedPut(size)
			}
		})
	}
}

func BenchmarkMapRepeatedIncrement(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				repeatedIncrement(size)
			}
		})
	}
}

func BenchmarkMapIncreasingPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				increasingPut(size)
			}
		})
	}
}

func BenchmarkMapDecreasingPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			for b.Loop() {
				decreasingPut(size)
			}
		})
	}
}

// --- Save benchmarks ---

func BenchmarkSaveRepeatedPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			doc := repeatedPut(size)
			b.ResetTimer()
			for b.Loop() {
				doc.Save()
			}
		})
	}
}

func BenchmarkSaveIncreasingPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			doc := increasingPut(size)
			b.ResetTimer()
			for b.Loop() {
				doc.Save()
			}
		})
	}
}

// --- Load benchmarks ---

func BenchmarkLoadRepeatedPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			data, _ := repeatedPut(size).Save()
			b.ResetTimer()
			for b.Loop() {
				Load(data)
			}
		})
	}
}

func BenchmarkLoadIncreasingPut(b *testing.B) {
	for _, size := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			data, _ := increasingPut(size).Save()
			b.ResetTimer()
			for b.Loop() {
				Load(data)
			}
		})
	}
}

// --- Load/Save document pattern benchmarks ---

func BenchmarkLoadSaveBigPaste(b *testing.B) {
	doc := bigPasteDoc(1000)
	b.ResetTimer()
	for b.Loop() {
		data, _ := doc.Save()
		Load(data)
	}
}

func BenchmarkLoadSaveTypingDoc(b *testing.B) {
	doc := poorlySimulatedTypingDoc(1000)
	b.ResetTimer()
	for b.Loop() {
		data, _ := doc.Save()
		Load(data)
	}
}

func BenchmarkLoadSaveMapsInMaps(b *testing.B) {
	doc := mapsInMapsDoc(1000)
	b.ResetTimer()
	for b.Loop() {
		data, _ := doc.Save()
		Load(data)
	}
}

func BenchmarkLoadSaveDeepHistory(b *testing.B) {
	doc := deepHistoryDoc(1000)
	b.ResetTimer()
	for b.Loop() {
		data, _ := doc.Save()
		Load(data)
	}
}
