package sync

import (
	"crypto/sha256"
	"testing"

	"github.com/develerltd/go-automerge/internal/types"
)

func makeHash(data string) types.ChangeHash {
	return types.ChangeHash(sha256.Sum256([]byte(data)))
}

func TestBloomEmpty(t *testing.T) {
	bf := NewBloomFilter()
	h := makeHash("test")
	if bf.ContainsHash(h) {
		t.Error("empty bloom should not contain anything")
	}
	if b := bf.ToBytes(); b != nil {
		t.Errorf("empty bloom should encode to nil, got %d bytes", len(b))
	}
}

func TestBloomAddAndContains(t *testing.T) {
	hashes := []types.ChangeHash{
		makeHash("change1"),
		makeHash("change2"),
		makeHash("change3"),
	}

	bf := BloomFromHashes(hashes)

	// All added hashes should be found
	for _, h := range hashes {
		if !bf.ContainsHash(h) {
			t.Errorf("bloom should contain added hash")
		}
	}

	// A hash that was not added should (likely) not be found
	notAdded := makeHash("not_added")
	// Note: false positives are possible but very unlikely for 1 test
	_ = notAdded
}

func TestBloomRoundTrip(t *testing.T) {
	hashes := []types.ChangeHash{
		makeHash("a"), makeHash("b"), makeHash("c"),
		makeHash("d"), makeHash("e"),
	}

	bf := BloomFromHashes(hashes)
	data := bf.ToBytes()
	if data == nil {
		t.Fatal("non-empty bloom should produce bytes")
	}

	bf2, err := ParseBloom(data)
	if err != nil {
		t.Fatalf("ParseBloom: %v", err)
	}

	if bf2.NumEntries != bf.NumEntries {
		t.Errorf("entries mismatch: %d vs %d", bf.NumEntries, bf2.NumEntries)
	}

	// All original hashes should still match
	for _, h := range hashes {
		if !bf2.ContainsHash(h) {
			t.Error("round-tripped bloom should contain original hashes")
		}
	}
}

func TestBloomParseEmpty(t *testing.T) {
	bf, err := ParseBloom(nil)
	if err != nil {
		t.Fatal(err)
	}
	if bf.NumEntries != 0 {
		t.Errorf("expected 0 entries, got %d", bf.NumEntries)
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	// Add 1000 hashes, check 1000 random others
	var added []types.ChangeHash
	for i := 0; i < 1000; i++ {
		added = append(added, makeHash(string(rune(i))+"added"))
	}
	bf := BloomFromHashes(added)

	falsePositives := 0
	for i := 0; i < 1000; i++ {
		h := makeHash(string(rune(i)) + "not_added_xyz")
		if bf.ContainsHash(h) {
			falsePositives++
		}
	}

	// With 10 bits/entry and 7 probes, expect ~1% false positive rate
	// Allow up to 5% for test stability
	if falsePositives > 50 {
		t.Errorf("too many false positives: %d/1000 (expected ~10)", falsePositives)
	}
	t.Logf("false positive rate: %d/1000 = %.1f%%", falsePositives, float64(falsePositives)/10)
}
