package sync

import (
	"encoding/binary"
	"math"

	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/types"
)

const (
	bitsPerEntry = 10
	numProbes    = 7
)

// BloomFilter is a probabilistic data structure for testing set membership.
// Used in the sync protocol to summarize which changes a peer has.
type BloomFilter struct {
	NumEntries      uint32
	NumBitsPerEntry uint32
	NumProbes       uint32
	Bits            []byte
}

// NewBloomFilter creates an empty bloom filter.
func NewBloomFilter() *BloomFilter {
	return &BloomFilter{
		NumBitsPerEntry: bitsPerEntry,
		NumProbes:       numProbes,
	}
}

// BloomFromHashes creates a bloom filter containing the given change hashes.
func BloomFromHashes(hashes []types.ChangeHash) *BloomFilter {
	n := uint32(len(hashes))
	cap := bitsCapacity(n, bitsPerEntry)
	bf := &BloomFilter{
		NumEntries:      n,
		NumBitsPerEntry: bitsPerEntry,
		NumProbes:       numProbes,
		Bits:            make([]byte, cap),
	}
	for _, h := range hashes {
		bf.AddHash(h)
	}
	return bf
}

// AddHash adds a change hash to the bloom filter.
func (bf *BloomFilter) AddHash(hash types.ChangeHash) {
	probes := bf.getProbes(hash)
	for _, p := range probes {
		bf.setBit(p)
	}
}

// ContainsHash tests whether the bloom filter probably contains the given hash.
// May return false positives but never false negatives.
func (bf *BloomFilter) ContainsHash(hash types.ChangeHash) bool {
	if bf.NumEntries == 0 {
		return false
	}
	probes := bf.getProbes(hash)
	for _, p := range probes {
		if !bf.getBit(p) {
			return false
		}
	}
	return true
}

// ToBytes encodes the bloom filter to bytes.
// Empty filter returns nil.
func (bf *BloomFilter) ToBytes() []byte {
	if bf.NumEntries == 0 {
		return nil
	}
	var buf []byte
	buf = encoding.AppendULEB128(buf, uint64(bf.NumEntries))
	buf = encoding.AppendULEB128(buf, uint64(bf.NumBitsPerEntry))
	buf = encoding.AppendULEB128(buf, uint64(bf.NumProbes))
	buf = append(buf, bf.Bits...)
	return buf
}

// ParseBloom decodes a bloom filter from bytes.
// Empty input returns an empty filter.
func ParseBloom(data []byte) (*BloomFilter, error) {
	if len(data) == 0 {
		return NewBloomFilter(), nil
	}
	r := encoding.NewReader(data)

	numEntries, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	numBitsPerEntry, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	numProbes, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}

	cap := bitsCapacity(uint32(numEntries), uint32(numBitsPerEntry))
	bits, err := r.ReadBytes(cap)
	if err != nil {
		return nil, err
	}
	// Copy bits to avoid retaining reference to input
	bitsCopy := make([]byte, len(bits))
	copy(bitsCopy, bits)

	return &BloomFilter{
		NumEntries:      uint32(numEntries),
		NumBitsPerEntry: uint32(numBitsPerEntry),
		NumProbes:       uint32(numProbes),
		Bits:            bitsCopy,
	}, nil
}

// getProbes computes probe positions using double-hashing from the change hash.
func (bf *BloomFilter) getProbes(hash types.ChangeHash) []uint32 {
	modulo := uint32(8 * len(bf.Bits))
	if modulo == 0 {
		return nil
	}

	h := hash[:]
	x := binary.LittleEndian.Uint32(h[0:4]) % modulo
	y := binary.LittleEndian.Uint32(h[4:8]) % modulo
	z := binary.LittleEndian.Uint32(h[8:12]) % modulo

	probes := make([]uint32, bf.NumProbes)
	probes[0] = x
	for i := uint32(1); i < bf.NumProbes; i++ {
		x = (x + y) % modulo
		y = (y + z) % modulo
		probes[i] = x
	}
	return probes
}

func (bf *BloomFilter) setBit(probe uint32) {
	byteIdx := probe >> 3
	bitIdx := probe & 7
	if int(byteIdx) < len(bf.Bits) {
		bf.Bits[byteIdx] |= 1 << bitIdx
	}
}

func (bf *BloomFilter) getBit(probe uint32) bool {
	byteIdx := probe >> 3
	bitIdx := probe & 7
	if int(byteIdx) >= len(bf.Bits) {
		return false
	}
	return bf.Bits[byteIdx]&(1<<bitIdx) != 0
}

func bitsCapacity(numEntries, numBitsPerEntry uint32) int {
	f := math.Ceil(float64(numEntries) * float64(numBitsPerEntry) / 8.0)
	return int(f)
}
