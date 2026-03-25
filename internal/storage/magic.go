package storage

import (
	"crypto/sha256"

	"github.com/develerltd/go-automerge/internal/encoding"
)

// MagicBytes identifies an automerge binary document.
var MagicBytes = [4]byte{0x85, 0x6f, 0x4a, 0x83}

// ChunkType identifies the type of a chunk.
type ChunkType byte

const (
	ChunkTypeDocument   ChunkType = 0
	ChunkTypeChange     ChunkType = 1
	ChunkTypeCompressed ChunkType = 2
	ChunkTypeBundle     ChunkType = 3
)

func (ct ChunkType) String() string {
	switch ct {
	case ChunkTypeDocument:
		return "Document"
	case ChunkTypeChange:
		return "Change"
	case ChunkTypeCompressed:
		return "Compressed"
	case ChunkTypeBundle:
		return "Bundle"
	default:
		return "Unknown"
	}
}

// ChangeHash is a SHA-256 hash identifying a change.
type ChangeHash [32]byte

// ComputeHash computes the SHA-256 hash for a chunk with the given type and data.
// The hash covers: [chunk_type_byte, data_length_uleb128, data_bytes].
func ComputeHash(chunkType ChunkType, data []byte) ChangeHash {
	h := sha256.New()
	h.Write([]byte{byte(chunkType)})
	var lenBuf [10]byte
	n := encoding.AppendULEB128(lenBuf[:0], uint64(len(data)))
	h.Write(n)
	h.Write(data)
	var hash ChangeHash
	copy(hash[:], h.Sum(nil))
	return hash
}

// Checksum returns the first 4 bytes of a hash, used as the chunk checksum.
func Checksum(hash ChangeHash) [4]byte {
	return [4]byte{hash[0], hash[1], hash[2], hash[3]}
}
