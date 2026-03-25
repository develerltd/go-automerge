package storage

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/encoding"
)

// Header represents a parsed chunk header.
type Header struct {
	ChecksumBytes [4]byte
	Type          ChunkType
	DataLen       int
	HeaderSize    int  // total header size including magic + checksum + type + length
	Hash          ChangeHash
}

// ParseHeader parses a chunk header from the reader. The reader should be positioned
// at the start of a chunk (magic bytes).
func ParseHeader(r *encoding.Reader) (Header, error) {
	startOff := r.Offset

	// Magic bytes
	magic, err := r.ReadBytes(4)
	if err != nil {
		return Header{}, fmt.Errorf("reading magic bytes: %w", err)
	}
	if magic[0] != MagicBytes[0] || magic[1] != MagicBytes[1] ||
		magic[2] != MagicBytes[2] || magic[3] != MagicBytes[3] {
		return Header{}, fmt.Errorf("invalid magic bytes: %x", magic)
	}

	// Checksum (4 bytes)
	checksumBytes, err := r.ReadBytes(4)
	if err != nil {
		return Header{}, fmt.Errorf("reading checksum: %w", err)
	}
	var checksum [4]byte
	copy(checksum[:], checksumBytes)

	// Chunk type (1 byte)
	typeByte, err := r.ReadByte()
	if err != nil {
		return Header{}, fmt.Errorf("reading chunk type: %w", err)
	}
	if typeByte > 3 {
		return Header{}, fmt.Errorf("unknown chunk type: %d", typeByte)
	}
	chunkType := ChunkType(typeByte)

	// Data length (uLEB128)
	dataLen, err := r.ReadULEB128()
	if err != nil {
		return Header{}, fmt.Errorf("reading data length: %w", err)
	}

	headerSize := r.Offset - startOff

	// Compute the hash to verify the checksum
	// We need to peek at the data to compute the hash
	if r.Offset+int(dataLen) > len(r.Data) {
		return Header{}, fmt.Errorf("chunk data extends beyond input: offset %d + len %d > %d",
			r.Offset, dataLen, len(r.Data))
	}
	data := r.Data[r.Offset : r.Offset+int(dataLen)]
	hash := ComputeHash(chunkType, data)

	return Header{
		ChecksumBytes: checksum,
		Type:          chunkType,
		DataLen:       int(dataLen),
		HeaderSize:    headerSize,
		Hash:          hash,
	}, nil
}

// ChecksumValid returns true if the stored checksum matches the computed hash.
func (h Header) ChecksumValid() bool {
	computed := Checksum(h.Hash)
	return h.ChecksumBytes == computed
}

// AppendHeader appends a chunk header to dst for the given chunk type and data.
func AppendHeader(dst []byte, chunkType ChunkType, data []byte) []byte {
	hash := ComputeHash(chunkType, data)
	checksum := Checksum(hash)

	dst = append(dst, MagicBytes[:]...)
	dst = append(dst, checksum[:]...)
	dst = append(dst, byte(chunkType))
	dst = encoding.AppendULEB128(dst, uint64(len(data)))
	return dst
}
