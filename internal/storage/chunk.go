package storage

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/encoding"
)

// Chunk represents a parsed chunk from an automerge binary.
type Chunk struct {
	Header  Header
	Type    ChunkType
	RawData []byte // the chunk data (after the header)
}

// ParsedChange holds a parsed change from a change chunk.
type ParsedChange struct {
	Hash        ChangeHash
	Deps        []ChangeHash
	Actor       []byte
	OtherActors [][]byte
	Seq         uint64
	StartOp     uint64
	Timestamp   int64
	Message     string
	OpColumns   columnar.RawColumns
	OpData      []byte
	ExtraBytes  []byte
	RawData     []byte // original raw chunk data (for sync)
}

// ParsedDocument holds a parsed document chunk.
type ParsedDocument struct {
	Hash          ChangeHash
	Actors        [][]byte
	Heads         []ChangeHash
	ChangeColumns columnar.RawColumns
	ChangeData    []byte
	OpColumns     columnar.RawColumns
	OpData        []byte
	HeadIndices   []uint64
}

// ParseChunks parses all chunks from the input data.
func ParseChunks(data []byte) ([]Chunk, error) {
	r := encoding.NewReader(data)
	var chunks []Chunk

	for !r.Done() {
		header, err := ParseHeader(r)
		if err != nil {
			return nil, fmt.Errorf("parsing chunk header at offset %d: %w", r.Offset, err)
		}

		// For compressed chunks, the stored checksum is for the uncompressed data,
		// so we can't validate it here. It's validated after decompression.
		if header.Type != ChunkTypeCompressed && !header.ChecksumValid() {
			return nil, fmt.Errorf("checksum mismatch at offset %d", r.Offset)
		}

		chunkData, err := r.ReadBytes(header.DataLen)
		if err != nil {
			return nil, fmt.Errorf("reading chunk data: %w", err)
		}

		chunks = append(chunks, Chunk{
			Header:  header,
			Type:    header.Type,
			RawData: chunkData,
		})
	}

	return chunks, nil
}

// ParseChangeChunk parses the data portion of a change chunk.
func ParseChangeChunk(data []byte, hash ChangeHash) (*ParsedChange, error) {
	r := encoding.NewReader(data)

	// Dependencies: length-prefixed array of 32-byte hashes
	deps, err := readHashArray(r)
	if err != nil {
		return nil, fmt.Errorf("reading dependencies: %w", err)
	}

	// Actor: length-prefixed bytes
	actor, err := r.ReadLenPrefixedBytes()
	if err != nil {
		return nil, fmt.Errorf("reading actor: %w", err)
	}

	// Seq
	seq, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading seq: %w", err)
	}

	// StartOp
	startOp, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading start_op: %w", err)
	}

	// Timestamp (signed)
	timestamp, err := r.ReadSLEB128()
	if err != nil {
		return nil, fmt.Errorf("reading timestamp: %w", err)
	}

	// Message: length-prefixed string
	msgLen, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading message length: %w", err)
	}
	var message string
	if msgLen > 0 {
		msgBytes, err := r.ReadBytes(int(msgLen))
		if err != nil {
			return nil, fmt.Errorf("reading message: %w", err)
		}
		message = string(msgBytes)
	}

	// Other actors: length-prefixed array of length-prefixed byte sequences
	otherActors, err := readActorArray(r)
	if err != nil {
		return nil, fmt.Errorf("reading other actors: %w", err)
	}

	// Op columns metadata + data
	opCols, err := columnar.ParseRawColumns(r)
	if err != nil {
		return nil, fmt.Errorf("reading op columns: %w", err)
	}

	// Calculate total op data length and read it
	totalOpLen := 0
	for _, col := range opCols {
		totalOpLen += len(col.Data)
	}

	// Extra bytes (whatever remains)
	var extraBytes []byte
	if !r.Done() {
		extraBytes = r.Data[r.Offset:]
	}

	return &ParsedChange{
		Hash:        hash,
		Deps:        deps,
		Actor:       actor,
		OtherActors: otherActors,
		Seq:         seq,
		StartOp:     startOp,
		Timestamp:   timestamp,
		Message:     message,
		OpColumns:   opCols,
		ExtraBytes:  extraBytes,
		RawData:     data,
	}, nil
}

// ParseDocumentChunk parses the data portion of a document chunk.
// Document chunks have a different layout than change chunks:
// [actors, heads, change_col_meta, op_col_meta, change_col_data, op_col_data, head_indices]
func ParseDocumentChunk(data []byte, hash ChangeHash) (*ParsedDocument, error) {
	r := encoding.NewReader(data)

	// Actors: length-prefixed array of length-prefixed byte sequences
	actors, err := readActorArray(r)
	if err != nil {
		return nil, fmt.Errorf("reading actors: %w", err)
	}

	// Heads: length-prefixed array of 32-byte hashes
	heads, err := readHashArray(r)
	if err != nil {
		return nil, fmt.Errorf("reading heads: %w", err)
	}

	// Change columns metadata (specs + lengths only, data comes later)
	changeColMeta, err := columnar.ParseColumnMeta(r)
	if err != nil {
		return nil, fmt.Errorf("reading change column metadata: %w", err)
	}

	// Op columns metadata (specs + lengths only, data comes later)
	opColMeta, err := columnar.ParseColumnMeta(r)
	if err != nil {
		return nil, fmt.Errorf("reading op column metadata: %w", err)
	}

	// Now read change column data
	changeCols, err := columnar.ReadColumnsData(r, changeColMeta)
	if err != nil {
		return nil, fmt.Errorf("reading change column data: %w", err)
	}

	// Then read op column data
	opCols, err := columnar.ReadColumnsData(r, opColMeta)
	if err != nil {
		return nil, fmt.Errorf("reading op column data: %w", err)
	}

	// Head indices (optional suffix)
	var headIndices []uint64
	if !r.Done() {
		headIndices = make([]uint64, 0, len(heads))
		for i := 0; i < len(heads) && !r.Done(); i++ {
			idx, err := r.ReadULEB128()
			if err != nil {
				return nil, fmt.Errorf("reading head index %d: %w", i, err)
			}
			headIndices = append(headIndices, idx)
		}
	}

	return &ParsedDocument{
		Hash:          hash,
		Actors:        actors,
		Heads:         heads,
		ChangeColumns: changeCols,
		OpColumns:     opCols,
		HeadIndices:   headIndices,
	}, nil
}

// DecompressChangeChunk decompresses a compressed change chunk and parses it.
func DecompressChangeChunk(compressedData []byte, storedChecksum [4]byte) (*ParsedChange, error) {
	// Decompress
	fr := flate.NewReader(bytes.NewReader(compressedData))
	defer fr.Close()
	decompressed, err := io.ReadAll(fr)
	if err != nil {
		return nil, fmt.Errorf("decompressing change chunk: %w", err)
	}

	// The hash is computed as if it were an uncompressed change (type=1)
	hash := ComputeHash(ChunkTypeChange, decompressed)

	// Verify checksum
	computed := Checksum(hash)
	if computed != storedChecksum {
		return nil, fmt.Errorf("compressed change checksum mismatch: stored %x, computed %x",
			storedChecksum, computed)
	}

	return ParseChangeChunk(decompressed, hash)
}

// Helper functions

func readHashArray(r *encoding.Reader) ([]ChangeHash, error) {
	count, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	hashes := make([]ChangeHash, count)
	for i := range hashes {
		hashBytes, err := r.ReadBytes(32)
		if err != nil {
			return nil, fmt.Errorf("reading hash %d: %w", i, err)
		}
		copy(hashes[i][:], hashBytes)
	}
	return hashes, nil
}

func readActorArray(r *encoding.Reader) ([][]byte, error) {
	count, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	actors := make([][]byte, count)
	for i := range actors {
		actor, err := r.ReadLenPrefixedBytes()
		if err != nil {
			return nil, fmt.Errorf("reading actor %d: %w", i, err)
		}
		// Make a copy since ReadLenPrefixedBytes returns a slice of the underlying data
		actorCopy := make([]byte, len(actor))
		copy(actorCopy, actor)
		actors[i] = actorCopy
	}
	return actors, nil
}
