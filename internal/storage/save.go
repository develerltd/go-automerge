package storage

import (
	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/encoding"
)

// SaveDocument serializes a document chunk to the automerge binary format.
func SaveDocument(actors [][]byte, heads []ChangeHash, changeCols, opCols columnar.RawColumns) []byte {
	// Build the document chunk data
	var data []byte

	// Actors table
	data = encoding.AppendULEB128(data, uint64(len(actors)))
	for _, actor := range actors {
		data = encoding.AppendULEB128(data, uint64(len(actor)))
		data = append(data, actor...)
	}

	// Heads
	data = encoding.AppendULEB128(data, uint64(len(heads)))
	for _, head := range heads {
		data = append(data, head[:]...)
	}

	// Change column metadata
	data = columnar.AppendColumnMeta(data, changeCols)

	// Op column metadata
	data = columnar.AppendColumnMeta(data, opCols)

	// Change column data
	data = columnar.AppendColumnData(data, changeCols)

	// Op column data
	data = columnar.AppendColumnData(data, opCols)

	// Build the full binary: header + data
	var out []byte
	out = AppendHeader(out, ChunkTypeDocument, data)
	out = append(out, data...)

	return out
}

// AppendChangeChunkData builds the data portion of a change chunk (without the header).
// This is used for computing the change hash.
func AppendChangeChunkData(
	deps []ChangeHash,
	actor []byte,
	seq uint64,
	startOp uint64,
	timestamp int64,
	message string,
	otherActors [][]byte,
	opCols columnar.RawColumns,
) []byte {
	var data []byte

	// Dependencies
	data = encoding.AppendULEB128(data, uint64(len(deps)))
	for _, dep := range deps {
		data = append(data, dep[:]...)
	}

	// Actor (length-prefixed)
	data = encoding.AppendULEB128(data, uint64(len(actor)))
	data = append(data, actor...)

	// Seq
	data = encoding.AppendULEB128(data, seq)

	// StartOp
	data = encoding.AppendULEB128(data, startOp)

	// Timestamp (signed LEB128)
	data = encoding.AppendSLEB128(data, timestamp)

	// Message (length-prefixed, 0 for empty)
	if message == "" {
		data = encoding.AppendULEB128(data, 0)
	} else {
		data = encoding.AppendULEB128(data, uint64(len(message)))
		data = append(data, message...)
	}

	// Other actors
	data = encoding.AppendULEB128(data, uint64(len(otherActors)))
	for _, a := range otherActors {
		data = encoding.AppendULEB128(data, uint64(len(a)))
		data = append(data, a...)
	}

	// Op columns (metadata + data inline for change format)
	data = columnar.AppendRawColumns(data, opCols)

	return data
}
