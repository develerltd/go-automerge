package storage

import (
	"fmt"
)

// LoadedDocument represents the result of loading an automerge binary.
type LoadedDocument struct {
	Document *ParsedDocument
	Changes  []*ParsedChange
}

// Load parses an automerge binary file into its constituent chunks.
func Load(data []byte) (*LoadedDocument, error) {
	chunks, err := ParseChunks(data)
	if err != nil {
		return nil, fmt.Errorf("parsing chunks: %w", err)
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found in input")
	}

	result := &LoadedDocument{}

	for i, chunk := range chunks {
		switch chunk.Type {
		case ChunkTypeDocument:
			if result.Document != nil {
				return nil, fmt.Errorf("multiple document chunks found")
			}
			doc, err := ParseDocumentChunk(chunk.RawData, chunk.Header.Hash)
			if err != nil {
				return nil, fmt.Errorf("parsing document chunk %d: %w", i, err)
			}
			result.Document = doc

		case ChunkTypeChange:
			change, err := ParseChangeChunk(chunk.RawData, chunk.Header.Hash)
			if err != nil {
				return nil, fmt.Errorf("parsing change chunk %d: %w", i, err)
			}
			result.Changes = append(result.Changes, change)

		case ChunkTypeCompressed:
			change, err := DecompressChangeChunk(chunk.RawData, chunk.Header.ChecksumBytes)
			if err != nil {
				return nil, fmt.Errorf("parsing compressed change chunk %d: %w", i, err)
			}
			result.Changes = append(result.Changes, change)

		case ChunkTypeBundle:
			// Bundle chunks use hexane columnar encoding, a separate format from standard
			// automerge columns. They're an optimization for compressing multiple changes
			// and are not commonly used in practice (no standard fixtures use them).
			return nil, fmt.Errorf("bundle chunks (type 0x03) are not yet supported; " +
				"save the document without bundles to use with this implementation")

		default:
			return nil, fmt.Errorf("unknown chunk type %d at chunk %d", chunk.Type, i)
		}
	}

	return result, nil
}
