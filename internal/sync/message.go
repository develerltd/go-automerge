package sync

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/types"
)

const (
	MessageV1Type    byte = 0x42
	MessageV2Type    byte = 0x43
	SyncStateType    byte = 0x43
	CapabilityV1     byte = 0x01
	CapabilityV2     byte = 0x02
)

// Have represents what a peer has: a baseline (last_sync heads) and a bloom filter
// of all changes since that baseline.
type Have struct {
	LastSync []types.ChangeHash
	Bloom    *BloomFilter
}

// Message is a sync protocol message exchanged between peers.
type Message struct {
	Heads   []types.ChangeHash
	Need    []types.ChangeHash
	Have    []Have
	Changes [][]byte // raw change chunk bytes
	Version byte     // MessageV1Type or MessageV2Type
}

// Encode serializes the message to bytes.
func (m *Message) Encode() []byte {
	version := m.Version
	if version == 0 {
		version = MessageV1Type
	}

	var buf []byte
	buf = append(buf, version)
	buf = encodeHashes(buf, m.Heads)
	buf = encodeHashes(buf, m.Need)

	// Have array
	buf = encoding.AppendULEB128(buf, uint64(len(m.Have)))
	for _, h := range m.Have {
		buf = encodeHashes(buf, h.LastSync)
		bloomBytes := h.Bloom.ToBytes()
		buf = encoding.AppendULEB128(buf, uint64(len(bloomBytes)))
		buf = append(buf, bloomBytes...)
	}

	// Changes
	buf = encoding.AppendULEB128(buf, uint64(len(m.Changes)))
	for _, change := range m.Changes {
		buf = encoding.AppendULEB128(buf, uint64(len(change)))
		buf = append(buf, change...)
	}

	return buf
}

// DecodeMessage parses a sync message from bytes.
func DecodeMessage(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	r := encoding.NewReader(data)

	version, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading version: %w", err)
	}
	if version != MessageV1Type && version != MessageV2Type {
		return nil, fmt.Errorf("unknown message version: 0x%02x", version)
	}

	heads, err := decodeHashes(r)
	if err != nil {
		return nil, fmt.Errorf("reading heads: %w", err)
	}

	need, err := decodeHashes(r)
	if err != nil {
		return nil, fmt.Errorf("reading need: %w", err)
	}

	// Have array
	haveCount, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading have count: %w", err)
	}
	have := make([]Have, haveCount)
	for i := range have {
		lastSync, err := decodeHashes(r)
		if err != nil {
			return nil, fmt.Errorf("reading have[%d] last_sync: %w", i, err)
		}
		bloomLen, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading have[%d] bloom length: %w", i, err)
		}
		var bloom *BloomFilter
		if bloomLen == 0 {
			bloom = NewBloomFilter()
		} else {
			bloomData, err := r.ReadBytes(int(bloomLen))
			if err != nil {
				return nil, fmt.Errorf("reading have[%d] bloom data: %w", i, err)
			}
			bloom, err = ParseBloom(bloomData)
			if err != nil {
				return nil, fmt.Errorf("parsing have[%d] bloom: %w", i, err)
			}
		}
		have[i] = Have{LastSync: lastSync, Bloom: bloom}
	}

	// Changes
	changesCount, err := r.ReadULEB128()
	if err != nil {
		return nil, fmt.Errorf("reading changes count: %w", err)
	}
	changes := make([][]byte, changesCount)
	for i := range changes {
		chLen, err := r.ReadULEB128()
		if err != nil {
			return nil, fmt.Errorf("reading change[%d] length: %w", i, err)
		}
		chData, err := r.ReadBytes(int(chLen))
		if err != nil {
			return nil, fmt.Errorf("reading change[%d] data: %w", i, err)
		}
		cp := make([]byte, len(chData))
		copy(cp, chData)
		changes[i] = cp
	}

	return &Message{
		Heads:   heads,
		Need:    need,
		Have:    have,
		Changes: changes,
		Version: version,
	}, nil
}

func encodeHashes(dst []byte, hashes []types.ChangeHash) []byte {
	dst = encoding.AppendULEB128(dst, uint64(len(hashes)))
	for _, h := range hashes {
		dst = append(dst, h[:]...)
	}
	return dst
}

func decodeHashes(r *encoding.Reader) ([]types.ChangeHash, error) {
	count, err := r.ReadULEB128()
	if err != nil {
		return nil, err
	}
	hashes := make([]types.ChangeHash, count)
	for i := range hashes {
		data, err := r.ReadBytes(32)
		if err != nil {
			return nil, fmt.Errorf("reading hash %d: %w", i, err)
		}
		copy(hashes[i][:], data)
	}
	return hashes, nil
}
