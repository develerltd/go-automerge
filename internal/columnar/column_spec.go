package columnar

import "fmt"

// ColumnType represents the encoding type of a column.
type ColumnType uint8

const (
	ColumnTypeGroup         ColumnType = 0
	ColumnTypeActor         ColumnType = 1
	ColumnTypeInteger       ColumnType = 2 // uLEB encoded
	ColumnTypeDeltaInteger  ColumnType = 3
	ColumnTypeBoolean       ColumnType = 4
	ColumnTypeString        ColumnType = 5
	ColumnTypeValueMetadata ColumnType = 6
	ColumnTypeValue         ColumnType = 7
)

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeGroup:
		return "Group"
	case ColumnTypeActor:
		return "Actor"
	case ColumnTypeInteger:
		return "Integer"
	case ColumnTypeDeltaInteger:
		return "DeltaInteger"
	case ColumnTypeBoolean:
		return "Boolean"
	case ColumnTypeString:
		return "String"
	case ColumnTypeValueMetadata:
		return "ValueMetadata"
	case ColumnTypeValue:
		return "Value"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}

// ColumnID is the logical column identifier (upper 28 bits of the spec).
type ColumnID uint32

// ColumnSpec is a 32-bit value encoding a column's ID, type, and deflate flag.
// Layout: [column_id (28 bits) << 4 | deflate_bit << 3 | type_bits (3 bits)]
type ColumnSpec uint32

// NewColumnSpec creates a column specification.
func NewColumnSpec(id ColumnID, colType ColumnType, deflate bool) ColumnSpec {
	raw := uint32(id) << 4
	raw |= uint32(colType) & 0x07
	if deflate {
		raw |= 0x08
	}
	return ColumnSpec(raw)
}

// ID returns the column ID.
func (cs ColumnSpec) ID() ColumnID {
	return ColumnID(uint32(cs) >> 4)
}

// Type returns the column type.
func (cs ColumnSpec) Type() ColumnType {
	return ColumnType(uint32(cs) & 0x07)
}

// Deflate returns true if the column is DEFLATE-compressed.
func (cs ColumnSpec) Deflate() bool {
	return uint32(cs)&0x08 != 0
}

// Deflated returns a copy of this spec with the deflate bit set.
func (cs ColumnSpec) Deflated() ColumnSpec {
	return NewColumnSpec(cs.ID(), cs.Type(), true)
}

// Inflated returns a copy of this spec with the deflate bit cleared.
func (cs ColumnSpec) Inflated() ColumnSpec {
	return NewColumnSpec(cs.ID(), cs.Type(), false)
}

// Normalized returns the spec with the deflate bit cleared, used for ordering.
func (cs ColumnSpec) Normalized() uint32 {
	return uint32(cs) & 0xFFFFFFF7 // clear bit 3
}

// Raw returns the raw uint32 value.
func (cs ColumnSpec) Raw() uint32 {
	return uint32(cs)
}

func (cs ColumnSpec) String() string {
	return fmt.Sprintf("ColumnSpec(id: %d, type: %s, deflate: %v)", cs.ID(), cs.Type(), cs.Deflate())
}

// Well-known operation column specs (non-deflated).
var (
	OpColObjActor   = NewColumnSpec(0, ColumnTypeActor, false)   // 1
	OpColObjCtr     = NewColumnSpec(0, ColumnTypeInteger, false) // 2
	OpColKeyActor   = NewColumnSpec(1, ColumnTypeActor, false)   // 17
	OpColKeyCtr     = NewColumnSpec(1, ColumnTypeDeltaInteger, false) // 19
	OpColKeyStr     = NewColumnSpec(1, ColumnTypeString, false)  // 21
	OpColActor      = NewColumnSpec(2, ColumnTypeActor, false)   // 33
	OpColCounter    = NewColumnSpec(2, ColumnTypeDeltaInteger, false) // 35
	OpColInsert     = NewColumnSpec(3, ColumnTypeBoolean, false) // 52
	OpColAction     = NewColumnSpec(4, ColumnTypeInteger, false) // 66
	OpColValueMeta  = NewColumnSpec(5, ColumnTypeValueMetadata, false) // 86
	OpColValue      = NewColumnSpec(5, ColumnTypeValue, false)   // 87
	OpColPredGroup  = NewColumnSpec(7, ColumnTypeGroup, false)   // 112
	OpColPredActor  = NewColumnSpec(7, ColumnTypeActor, false)   // 113
	OpColPredCtr    = NewColumnSpec(7, ColumnTypeDeltaInteger, false) // 115
	OpColSuccGroup  = NewColumnSpec(8, ColumnTypeGroup, false)   // 128
	OpColSuccActor  = NewColumnSpec(8, ColumnTypeActor, false)   // 129
	OpColSuccCtr    = NewColumnSpec(8, ColumnTypeDeltaInteger, false) // 131
	OpColMarkExpand = NewColumnSpec(9, ColumnTypeBoolean, false) // 148
	OpColMarkName   = NewColumnSpec(10, ColumnTypeString, false) // 165
)

// Well-known change column specs (non-deflated).
var (
	ChgColActor     = NewColumnSpec(0, ColumnTypeActor, false)        // 1
	ChgColSeq       = NewColumnSpec(0, ColumnTypeDeltaInteger, false) // 3
	ChgColMaxOp     = NewColumnSpec(1, ColumnTypeDeltaInteger, false) // 19
	ChgColTime      = NewColumnSpec(2, ColumnTypeDeltaInteger, false) // 35
	ChgColMessage   = NewColumnSpec(3, ColumnTypeString, false)       // 53
	ChgColDepsGroup = NewColumnSpec(4, ColumnTypeGroup, false)        // 64
	ChgColDepsIdx   = NewColumnSpec(4, ColumnTypeDeltaInteger, false) // 67
	ChgColExtraMeta = NewColumnSpec(5, ColumnTypeValueMetadata, false) // 86
	ChgColExtra     = NewColumnSpec(5, ColumnTypeValue, false)        // 87
)
