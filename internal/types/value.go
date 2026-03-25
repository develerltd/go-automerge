package types

import "fmt"

// ScalarType represents the type of a scalar value.
type ScalarType uint8

const (
	ScalarTypeNull      ScalarType = 0
	ScalarTypeFalse     ScalarType = 1
	ScalarTypeTrue      ScalarType = 2
	ScalarTypeUint      ScalarType = 3
	ScalarTypeInt       ScalarType = 4
	ScalarTypeFloat64   ScalarType = 5
	ScalarTypeString    ScalarType = 6
	ScalarTypeBytes     ScalarType = 7
	ScalarTypeCounter   ScalarType = 8
	ScalarTypeTimestamp  ScalarType = 9
	ScalarTypeUnknown   ScalarType = 255
)

func (st ScalarType) String() string {
	switch st {
	case ScalarTypeNull:
		return "null"
	case ScalarTypeFalse:
		return "false"
	case ScalarTypeTrue:
		return "true"
	case ScalarTypeUint:
		return "uint"
	case ScalarTypeInt:
		return "int"
	case ScalarTypeFloat64:
		return "float64"
	case ScalarTypeString:
		return "string"
	case ScalarTypeBytes:
		return "bytes"
	case ScalarTypeCounter:
		return "counter"
	case ScalarTypeTimestamp:
		return "timestamp"
	default:
		return fmt.Sprintf("unknown(%d)", st)
	}
}

// ScalarValue represents a primitive value in an automerge document.
type ScalarValue struct {
	Typ      ScalarType
	IntVal   int64
	UintVal  uint64
	FloatVal float64
	StrVal   string
	BytesVal []byte
}

func (v ScalarValue) Type() ScalarType { return v.Typ }
func (v ScalarValue) IsNull() bool     { return v.Typ == ScalarTypeNull }
func (v ScalarValue) Bool() bool       { return v.Typ == ScalarTypeTrue }
func (v ScalarValue) Int() int64       { return v.IntVal }
func (v ScalarValue) Uint() uint64     { return v.UintVal }
func (v ScalarValue) Float64() float64 { return v.FloatVal }
func (v ScalarValue) Str() string      { return v.StrVal }
func (v ScalarValue) Bytes() []byte    { return v.BytesVal }
func (v ScalarValue) Counter() int64   { return v.IntVal }
func (v ScalarValue) Timestamp() int64 { return v.IntVal }

func NewNull() ScalarValue          { return ScalarValue{Typ: ScalarTypeNull} }
func NewBool(b bool) ScalarValue {
	if b {
		return ScalarValue{Typ: ScalarTypeTrue}
	}
	return ScalarValue{Typ: ScalarTypeFalse}
}
func NewInt(v int64) ScalarValue     { return ScalarValue{Typ: ScalarTypeInt, IntVal: v} }
func NewUint(v uint64) ScalarValue   { return ScalarValue{Typ: ScalarTypeUint, UintVal: v} }
func NewFloat64(v float64) ScalarValue { return ScalarValue{Typ: ScalarTypeFloat64, FloatVal: v} }
func NewStr(v string) ScalarValue    { return ScalarValue{Typ: ScalarTypeString, StrVal: v} }
func NewBytes(v []byte) ScalarValue {
	cp := make([]byte, len(v))
	copy(cp, v)
	return ScalarValue{Typ: ScalarTypeBytes, BytesVal: cp}
}
func NewCounter(v int64) ScalarValue   { return ScalarValue{Typ: ScalarTypeCounter, IntVal: v} }
func NewTimestamp(v int64) ScalarValue  { return ScalarValue{Typ: ScalarTypeTimestamp, IntVal: v} }
func NewUnknownScalar(typeCode uint8, data []byte) ScalarValue {
	cp := make([]byte, len(data))
	copy(cp, data)
	return ScalarValue{Typ: ScalarTypeUnknown, BytesVal: cp, IntVal: int64(typeCode)}
}

// Equal compares two scalar values for equality.
func (v ScalarValue) Equal(other ScalarValue) bool {
	if v.Typ != other.Typ {
		return false
	}
	switch v.Typ {
	case ScalarTypeNull, ScalarTypeFalse, ScalarTypeTrue:
		return true
	case ScalarTypeInt, ScalarTypeCounter, ScalarTypeTimestamp:
		return v.IntVal == other.IntVal
	case ScalarTypeUint:
		return v.UintVal == other.UintVal
	case ScalarTypeFloat64:
		return v.FloatVal == other.FloatVal
	case ScalarTypeString:
		return v.StrVal == other.StrVal
	case ScalarTypeBytes, ScalarTypeUnknown:
		if len(v.BytesVal) != len(other.BytesVal) {
			return false
		}
		for i := range v.BytesVal {
			if v.BytesVal[i] != other.BytesVal[i] {
				return false
			}
		}
		return v.IntVal == other.IntVal // for Unknown, typeCode is stored in IntVal
	default:
		return false
	}
}

func (v ScalarValue) String() string {
	switch v.Typ {
	case ScalarTypeNull:
		return "null"
	case ScalarTypeFalse:
		return "false"
	case ScalarTypeTrue:
		return "true"
	case ScalarTypeUint:
		return fmt.Sprintf("%d", v.UintVal)
	case ScalarTypeInt:
		return fmt.Sprintf("%d", v.IntVal)
	case ScalarTypeFloat64:
		return fmt.Sprintf("%g", v.FloatVal)
	case ScalarTypeString:
		return v.StrVal
	case ScalarTypeBytes:
		return fmt.Sprintf("%x", v.BytesVal)
	case ScalarTypeCounter:
		return fmt.Sprintf("counter(%d)", v.IntVal)
	case ScalarTypeTimestamp:
		return fmt.Sprintf("timestamp(%d)", v.IntVal)
	default:
		return fmt.Sprintf("unknown(%d, %x)", v.IntVal, v.BytesVal)
	}
}

// Value represents either a scalar value or a composite object reference.
type Value struct {
	IsObject bool
	ObjType  ObjType
	Scalar   ScalarValue
}

func NewObjectValue(ot ObjType) Value    { return Value{IsObject: true, ObjType: ot} }
func NewScalarValue(sv ScalarValue) Value { return Value{Scalar: sv} }

func (v Value) String() string {
	if v.IsObject {
		return v.ObjType.String()
	}
	return v.Scalar.String()
}
