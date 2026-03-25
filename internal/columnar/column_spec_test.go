package columnar

import "testing"

// Tests ported from Rust: rust/automerge/src/storage/columns/column_specification.rs
func TestColumnSpecEncoding(t *testing.T) {
	scenarios := []struct {
		id      ColumnID
		colType ColumnType
		intVal  uint32
	}{
		{7, ColumnTypeGroup, 112},
		{0, ColumnTypeActor, 1},
		{0, ColumnTypeInteger, 2},
		{1, ColumnTypeDeltaInteger, 19},
		{3, ColumnTypeBoolean, 52},
		{1, ColumnTypeString, 21},
		{5, ColumnTypeValueMetadata, 86},
		{5, ColumnTypeValue, 87},
	}

	for i, s := range scenarios {
		spec := NewColumnSpec(s.id, s.colType, false)

		if spec.Raw() != s.intVal {
			t.Fatalf("scenario %d: encoding expected %d, got %d", i+1, s.intVal, spec.Raw())
		}
		if spec.Type() != s.colType {
			t.Fatalf("scenario %d: col type expected %v, got %v", i+1, s.colType, spec.Type())
		}
		if spec.Deflate() {
			t.Fatalf("scenario %d: deflate should be false", i+1)
		}
		if spec.ID() != s.id {
			t.Fatalf("scenario %d: id expected %d, got %d", i+1, s.id, spec.ID())
		}

		// Test with deflate
		deflated := NewColumnSpec(s.id, s.colType, true)
		if deflated.ID() != spec.ID() {
			t.Fatalf("scenario %d: deflated id mismatch", i+1)
		}
		if deflated.Type() != spec.Type() {
			t.Fatalf("scenario %d: deflated type mismatch", i+1)
		}
		if !deflated.Deflate() {
			t.Fatalf("scenario %d: deflated should be true", i+1)
		}
		expected := s.intVal | 0x08
		if deflated.Raw() != expected {
			t.Fatalf("scenario %d: deflated raw expected %d, got %d", i+1, expected, deflated.Raw())
		}
		if deflated.Normalized() != spec.Normalized() {
			t.Fatalf("scenario %d: normalize test failed", i+1)
		}
	}
}

func TestWellKnownColumnValues(t *testing.T) {
	// Verify the well-known column specs have the expected raw values
	checks := []struct {
		name string
		spec ColumnSpec
		raw  uint32
	}{
		{"OpColObjActor", OpColObjActor, 1},
		{"OpColObjCtr", OpColObjCtr, 2},
		{"OpColKeyActor", OpColKeyActor, 17},
		{"OpColKeyCtr", OpColKeyCtr, 19},
		{"OpColKeyStr", OpColKeyStr, 21},
		{"OpColActor", OpColActor, 33},
		{"OpColCounter", OpColCounter, 35},
		{"OpColInsert", OpColInsert, 52},
		{"OpColAction", OpColAction, 66},
		{"OpColValueMeta", OpColValueMeta, 86},
		{"OpColValue", OpColValue, 87},
		{"OpColPredGroup", OpColPredGroup, 112},
		{"OpColPredActor", OpColPredActor, 113},
		{"OpColPredCtr", OpColPredCtr, 115},
		{"OpColSuccGroup", OpColSuccGroup, 128},
		{"OpColSuccActor", OpColSuccActor, 129},
		{"OpColSuccCtr", OpColSuccCtr, 131},
		{"OpColMarkExpand", OpColMarkExpand, 148},
		{"OpColMarkName", OpColMarkName, 165},
		{"ChgColActor", ChgColActor, 1},
		{"ChgColSeq", ChgColSeq, 3},
		{"ChgColMaxOp", ChgColMaxOp, 19},
		{"ChgColTime", ChgColTime, 35},
		{"ChgColMessage", ChgColMessage, 53},
		{"ChgColDepsGroup", ChgColDepsGroup, 64},
		{"ChgColDepsIdx", ChgColDepsIdx, 67},
		{"ChgColExtraMeta", ChgColExtraMeta, 86},
		{"ChgColExtra", ChgColExtra, 87},
	}
	for _, c := range checks {
		if c.spec.Raw() != c.raw {
			t.Errorf("%s: expected raw %d, got %d", c.name, c.raw, c.spec.Raw())
		}
	}
}
