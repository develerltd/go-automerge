package columnar

import (
	"testing"

	"github.com/develerltd/go-automerge/internal/encoding"
)

func TestParseRawColumns(t *testing.T) {
	// Build test data: 2 columns
	// Column 1: OpColAction (spec=66), data = [0x01, 0x02]
	// Column 2: OpColInsert (spec=52), data = [0x03]
	var buf []byte
	buf = encoding.AppendULEB128(buf, 2)          // column count
	buf = encoding.AppendULEB128(buf, 52)          // spec: insert (52)
	buf = encoding.AppendULEB128(buf, 1)           // length: 1
	buf = encoding.AppendULEB128(buf, 66)          // spec: action (66)
	buf = encoding.AppendULEB128(buf, 2)           // length: 2
	buf = append(buf, 0x03)                        // insert data
	buf = append(buf, 0x01, 0x02)                  // action data

	r := encoding.NewReader(buf)
	cols, err := ParseRawColumns(r)
	if err != nil {
		t.Fatalf("ParseRawColumns: %v", err)
	}
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}

	// Check first column (insert)
	if cols[0].Spec.Raw() != 52 {
		t.Errorf("col 0 spec: expected 52, got %d", cols[0].Spec.Raw())
	}
	if len(cols[0].Data) != 1 || cols[0].Data[0] != 0x03 {
		t.Errorf("col 0 data: expected [0x03], got %v", cols[0].Data)
	}

	// Check second column (action)
	if cols[1].Spec.Raw() != 66 {
		t.Errorf("col 1 spec: expected 66, got %d", cols[1].Spec.Raw())
	}
	if len(cols[1].Data) != 2 || cols[1].Data[0] != 0x01 || cols[1].Data[1] != 0x02 {
		t.Errorf("col 1 data: expected [0x01, 0x02], got %v", cols[1].Data)
	}

	// Find by spec
	found := cols.FindData(OpColAction)
	if found == nil {
		t.Fatal("FindData(OpColAction) returned nil")
	}
	if len(found) != 2 {
		t.Errorf("FindData(OpColAction) data length: expected 2, got %d", len(found))
	}
}

func TestAppendAndParseRoundtrip(t *testing.T) {
	columns := RawColumns{
		{Spec: OpColAction, Data: []byte{0x01, 0x02, 0x03}},
		{Spec: OpColInsert, Data: []byte{0xFF}},
		{Spec: OpColObjActor, Data: []byte{0x10, 0x20}},
	}

	buf := AppendRawColumns(nil, columns)

	r := encoding.NewReader(buf)
	parsed, err := ParseRawColumns(r)
	if err != nil {
		t.Fatalf("ParseRawColumns: %v", err)
	}
	if len(parsed) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(parsed))
	}

	// They should be sorted by normalized spec
	// ObjActor=1, Insert=52, Action=66
	if parsed[0].Spec.Normalized() != OpColObjActor.Normalized() {
		t.Errorf("col 0: expected ObjActor, got spec %d", parsed[0].Spec.Raw())
	}
	if parsed[1].Spec.Normalized() != OpColInsert.Normalized() {
		t.Errorf("col 1: expected Insert, got spec %d", parsed[1].Spec.Raw())
	}
	if parsed[2].Spec.Normalized() != OpColAction.Normalized() {
		t.Errorf("col 2: expected Action, got spec %d", parsed[2].Spec.Raw())
	}
}
