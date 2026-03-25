package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixturesDir returns the path to the automerge Rust test fixtures.
func fixturesDir() string {
	return filepath.Join("..", "..", "..", "automerge", "rust", "automerge", "tests", "fixtures")
}

// fuzzCrashersDir returns the path to the fuzz crasher fixtures.
func fuzzCrashersDir() string {
	return filepath.Join("..", "..", "..", "automerge", "rust", "automerge", "tests", "fuzz-crashers")
}

// interopDir returns the path to the interop directory.
func interopDir() string {
	return filepath.Join("..", "..", "..", "automerge", "interop")
}

func TestLoadStandardFixtures(t *testing.T) {
	dir := fixturesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skipf("fixtures directory not found at %s: %v", dir, err)
	}

	loaded := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".automerge") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatalf("reading fixture: %v", err)
			}
			doc, err := Load(data)
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}
			if doc.Document == nil && len(doc.Changes) == 0 {
				t.Fatal("loaded document has no document chunk and no changes")
			}
			t.Logf("loaded %s: doc=%v, changes=%d",
				entry.Name(), doc.Document != nil, len(doc.Changes))
		})
		loaded++
	}
	if loaded == 0 {
		t.Skip("no .automerge fixtures found")
	}
}

func TestLoadExemplar(t *testing.T) {
	path := filepath.Join(interopDir(), "exemplar")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("exemplar file not found at %s: %v", path, err)
	}

	doc, err := Load(data)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if doc.Document == nil && len(doc.Changes) == 0 {
		t.Fatal("loaded exemplar has no document chunk and no changes")
	}

	// If there's a document chunk, verify basic structure
	if doc.Document != nil {
		d := doc.Document
		t.Logf("exemplar document: actors=%d, heads=%d, change_cols=%d, op_cols=%d",
			len(d.Actors), len(d.Heads), len(d.ChangeColumns), len(d.OpColumns))
		if len(d.Actors) == 0 {
			t.Error("expected at least one actor")
		}
		if len(d.Heads) == 0 {
			t.Error("expected at least one head")
		}
	}

	t.Logf("exemplar: doc=%v, changes=%d", doc.Document != nil, len(doc.Changes))
}

func TestLoadFuzzCrashers(t *testing.T) {
	dir := fuzzCrashersDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skipf("fuzz-crashers directory not found at %s: %v", dir, err)
	}

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".automerge") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatalf("reading fixture: %v", err)
			}
			// Fuzz crashers may load or may error — either is acceptable.
			// They must NOT panic.
			doc, err := Load(data)
			if err != nil {
				t.Logf("fuzz crasher %s: clean error: %v", entry.Name(), err)
			} else {
				t.Logf("fuzz crasher %s: loaded OK (doc=%v, changes=%d)",
					entry.Name(), doc.Document != nil, len(doc.Changes))
			}
		})
	}
}
