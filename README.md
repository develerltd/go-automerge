# go-automerge

[![Go](https://github.com/develerltd/go-automerge/actions/workflows/go.yml/badge.svg)](https://github.com/develerltd/go-automerge/actions/workflows/go.yml)
[![Cross-Implementation Tests](https://github.com/develerltd/go-automerge/actions/workflows/cross-impl.yml/badge.svg)](https://github.com/develerltd/go-automerge/actions/workflows/cross-impl.yml)

A pure Go implementation of [Automerge](https://automerge.org/), a JSON-like data structure (CRDT) that can be modified concurrently by different users and merged automatically.

Binary-compatible with the [Rust automerge](https://github.com/automerge/automerge) library -- documents created by either implementation can be loaded, modified, and saved by the other.

## Status

Tracking upstream automerge **v0.8.0** ([`8246d0f`](https://github.com/automerge/automerge/commit/8246d0f8218cd49dc1af56ba6eefd88ed215665c)).

Supported features:

- Document creation, loading, and saving (binary format)
- Map, list, and text CRDT types
- All scalar types: string, int, uint, float64, bool, bytes, timestamp, counter, null
- Concurrent editing with automatic conflict resolution
- Batch insertion of nested objects (v0.8.0)
- Sync protocol
- Historical queries (point-in-time reads)
- Cursors and marks

## Install

```bash
go get github.com/develerltd/go-automerge@v0.8.0
```

Requires Go 1.24 or later.

## Usage

```go
package main

import (
    "fmt"

    "github.com/develerltd/go-automerge/automerge"
)

func main() {
    // Create a new document
    doc := automerge.New()

    // Put values into the root map
    doc.Put(automerge.Root, automerge.MapProp("title"), automerge.NewStr("Hello"))
    doc.Put(automerge.Root, automerge.MapProp("count"), automerge.NewInt(42))
    doc.Commit("initial", 0)

    // Save to bytes (binary format, compatible with Rust automerge)
    data := doc.Save()

    // Load from bytes
    doc2, err := automerge.Load(data)
    if err != nil {
        panic(err)
    }

    val, _, err := doc2.Get(automerge.Root, automerge.MapProp("title"))
    if err != nil {
        panic(err)
    }
    fmt.Println(val.Str()) // "Hello"
}
```

## Compatibility

Cross-implementation tests verify binary compatibility with the Rust automerge library:

- Loading Rust-generated fixtures in Go (10 fixture types covering all features)
- Round-trip: Load -> Save -> Load produces identical documents
- Double round-trip: Save -> Load -> Save produces identical bytes
- Go-created documents are loadable by the Rust CLI

CI runs these tests weekly against the pinned upstream commit to detect drift.

## Architecture

| Package            | Description                          |
|--------------------|--------------------------------------|
| `automerge`        | Public API (Doc, AutoCommit, types)  |
| `internal/opset`   | Operation set and CRDT logic         |
| `internal/columnar`| Columnar encoding format             |
| `internal/storage` | Binary save/load                     |
| `internal/encoding`| LEB128, RLE, delta encoders          |
| `internal/hexane`  | Span tree and compressed slab store  |
| `internal/types`   | Shared type definitions              |
| `internal/sync`    | Sync protocol                        |

## Performance

Benchmarks run on an Intel i9-13900HX (linux/amd64). Go uses per-object B-trees
for the mutable OpSet, matching the Rust architecture. Rust benchmarks use
criterion; Go benchmarks use `testing.B` with `-count=3`.

```bash
go test ./automerge/ -bench=. -benchmem
```

### Map operations (create document with n ops)

| Benchmark | n | Rust | Go | Go/Rust |
|-----------|---|------|-----|---------|
| Repeated put | 100 | 623 µs | 164 µs | **0.26x** |
| | 1,000 | 6.67 ms | 2.23 ms | **0.33x** |
| | 10,000 | 68.5 ms | 28.9 ms | **0.42x** |
| Repeated increment | 100 | 660 µs | 163 µs | **0.25x** |
| | 1,000 | 9.77 ms | 2.15 ms | **0.22x** |
| | 10,000 | 431 ms | 28.4 ms | **0.07x** |
| Increasing put | 100 | 657 µs | 93 µs | **0.14x** |
| | 1,000 | 7.25 ms | 1.31 ms | **0.18x** |
| | 10,000 | 83.7 ms | 15.6 ms | **0.19x** |
| Decreasing put | 100 | 513 µs | 85 µs | **0.17x** |
| | 1,000 | 5.49 ms | 1.26 ms | **0.23x** |
| | 10,000 | 56.1 ms | 14.1 ms | **0.25x** |

Go is **2-15x faster** than Rust for all mutation benchmarks.

### Save (serialize to bytes)

| Benchmark | n | Rust | Go | Go/Rust |
|-----------|---|------|-----|---------|
| Repeated put | 100 | 4.8 µs | 14.4 µs | 3.0x |
| | 1,000 | 42.5 µs | 159 µs | 3.7x |
| | 10,000 | 273 µs | 1.16 ms | 4.3x |
| Increasing put | 100 | 20.0 µs | 17.9 µs | **0.9x** |
| | 1,000 | 200 µs | 238 µs | 1.2x |
| | 10,000 | 2.50 ms | 1.51 ms | **0.6x** |

Save is 1-4x slower for repeated-put (materializing all ops from trees), but
comparable or faster for increasing-put at scale.

### Load (deserialize from bytes)

| Benchmark | n | Rust | Go | Go/Rust |
|-----------|---|------|-----|---------|
| Repeated put | 100 | 40.2 µs | 100 µs | 2.5x |
| | 1,000 | 278 µs | 1.12 ms | 4.0x |
| | 10,000 | 2.54 ms | 11.8 ms | 4.7x |
| Increasing put | 100 | 56.6 µs | 126 µs | 2.2x |
| | 1,000 | 476 µs | 175 µs | **0.37x** |
| | 10,000 | 4.06 ms | 1.48 ms | **0.36x** |

Load is 2-5x slower for repeated-put (many B-tree inserts into one object), but
**2-3x faster** for increasing-put at scale (many small per-object trees).

### Load+Save document patterns (n=1000)

| Benchmark | Rust | Go | Go/Rust |
|-----------|------|-----|---------|
| Big paste | 37.2 µs | 74.4 µs | 2.0x |
| Typing doc | 1.02 ms | 1.43 ms | 1.4x |
| Maps in maps | 483 µs | 2.19 ms | 4.5x |
| Deep history | 1.47 ms | 2.91 ms | 2.0x |

### Batch insertion

`BatchCreateObject` and `InitFromHydrate` use BFS traversal with bulk OpSet
insertion. For a structure with 50 maps each containing 3 scalars and a
2-element list (~300 ops total):

| Method | Time | Allocs | Memory |
|--------|------|--------|--------|
| `InitFromHydrate` (batch) | **269 µs** | 1.9 K | 1.1 MB |
| Individual put/insert calls | 654 µs | 7.4 K | 1.3 MB |

**~2.4x faster, ~3.9x fewer allocations.**

See `automerge/bench_test.go` for the full benchmark suite.

## Upstream

This is a pure Go port of [automerge/automerge](https://github.com/automerge/automerge) (Rust).
Currently tracking version **0.8.0**. No CGo required.

## License

[MIT](LICENSE)
