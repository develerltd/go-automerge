# go-automerge

[![Go](https://github.com/develerltd/go-automerge/actions/workflows/go.yml/badge.svg)](https://github.com/develerltd/go-automerge/actions/workflows/go.yml)
[![Cross-Implementation Tests](https://github.com/develerltd/go-automerge/actions/workflows/cross-impl.yml/badge.svg)](https://github.com/develerltd/go-automerge/actions/workflows/cross-impl.yml)

A pure Go implementation of [Automerge](https://automerge.org/), a JSON-like data structure (CRDT) that can be modified concurrently by different users and merged automatically.

Binary-compatible with the [Rust automerge](https://github.com/automerge/automerge) library -- documents created by either implementation can be loaded, modified, and saved by the other.

## Status

Tracking upstream automerge **v0.7.4** ([`52b40fa`](https://github.com/automerge/automerge/commit/52b40fa5f191e7e077075b25b2436096cc23cec6)).

Supported features:

- Document creation, loading, and saving (binary format)
- Map, list, and text CRDT types
- All scalar types: string, int, uint, float64, bool, bytes, timestamp, counter, null
- Concurrent editing with automatic conflict resolution
- Sync protocol
- Historical queries (point-in-time reads)
- Cursors and marks

## Install

```bash
go get github.com/develerltd/go-automerge@v0.7.4
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

## Upstream

This is a pure Go port of [automerge/automerge](https://github.com/automerge/automerge) (Rust).
Currently tracking version **0.7.4**. No CGo required.

## License

[MIT](LICENSE)
