package hexane

import (
	"github.com/develerltd/go-automerge/internal/encoding"
)

// Write action kinds for SlabWriter.
const (
	waSlabHead = iota // slab start marker
	waSlab            // slab boundary marker (with metadata)
	waLitValue        // single packed value (in a literal run)
	waCopy            // copy bytes from source slab
	waLitHead         // literal run header (sLEB -count)
	waBoolRun         // boolean run (uLEB count)
	waRun             // value run (sLEB count + packed value)
	waNullRun         // null run (0 + uLEB count)
	waRaw             // raw bytes
)

type writeAction[T any] struct {
	kind int

	// For waRun, waLitValue:
	value T

	// For waRun, waNullRun, waBoolRun, waLitHead:
	count int64

	// For waBoolRun:
	boolVal bool

	// For waCopy, waRaw:
	source []byte
	start  int
	end    int

	// For waCopy:
	copyAcc  Acc
	copyBool int8 // -1 = none, 0 = false, 1 = true

	// For waSlab:
	slabLen   int
	slabAcc   Acc
	slabAbs   int64
	slabWidth int
}

func (a *writeAction[T]) write(out []byte, packer Packer[T]) []byte {
	switch a.kind {
	case waSlabHead, waSlab:
		// markers only, no output
	case waLitValue:
		out = packer.Pack(a.value, out)
	case waCopy:
		out = append(out, a.source[a.start:a.end]...)
	case waLitHead:
		out = encoding.AppendSLEB128(out, -a.count)
	case waBoolRun:
		out = encoding.AppendULEB128(out, uint64(a.count))
	case waRun:
		out = encoding.AppendSLEB128(out, a.count)
		out = packer.Pack(a.value, out)
	case waNullRun:
		out = append(out, 0)
		out = encoding.AppendULEB128(out, uint64(a.count))
	case waRaw:
		out = append(out, a.source[a.start:a.end]...)
	}
	return out
}

func (a *writeAction[T]) actionAcc(packer Packer[T]) Acc {
	switch a.kind {
	case waLitValue:
		return packer.ItemAgg(a.value).MulUint(1)
	case waCopy:
		return a.copyAcc
	case waBoolRun:
		if a.boolVal {
			return AccFrom(uint64(a.count))
		}
	case waRun:
		return packer.ItemAgg(a.value).MulUint(int(a.count))
	}
	return Acc{}
}

func (a *writeAction[T]) actionAbs(packer Packer[T]) int64 {
	switch a.kind {
	case waLitValue:
		return packer.Abs(a.value)
	case waRun:
		return packer.Abs(a.value) * a.count
	}
	return 0
}

func (a *writeAction[T]) copyWidth() int {
	if a.kind == waCopy {
		return a.end - a.start
	}
	return 0
}

// SlabWriter accumulates write actions and breaks them into slabs at the target byte size.
//
// This is the core output target for the RLE/Boolean/Delta/Raw encoder state machines.
// When the accumulated byte width exceeds the max, a new slab boundary is inserted.
// Call Finish() to produce the final []Slab.
type SlabWriter[T any] struct {
	packer   Packer[T]
	actions  []writeAction[T]
	width    int
	items    int
	acc      Acc
	abs      int64
	initAbs  int64
	litItems int
	litHead  int
	slabHead int
	numSlabs int
	max      int
	locked   bool
}

// NewSlabWriter creates a new SlabWriter with the given packer, max slab byte size,
// and locked flag. If locked, the writer never splits into multiple slabs.
func NewSlabWriter[T any](packer Packer[T], max int, locked bool) *SlabWriter[T] {
	sw := &SlabWriter[T]{
		packer: packer,
		max:    max,
		locked: locked,
	}
	sw.actions = append(sw.actions, writeAction[T]{kind: waSlabHead})
	return sw
}

// IsLocked returns true if the writer never splits into multiple slabs.
func (sw *SlabWriter[T]) IsLocked() bool { return sw.locked }

// Unlock allows the writer to split into multiple slabs.
func (sw *SlabWriter[T]) Unlock() { sw.locked = false }

// SetAbs sets the current absolute value (for delta encoding).
func (sw *SlabWriter[T]) SetAbs(abs int64) { sw.abs = abs }

// SetInitAbs sets the initial absolute value for the first slab.
func (sw *SlabWriter[T]) SetInitAbs(abs int64) { sw.initAbs = abs }

// IsEmpty returns true if no write actions have been accumulated.
func (sw *SlabWriter[T]) IsEmpty() bool {
	return len(sw.actions) <= 1 // first action is slabHead
}

// FlushNull writes a null run of count items.
func (sw *SlabWriter[T]) FlushNull(count int) {
	width := 1 + encoding.ULEBSize(uint64(count))
	sw.push(writeAction[T]{kind: waNullRun, count: int64(count)}, count, width)
}

// FlushLitRun writes a literal run of distinct values.
func (sw *SlabWriter[T]) FlushLitRun(values []T) {
	for _, v := range values {
		sw.pushLit(writeAction[T]{kind: waLitValue, value: v}, 1, 1)
	}
}

// FlushRun writes a value run (count repetitions of value).
func (sw *SlabWriter[T]) FlushRun(count int, value T) {
	valueWidth := sw.packer.Width(value)
	width := encoding.SLEBSize(int64(count)) + valueWidth
	sw.push(writeAction[T]{kind: waRun, count: int64(count), value: value}, count, width)
}

// FlushBoolRun writes a boolean run.
func (sw *SlabWriter[T]) FlushBoolRun(count int, value bool) {
	width := encoding.ULEBSize(uint64(count))
	sw.push(writeAction[T]{kind: waBoolRun, count: int64(count), boolVal: value}, count, width)
}

// FlushBytes writes raw bytes.
func (sw *SlabWriter[T]) FlushBytes(data []byte) {
	items := len(data)
	sw.push(writeAction[T]{kind: waRaw, source: data, start: 0, end: len(data)}, items, items)
}

// Copy copies a byte range from a source slab.
// lit is the number of items that are part of a literal run (0 if not in a lit run).
// size is the total number of items in this copy.
func (sw *SlabWriter[T]) Copy(source []byte, start, end, lit, size int, acc Acc, boolState *bool) {
	if start >= end {
		return
	}
	bs := int8(-1)
	if boolState != nil {
		if *boolState {
			bs = 1
		} else {
			bs = 0
		}
	}
	action := writeAction[T]{
		kind:     waCopy,
		source:   source,
		start:    start,
		end:      end,
		copyAcc:  acc,
		copyBool: bs,
	}
	if lit > 0 {
		sw.pushLit(action, lit, size)
	} else {
		width := end - start
		sw.push(action, size, width)
	}
}

func (sw *SlabWriter[T]) pushLit(action writeAction[T], lit, items int) {
	width := action.end - action.start
	if action.kind == waLitValue {
		width = sw.packer.Width(action.value)
	}
	if width == 0 {
		return
	}

	sw.checkCopyOverflow(action.copyWidth())

	width += headerSize(sw.litItems+lit) - headerSize(sw.litItems)
	sw.abs += action.actionAbs(sw.packer)
	sw.acc.AddAssign(action.actionAcc(sw.packer))
	sw.width += width
	sw.items += items
	if sw.litItems == 0 && lit > 0 {
		sw.litHead = len(sw.actions)
		sw.actions = append(sw.actions, writeAction[T]{kind: waLitHead})
	}
	sw.litItems += lit
	sw.actions = append(sw.actions, action)
	if items > lit {
		// copy contains non-lit-run elements at the end
		sw.closeLit()
	}
	sw.checkMax()
}

func (sw *SlabWriter[T]) push(action writeAction[T], items, width int) {
	if width == 0 {
		return
	}
	sw.checkCopyOverflow(action.copyWidth())
	sw.checkBoolState(&action)
	sw.abs += action.actionAbs(sw.packer)
	sw.acc.AddAssign(action.actionAcc(sw.packer))
	sw.width += width
	sw.items += items
	sw.closeLit()
	sw.actions = append(sw.actions, action)
	sw.checkMax()
}

func (sw *SlabWriter[T]) checkBoolState(action *writeAction[T]) {
	if sw.width == 0 {
		isBoolTrue := (action.kind == waBoolRun && action.boolVal) ||
			(action.kind == waCopy && action.copyBool == 1)
		if isBoolTrue {
			// Insert a zero-count false run before the first true run
			sw.push(writeAction[T]{kind: waBoolRun, boolVal: false}, 0, 1)
		}
	}
}

func (sw *SlabWriter[T]) closeLit() {
	if sw.litItems > 0 {
		sw.actions[sw.litHead] = writeAction[T]{kind: waLitHead, count: int64(sw.litItems)}
		sw.litItems = 0
	}
}

func (sw *SlabWriter[T]) checkMax() {
	if sw.width >= sw.max && !sw.locked {
		sw.closeLit()
		sw.closeSlab()
		sw.width = 0
		sw.acc = Acc{}
		sw.items = 0
	}
}

// ManualSlabBreak forces a slab boundary at the current position.
func (sw *SlabWriter[T]) ManualSlabBreak() {
	if sw.width > 0 {
		sw.closeLit()
		sw.closeSlab()
		sw.width = 0
		sw.acc = Acc{}
		sw.items = 0
	}
}

func (sw *SlabWriter[T]) closeSlab() {
	sw.actions[sw.slabHead] = writeAction[T]{
		kind:      waSlab,
		slabLen:   sw.items,
		slabAcc:   sw.acc,
		slabAbs:   sw.abs,
		slabWidth: sw.width,
	}
	sw.numSlabs++
	sw.slabHead = len(sw.actions)
	sw.actions = append(sw.actions, writeAction[T]{kind: waSlabHead})
}

func (sw *SlabWriter[T]) checkCopyOverflow(copyW int) {
	if sw.width+copyW > sw.max && sw.width > 0 && !sw.locked {
		sw.closeLit()
		sw.closeSlab()
		sw.width = 0
		sw.acc = Acc{}
		sw.items = 0
	}
}

// Write appends all accumulated encoded bytes to out and returns the extended slice.
// Only valid for locked (single-slab) writers.
func (sw *SlabWriter[T]) Write(out []byte) []byte {
	sw.closeLit()
	for i := range sw.actions {
		out = sw.actions[i].write(out, sw.packer)
	}
	return out
}

// Finish produces the final slabs from all accumulated write actions.
func (sw *SlabWriter[T]) Finish() []Slab {
	sw.closeLit()
	if sw.items > 0 {
		sw.closeSlab()
	}
	if sw.numSlabs == 0 {
		return nil
	}
	// Remove trailing SlabHead
	sw.actions = sw.actions[:len(sw.actions)-1]

	result := make([]Slab, 0, sw.numSlabs)
	var buffer []byte
	var slabLen int
	var slabAcc Acc
	var slabAbs int64 = sw.initAbs
	var nextAbs int64

	for i := range sw.actions {
		a := &sw.actions[i]
		switch a.kind {
		case waSlab:
			if buffer != nil {
				result = append(result, NewSlab(buffer, slabLen, slabAcc, slabAbs))
				slabAbs = nextAbs
			}
			buffer = make([]byte, 0, a.slabWidth)
			slabLen = a.slabLen
			slabAcc = a.slabAcc
			nextAbs = a.slabAbs
		default:
			buffer = a.write(buffer, sw.packer)
		}
	}
	result = append(result, NewSlab(buffer, slabLen, slabAcc, slabAbs))
	return result
}

// headerSize returns the number of bytes needed for a lit run header of the given length.
func headerSize(lit int) int {
	if lit == 0 {
		return 0
	}
	return encoding.SLEBSize(-int64(lit))
}
