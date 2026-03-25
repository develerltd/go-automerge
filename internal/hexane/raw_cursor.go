package hexane

// rawLoad creates a single slab from raw bytes.
// For raw cursors, each byte is one item.
func rawLoad(data []byte) ([]Slab, int, error) {
	length := len(data)
	if length == 0 {
		return nil, 0, nil
	}
	dataCopy := make([]byte, length)
	copy(dataCopy, data)
	slab := NewSlab(dataCopy, length, Acc{}, 0)
	return []Slab{slab}, length, nil
}

// rawSplice performs a splice within a raw byte slab.
func rawSplice(slab *Slab, index, del int, values [][]byte, slabSize int) SpliceResult {
	data := slab.Bytes()

	overflow := 0
	actualDel := del
	if index+del > len(data) {
		overflow = index + del - len(data)
		actualDel = len(data) - index
	}

	// Build new data
	var newData []byte
	newData = append(newData, data[:index]...)
	totalAdd := 0
	for _, v := range values {
		newData = append(newData, v...)
		totalAdd += len(v)
	}
	newData = append(newData, data[index+actualDel:]...)

	// Split into slabs
	var slabs []Slab
	for len(newData) > 0 {
		size := slabSize
		if size > len(newData) {
			size = len(newData)
		}
		chunk := make([]byte, size)
		copy(chunk, newData[:size])
		slabs = append(slabs, NewSlab(chunk, size, Acc{}, 0))
		newData = newData[size:]
	}
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}

	return SpliceResult{
		Add:      totalAdd,
		Del:      actualDel,
		Overflow: overflow,
		Slabs:    slabs,
	}
}

// rawEncode encodes raw byte values to output bytes.
func rawEncode(values [][]byte) []byte {
	var out []byte
	for _, v := range values {
		out = append(out, v...)
	}
	return out
}

// rawInitEmpty creates an empty raw slab. Raw columns don't have null representation.
func rawInitEmpty(_ int) Slab {
	return Slab{}
}
