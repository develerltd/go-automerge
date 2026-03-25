package hexane

// B-tree branching factor. Each node holds up to 2B-1 elements and 2B children.
const btreeOrder = 16

// SpanTree is a weighted B-tree that maintains a sequence of elements with
// associated per-item and cumulative weights/aggregates.
//
// The tree supports O(log n) insert, remove, replace, get, and weighted search.
// It is generic over element type T and weight type W.
type SpanTree[T, W any] struct {
	root     *treeNode[T, W]
	weighter Weighter[T, W]
}

type treeNode[T, W any] struct {
	elements []T
	children []*treeNode[T, W]
	length   int
	weight   W
}

// SubCursor represents a position in the sequence with its cumulative weight.
type SubCursor[T, W any] struct {
	Index   int
	Weight  W
	Element *T
}

// NewSpanTree creates an empty SpanTree with the given weighter.
func NewSpanTree[T, W any](w Weighter[T, W]) *SpanTree[T, W] {
	return &SpanTree[T, W]{weighter: w}
}

// LoadSpanTree builds a SpanTree from a slice of elements.
func LoadSpanTree[T, W any](w Weighter[T, W], elements []T) *SpanTree[T, W] {
	t := NewSpanTree[T, W](w)
	for i := range elements {
		t.Push(elements[i])
	}
	return t
}

// Len returns the total number of elements.
func (t *SpanTree[T, W]) Len() int {
	if t.root == nil {
		return 0
	}
	return t.root.length
}

// IsEmpty returns true if the tree has no elements.
func (t *SpanTree[T, W]) IsEmpty() bool { return t.Len() == 0 }

// Weight returns the total accumulated weight, or nil if empty.
func (t *SpanTree[T, W]) Weight() *W {
	if t.root == nil {
		return nil
	}
	return &t.root.weight
}

// Get returns a pointer to the element at index, or nil if out of bounds.
func (t *SpanTree[T, W]) Get(index int) *T {
	if t.root == nil {
		return nil
	}
	return t.root.get(index)
}

// GetCursor returns the element at index with its cumulative weight.
func (t *SpanTree[T, W]) GetCursor(index int) *SubCursor[T, W] {
	if t.root == nil {
		return nil
	}
	return t.root.getCursor(0, index, t.weighter.Zero, &t.weighter)
}

// Last returns the last element, or nil if empty.
func (t *SpanTree[T, W]) Last() *T {
	if t.root == nil {
		return nil
	}
	return t.root.last()
}

// GetLastCursor returns the last element with its cumulative weight.
func (t *SpanTree[T, W]) GetLastCursor() *SubCursor[T, W] {
	if t.IsEmpty() {
		return nil
	}
	elem := t.Last()
	weight := t.root.lastWeight(&t.weighter)
	return &SubCursor[T, W]{Index: t.Len() - 1, Weight: weight, Element: elem}
}

// Push appends an element to the end.
func (t *SpanTree[T, W]) Push(element T) {
	t.Insert(t.Len(), element)
}

// Insert inserts an element at the given index. Panics if index > Len().
func (t *SpanTree[T, W]) Insert(index int, element T) {
	w := &t.weighter
	weight := w.Alloc(&element)

	if t.root == nil {
		t.root = &treeNode[T, W]{
			elements: []T{element},
			length:   1,
			weight:   weight,
		}
		return
	}

	root := t.root
	if root.isFull() {
		newRoot := &treeNode[T, W]{weight: w.Zero}
		newRoot.length += root.length
		w.Union(&newRoot.weight, root.weight)
		newRoot.children = append(newRoot.children, root)
		t.root = newRoot
		newRoot.splitChild(0, w)

		firstChildLen := newRoot.children[0].length
		var child *treeNode[T, W]
		var insertionIndex int
		if firstChildLen < index {
			child = newRoot.children[1]
			insertionIndex = index - (firstChildLen + 1)
		} else {
			child = newRoot.children[0]
			insertionIndex = index
		}
		newRoot.length++
		w.Union(&newRoot.weight, weight)
		child.insertIntoNonFull(insertionIndex, element, w)
	} else {
		root.insertIntoNonFull(index, element, w)
	}
}

// Remove removes and returns the element at index. Panics if empty or out of bounds.
func (t *SpanTree[T, W]) Remove(index int) T {
	if t.root == nil {
		panic("remove from empty tree")
	}
	w := &t.weighter
	old := t.root.remove(index, w)

	if len(t.root.elements) == 0 {
		if t.root.isLeaf() {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
	}
	return old
}

// Replace replaces the element at index and returns the old value.
func (t *SpanTree[T, W]) Replace(index int, element T) T {
	if t.root == nil {
		panic("replace in empty tree")
	}
	return t.root.replace(index, element, &t.weighter)
}

// Splice replaces elements in [start, end) with values.
func (t *SpanTree[T, W]) Splice(start, end int, values []T) {
	if end > t.Len() {
		end = t.Len()
	}
	idx := start
	toDelete := end - start
	vi := 0
	for toDelete > 0 {
		if vi < len(values) {
			t.Replace(idx, values[vi])
			idx++
			vi++
		} else {
			t.Remove(idx)
		}
		toDelete--
	}
	for vi < len(values) {
		t.Insert(idx, values[vi])
		idx++
		vi++
	}
}

// ToSlice returns all elements as a slice.
func (t *SpanTree[T, W]) ToSlice() []T {
	if t.root == nil {
		return nil
	}
	result := make([]T, 0, t.Len())
	t.root.collect(&result)
	return result
}

// GetWhere finds the first element where f(accumulatedWeight, nextWeight) is true.
// The search prunes branches using the weight predicate for O(log n) performance.
func (t *SpanTree[T, W]) GetWhere(f func(accW, nextW W) bool) *SubCursor[T, W] {
	iter := t.IterWhere(f)
	return iter.Next()
}

// GetWhereOrLast returns GetWhere result, or the last element if none matches.
func (t *SpanTree[T, W]) GetWhereOrLast(f func(accW, nextW W) bool) *SubCursor[T, W] {
	if c := t.GetWhere(f); c != nil {
		return c
	}
	return t.GetLastCursor()
}

// --- treeNode methods ---

func (n *treeNode[T, W]) isLeaf() bool { return len(n.children) == 0 }

func (n *treeNode[T, W]) isFull() bool { return len(n.elements) >= 2*btreeOrder-1 }

func (n *treeNode[T, W]) get(index int) *T {
	if n.isLeaf() {
		if index < len(n.elements) {
			return &n.elements[index]
		}
		return nil
	}
	cumLen := 0
	for ci, child := range n.children {
		if cumLen+child.length > index {
			return child.get(index - cumLen)
		}
		if cumLen+child.length == index {
			if ci < len(n.elements) {
				return &n.elements[ci]
			}
			return nil
		}
		cumLen += child.length + 1
	}
	return nil
}

func (n *treeNode[T, W]) last() *T {
	if n.isLeaf() {
		if len(n.elements) == 0 {
			return nil
		}
		return &n.elements[len(n.elements)-1]
	}
	return n.children[len(n.children)-1].last()
}

func (n *treeNode[T, W]) lastWeight(w *Weighter[T, W]) W {
	weight := w.Zero
	if n.isLeaf() {
		for i := 0; i < len(n.elements)-1; i++ {
			w.Union(&weight, w.Alloc(&n.elements[i]))
		}
		return weight
	}
	// All elements
	for i := range n.elements {
		w.Union(&weight, w.Alloc(&n.elements[i]))
	}
	// All children except last, plus last.lastWeight
	for i := 0; i < len(n.children)-1; i++ {
		w.Union(&weight, n.children[i].weight)
	}
	lastChildW := n.children[len(n.children)-1].lastWeight(w)
	return w.And(weight, lastChildW)
}

func (n *treeNode[T, W]) getCursor(current, index int, acc W, w *Weighter[T, W]) *SubCursor[T, W] {
	if n.isLeaf() {
		for i := range n.elements {
			if current == index {
				return &SubCursor[T, W]{Index: index, Weight: acc, Element: &n.elements[i]}
			}
			current++
			w.Union(&acc, w.Alloc(&n.elements[i]))
		}
	} else {
		for i := 0; i < len(n.children); i++ {
			child := n.children[i]
			if current+child.length > index {
				return child.getCursor(current, index, acc, w)
			}
			current += child.length
			w.Union(&acc, child.weight)
			if i < len(n.elements) {
				if current == index {
					return &SubCursor[T, W]{Index: index, Weight: acc, Element: &n.elements[i]}
				}
				current++
				w.Union(&acc, w.Alloc(&n.elements[i]))
			}
		}
	}
	return nil
}

func (n *treeNode[T, W]) findChildIndex(index int) (childIdx, subIdx int) {
	cumLen := 0
	for ci, child := range n.children {
		if cumLen+child.length >= index {
			return ci, index - cumLen
		}
		cumLen += child.length + 1
	}
	panic("index not found in node")
}

func (n *treeNode[T, W]) insertIntoNonFull(index int, element T, w *Weighter[T, W]) {
	if n.isLeaf() {
		n.length++
		elemW := w.Alloc(&element)
		w.Union(&n.weight, elemW)
		// Insert into elements slice
		n.elements = append(n.elements, element) // grow
		copy(n.elements[index+1:], n.elements[index:len(n.elements)-1])
		n.elements[index] = element
	} else {
		ci, si := n.findChildIndex(index)
		elemW := w.Alloc(&element)
		child := n.children[ci]
		if child.isFull() {
			n.splitChild(ci, w)
			ci, si = n.findChildIndex(index)
			child = n.children[ci]
			child.insertIntoNonFull(si, element, w)
		} else {
			child.insertIntoNonFull(si, element, w)
		}
		n.length++
		w.Union(&n.weight, elemW)
	}
}

func (n *treeNode[T, W]) splitChild(fullChildIdx int, w *Weighter[T, W]) {
	fullChild := n.children[fullChildIdx]
	b := btreeOrder

	// New sibling gets elements[B:] from full child
	siblingElems := make([]T, len(fullChild.elements)-b)
	copy(siblingElems, fullChild.elements[b:])

	var siblingChildren []*treeNode[T, W]
	if !fullChild.isLeaf() {
		siblingChildren = make([]*treeNode[T, W], len(fullChild.children)-b)
		copy(siblingChildren, fullChild.children[b:])
		fullChild.children = fullChild.children[:b]
	}

	// Middle element moves up
	middle := fullChild.elements[b-1]
	fullChild.elements = fullChild.elements[:b-1]

	sibling := &treeNode[T, W]{
		elements: siblingElems,
		children: siblingChildren,
	}

	fullChild.recomputeLen(w)
	sibling.recomputeLen(w)

	// Insert sibling after full child
	n.children = append(n.children, nil)
	copy(n.children[fullChildIdx+2:], n.children[fullChildIdx+1:])
	n.children[fullChildIdx+1] = sibling

	// Insert middle element
	n.elements = append(n.elements, middle) // grow
	copy(n.elements[fullChildIdx+1:], n.elements[fullChildIdx:len(n.elements)-1])
	n.elements[fullChildIdx] = middle
}

func (n *treeNode[T, W]) recomputeLen(w *Weighter[T, W]) {
	n.length = len(n.elements)
	for _, c := range n.children {
		n.length += c.length
	}
	n.recomputeWeight(w)
}

func (n *treeNode[T, W]) recomputeWeight(w *Weighter[T, W]) {
	acc := w.Zero
	for i := range n.elements {
		acc = w.And(acc, w.Alloc(&n.elements[i]))
	}
	for _, c := range n.children {
		acc = w.And(acc, c.weight)
	}
	n.weight = acc
}

func (n *treeNode[T, W]) subtractWeight(weight W, w *Weighter[T, W]) {
	if !w.MaybeSub(&n.weight, weight) {
		n.recomputeWeight(w)
	}
}

func (n *treeNode[T, W]) cumulativeIndex(childIdx int) int {
	total := 0
	for i := 0; i < childIdx; i++ {
		total += n.children[i].length + 1
	}
	return total
}

func (n *treeNode[T, W]) remove(index int, w *Weighter[T, W]) T {
	if n.isLeaf() {
		return n.removeFromLeaf(index, w)
	}

	totalIdx := 0
	for ci, child := range n.children {
		cmpVal := totalIdx + child.length
		if cmpVal < index {
			totalIdx += child.length + 1
			continue
		}
		if cmpVal == index {
			elemIdx := ci
			if elemIdx >= len(n.elements) {
				elemIdx = len(n.elements) - 1
			}
			return n.removeElementFromNonLeaf(index, elemIdx, w)
		}
		// cmpVal > index
		return n.removeFromInternalChild(index, ci, w)
	}
	panic("index not found to remove")
}

func (n *treeNode[T, W]) removeFromLeaf(index int, w *Weighter[T, W]) T {
	elem := n.elements[index]
	copy(n.elements[index:], n.elements[index+1:])
	n.elements = n.elements[:len(n.elements)-1]
	n.length--
	elemW := w.Alloc(&elem)
	n.subtractWeight(elemW, w)
	return elem
}

func (n *treeNode[T, W]) removeElementFromNonLeaf(index, elemIdx int, w *Weighter[T, W]) T {
	b := btreeOrder
	var result T

	if len(n.children[elemIdx].elements) >= b {
		totalIdx := n.cumulativeIndex(elemIdx)
		pred := n.children[elemIdx].remove(index-1-totalIdx, w)
		result = n.elements[elemIdx]
		n.elements[elemIdx] = pred
	} else if len(n.children[elemIdx+1].elements) >= b {
		totalIdx := n.cumulativeIndex(elemIdx + 1)
		succ := n.children[elemIdx+1].remove(index+1-totalIdx, w)
		result = n.elements[elemIdx]
		n.elements[elemIdx] = succ
	} else {
		// Remove middle element and merge children
		middle := n.elements[elemIdx]
		copy(n.elements[elemIdx:], n.elements[elemIdx+1:])
		n.elements = n.elements[:len(n.elements)-1]

		succChild := n.children[elemIdx+1]
		copy(n.children[elemIdx+1:], n.children[elemIdx+2:])
		n.children = n.children[:len(n.children)-1]

		n.children[elemIdx].merge(middle, succChild, w)

		totalIdx := n.cumulativeIndex(elemIdx)
		result = n.children[elemIdx].remove(index-totalIdx, w)
	}

	n.length--
	resultW := w.Alloc(&result)
	n.subtractWeight(resultW, w)
	return result
}

func (n *treeNode[T, W]) removeFromInternalChild(index int, childIdx int, w *Weighter[T, W]) T {
	b := btreeOrder

	childUnderflow := len(n.children[childIdx].elements) < b
	leftSibUnderflow := childIdx == 0 || len(n.children[childIdx-1].elements) < b
	rightSibUnderflow := childIdx+1 >= len(n.children) || len(n.children[childIdx+1].elements) < b

	if childUnderflow && leftSibUnderflow && rightSibUnderflow {
		// Merge with a sibling
		if childIdx > 0 {
			middle := n.elements[childIdx-1]
			copy(n.elements[childIdx-1:], n.elements[childIdx:])
			n.elements = n.elements[:len(n.elements)-1]

			succ := n.children[childIdx]
			copy(n.children[childIdx:], n.children[childIdx+1:])
			n.children = n.children[:len(n.children)-1]
			childIdx--
			n.children[childIdx].merge(middle, succ, w)
		} else {
			middle := n.elements[childIdx]
			copy(n.elements[childIdx:], n.elements[childIdx+1:])
			n.elements = n.elements[:len(n.elements)-1]

			succ := n.children[childIdx+1]
			copy(n.children[childIdx+1:], n.children[childIdx+2:])
			n.children = n.children[:len(n.children)-1]
			n.children[childIdx].merge(middle, succ, w)
		}
	} else if childUnderflow {
		if childIdx > 0 && len(n.children[childIdx-1].elements) >= b {
			// Borrow from left sibling
			leftSib := n.children[childIdx-1]
			lastElem := leftSib.elements[len(leftSib.elements)-1]
			leftSib.elements = leftSib.elements[:len(leftSib.elements)-1]
			leftSib.length--
			leftSibElemW := w.Alloc(&lastElem)
			leftSib.subtractWeight(leftSibElemW, w)

			parentElem := n.elements[childIdx-1]
			n.elements[childIdx-1] = lastElem

			child := n.children[childIdx]
			child.length++
			parentW := w.Alloc(&parentElem)
			w.Union(&child.weight, parentW)
			child.elements = append(child.elements, parentElem)
			copy(child.elements[1:], child.elements[:len(child.elements)-1])
			child.elements[0] = parentElem

			if !leftSib.isLeaf() {
				lastChild := leftSib.children[len(leftSib.children)-1]
				leftSib.children = leftSib.children[:len(leftSib.children)-1]
				leftSib.length -= lastChild.length
				leftSib.subtractWeight(lastChild.weight, w)
				child.length += lastChild.length
				w.Union(&child.weight, lastChild.weight)
				child.children = append(child.children, nil)
				copy(child.children[1:], child.children)
				child.children[0] = lastChild
			}
		} else if childIdx+1 < len(n.children) && len(n.children[childIdx+1].elements) >= b {
			// Borrow from right sibling
			rightSib := n.children[childIdx+1]
			firstElem := rightSib.elements[0]
			copy(rightSib.elements, rightSib.elements[1:])
			rightSib.elements = rightSib.elements[:len(rightSib.elements)-1]
			rightSib.length--
			firstElemW := w.Alloc(&firstElem)
			rightSib.subtractWeight(firstElemW, w)

			parentElem := n.elements[childIdx]
			n.elements[childIdx] = firstElem

			child := n.children[childIdx]
			child.length++
			parentW := w.Alloc(&parentElem)
			w.Union(&child.weight, parentW)
			child.elements = append(child.elements, parentElem)

			if !rightSib.isLeaf() {
				firstChild := rightSib.children[0]
				copy(rightSib.children, rightSib.children[1:])
				rightSib.children = rightSib.children[:len(rightSib.children)-1]
				rightSib.length -= firstChild.length
				rightSib.subtractWeight(firstChild.weight, w)
				child.length += firstChild.length
				w.Union(&child.weight, firstChild.weight)
				child.children = append(child.children, firstChild)
			}
		}
	}

	totalIdx := n.cumulativeIndex(childIdx)
	v := n.children[childIdx].remove(index-totalIdx, w)
	n.length--
	vW := w.Alloc(&v)
	n.subtractWeight(vW, w)
	return v
}

func (n *treeNode[T, W]) replace(index int, element T, w *Weighter[T, W]) T {
	if n.isLeaf() {
		newW := w.Alloc(&element)
		w.Union(&n.weight, newW)
		old := n.elements[index]
		n.elements[index] = element
		oldW := w.Alloc(&old)
		n.subtractWeight(oldW, w)
		return old
	}

	totalIdx := 0
	for ci, child := range n.children {
		cmpVal := totalIdx + child.length
		if cmpVal < index {
			totalIdx += child.length + 1
			continue
		}
		if cmpVal == index {
			elemIdx := ci
			if elemIdx >= len(n.elements) {
				elemIdx = len(n.elements) - 1
			}
			newW := w.Alloc(&element)
			w.Union(&n.weight, newW)
			old := n.elements[elemIdx]
			n.elements[elemIdx] = element
			oldW := w.Alloc(&old)
			n.subtractWeight(oldW, w)
			return old
		}
		// cmpVal > index
		newW := w.Alloc(&element)
		w.Union(&n.weight, newW)
		old := child.replace(index-totalIdx, element, w)
		oldW := w.Alloc(&old)
		n.subtractWeight(oldW, w)
		return old
	}
	panic("index not found to replace")
}

func (n *treeNode[T, W]) merge(middle T, successor *treeNode[T, W], w *Weighter[T, W]) {
	n.length += successor.length + 1
	w.Union(&n.weight, successor.weight)
	midW := w.Alloc(&middle)
	w.Union(&n.weight, midW)
	n.elements = append(n.elements, middle)
	n.elements = append(n.elements, successor.elements...)
	n.children = append(n.children, successor.children...)
}

func (n *treeNode[T, W]) collect(out *[]T) {
	if n.isLeaf() {
		*out = append(*out, n.elements...)
		return
	}
	for i, child := range n.children {
		child.collect(out)
		if i < len(n.elements) {
			*out = append(*out, n.elements[i])
		}
	}
}
