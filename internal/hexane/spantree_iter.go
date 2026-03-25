package hexane

// SpanTreeIter is a forward iterator over a SpanTree that tracks index and weight.
type SpanTreeIter[T, W any] struct {
	tree   *SpanTree[T, W]
	index  int
	weight W
}

// Iter returns a new forward iterator starting at index 0.
func (t *SpanTree[T, W]) Iter() *SpanTreeIter[T, W] {
	return &SpanTreeIter[T, W]{
		tree:   t,
		index:  0,
		weight: t.weighter.Zero,
	}
}

// Index returns the current position.
func (it *SpanTreeIter[T, W]) Index() int { return it.index }

// Weight returns the accumulated weight so far.
func (it *SpanTreeIter[T, W]) Weight() W { return it.weight }

// Next returns the next element, or nil if exhausted.
func (it *SpanTreeIter[T, W]) Next() *T {
	elem := it.tree.Get(it.index)
	if elem == nil {
		return nil
	}
	it.index++
	w := &it.tree.weighter
	w.Union(&it.weight, w.Alloc(elem))
	return elem
}

// IterFromCursor creates an iterator positioned after the given cursor element.
// The returned iterator's Weight() includes the cursor element, and Next() returns
// the element after the cursor position.
func (t *SpanTree[T, W]) IterFromCursor(cursor *SubCursor[T, W]) *SpanTreeIter[T, W] {
	w := &t.weighter
	weight := cursor.Weight
	w.Union(&weight, w.Alloc(cursor.Element))
	return &SpanTreeIter[T, W]{
		tree:   t,
		index:  cursor.Index + 1,
		weight: weight,
	}
}

// IterWhere returns a SpanTreeFnIter that only visits elements where f returns true.
// The predicate receives (accumulated weight so far, next subtree weight) and should
// return true if the search target might be in this subtree/element.
func (t *SpanTree[T, W]) IterWhere(f func(accW, nextW W) bool) *SpanTreeFnIter[T, W] {
	iter := &SpanTreeFnIter[T, W]{
		tree: t,
		f:    f,
	}
	if t.root != nil {
		iter.stack = append(iter.stack, newNodeWalker[T, W](t.root, 0, t.weighter.Zero))
	}
	return iter
}

// SpanTreeFnIter is a stack-based filtered iterator that uses the weight predicate
// to prune branches during traversal.
type SpanTreeFnIter[T, W any] struct {
	tree  *SpanTree[T, W]
	stack []nodeWalker[T, W]
	f     func(accW, nextW W) bool
}

type nodeWalker[T, W any] struct {
	node     *treeNode[T, W]
	childIdx int
	elemIdx  int
	index    int
	acc      W
}

func newNodeWalker[T, W any](node *treeNode[T, W], index int, acc W) nodeWalker[T, W] {
	return nodeWalker[T, W]{node: node, childIdx: 0, elemIdx: 0, index: index, acc: acc}
}

// Next returns the next matching SubCursor, or nil when exhausted.
func (it *SpanTreeFnIter[T, W]) Next() *SubCursor[T, W] {
	w := &it.tree.weighter
	for len(it.stack) > 0 {
		top := &it.stack[len(it.stack)-1]

		// Try to descend into next child
		if top.childIdx < len(top.node.children) {
			child := top.node.children[top.childIdx]
			top.childIdx++

			index := top.index
			acc := top.acc

			top.index += child.length
			w.Union(&top.acc, child.weight)

			if it.f(acc, child.weight) {
				it.stack = append(it.stack, newNodeWalker[T, W](child, index, acc))
				continue
			}
		}

		// Try next element
		if top.elemIdx < len(top.node.elements) {
			e := &top.node.elements[top.elemIdx]
			top.elemIdx++

			elemW := w.Alloc(e)
			index := top.index
			acc := top.acc

			top.index++
			w.Union(&top.acc, elemW)

			if it.f(acc, elemW) {
				return &SubCursor[T, W]{Index: index, Weight: acc, Element: e}
			}
			continue
		}

		// Pop exhausted walker
		it.stack = it.stack[:len(it.stack)-1]
	}
	return nil
}
