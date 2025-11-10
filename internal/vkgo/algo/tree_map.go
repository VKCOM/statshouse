// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package algo

import (
	"log"
)

type allocator[T any] interface {
	allocate() *T
	deallocate(*T) // must assign empty value
}

type comparator[T any] interface {
	// Cmp TODO may be replace with `Cmp(k T, v T) int` to reduce virtual calls ???
	// returns whether k < v
	Cmp(k T, v T) bool
}

// TreeNode AVL Tree node
type TreeNode[T any] struct {
	value  T
	left   *TreeNode[T]
	right  *TreeNode[T]
	height int32
}

func (n *TreeNode[T]) getHeight() int32 {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *TreeNode[T]) calcBalance() int32 {
	if n == nil {
		return 0
	}
	return n.right.getHeight() - n.left.getHeight()
}

func (n *TreeNode[T]) updateHeight() {
	n.height = 1 + max(n.left.getHeight(), n.right.getHeight())
}

func (n *TreeNode[T]) rotateRight() *TreeNode[T] {
	l := n.left
	n.left = l.right
	l.right = n
	n.updateHeight()
	l.updateHeight()
	return l
}

func (n *TreeNode[T]) rotateLeft() *TreeNode[T] {
	r := n.right
	n.right = r.left
	r.left = n
	n.updateHeight()
	r.updateHeight()
	return r
}

func (n *TreeNode[T]) bigRotateRight() *TreeNode[T] {
	n.left = n.left.rotateLeft()
	return n.rotateRight()
}

func (n *TreeNode[T]) bigRotateLeft() *TreeNode[T] {
	n.right = n.right.rotateRight()
	return n.rotateLeft()
}

func (n *TreeNode[T]) repairBalance() *TreeNode[T] {
	n.updateHeight()
	if n.calcBalance() == 2 {
		if n.right.calcBalance() == -1 {
			return n.bigRotateLeft()
		} else {
			return n.rotateLeft()
		}
	} else if n.calcBalance() == -2 {
		if n.left.calcBalance() == 1 {
			return n.bigRotateRight()
		} else {
			return n.rotateRight()
		}
	}
	return n
}

// TODO make `n` receiver when methods are allowed to be generics
func insert[T any, C comparator[T], A allocator[TreeNode[T]]](n *TreeNode[T], value T, alloc A) *TreeNode[T] {
	if n == nil {
		n = alloc.allocate()
		n.height = 0
		n.value = value
		return n
	}
	// TODO replace recursion with loop
	var comp C
	if comp.Cmp(n.value, value) {
		n.right = insert[T, C, A](n.right, value, alloc)
	} else if comp.Cmp(value, n.value) {
		n.left = insert[T, C, A](n.left, value, alloc)
	} else { // n.value == value
		n.value = value
	}
	return n.repairBalance()
}

func remove[T any, C comparator[T], A allocator[TreeNode[T]]](n *TreeNode[T], value T, alloc A) *TreeNode[T] {
	var comp C
	if n == nil {
		return nil
	} else if comp.Cmp(n.value, value) {
		n.right = remove[T, C, A](n.right, value, alloc)
	} else if comp.Cmp(value, n.value) {
		n.left = remove[T, C, A](n.left, value, alloc)
	} else { // n.value == value
		if n.left == nil {
			root := n.right
			alloc.deallocate(n)
			return root
		}
		if n.right == nil {
			root := n.left
			alloc.deallocate(n)
			return root
		}
		n.value, n.right = extractMin[T, A](n.right, alloc)
	}
	return n.repairBalance()
}

// Pre:     n != nil
// Post:    node with minimum element in `n` subtree is removed
// Returns: (minimum value in `n` subtree, new subtree root)
func extractMin[T any, A allocator[TreeNode[T]]](n *TreeNode[T], alloc A) (T, *TreeNode[T]) {
	if n == nil {
		panic("TreeNode::extractMin() invariant violated")
	}
	if n.left == nil {
		value, root := n.value, n.right
		alloc.deallocate(n)
		return value, root
	}
	var value T
	value, n.left = extractMin[T, A](n.left, alloc)
	return value, n.repairBalance()

}

// Pre: n != nil
func (n *TreeNode[T]) findMin() *TreeNode[T] {
	if n == nil {
		panic("TreeNode::findMin() invariant violated")
	}
	if n.left != nil {
		return n.left.findMin()
	}
	return n
}

// Pre: n != nil
func (n *TreeNode[T]) findMax() *TreeNode[T] {
	if n == nil {
		panic("TreeNode::findMax() invariant violated")
	}
	if n.right != nil {
		return n.right.findMax()
	}
	return n
}

func find[T any, C comparator[T]](n *TreeNode[T], value T) *TreeNode[T] {
	var comp C
	for {
		if n == nil {
			return nil
		}
		if comp.Cmp(n.value, value) {
			n = n.right
		} else if comp.Cmp(value, n.value) {
			n = n.left
		} else {
			return n
		}
	}
}

func validate[T any, C comparator[T]](n *TreeNode[T], leftBorder T, leftBorderExists bool, rightBorder T, rightBorderExists bool) {
	if n == nil {
		return
	}
	var comp C
	if leftBorderExists && !comp.Cmp(leftBorder, n.value) {
		log.Panicf("invariant violated: node value %+v <= left border %+v", n.value, leftBorder)
	}
	if rightBorderExists && !comp.Cmp(n.value, rightBorder) {
		log.Panicf("invariant violated: node value %+v <= right border %+v", n.value, rightBorder)
	}
	validate[T, C](n.left, leftBorder, leftBorderExists, n.value, true)
	validate[T, C](n.right, n.value, true, rightBorder, rightBorderExists)
}

type Entry[K any, V any] struct {
	K K
	V V
}

// TreeMap TODO make allocator generic to avoid virtual calls
type TreeMap[K any, V any, C comparator[K]] struct {
	root  *TreeNode[Entry[K, V]]
	alloc allocator[TreeNode[Entry[K, V]]]
}

type entryComparator[K any, V any, C comparator[K]] struct {
}

func (entryComparator[K, V, C]) Cmp(e1 Entry[K, V], e2 Entry[K, V]) bool {
	var comp C
	return comp.Cmp(e1.K, e2.K)
}

func NewTreeMap[K any, V any, C comparator[K]](alloc allocator[TreeNode[Entry[K, V]]]) TreeMap[K, V, C] {
	return TreeMap[K, V, C]{
		root:  nil,
		alloc: alloc,
	}
}

// TODO add GetIter method, to implement Emplace/Update and DeleteByIter without extra search

func (t *TreeMap[K, V, C]) GetPtr(key K) *V {
	var empty V
	n := find[Entry[K, V], entryComparator[K, V, C]](
		t.root,
		Entry[K, V]{key, empty},
	)
	if n == nil {
		return nil
	}
	return &n.value.V
}

func (t *TreeMap[K, V, C]) Get(key K) (value V, exists bool) {
	valPtr := t.GetPtr(key)
	if valPtr == nil {
		var empty V
		return empty, false
	}
	return *valPtr, true
}

func (t *TreeMap[K, V, C]) Set(key K, value V) {
	t.root = insert[Entry[K, V], entryComparator[K, V, C]](t.root, Entry[K, V]{K: key, V: value}, t.alloc)
}

func (t *TreeMap[K, V, C]) Delete(key K) {
	var empty V
	t.root = remove[Entry[K, V], entryComparator[K, V, C]](t.root, Entry[K, V]{key, empty}, t.alloc)
}

func (t *TreeMap[K, V, C]) Empty() bool {
	return t.root == nil
}

func (t *TreeMap[K, V, C]) Front() Entry[K, V] {
	if t.root == nil {
		panic("called Front() on empty TreeMap")
	}
	return t.root.findMin().value
}

func (t *TreeMap[K, V, C]) Back() Entry[K, V] {
	if t.root == nil {
		panic("called Back() on empty TreeMap")
	}
	return t.root.findMax().value
}

func (t *TreeMap[K, V, C]) LenMoreThan1() bool {
	return t.root != nil && (t.root.left != nil || t.root.right != nil)
}

func (t *TreeMap[K, V, C]) validate() {
	var empty Entry[K, V]
	validate[Entry[K, V], entryComparator[K, V, C]](t.root, empty, false, empty, false)
}
