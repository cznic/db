// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"sync"
)

var (
	sListPool     = sync.Pool{New: func() interface{} { return &sList{buf8: make([]byte, 8)} }}
	sListNodePool = sync.Pool{New: func() interface{} { return &sListNode{} }}
)

// ---------------------------------------------------------------------- sList

// sList is a linked list[0].
//
//   [0]: http://en.wikipedia.org/wiki/Linked_list
type sList struct {
	buf8  []byte // NOTE: Not safe for parallel mutations.
	first int64  // 0, 8
	h     int64  //
	s     store  //
}

func (l *sList) setFirst(h int64) error {
	l.first = h
	return write8(l.s, l.h, 0, h, l.buf8)
}

func newSList(s store) (*sList, error) {
	l := sListPool.Get().(*sList)
	l.s, l.h, l.first = s, 0, 0
	if err := l.save(); err != nil {
		return nil, err
	}

	return l, nil
}

func openSList(s store, h int64) (*sList, error) {
	l := sListPool.Get().(*sList)
	first, err := read8(s, h, 0, l.buf8)
	if err != nil {
		return nil, err
	}

	l.s, l.h, l.first = s, h, first
	return l, nil
}

func (l *sList) clear() error {
	for h := l.first; h != 0; {
		next, err := read8(l.s, h, 0, l.buf8)
		if err != nil {
			return err
		}

		if err := l.s.delete(h); err != nil {
			return err
		}

		h = next
	}
	return l.setFirst(0)
}

func (l *sList) close() {
	l.s = nil
	sListPool.Put(l)
}

func (l *sList) delete() error {
	if err := l.clear(); err != nil {
		return err
	}

	if err := l.s.delete(l.h); err != nil {
		return err
	}

	l.close()
	return nil
}

func (l *sList) firstNode(b []byte) (*sListNode, error) {
	if l.first == 0 {
		return nil, nil
	}

	return l.openNode(l.first, b)
}

func (l *sList) insertBeginning(n *sListNode) error {
	n.next = l.first
	if err := write8(l.s, n.h, 0, n.next, l.buf8); err != nil {
		return err
	}

	l.first = n.h
	return write8(l.s, l.h, 0, l.first, l.buf8)
}

func (l *sList) newNode(data []byte) (*sListNode, error) {
	n := sListNodePool.Get().(*sListNode)
	n.l, n.h, n.next, n.data = l, 0, 0, data
	h, err := n.l.s.alloc(int64(len(n.data) + 8))
	if err != nil {
		return nil, err
	}

	n.h = h
	if err = n.l.s.writeAt(n.h, data, 8); err != nil {
		return nil, err
	}

	return n, nil
}

func (l *sList) openNode(h int64, b []byte) (*sListNode, error) {
	next, err := read8(l.s, h, 0, l.buf8)
	if err != nil {
		return nil, err
	}

	n := sListNodePool.Get().(*sListNode)
	n.l, n.h, n.next, n.data = l, h, next, nil
	if err := l.s.readAt(h, b, 8); err != nil {
		return nil, err
	}

	n.data = b
	return n, nil
}

func (l *sList) removeBeginning() error {
	if l.first == 0 {
		return ErrNoNode
	}

	firstNodeNext, err := read8(l.s, l.first, 0, l.buf8)
	if err != nil {
		return err
	}

	if err := l.s.delete(l.first); err != nil {
		return err
	}

	l.first = firstNodeNext
	return write8(l.s, l.h, 0, firstNodeNext, l.buf8)
}

func (l *sList) save() error {
	n2b(l.buf8, l.first)
	if h := l.h; h > 0 {
		return write8(l.s, l.h, 0, l.first, l.buf8)
	}

	h, err := l.s.create(l.buf8)
	if err != nil {
		return err
	}

	l.h = h
	return nil
}

// ------------------------------------------------------------------ sListNode

// sListNode is a node of sList.
type sListNode struct {
	data []byte // 8, n:
	h    int64  //
	l    *sList //
	next int64  // 0, 8:
}

func (n *sListNode) setN(next int64) error {
	n.next = next
	return write8(n.l.s, n.h, 0, next, n.l.buf8)
}

func (n *sListNode) close() { sListNodePool.Put(n) }

func (n *sListNode) insertAfter(newNode *sListNode) error {
	newNode.next = n.next
	if err := write8(n.l.s, newNode.h, 0, newNode.next, n.l.buf8); err != nil {
		return err
	}

	n.next = newNode.h
	return write8(n.l.s, n.h, 0, n.next, n.l.buf8)
}

func (n *sListNode) nextNode(b []byte) (*sListNode, error) {
	h := n.next
	if h == 0 {
		return nil, nil
	}

	return n.l.openNode(h, b)
}

func (n *sListNode) removeAfter() error {
	if n.next == 0 {
		return ErrNoNode
	}

	nodeNextNext, err := read8(n.l.s, n.next, 0, n.l.buf8)
	if err != nil {
		return err
	}

	if err := n.l.s.delete(n.next); err != nil {
		return err
	}

	return n.setN(nodeNextNext)
}
