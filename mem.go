// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"
	"io"
	"sync"

	"github.com/cznic/mathutil"
)

var (
	_ store = (*memory)(nil)

	memoryPool   mPool
	memoryPoolMu sync.Mutex
)

type mPool []*memory

func (p *mPool) get() *memory {
	memoryPoolMu.Lock()
	defer memoryPoolMu.Unlock()

	s := *p
	n := len(s)
	if n == 0 {
		return &memory{}
	}

	x := s[n-1]
	x.items, x.recycler = x.items[:0], x.recycler[:0]
	s = s[:n-1]
	*p = s
	return x
}

func (p *mPool) put(x *memory) {
	memoryPoolMu.Lock()
	defer memoryPoolMu.Unlock()

	*p = append(*p, x)
}

type memory struct {
	items    [][]byte
	recycler []int
}

func newMemory() *memory { return memoryPool.get() }

func (m *memory) alloc(size int64) (int64, error) {
	if size > mathutil.MaxInt {
		return -1, fmt.Errorf("allocation size overflow: %d", size)
	}

	b := get(int(size))
	if n := len(m.recycler); n != 0 {
		h := m.recycler[n-1]
		m.recycler = m.recycler[:n-1]
		m.items[h-1] = b
		return int64(h), nil
	}

	h := len(m.items) + 1
	m.items = append(m.items, b)
	return int64(h), nil
}

func (m *memory) close() { memoryPool.put(m) }

func (m *memory) create(b []byte) (int64, error) {
	bb := get(len(b))
	copy(bb, b)
	if n := len(m.recycler); n != 0 {
		h := m.recycler[n-1]
		m.recycler = m.recycler[:n-1]
		m.items[h-1] = bb
		return int64(h), nil
	}

	h := len(m.items) + 1
	m.items = append(m.items, bb)
	return int64(h), nil
}

func (m *memory) delete(handle int64) error {
	h := int(handle)
	m.recycler = append(m.recycler, h)
	h--
	put(m.items[h])
	m.items[h] = nil
	return nil
}

func (m *memory) read(handle int64, b []byte) error {
	s := m.items[int(handle)-1]
	n := len(s)
	if cap(b) < n {
		return fmt.Errorf("insufficient read buffer, got %d need %d", len(b), n)
	}

	b = b[:n]
	copy(b, s)
	return nil
}

func (m *memory) readAt(handle int64, b []byte, off int64) error {
	if n := copy(b, m.items[int(handle)-1][off:]); n < len(b) {
		return io.EOF
	}

	return nil
}

func (m *memory) size(handle int64) (int64, error) {
	return int64(len(m.items[int(handle)-1])), nil
}

func (m *memory) update(handle int64, b []byte) error {
	h := int(handle) - 1
	put(m.items[h])
	bb := get(len(b))
	copy(bb, b)
	m.items[h] = bb
	return nil
}

func (m *memory) writeAt(handle int64, p []byte, off int64) error {
	b := m.items[int(handle)-1]
	copy(b[off:], p)
	return nil
}
