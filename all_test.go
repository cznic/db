// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
)

func caller(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(2)
	fmt.Fprintf(os.Stderr, "# caller: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
	_, fn, fl, _ = runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "# \tcallee: %s:%d: ", path.Base(fn), fl)
	fmt.Fprintln(os.Stderr)
	os.Stderr.Sync()
}

func dbg(s string, va ...interface{}) {
	if s == "" {
		s = strings.Repeat("%v ", len(va))
	}
	_, fn, fl, _ := runtime.Caller(1)
	fmt.Fprintf(os.Stderr, "# dbg %s:%d: ", path.Base(fn), fl)
	fmt.Fprintf(os.Stderr, s, va...)
	fmt.Fprintln(os.Stderr)
	os.Stderr.Sync()
}

func TODO(...interface{}) string { //TODOOK
	_, fn, fl, _ := runtime.Caller(1)
	return fmt.Sprintf("# TODO: %s:%d:\n", path.Base(fn), fl) //TODOOK
}

func use(...interface{}) {}

func init() {
	use(caller, dbg, TODO) //TODOOK
}

// ============================================================================

//TODO- (A begin)
type tester interface {
	alloc(size int64) (handle int64, err error)
	create(b []byte) (handle int64, err error)
	delete(handle int64) error
	free()
	read(handle int64, b []byte) error
	readAt(handle int64, b []byte, off int64) error
	size(handle int64) (int64, error)
	update(handle int64, b []byte) error
	writeAt(handle int64, b []byte, off int64) error
}

type memTester struct {
	*memory
	balance int
}

func newMemTester() *memTester {
	return &memTester{memory: newMemory()}
}

func (m *memTester) free() {
	memoryPool.put(m.memory)
	if m.balance != 0 {
		panic(fmt.Sprintf("%T balance %d", m, m.balance))
	}
}

func (m *memTester) alloc(sz int64) (handle int64, err error) {
	m.balance++
	return m.memory.alloc(sz)
}

func (m *memTester) create(b []byte) (handle int64, err error) {
	m.balance++
	return m.memory.create(b)
}

func (m *memTester) delete(handle int64) error {
	m.balance--
	return m.memory.delete(handle)
}

func create(s store, t *testing.T, v ...int64) (r []int64) {
	r = make([]int64, len(v))
	var b [8]byte
	for i, v := range v {
		var err error
		r[i], err = s.create(n2b(b[:], v))
		if err != nil {
			t.Fatal(i, v, err)
		}
	}
	return r
}

func read(s store, t *testing.T, hv ...int64) (r []int64) {
	if n := len(hv); n%2 != 0 {
		panic(n)
	}

	var b [8]byte
	for i := 0; i < len(hv); i += 2 {
		if err := s.read(hv[i], b[:]); err != nil {
			panic(err)
		}

		n := b2n(b[:])
		if g, e := n, hv[i+1]; g != e {
			t.Fatal(i, g, e)
		}
	}
	return
}

func update(s store, t *testing.T, hv ...int64) (r []int64) {
	if n := len(hv); n%2 != 0 {
		panic(n)
	}

	var b [8]byte
	for i := 0; i < len(hv); i += 2 {
		h, v := hv[i], hv[i+1]
		if err := s.update(h, n2b(b[:], v)); err != nil {
			t.Fatal(i, h, v, err)
		}
	}
	return
}

func del(s store, t *testing.T, h ...int64) (r []int64) {
	for _, h := range h {
		if err := s.delete(h); err != nil {
			t.Fatal(err)
		}
	}
	return
}

func TestMemoryStore(t *testing.T) {
	testStore(t, newMemTester())
}

func testStore(t *testing.T, s tester) {
	defer s.free()

	const bigBlob = 3*64*1024 + 17

	var h1, h2, h3 int64
	tab := []struct {
		f func(store, *testing.T, ...int64) []int64
		v []int64
	}{

		{create, []int64{11, 12, 13}},
		{read, []int64{-1, 11, -2, 12, -3, 13}},
		{update, []int64{-2, 22}},
		{read, []int64{-1, 11, -2, 22, -3, 13}},
		{del, []int64{-1}},
		{read, []int64{-2, 22, -3, 13}},
		{del, []int64{-3}},
		{read, []int64{-2, 22}},
		{del, []int64{-2}},
		{create, []int64{14, 15, 16}},
		{read, []int64{-1, 14, -2, 15, -3, 16}},
		{del, []int64{-1, -2, -3}},
	}
	for _, test := range tab {
		a := make([]int64, len(test.v))
		for i, v := range test.v {
			switch v {
			case -1:
				v = h1
			case -2:
				v = h2
			case -3:
				v = h3
			}
			a[i] = v
		}
		for i, v := range test.f(s, t, a...) {
			switch i {
			case 0:
				h1 = v
			case 1:
				h2 = v
			case 2:
				h3 = v
			}
		}
	}

	rng := rand.New(rand.NewSource(42))
	b := make([]byte, bigBlob)
	b2 := make([]byte, bigBlob)
	for i := range b {
		b[i] = byte(rng.Int())
	}

	h, err := s.create(b)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.read(h, b2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(b, b2) {
		t.Fatal(false)
	}

	for i, v := range b {
		b[i] = (v + 123) ^ 0x55
	}

	if err = s.update(h, b); err != nil {
		t.Fatal(err)
	}

	if err = s.read(h, b2); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(b, b2) {
		t.Fatal(false)
	}

	if err := s.delete(h); err != nil {
		t.Fatal(err)
	}
}

//TODO- (A end)
