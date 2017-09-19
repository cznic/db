// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//TODO credit B authors

package db

import (
	"testing"

	"github.com/cznic/file"
)

func (t *BTree) cmp(n int) func(off int64) (int, error) {
	return func(off int64) (int, error) {
		p, err := t.r8(off)
		if err != nil {
			return 0, err
		}

		m, err := t.r4(p)
		if err != nil {
			return 0, err
		}

		if n < m {
			return -1, nil
		}

		if n > m {
			return 1, nil
		}

		return 0, nil
	}
}

func (t *BTree) len(tb testing.TB) int64 {
	c, err := t.Len()
	if err != nil {
		tb.Fatal(err)
	}

	return c
}

func (t *BTree) get(tb testing.TB, k int) (y int, yy bool) {
	off, ok, err := t.Get(t.cmp(k))
	if err != nil {
		tb.Fatal(err)
	}

	if !ok {
		return 0, false
	}

	p, err := t.r8(off)
	if err != nil {
		tb.Fatal(err)
	}

	n, err := t.r4(p)
	if err != nil {
		tb.Fatal(err)
	}

	return n, true
}

func (t *BTree) set(tb testing.TB, k, v int) {
	kalloc := true
	koff, voff, err := t.Set(t.cmp(k), func(off int64) error {
		p, err := t.r8(off)
		if err != nil {
			return err
		}

		kalloc = false
		return t.Free(p)
	})
	if err != nil {
		tb.Fatal(err)
	}

	var p, q int64
	if kalloc {
		if p, err = t.Alloc(4); err != nil {
			tb.Fatal(err)
		}

		if err := t.w4(p, k); err != nil {
			tb.Fatal(err)
		}

		if err := t.w8(koff, p); err != nil {
			tb.Fatal(err)
		}
	}

	if q, err = t.Alloc(4); err != nil {
		tb.Fatal(err)
	}

	if err := t.w4(q, v); err != nil {
		tb.Fatal(err)
	}

	if err := t.w8(voff, q); err != nil {
		tb.Fatal(err)
	}
}

func (t *BTree) remove(tb testing.TB) {
	if err := t.Remove(func(k, v int64) error {
		p, err := t.r8(k)
		if err != nil {
			return err
		}

		if err := t.Free(p); err != nil {
			return err
		}

		q, err := t.r8(v)
		if err != nil {
			return err
		}

		return t.Free(q)
	}); err != nil {
		tb.Fatal(err)
	}
}

func testBTreeGet0(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tr, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer tr.remove(t)

	if g, e := tr.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	_, ok := tr.get(t, 42)
	if g, e := ok, false; g != e {
		t.Fatal(g, e)
	}
}

func TestBTreeGet0(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeGet0(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeGet0(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testBTreeGet0(t, tmpFile) }))
}

func testBTreeSetGet0(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tr, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer tr.remove(t)

	set := tr.set
	set(t, 42, 314)
	if g, e := tr.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	v, ok := tr.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 314; g != e {
		t.Fatal(g, e)
	}

	set(t, 42, 278)
	if g, e := tr.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	v, ok = tr.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 278; g != e {
		t.Fatal(g, e)
	}

	set(t, 420, 5)
	if g, e := tr.len(t), int64(2); g != e {
		t.Fatal(g, e)
	}

	v, ok = tr.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 278; g != e {
		t.Fatal(g, e)
	}

	v, ok = tr.get(t, 420)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 5; g != e {
		t.Fatal(g, e)
	}
}

func TestBTreeSetGet0(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet0(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet0(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet0(t, tmpFile) }))
}

func testBTreeSetGet1(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	const N = 1 << 10
	for _, x := range []int{0, -1, 0x5555555, 0xaaaaaaa, 0x3333333, 0xccccccc, 0x31415926, 0x2718282} {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			tr, err := db.NewBTree(16, 16, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer tr.remove(t)

			set := tr.set
			a := make([]int, N)
			for i := range a {
				a[i] = (i ^ x) << 1
			}
			for i, k := range a {
				set(t, k, k^x)
				if g, e := tr.len(t), int64(i+1); g != e {
					t.Fatal(i, g, e)
				}
			}

			for i, k := range a {
				v, ok := tr.get(t, k)
				if !ok {
					t.Fatal(i, k, ok)
				}

				if g, e := v, k^x; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = tr.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}

			for _, k := range a {
				tr.set(t, k, (k^x)+42)
			}

			for i, k := range a {
				v, ok := tr.get(t, k)
				if !ok {
					t.Fatal(i, k, v, ok)
				}

				if g, e := v, k^x+42; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = tr.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}
		}()
	}
}

func TestBTreeSetGet1(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet1(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet1(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet1(t, tmpFile) }))
}
