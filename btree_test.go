// Copyright 2014 The b Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE-B file.

// Modifications are
//
// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

func (t *BTree) delete(tb testing.TB, k int) bool {
	ok, err := t.Delete(t.cmp(k), func(k, v int64) error {
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
	})
	if err != nil {
		tb.Fatal(err)
	}

	return ok
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

func (t *BTree) seek(tb testing.TB, k int) (*Enumerator, bool) {
	en, hit, err := t.Seek(t.cmp(k))
	if err != nil {
		tb.Fatal(err)
	}

	return en, hit
}

func (t *BTree) seekFirst(tb testing.TB) *Enumerator {
	en, err := t.SeekFirst()
	if err != nil {
		tb.Fatal(err)
	}

	return en
}

func (t *BTree) seekLast(tb testing.TB) *Enumerator {
	en, err := t.SeekLast()
	if err != nil {
		tb.Fatal(err)
	}

	return en
}

func (e *Enumerator) next(tb testing.TB) (int, int, bool) {
	if e.Next() {
		p, err := e.r8(e.K)
		if err != nil {
			tb.Fatal(err)
		}

		k, err := e.r4(p)
		if err != nil {
			tb.Fatal(err)
		}

		q, err := e.r8(e.V)
		if err != nil {
			tb.Fatal(err)
		}

		v, err := e.r4(q)
		if err != nil {
			tb.Fatal(err)
		}

		return k, v, true
	}

	return 0, 0, false
}

func (e *Enumerator) prev(tb testing.TB) (int, int, bool) {
	if e.Prev() {
		p, err := e.r8(e.K)
		if err != nil {
			tb.Fatal(err)
		}

		k, err := e.r4(p)
		if err != nil {
			tb.Fatal(err)
		}

		q, err := e.r8(e.V)
		if err != nil {
			tb.Fatal(err)
		}

		v, err := e.r4(q)
		if err != nil {
			tb.Fatal(err)
		}

		return k, v, true
	}

	return 0, 0, false
}

func testBTreeGet0(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	if g, e := bt.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	_, ok := bt.get(t, 42)
	if g, e := ok, false; g != e {
		t.Fatal(g, e)
	}
}

func TestBTreeGet0(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeGet0(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeGet0(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeGet0(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeGet0(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeGet0(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeGet0(t, tmpFileWAL) }))
}

func testBTreeSetGet0(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	bt.set(t, 42, 314)
	if g, e := bt.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	v, ok := bt.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 314; g != e {
		t.Fatal(g, e)
	}

	bt.set(t, 42, 278)
	if g, e := bt.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	v, ok = bt.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 278; g != e {
		t.Fatal(g, e)
	}

	bt.set(t, 420, 5)
	if g, e := bt.len(t), int64(2); g != e {
		t.Fatal(g, e)
	}

	v, ok = bt.get(t, 42)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 278; g != e {
		t.Fatal(g, e)
	}

	v, ok = bt.get(t, 420)
	if !ok {
		t.Fatal(ok)
	}

	if g, e := v, 5; g != e {
		t.Fatal(g, e)
	}
}

func TestBTreeSetGet0(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet0(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSetGet0(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet0(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSetGet0(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet0(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSetGet0(t, tmpFileWAL) }))
}

func testBTreeSetGet1(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	const N = 1 << 10
	for _, x := range []int{0, -1, 0x5555555, 0xaaaaaaa, 0x3333333, 0xccccccc, 0x31415926, 0x2718282} {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(16, 16, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			a := make([]int, N)
			for i := range a {
				a[i] = (i ^ x) << 1
			}
			for i, k := range a {
				bt.set(t, k, k^x)
				if g, e := bt.len(t), int64(i+1); g != e {
					t.Fatal(i, g, e)
				}
			}

			for i, k := range a {
				v, ok := bt.get(t, k)
				if !ok {
					t.Fatal(i, k, ok)
				}

				if g, e := v, k^x; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = bt.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}

			for _, k := range a {
				bt.set(t, k, (k^x)+42)
			}

			for i, k := range a {
				v, ok := bt.get(t, k)
				if !ok {
					t.Fatal(i, k, v, ok)
				}

				if g, e := v, k^x+42; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = bt.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}
		}()
	}
}

func TestBTreeSetGet1(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet1(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSetGet1(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet1(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSetGet1(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet1(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSetGet1(t, tmpFileWAL) }))
}

// verify how splitX works when splitting X for k pointing directly at split edge
func testBTreeSplitXOnEdge(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	kd := bt.kd
	kx := bt.kx

	// one index page with 2*kx+2 elements (last has .k=∞  so x.c=2*kx+1)
	// which will splitX on next Set
	for i := 0; i <= (2*kx+1)*2*kd; i++ {
		// odd keys are left to be filled in second test
		bt.set(t, 2*i, 2*i)
	}

	r, err := bt.root()
	if err != nil {
		t.Fatal(err)
	}

	x0 := bt.openXPage(r)
	x0c, err := x0.len()
	if err != nil {
		t.Fatal(err)
	}

	if x0c != 2*kx+1 {
		t.Fatalf("x0.c: %v  ; expected %v", x0c, 2*kx+1)
	}

	// set element with k directly at x0[kx].k
	kedge := 2 * (kx + 1) * (2 * kd)
	pk, err := x0.key(kx)
	if err != nil {
		t.Fatal(err)
	}

	if pk, err = x0.r8(pk); err != nil {
		t.Fatal(err)
	}

	k, err := x0.r4(pk)
	if err != nil {
		t.Fatal(err)
	}

	if k != kedge {
		t.Fatalf("edge key before splitX: %v  ; expected %v", k, kedge)
	}

	bt.set(t, kedge, 777)

	// if splitX was wrong kedge:777 would land into wrong place with Get failing
	v, ok := bt.get(t, kedge)
	if !(v == 777 && ok) {
		t.Fatalf("after splitX: Get(%v) -> %v, %v  ; expected 777, true", kedge, v, ok)
	}

	// now check the same when splitted X has parent
	if r, err = bt.root(); err != nil {
		t.Fatal(err)
	}

	xr := bt.openXPage(r)
	xrc, err := xr.len()
	if err != nil {
		t.Fatal(err)
	}

	if xrc != 1 { // second x comes with k=∞ with .c index
		t.Fatalf("after splitX: xr.c: %v  ; expected 1", xrc)
	}

	xr0ch, err := xr.child(0)
	if err != nil {
		t.Fatal(err)
	}

	if xr0ch != x0.off {
		t.Fatal("xr[0].ch is not x0")
	}

	for i := 0; i <= (2*kx)*kd; i++ {
		bt.set(t, 2*i+1, 2*i+1)
	}

	// check x0 is in pre-splitX condition and still at the right place
	if x0c, err = x0.len(); err != nil {
		t.Fatal(err)
	}

	if x0c != 2*kx+1 {
		t.Fatalf("x0.c: %v  ; expected %v", x0c, 2*kx+1)
	}

	if xr0ch, err = xr.child(0); err != nil {
		t.Fatal(err)
	}

	if xr0ch != x0.off {
		t.Fatal("xr[0].ch is not x0")
	}

	// set element with k directly at x0[kx].k
	kedge = (kx + 1) * (2 * kd)
	if pk, err = x0.key(kx); err != nil {
		t.Fatal(err)
	}

	if pk, err = x0.r8(pk); err != nil {
		t.Fatal(err)
	}

	x0kxk, err := x0.r4(pk)
	if err != nil {
		t.Fatal(err)
	}

	if x0kxk != kedge {
		t.Fatalf("edge key before splitX: %v  ; expected %v", x0kxk, kedge)
	}

	bt.set(t, kedge, 888)

	// if splitX was wrong kedge:888 would land into wrong place
	v, ok = bt.get(t, kedge)
	if !(v == 888 && ok) {
		t.Fatalf("after splitX: Get(%v) -> %v, %v  ; expected 888, true", kedge, v, ok)
	}
}

func TestBTreeSplitXOnEdge(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSplitXOnEdge(t, tmpFileWAL) }))
}

func testBTreeSetGet2(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	const N = 1 << 10
	for _, x := range []int{0, -1, 0x5555555, 0xaaaaaaa, 0x3333333, 0xccccccc, 0x31415926, 0x2718282} {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(16, 16, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			rng := rng()
			a := make([]int, N)
			for i := range a {
				a[i] = (rng.Next() ^ x) << 1
			}
			for i, k := range a {
				bt.set(t, k, k^x)
				if g, e := bt.len(t), int64(i)+1; g != e {
					t.Fatal(i, x, g, e)
				}
			}

			for i, k := range a {
				v, ok := bt.get(t, k)
				if !ok {
					t.Fatal(i, k, v, ok)
				}

				if g, e := v, k^x; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = bt.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}

			for _, k := range a {
				bt.set(t, k, (k^x)+42)
			}

			for i, k := range a {
				v, ok := bt.get(t, k)
				if !ok {
					t.Fatal(i, k, v, ok)
				}

				if g, e := v, k^x+42; g != e {
					t.Fatal(i, g, e)
				}

				k |= 1
				_, ok = bt.get(t, k)
				if ok {
					t.Fatal(i, k)
				}
			}
		}()
	}
}

func TestBTreeSetGet2(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet2(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSetGet2(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet2(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSetGet2(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet2(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSetGet2(t, tmpFileWAL) }))
}

func testBTreeSetGet3(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	var i int
	for i = 0; ; i++ {
		bt.set(t, i, -i)
		r, err := bt.root()
		if err != nil {
			t.Fatal(err)
		}

		p, err := bt.openPage(r)
		if err != nil {
			t.Fatal(err)
		}

		if _, ok := p.(btXPage); ok {
			break
		}
	}
	for j := 0; j <= i; j++ {
		bt.set(t, j, j)
	}

	for j := 0; j <= i; j++ {
		v, ok := bt.get(t, j)
		if !ok {
			t.Fatal(j)
		}

		if g, e := v, j; g != e {
			t.Fatal(g, e)
		}
	}
}

func TestBTreeSetGet3(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSetGet3(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSetGet3(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSetGet3(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSetGet3(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSetGet3(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSetGet3(t, tmpFileWAL) }))
}

func testBTreeDelete0(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	if ok := bt.delete(t, 0); ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	bt.set(t, 0, 0)
	if ok := bt.delete(t, 1); ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 0); !ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 0); ok {
		t.Fatal(ok)
	}

	bt.set(t, 0, 0)
	bt.set(t, 1, 1)
	if ok := bt.delete(t, 1); !ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 1); ok {
		t.Fatal(ok)
	}

	if ok := bt.delete(t, 0); !ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 0); ok {
		t.Fatal(ok)
	}

	bt.set(t, 0, 0)
	bt.set(t, 1, 1)
	if ok := bt.delete(t, 0); !ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(1); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 0); ok {
		t.Fatal(ok)
	}

	if ok := bt.delete(t, 1); !ok {
		t.Fatal(ok)
	}

	if g, e := bt.len(t), int64(0); g != e {
		t.Fatal(g, e)
	}

	if ok := bt.delete(t, 1); ok {
		t.Fatal(ok)
	}
}

func TestBTreeDelete0(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeDelete0(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeDelete0(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeDelete0(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeDelete0(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeDelete0(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeDelete0(t, tmpFileWAL) }))
}

func testBTreeDelete1(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	const N = 1 << 11
	for _, x := range []int{0, -1, 0x5555555, 0xaaaaaaa, 0x3333333, 0xccccccc, 0x31415926, 0x2718282} {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(16, 16, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			a := make([]int, N)
			for i := range a {
				a[i] = (i ^ x) << 1
			}
			for _, k := range a {
				bt.set(t, k, 0)
			}
			for i, k := range a {
				ok := bt.delete(t, k)
				if !ok {
					t.Fatal(i, x, k)
				}

				if g, e := bt.len(t), int64(N-i-1); g != e {
					t.Fatal(i, g, e)
				}
			}
		}()
	}
}

func TestBTreeDelete1(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeDelete1(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeDelete1(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeDelete1(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeDelete1(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeDelete1(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeDelete1(t, tmpFileWAL) }))
}

func testBTreeDelete2(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	const N = 1 << 11
	for _, x := range []int{0, -1, 0x5555555, 0xaaaaaaa, 0x3333333, 0xccccccc, 0x31415926, 0x2718282} {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(16, 16, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			a := make([]int, N)
			rng := rng()
			for i := range a {
				a[i] = (rng.Next() ^ x) << 1
			}
			for _, k := range a {
				bt.set(t, k, 0)
			}

			for i, k := range a {
				ok := bt.delete(t, k)
				if !ok {
					t.Fatal(i, x, k)
				}

				if g, e := bt.len(t), int64(N-i-1); g != e {
					t.Fatal(i, g, e)
				}
			}
		}()
	}
}

func TestBTreeDelete2(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeDelete2(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeDelete2(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeDelete2(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeDelete2(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeDelete2(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeDelete2(t, tmpFileWAL) }))
}

func testBTreeEnumeratorNext(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(2, 4, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	// seeking within 5 keys: 10, 20, 30, 40, 50
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, []int{10, 20, 30, 40, 50}},
		{10, true, []int{10, 20, 30, 40, 50}},
		{15, false, []int{20, 30, 40, 50}},
		{20, true, []int{20, 30, 40, 50}},
		{25, false, []int{30, 40, 50}},
		// 5
		{30, true, []int{30, 40, 50}},
		{35, false, []int{40, 50}},
		{40, true, []int{40, 50}},
		{45, false, []int{50}},
		{50, true, []int{50}},
		// 10
		{55, false, nil},
	}

	for i, test := range table {
		keys := test.keys

		bt.set(t, 10, 100)
		bt.set(t, 20, 200)
		bt.set(t, 30, 300)
		bt.set(t, 40, 400)
		bt.set(t, 50, 500)

		en, hit := bt.seek(t, test.k)

		if g, e := hit, test.hit; g != e {
			t.Fatal(i, g, e)
		}

		j := 0
		for {
			k, v, ok := en.next(t)
			if !ok {
				if err := en.Err(); err != nil {
					t.Fatal(i, err)
				}

				break
			}

			if j >= len(keys) {
				t.Fatal(i, j, len(keys))
			}

			if g, e := k, keys[j]; g != e {
				t.Fatal(i, j, g, e)
			}

			if g, e := v, 10*keys[j]; g != e {
				t.Fatal(i, g, e)
			}

			j++

		}

		if g, e := j, len(keys); g != e {
			t.Fatal(i, j, g, e)
		}
	}
}

func TestBTreeEnumeratorNext(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeEnumeratorNext(t, tmpFileWAL) }))
}

func testBTreeEnumeratorPrev(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(2, 4, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	// seeking within 5 keys: 10, 20, 30, 40, 50
	table := []struct {
		k    int
		hit  bool
		keys []int
	}{
		{5, false, nil},
		{10, true, []int{10}},
		{15, false, []int{10}},
		{20, true, []int{20, 10}},
		{25, false, []int{20, 10}},
		// 5
		{30, true, []int{30, 20, 10}},
		{35, false, []int{30, 20, 10}},
		{40, true, []int{40, 30, 20, 10}},
		{45, false, []int{40, 30, 20, 10}},
		{50, true, []int{50, 40, 30, 20, 10}},
		// 10
		{55, false, []int{50, 40, 30, 20, 10}},
	}

	for i, test := range table {
		keys := test.keys

		bt.set(t, 10, 100)
		bt.set(t, 20, 200)
		bt.set(t, 30, 300)
		bt.set(t, 40, 400)
		bt.set(t, 50, 500)

		en, hit := bt.seek(t, test.k)

		if g, e := hit, test.hit; g != e {
			t.Fatal(i, g, e)
		}

		j := 0
		for {
			k, v, ok := en.prev(t)
			if !ok {
				if err := en.Err(); err != nil {
					t.Fatal(i, err)
				}

				break
			}

			if j >= len(keys) {
				t.Fatal(i, j, len(keys), k, v)
			}

			if g, e := k, keys[j]; g != e {
				t.Fatal(i, j, g, e)
			}

			if g, e := v, 10*keys[j]; g != e {
				t.Fatal(i, g, e)
			}

			j++

		}

		if g, e := j, len(keys); g != e {
			t.Fatal(i, j, g, e)
		}
	}
}

func TestBTreeEnumeratorPrev(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeEnumeratorPrev(t, tmpFileWAL) }))
}

func testBTreeSeekFirst(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	for i := 0; i < 10; i++ {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(2, 4, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			for j := 0; j < i; j++ {
				bt.set(t, 10*j, 100*j)
			}

			switch {
			case i == 0:
				en := bt.seekFirst(t)
				_, _, ok := en.prev(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}

				en = bt.seekFirst(t)
				_, _, ok = en.next(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}
			default:
				en := bt.seekFirst(t)
				k, v, ok := en.prev(t)
				if g, e := ok, true; g != e {
					t.Fatal(i, g, e)
				}

				if g, e := k, 0; g != e {
					t.Fatal(i, g, e)
				}

				if g, e := v, 0; g != e {
					t.Fatal(i, g, e)
				}

				_, _, ok = en.prev(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}

				en = bt.seekFirst(t)
				for j := 0; j < i; j++ {
					k, v, ok := en.next(t)
					if g, e := ok, true; g != e {
						t.Fatal(i, g, e)
					}

					if g, e := k, 10*j; g != e {
						t.Fatal(i, g, e)
					}

					if g, e := v, 100*j; g != e {
						t.Fatal(i, g, e)
					}
				}
				_, _, ok = en.next(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}
			}

		}()
	}
}

func TestBTreeSeekFirst(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSeekFirst(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSeekFirst(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSeekFirst(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSeekFirst(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSeekFirst(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSeekFirst(t, tmpFileWAL) }))
}

func testBTreeSeekLast(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	for i := 0; i < 10; i++ {
		func() {
			db, f := tmpDB(t, ts)

			defer f()

			bt, err := db.NewBTree(2, 4, 8, 8)
			if err != nil {
				t.Fatal(err)
			}

			defer bt.remove(t)

			for j := 0; j < i; j++ {
				bt.set(t, 10*j, 100*j)
			}

			switch {
			case i == 0:
				en := bt.seekLast(t)
				_, _, ok := en.prev(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}

				en = bt.seekLast(t)
				_, _, ok = en.next(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}
			default:
				en := bt.seekLast(t)
				k, v, ok := en.next(t)

				if g, e := ok, true; g != e {
					t.Fatal(i, g, e)
				}

				if g, e := k, 10*(i-1); g != e {
					t.Fatal(i, g, e)
				}

				if g, e := v, 100*(i-1); g != e {
					t.Fatal(i, g, e)
				}

				_, _, ok = en.next(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}

				en = bt.seekLast(t)
				for j := i - 1; j >= 0; j-- {
					k, v, ok := en.prev(t)
					if g, e := ok, true; g != e {
						t.Fatal(i, g, e)
					}

					if g, e := k, 10*j; g != e {
						t.Fatal(i, g, e)
					}

					if g, e := v, 100*j; g != e {
						t.Fatal(i, g, e)
					}
				}
				_, _, ok = en.prev(t)
				if g, e := ok, false; g != e {
					t.Fatal(i, g, e)
				}
			}

		}()
	}
}

func TestBTreeSeekLast(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSeekLast(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSeekLast(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSeekLast(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSeekLast(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSeekLast(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSeekLast(t, tmpFileWAL) }))
}

func testBTreeSeek(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	const N = 1 << 10
	for i := 0; i < N; i++ {
		k := 2*i + 1
		bt.set(t, k, 0)
	}
	for i := 0; i < N; i++ {
		k := 2 * i
		e, ok := bt.seek(t, k)
		if ok {
			t.Fatal(i, k)
		}

		for j := i; j < N; j++ {
			k2, _, ok := e.next(t)
			if !ok {
				t.Fatal(i, k, err)
			}

			if g, e := k2, 2*j+1; g != e {
				t.Fatal(i, j, g, e)
			}
		}

		if _, _, ok = e.next(t); ok {
			t.Fatal(i)
		}
	}
}

func TestBTreeSeek(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeSeek(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeSeek(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeSeek(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeSeek(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeSeek(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeSeek(t, tmpFileWAL) }))
}

// https://github.com/cznic/b/pull/4
func testBTreeBPR4(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	bt, err := db.NewBTree(16, 16, 8, 8)
	if err != nil {
		t.Fatal(err)
	}

	defer bt.remove(t)

	kd := bt.kd
	for i := 0; i < 2*kd+1; i++ {
		k := 1000 * i
		bt.set(t, k, 0)
	}
	bt.delete(t, 1000*kd)
	for i := 0; i < kd; i++ {
		bt.set(t, 1000*(kd+1)-1-i, 0)
	}
	k := 1000*(kd+1) - 1 - kd
	bt.set(t, k, 0)
	if _, ok := bt.get(t, k); !ok {
		t.Fatalf("key lost: %v", k)
	}
}

func TestBTreeBPR4(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testBTreeBPR4(t, tmpMem) }) &&
		t.Run("MemWAL", func(t *testing.T) { testBTreeBPR4(t, tmpMemWAL) }) &&
		t.Run("Map", func(t *testing.T) { testBTreeBPR4(t, tmpMap) }) &&
		t.Run("MapWAL", func(t *testing.T) { testBTreeBPR4(t, tmpMapWAL) }) &&
		t.Run("File", func(t *testing.T) { testBTreeBPR4(t, tmpFile) }) &&
		t.Run("FileWAL", func(t *testing.T) { testBTreeBPR4(t, tmpFileWAL) }))
}
