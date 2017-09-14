// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"testing"

	"github.com/cznic/file"
)

func dListFill(t testing.TB, db *testDB, in []int) []DList {
	a := make([]DList, len(in))
	for i, v := range in {
		n, err := db.NewDList(8)
		if err != nil {
			t.Fatal(i, n, err)
		}

		if err := db.w8(n.DataOff(), int64(v)); err != nil {
			t.Fatal(err)
		}

		a[i] = n
		if i != 0 {
			if err := n.InsertAfter(a[i-1].Off); err != nil {
				t.Fatal(i, err)
			}
		}
	}
	return a
}

func dListVerify(iTest int, t testing.TB, db *testDB, in []DList, out []int) {
	if len(out) == 0 {
		return
	}

	defer func() {
		for i, v := range in {
			if err := db.Free(v.Off); err != nil {
				t.Error(i)
			}
		}
	}()

	off := in[0].Off
	var prev int64
	for i, ev := range out {
		n, err := db.OpenDList(off)
		if err != nil {
			t.Fatal(iTest, i, err)
		}

		p, err := n.Prev()
		if err != nil {
			t.Fatal(iTest, i, err)
		}

		if g, e := p, prev; g != e {
			t.Fatalf("test #%x, list item %v, got prev %#x, expected %#x", iTest, i, g, e)
		}

		v, err := db.r8(n.DataOff())
		if g, e := v, int64(ev); g != e {
			t.Fatalf("test #%v, list item #%v, got %v, expected %v", iTest, i, g, e)
		}

		prev = off
		if off, err = n.Next(); err != nil {
			t.Fatal(iTest, i, err)
		}

		if off == 0 {
			if i != len(out)-1 {
				t.Fatal(iTest, i)
			}

			break
		}
	}
	if off != 0 {
		t.Fatal(iTest, off)
	}
}

func TestDList(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testDList(t, tmpMem) }) &&
		t.Run("Cache", func(t *testing.T) { testDList(t, tmpCache) }) &&
		t.Run("File", func(t *testing.T) { testDList(t, tmpFile) }))
}

func testDList(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tab := []struct {
		data []int
	}{
		{[]int{10}},
		{[]int{10, 20}},
		{[]int{10, 20, 30}},
		{[]int{10, 20, 30, 40}},
	}

	for iTest, test := range tab {
		in := dListFill(t, db, test.data)
		dListVerify(iTest, t, db, in, test.data)
	}
}

func TestDListInsertAfter(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testDListInsertAfter(t, tmpMem) }) &&
		t.Run("Cache", func(t *testing.T) { testDListInsertAfter(t, tmpCache) }) &&
		t.Run("File", func(t *testing.T) { testDListInsertAfter(t, tmpFile) }))
}

func testDListInsertAfter(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tab := []struct {
		in    []int
		index int
		out   []int
	}{
		{[]int{10}, 0, []int{10, -1}},
		{[]int{10, 20}, 0, []int{10, -1, 20}},
		{[]int{10, 20}, 1, []int{10, 20, -1}},
		{[]int{10, 20, 30}, 0, []int{10, -1, 20, 30}},
		{[]int{10, 20, 30}, 1, []int{10, 20, -1, 30}},
		{[]int{10, 20, 30}, 2, []int{10, 20, 30, -1}},
		{[]int{10, 20, 30, 40}, 0, []int{10, -1, 20, 30, 40}},
		{[]int{10, 20, 30, 40}, 1, []int{10, 20, -1, 30, 40}},
		{[]int{10, 20, 30, 40}, 2, []int{10, 20, 30, -1, 40}},
		{[]int{10, 20, 30, 40}, 3, []int{10, 20, 30, 40, -1}},
	}
	for iTest, test := range tab {
		in := dListFill(t, db, test.in)
		i := test.index
		n, err := db.NewDList(8)
		if err != nil {
			t.Fatal(iTest)
		}

		if err := n.w8(n.DataOff(), -1); err != nil {
			t.Fatal(iTest)
		}

		if err := n.InsertAfter(in[i].Off); err != nil {
			t.Fatal(iTest)
		}

		in = append(in[:i+1], append([]DList{n}, in[i+1:]...)...)
		dListVerify(iTest, t, db, in, test.out)
	}
}

func TestDListInsertBefore(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testDListInsertBefore(t, tmpMem) }) &&
		t.Run("Cache", func(t *testing.T) { testDListInsertBefore(t, tmpCache) }) &&
		t.Run("File", func(t *testing.T) { testDListInsertBefore(t, tmpFile) }))
}

func testDListInsertBefore(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tab := []struct {
		in    []int
		index int
		out   []int
	}{
		{[]int{10}, 0, []int{-1, 10}},
		{[]int{10, 20}, 0, []int{-1, 10, 20}},
		{[]int{10, 20}, 1, []int{10, -1, 20}},
		{[]int{10, 20, 30}, 0, []int{-1, 10, 20, 30}},
		{[]int{10, 20, 30}, 1, []int{10, -1, 20, 30}},
		{[]int{10, 20, 30}, 2, []int{10, 20, -1, 30}},
		{[]int{10, 20, 30, 40}, 0, []int{-1, 10, 20, 30, 40}},
		{[]int{10, 20, 30, 40}, 1, []int{10, -1, 20, 30, 40}},
		{[]int{10, 20, 30, 40}, 2, []int{10, 20, -1, 30, 40}},
		{[]int{10, 20, 30, 40}, 3, []int{10, 20, 30, -1, 40}},
	}
	for iTest, test := range tab {
		in := dListFill(t, db, test.in)
		i := test.index
		n, err := db.NewDList(8)
		if err != nil {
			t.Fatal(iTest)
		}

		if err := n.w8(n.DataOff(), -1); err != nil {
			t.Fatal(iTest)
		}

		if err := n.InsertBefore(in[i].Off); err != nil {
			t.Fatal(iTest)
		}

		in = append(in[:i], append([]DList{n}, in[i:]...)...)
		dListVerify(iTest, t, db, in, test.out)
	}
}
