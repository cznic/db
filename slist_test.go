// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"testing"

	"github.com/cznic/file"
)

func sListFill(t testing.TB, db *testDB, in []int) []SList {
	a := make([]SList, len(in))
	for i, v := range in {
		n, err := db.NewSList(8)
		if err != nil {
			t.Fatal(i, err)
		}

		if err := db.w8(n.DataOff(), int64(v)); err != nil {
			t.Fatal(err)
		}

		a[i] = n
		if i != 0 {
			if err := n.InsertAfter(a[i-1]); err != nil {
				t.Fatal(i, err)
			}
		}
	}
	return a
}

func sListVerify(iTest int, t testing.TB, db *testDB, in []SList, out []int) {
	defer func() {
		for i, v := range in {
			if err := db.Free(v.Off); err != nil {
				t.Error(i)
			}
		}
	}()

	off := in[0].Off
	for i, ev := range out {
		n, err := db.OpenSList(off)
		if err != nil {
			t.Fatal(iTest, i, err)
		}

		v, err := db.r8(n.DataOff())
		if g, e := v, int64(ev); g != e {
			t.Fatal(iTest, i, g, e)
		}

		next, err := n.Next()
		if err != nil {
			t.Fatal(iTest, i, err)
		}

		if next == 0 {
			if i != len(out)-1 {
				t.Fatal(iTest, i)
			}

			break
		}

		off = next
	}
}

func TestSListFill(t *testing.T) {
	t.Run("Mem", func(t *testing.T) { testSListFill(t, tmpMem) })
	t.Run("Cache", func(t *testing.T) { testSListFill(t, tmpCache) })
	t.Run("File", func(t *testing.T) { testSListFill(t, tmpFile) })
}

func testSListFill(t *testing.T, ts func(t testing.TB) (file.File, func())) {
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
		l := sListFill(t, db, test.data)
		sListVerify(iTest, t, db, l, test.data)
	}
}
