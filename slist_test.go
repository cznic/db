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

func sListVerify(iTest int, t testing.TB, db *testDB, in []SList, out []int) {
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
	for i, ev := range out {
		n, err := db.OpenSList(off)
		if err != nil {
			t.Fatal(iTest, i, err)
		}

		v, err := db.r8(n.DataOff())
		if g, e := v, int64(ev); g != e {
			t.Fatalf("test #%v, list item #%v, got %v, expected %v", iTest, i, g, e)
		}

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

func TestSList(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testSList(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testSList(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testSList(t, tmpFile) }))
}

func testSList(t *testing.T, ts func(t testing.TB) (file.File, func())) {
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
		in := sListFill(t, db, test.data)
		sListVerify(iTest, t, db, in, test.data)
	}
}

func TestSListInsertAfter(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testSListInsertAfter(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testSListInsertAfter(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testSListInsertAfter(t, tmpFile) }))
}

func testSListInsertAfter(t *testing.T, ts func(t testing.TB) (file.File, func())) {
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
		in := sListFill(t, db, test.in)
		i := test.index
		n, err := db.NewSList(8)
		if err != nil {
			t.Fatal(iTest)
		}

		if err := n.w8(n.DataOff(), -1); err != nil {
			t.Fatal(iTest)
		}

		if err := n.InsertAfter(in[i].Off); err != nil {
			t.Fatal(iTest)
		}

		in = append(in[:i+1], append([]SList{n}, in[i+1:]...)...)
		sListVerify(iTest, t, db, in, test.out)
	}
}

func TestSListInsertBefore(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testSListInsertBefore(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testSListInsertBefore(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testSListInsertBefore(t, tmpFile) }))
}

func testSListInsertBefore(t *testing.T, ts func(t testing.TB) (file.File, func())) {
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
		in := sListFill(t, db, test.in)
		i := test.index
		var prev int64
		if i != 0 {
			prev = in[i-1].Off
		}
		n, err := db.NewSList(8)
		if err != nil {
			t.Fatal(iTest)
		}

		if err := n.w8(n.DataOff(), -1); err != nil {
			t.Fatal(iTest)
		}

		if err := n.InsertBefore(prev, in[i].Off); err != nil {
			t.Fatal(iTest)
		}

		in = append(in[:i], append([]SList{n}, in[i:]...)...)
		sListVerify(iTest, t, db, in, test.out)
	}
}

func TestSListRemove(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testSListRemove(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testSListRemove(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testSListRemove(t, tmpFile) }))
}

func testSListRemove(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tab := []struct {
		in    []int
		index int
		out   []int
	}{
		{[]int{10}, 0, nil},
		{[]int{10, 20}, 0, []int{20}},
		{[]int{10, 20}, 1, []int{10}},
		{[]int{10, 20, 30}, 0, []int{20, 30}},
		{[]int{10, 20, 30}, 1, []int{10, 30}},
		{[]int{10, 20, 30}, 2, []int{10, 20}},
		{[]int{10, 20, 30, 40}, 0, []int{20, 30, 40}},
		{[]int{10, 20, 30, 40}, 1, []int{10, 30, 40}},
		{[]int{10, 20, 30, 40}, 2, []int{10, 20, 40}},
		{[]int{10, 20, 30, 40}, 3, []int{10, 20, 30}},
	}
	for iTest, test := range tab {
		in := sListFill(t, db, test.in)
		i := test.index
		var prev int64
		if i != 0 {
			prev = in[i-1].Off
		}
		if err := in[i].Remove(prev); err != nil {
			t.Fatal(iTest)
		}

		in = append(in[:i], in[i+1:]...)
		sListVerify(iTest, t, db, in, test.out)
	}
}

func TestSListRemoveToEnd(t *testing.T) {
	use(t.Run("Mem", func(t *testing.T) { testSListRemoveToEnd(t, tmpMem) }) &&
		t.Run("Map", func(t *testing.T) { testSListRemoveToEnd(t, tmpMap) }) &&
		t.Run("File", func(t *testing.T) { testSListRemoveToEnd(t, tmpFile) }))
}

func testSListRemoveToEnd(t *testing.T, ts func(t testing.TB) (file.File, func())) {
	db, f := tmpDB(t, ts)

	defer f()

	tab := []struct {
		in    []int
		index int
		out   []int
	}{
		{[]int{10}, 0, nil},
		{[]int{10, 20}, 0, nil},
		{[]int{10, 20}, 1, []int{10}},
		{[]int{10, 20, 30}, 0, nil},
		{[]int{10, 20, 30}, 1, []int{10}},
		{[]int{10, 20, 30}, 2, []int{10, 20}},
		{[]int{10, 20, 30, 40}, 0, nil},
		{[]int{10, 20, 30, 40}, 1, []int{10}},
		{[]int{10, 20, 30, 40}, 2, []int{10, 20}},
		{[]int{10, 20, 30, 40}, 3, []int{10, 20, 30}},
	}
	for iTest, test := range tab {
		in := sListFill(t, db, test.in)
		i := test.index
		var prev int64
		if i != 0 {
			prev = in[i-1].Off
		}
		if err := in[i].RemoveToLast(prev); err != nil {
			t.Fatal(iTest)
		}

		in = in[:i]
		sListVerify(iTest, t, db, in, test.out)
	}
}

func benchmarkNewSList(b *testing.B, ts func(t testing.TB) (file.File, func()), dataSize int64) {
	db, f := tmpDB(b, ts)

	defer f()

	a := make([]int64, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := db.NewSList(dataSize)
		if err != nil {
			b.Fatal(err)
		}

		a[i] = n.Off
	}
	b.StopTimer()
	for _, v := range a {
		if err := db.Free(v); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewSList0(b *testing.B) {
	b.Run("Mem", func(b *testing.B) { benchmarkNewSList(b, tmpMem, 0) })
	b.Run("Map", func(b *testing.B) { benchmarkNewSList(b, tmpMap, 0) })
	b.Run("File", func(b *testing.B) { benchmarkNewSList(b, tmpFile, 0) })
}

func benchmarkSListInsertAfter(b *testing.B, ts func(t testing.TB) (file.File, func()), dataSize int64) {
	db, f := tmpDB(b, ts)

	defer f()

	r, err := db.NewSList(dataSize)
	if err != nil {
		b.Fatal(err)
	}

	a := make([]SList, b.N)
	for i := range a {
		n, err := db.NewSList(dataSize)
		if err != nil {
			b.Fatal(err)
		}

		a[i] = n
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a[i].InsertAfter(r.Off); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := r.Free(r.Off); err != nil {
		b.Fatal(err)
	}
	for _, v := range a {
		if err := db.Free(v.Off); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSListInsertAfter(b *testing.B) {
	b.Run("Mem", func(b *testing.B) { benchmarkSListInsertAfter(b, tmpMem, 0) })
	b.Run("Map", func(b *testing.B) { benchmarkSListInsertAfter(b, tmpMap, 0) })
	b.Run("File", func(b *testing.B) { benchmarkSListInsertAfter(b, tmpFile, 0) })
}

func benchmarkSListInsertBefore(b *testing.B, ts func(t testing.TB) (file.File, func()), dataSize int64) {
	db, f := tmpDB(b, ts)

	defer f()

	r, err := db.NewSList(dataSize)
	if err != nil {
		b.Fatal(err)
	}

	a := make([]SList, b.N)
	for i := range a {
		n, err := db.NewSList(dataSize)
		if err != nil {
			b.Fatal(err)
		}

		a[i] = n
	}
	var prev int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a[i].InsertBefore(prev, r.Off); err != nil {
			b.Fatal(err)
		}

		prev = a[i].Off
	}
	b.StopTimer()
	if err := r.Free(r.Off); err != nil {
		b.Fatal(err)
	}
	for _, v := range a {
		if err := db.Free(v.Off); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSListInsertBefore(b *testing.B) {
	b.Run("Mem", func(b *testing.B) { benchmarkSListInsertBefore(b, tmpMem, 0) })
	b.Run("Map", func(b *testing.B) { benchmarkSListInsertBefore(b, tmpMap, 0) })
	b.Run("File", func(b *testing.B) { benchmarkSListInsertBefore(b, tmpFile, 0) })
}

func benchmarkSListNext(b *testing.B, ts func(t testing.TB) (file.File, func()), dataSize int64) {
	db, f := tmpDB(b, ts)

	defer f()

	n, err := db.NewSList(dataSize)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := n.Next()
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := n.Free(n.Off); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkSListNext(b *testing.B) {
	b.Run("Mem", func(b *testing.B) { benchmarkSListNext(b, tmpMem, 0) })
	b.Run("Map", func(b *testing.B) { benchmarkSListNext(b, tmpMap, 0) })
	b.Run("File", func(b *testing.B) { benchmarkSListNext(b, tmpFile, 0) })
}

func benchmarkSListRemove(b *testing.B, ts func(t testing.TB) (file.File, func()), dataSize int64) {
	db, f := tmpDB(b, ts)

	defer f()

	r, err := db.NewSList(dataSize)
	if err != nil {
		b.Fatal(err)
	}

	a := make([]SList, b.N)
	for i := range a {
		n, err := db.NewSList(dataSize)
		if err != nil {
			b.Fatal(err)
		}

		if err := n.InsertAfter(r.Off); err != nil {
			b.Fatal(err)
		}

		a[i] = n
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := a[i].Remove(r.Off); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := r.Free(r.Off); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkSListRemove(b *testing.B) {
	b.Run("Mem", func(b *testing.B) { benchmarkSListRemove(b, tmpMem, 0) })
	b.Run("Map", func(b *testing.B) { benchmarkSListRemove(b, tmpMap, 0) })
	b.Run("File", func(b *testing.B) { benchmarkSListRemove(b, tmpFile, 0) })
}
