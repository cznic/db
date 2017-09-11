// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
	"testing"
)

func sListFill(t *testing.T, s store, in []int) (*sList, []*sListNode) {
	l, err := newSList(s)
	if err != nil {
		t.Fatal(err)
	}

	a := make([]*sListNode, len(in))
	var b [8]byte
	for i := len(in) - 1; i >= 0; i-- {
		data := in[i]
		n, err := l.newNode(n2b(b[:], int64(data)))
		if err != nil {
			t.Fatal(err)
		}

		if err := l.insertBeginning(n); err != nil {
			t.Fatal(err)
		}
		a[i] = n
	}
	for i, v := range a {
		n, err := l.openNode(v.h, b[:])
		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(n.data, v.data) {
			t.Fatal(false)
		}

		a[i] = n
	}
	return l, a
}

func sListVerify(iTest int, t *testing.T, l *sList, out []int) {
	defer func() {
		if err := l.delete(); err != nil {
			t.Fatal(iTest, err)
		}
	}()

	l, err := openSList(l.s, l.h)
	if err != nil {
		t.Fatal(iTest, err)
	}

	var b [8]byte
	n, err := l.firstNode(b[:])
	if err != nil {
		t.Fatal(iTest, err)
	}

	var next int64
	switch len(out) {
	default:
		for i, v := range out {
			if n == nil {
				t.Fatal(iTest, i, n)
			}

			if g, e := n.h, next; next != 0 && g != e {
				t.Fatal(iTest, i, g, e)
			}

			if g, e := b2n(n.data), int64(v); g != e {
				t.Fatal(iTest, i, g, e)
			}

			next = n.next
			n, err = n.nextNode(b[:])
			if err != nil {
				t.Fatal(iTest, i, err)
			}
		}
		fallthrough
	case 0:
		if g, e := next, int64(0); g != e {
			t.Fatal(iTest, g, e)
		}

		if n != nil {
			t.Fatal(iTest, n)
		}
	}
}

func TestSListInsertBeginning(t *testing.T) {
	testSListInsertBeginning(t, newMemTester())
}

func testSListInsertBeginning(t *testing.T, s tester) {
	defer s.free()

	tab := []struct {
		data []int
	}{
		{nil},
		{[]int{10}},
		{[]int{10, 20}},
		{[]int{10, 20, 30}},
		{[]int{10, 20, 30, 40}},
	}

	for iTest, test := range tab {
		l, _ := sListFill(t, s, test.data)
		sListVerify(iTest, t, l, test.data)
	}
}

func TestSListRemoveBeginning(t *testing.T) {
	testSListRemoveBeginning(t, newMemTester())
}

func testSListRemoveBeginning(t *testing.T, s tester) {
	defer s.free()

	tab := []struct {
		in, out []int
		fail    bool
	}{
		{nil, nil, true},
		{[]int{1}, nil, false},
		{[]int{1, 2}, []int{2}, false},
		{[]int{1, 2, 3}, []int{2, 3}, false},
		{[]int{1, 2, 3, 4}, []int{2, 3, 4}, false},
	}

	for iTest, test := range tab {
		in := test.in
		l, _ := sListFill(t, s, in)
		err := l.removeBeginning()
		if g, e := err != nil, test.fail; g != e {
			t.Fatal(g, e)
		}

		sListVerify(iTest, t, l, test.out)
	}
}

func TestSListInsertAfter(t *testing.T) {
	testSListInsertAfter(t, newMemTester())
}

func testSListInsertAfter(t *testing.T, s tester) {
	defer s.free()

	tab := []struct {
		in          []int
		after       int
		newNodeData int
		out         []int
	}{
		{[]int{1}, 0, 99, []int{1, 99}},
		{[]int{1, 2}, 0, 99, []int{1, 99, 2}},
		{[]int{1, 2}, 1, 99, []int{1, 2, 99}},
		{[]int{1, 2, 3}, 0, 99, []int{1, 99, 2, 3}},
		{[]int{1, 2, 3}, 1, 99, []int{1, 2, 99, 3}},
		{[]int{1, 2, 3}, 2, 99, []int{1, 2, 3, 99}},
		{[]int{1, 2, 3, 4}, 0, 99, []int{1, 99, 2, 3, 4}},
		{[]int{1, 2, 3, 4}, 1, 99, []int{1, 2, 99, 3, 4}},
		{[]int{1, 2, 3, 4}, 2, 99, []int{1, 2, 3, 99, 4}},
		{[]int{1, 2, 3, 4}, 3, 99, []int{1, 2, 3, 4, 99}},
	}

	var b [8]byte
	for iTest, test := range tab {
		l, a := sListFill(t, s, test.in)
		n, err := l.newNode(n2b(b[:], int64(test.newNodeData)))
		if err != nil {
			t.Fatal(err)
		}

		if err := a[test.after].insertAfter(n); err != nil {
			t.Fatal(err)
		}

		sListVerify(iTest, t, l, test.out)
	}
}

func TestSListRemoveAfter(t *testing.T) {
	testSListRemoveAfter(t, newMemTester())
}

func testSListRemoveAfter(t *testing.T, s tester) {
	defer s.free()

	tab := []struct {
		in    []int
		after int
		fail  bool
		out   []int
	}{
		{[]int{1}, 0, true, []int{1}},
		{[]int{1, 2}, 0, false, []int{1}},
		{[]int{1, 2}, 1, true, []int{1, 2}},
		{[]int{1, 2, 3}, 0, false, []int{1, 3}},
		{[]int{1, 2, 3}, 1, false, []int{1, 2}},
		{[]int{1, 2, 3}, 2, true, []int{1, 2, 3}},
		{[]int{1, 2, 3, 4}, 0, false, []int{1, 3, 4}},
		{[]int{1, 2, 3, 4}, 1, false, []int{1, 2, 4}},
		{[]int{1, 2, 3, 4}, 2, false, []int{1, 2, 3}},
		{[]int{1, 2, 3, 4}, 3, true, []int{1, 2, 3, 4}},
	}

	for iTest, test := range tab {
		l, a := sListFill(t, s, test.in)
		err := a[test.after].removeAfter()
		if g, e := err != nil, test.fail; g != e {
			t.Fatal(iTest, g, e)
		}

		sListVerify(iTest, t, l, test.out)
	}
}

func BenchmarkSListClear1e1(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e1)
}

func BenchmarkSListClear1e2(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e2)
}

func BenchmarkSListClear1e3(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e3)
}

func BenchmarkSListClear1e4(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e4)
}

func BenchmarkSListClear1e5(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e5)
}

func BenchmarkSListClear1e6(b *testing.B) {
	benchmarkSListClear(b, newMemTester(), 1e6)
}

func benchmarkSListClear(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for i := 0; i < n; i++ {
			node, err := l.newNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if err := l.insertBeginning(node); err != nil {
				b.Fatal(err)
			}

			node.close()
		}
		b.StartTimer()
		if err := l.clear(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkSListInsertBeginning1e1(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e1)
}

func BenchmarkSListInsertBeginning1e2(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e2)
}

func BenchmarkSListInsertBeginning1e3(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e3)
}

func BenchmarkSListInsertBeginning1e4(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e4)
}

func BenchmarkSListInsertBeginning1e5(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e5)
}

func BenchmarkSListInsertBeginning1e6(b *testing.B) {
	benchmarkSListInsertBeginning(b, newMemTester(), 1e6)
}

func benchmarkSListInsertBeginning(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := 0; i < n; i++ {
			node, err := l.newNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if err := l.insertBeginning(node); err != nil {
				b.Fatal(err)
			}

			node.close()
		}
		b.StopTimer()
		if err := l.clear(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
}

func BenchmarkSListNext1e1(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e1)
}

func BenchmarkSListNext1e2(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e2)
}

func BenchmarkSListNext1e3(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e3)
}

func BenchmarkSListNext1e4(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e4)
}

func BenchmarkSListNext1e5(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e5)
}

func BenchmarkSListNext1e6(b *testing.B) {
	benchmarkSListNext(b, newMemTester(), 1e6)
}

func benchmarkSListNext(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	for i := 0; i < n; i++ {
		node, err := l.newNode(d)
		if err != nil {
			b.Fatal(err)
		}

		if err := l.insertBeginning(node); err != nil {
			b.Fatal(err)
		}

		node.close()
	}

	first, err := l.firstNode(d)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := 0
		for node := first; node != nil; {
			next, err := node.nextNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if m != 0 {
				node.close()
			}
			m++
			node = next
		}
		if g, e := m, n; g != e {
			b.Fatal(g, e)
		}
	}
	b.StopTimer()
	first.close()
}

func BenchmarkSListRemoveBeginning1e1(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e1)
}

func BenchmarkSListRemoveBeginning1e2(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e2)
}

func BenchmarkSListRemoveBeginning1e3(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e3)
}

func BenchmarkSListRemoveBeginning1e4(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e4)
}

func BenchmarkSListRemoveBeginning1e5(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e5)
}

func BenchmarkSListRemoveBeginning1e6(b *testing.B) {
	benchmarkSListRemoveBeginning(b, newMemTester(), 1e6)
}

func benchmarkSListRemoveBeginning(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for i := 0; i < n; i++ {
			node, err := l.newNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if err := l.insertBeginning(node); err != nil {
				b.Fatal(err)
			}

			node.close()
		}

		h := l.h
		l.close()
		if l, err = openSList(s, h); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		for l.first != 0 {
			if err := l.removeBeginning(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

func BenchmarkSListInsertAfter1e1(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e1)
}

func BenchmarkSListInsertAfter1e2(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e2)
}

func BenchmarkSListInsertAfter1e3(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e3)
}

func BenchmarkSListInsertAfter1e4(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e4)
}

func BenchmarkSListInsertAfter1e5(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e5)
}

func BenchmarkSListInsertAfter1e6(b *testing.B) {
	benchmarkSListInsertAfter(b, newMemTester(), 1e6)
}

func benchmarkSListInsertAfter(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node0, err := l.newNode(d)
		if err != nil {
			b.Fatal(err)
		}

		if err := l.insertBeginning(node0); err != nil {
			b.Fatal(err)
		}

		for i := 0; i < n; i++ {
			node, err := l.newNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if err := node0.insertAfter(node); err != nil {
				b.Fatal(err)
			}

			node.close()
		}

		node0.close()
		b.StopTimer()
		if err := l.clear(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.StopTimer()
}

func BenchmarkSListRemoveAfter1e1(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e1)
}

func BenchmarkSListRemoveAfter1e2(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e2)
}

func BenchmarkSListRemoveAfter1e3(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e3)
}

func BenchmarkSListRemoveAfter1e4(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e4)
}

func BenchmarkSListRemoveAfter1e5(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e5)
}

func BenchmarkSListRemoveAfter1e6(b *testing.B) {
	benchmarkSListRemoveAfter(b, newMemTester(), 1e6)
}

func benchmarkSListRemoveAfter(b *testing.B, s tester, n int) {
	defer s.free()

	d := get(8)
	defer put(d)

	l, err := newSList(s)
	if err != nil {
		b.Fatal(err)
	}

	defer func(l *sList) {
		if err := l.delete(); err != nil {
			b.Fatal(err)
		}
	}(l)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var node0 *sListNode
		for i := 0; i < n; i++ {
			node, err := l.newNode(d)
			if err != nil {
				b.Fatal(err)
			}

			if err := l.insertBeginning(node); err != nil {
				b.Fatal(err)
			}

			switch {
			case i == 0:
				node0 = node
			default:
				node.close()
			}
		}

		b.StartTimer()
		for node0.next != 0 {
			if err := node0.removeAfter(); err != nil {
				b.Fatal(err)
			}
		}
		node0.close()
	}
	b.StopTimer()
}
