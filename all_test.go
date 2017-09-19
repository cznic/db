// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/cznic/file"
	"github.com/cznic/strutil"
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

var (
	_ Storage = (*storage)(nil)
)

type testDB struct{ *DB }

func (t *testDB) size() int64 {
	fi, err := t.Stat()
	if err != nil {
		panic(err)
	}

	return fi.Size()
}

type storage struct {
	*file.Allocator
	file.File
}

func (s *storage) Close() error          { return s.Allocator.Close() }
func (s *storage) SetRoot(n int64) error { return w8(s, 0, n) }

func (s *storage) Root() (int64, error) {
	fi, err := s.Stat()
	if err != nil {
		return 0, err
	}

	if fi.Size() == 0 {
		return 0, nil
	}

	return r8(s, 0)
}

func tmpMem(t testing.TB) (file.File, func()) {
	f, err := file.Mem("")
	if err != nil {
		t.Fatal(err)
	}

	return f, func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}
}

func tmpMap(t testing.TB) (file.File, func()) {
	dir, err := ioutil.TempDir("", "file-test-")
	if err != nil {
		t.Fatal(err)
	}

	nm := filepath.Join(dir, "f")
	f0, err := os.OpenFile(nm, os.O_CREATE|os.O_RDWR, 0660)
	if err != nil {
		t.Fatal(err)
	}

	f, err := file.Map(f0)
	if err != nil {
		t.Fatal(err)
	}

	return f, func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
}

func tmpFile(t testing.TB) (file.File, func()) {
	dir, err := ioutil.TempDir("", "file-test-")
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.Create(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatal(err)
	}

	return f, func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}
}

func tmpDB(t testing.TB, ts func(t testing.TB) (file.File, func())) (*testDB, func()) {
	f, g := ts(t)
	a, err := file.NewAllocator(f)
	if err != nil {
		t.Fatal(err)
	}

	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	sz0 := fi.Size()
	db, err := NewDB(&storage{a, f})
	if err != nil {
		t.Fatal(err)
	}

	r := &testDB{db}
	return r,
		func() {
			defer g()

			if g, e := r.size(), sz0; g != e {
				t.Errorf("storage leak, size %#x, expected %#x", g, e)
			}

			if err := r.Close(); err != nil {
				t.Errorf("error closing db: %v", err)
			}
		}
}

func (t *BTree) dump() (r string) {
	var buf bytes.Buffer

	defer func() {
		if err := recover(); err != nil {
			dbg("%q\n%s", err, debug.Stack())
			r = fmt.Sprint(err)
		}
	}()

	f := strutil.IndentFormatter(&buf, "\t")

	num := map[int64]int{}
	visited := map[int64]bool{}

	handle := func(off int64) int {
		if off == 0 {
			return 0
		}

		if n, ok := num[off]; ok {
			return n
		}

		n := len(num) + 1
		num[off] = n
		return n
	}

	var pagedump func(int64, string)
	pagedump = func(off int64, pref string) {
		if off == 0 || visited[off] {
			return
		}

		visited[off] = true
		p, err := t.openPage(off)
		if err != nil {
			panic(err)
		}

		switch x := p.(type) {
		case btDPage:
			c, err := x.len()
			if err != nil {
				panic(err)
			}

			p, err := x.prev()
			if err != nil {
				panic(err)
			}

			n, err := x.next()
			if err != nil {
				panic(err)
			}

			f.Format("%sD#%d(%#x) P#%d N#%d len %d {", pref, handle(off), off, handle(p), handle(n), c)
			for i := 0; i < c; i++ {
				if i != 0 {
					f.Format(" ")
				}
				koff := x.koff(i)
				voff := x.voff(i)
				p, err := x.r8(koff)
				if err != nil {
					panic(err)
				}

				k, err := x.r4(p)
				if err != nil {
					panic(err)
				}

				q, err := x.r8(voff)
				if err != nil {
					panic(err)
				}

				v, err := x.r4(q)
				if err != nil {
					panic(err)
				}

				f.Format("%v:%v", k, v)
			}
			f.Format("}\n")
		case btXPage:
			c, err := x.len()
			if err != nil {
				panic(err)
			}

			f.Format("%sX#%d(%#x) len %d {", pref, handle(off), off, c)
			a := []int64{}
			for i := 0; i <= c; i++ {
				ch, err := x.child(i)
				if err != nil {
					panic(err)
				}

				a = append(a, ch)
				if i != 0 {
					f.Format(" ")
				}
				f.Format("(C#%d(%#x)", handle(ch), ch)
				if i != c {
					ko, err := x.key(i)
					if err != nil {
						panic(err)
					}

					p, err := x.r8(ko)
					if err != nil {
						panic(err)
					}

					k, err := x.r4(p)
					if err != nil {
						panic(err)
					}

					f.Format(" K %v(%#x))", k, ko)
				}
				f.Format(")")
			}
			f.Format("}\n")
			for _, p := range a {
				pagedump(p, pref+". ")
			}
		default:
			panic(fmt.Errorf("%T", x))
		}
	}

	root, err := t.root()
	if err != nil {
		return err.Error()
	}

	pagedump(root, "")
	s := buf.String()
	if s != "" {
		s = s[:len(s)-1]
	}
	return s
}
