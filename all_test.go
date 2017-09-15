// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cznic/file"
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

	return f, func() {}
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
