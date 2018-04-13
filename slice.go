// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"
)

const (
	oSliceCap    = 8 * iota // int64	0	8
	oSliceData              // int64	8	8
	oSliceLen               // int64	16	8
	oSliceSzItem            // int64	24	8

	szSlice
)

// Slice is a numbered sequence of items.
type Slice struct {
	*DB
	Off    int64 // Location in the database. R/O
	SzItem int64 // The szItem argument of NewSlice. R/O
}

// NewSlice allocates and returns a new Slice or an error, if any. The len is
// the initial length of the Slice and cap is its initial capacity. The szItem
// argument is the size of an item. The slice items up to and cap are zeroed.
func (db *DB) NewSlice(len, cap, szItem int64) (s *Slice, err error) {
	if len < 0 || cap < 0 || len > cap || szItem < 0 {
		panic(fmt.Errorf("%T.NewSlice: invalid argument", db))
	}

	var data, off int64

	defer func() {
		if err != nil {
			if data != 0 {
				db.Free(data)
			}
			if off != 0 {
				db.Free(off)
			}
		}
	}()

	if n := cap * szItem; n != 0 {
		if data, err = db.Calloc(n); err != nil {
			return nil, err
		}
	}

	if off, err = db.Calloc(szSlice); err != nil {
		return nil, err
	}

	r := &Slice{DB: db, Off: off, SzItem: szItem}
	if err = r.setCap(cap); err != nil {
		return nil, err
	}

	if data != 0 {
		if err = r.setData(data); err != nil {
			return nil, err
		}
	}

	if err = r.setLen(len); err != nil {
		return nil, err
	}

	if err = r.setSzItem(szItem); err != nil {
		return nil, err
	}

	return r, nil
}

// OpenSlice returns an existing Slice found at offset off or an error, if any.
// The off argument must have been acquired from NewSlice.
func (db *DB) OpenSlice(off int64) (*Slice, error) {
	szItem, err := db.r8(off + oSliceSzItem)
	if err != nil {
		return nil, err
	}

	if szItem < 0 {
		return nil, fmt.Errorf("%T.OpenSlice: corrupted database", db)
	}

	return &Slice{DB: db, Off: off, SzItem: szItem}, nil
}

func (s *Slice) cap() (int64, error)     { return s.r8(s.Off + oSliceCap) }
func (s *Slice) data() (int64, error)    { return s.r8(s.Off + oSliceData) }
func (s *Slice) len() (int64, error)     { return s.r8(s.Off + oSliceLen) }
func (s *Slice) setCap(n int64) error    { return s.w8(s.Off+oSliceCap, n) }
func (s *Slice) setData(n int64) error   { return s.w8(s.Off+oSliceData, n) }
func (s *Slice) setLen(n int64) error    { return s.w8(s.Off+oSliceLen, n) }
func (s *Slice) setSzItem(n int64) error { return s.w8(s.Off+oSliceSzItem, n) }

// Cap returns the capacity of s or an error, if any.
func (s *Slice) Cap() (int64, error) { return s.r8(s.Off + oSliceCap) }

// Len returns the number of items in s or an error, if any.
func (s *Slice) Len() (int64, error) { return s.r8(s.Off + oSliceLen) }

//TODO SetCap?

// SetLen sets the number of items in s. SetLen panics if n is negative or
// greater than the capacity of s.
func (s *Slice) SetLen(n int64) error {
	if n < 0 {
		panic(fmt.Errorf("%T.SetLen: negative value: %v", s, n))
	}

	cap, err := s.cap()
	if err != nil {
		return err
	}

	if n < 0 {
		panic(fmt.Errorf("%T.SetLen: value greater than capacity: %v, %v", s, n, cap))
	}

	return s.setLen(n)
}
