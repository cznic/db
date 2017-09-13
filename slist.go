// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

const (
	oSListNext = 8 * iota // int64		0	8
	oSListData            // [dataSize]byte	8	dataSize
)

type SList struct {
	*DB
	Off int64
}

func (db *DB) NewSList(dataSize int64) (SList, error) {
	off, err := db.Alloc(oSListData + dataSize)
	if err != nil {
		return SList{}, err
	}

	r, err := db.OpenSList(off)
	if err != nil {
		return SList{}, err
	}

	return r, r.SetNext(0)
}

func (db *DB) OpenSList(off int64) (SList, error) { return SList{db, off}, nil }

func (s SList) DataOff() int64                  { return s.Off + oSListData }
func (s SList) InsertBefore(before SList) error { return s.SetNext(before.Off) }
func (s SList) Next() (int64, error)            { return s.r8(s.Off + oSListNext) }
func (s SList) SetNext(off int64) error         { return s.w8(s.Off+oSListNext, off) }

func (s SList) InsertAfter(after SList) error {
	afterNext, err := after.Next()
	if err != nil {
		return err
	}

	if err = after.SetNext(s.Off); err != nil {
		return err
	}

	return s.SetNext(afterNext)
}
