// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"

	"github.com/cznic/mathutil"
)

const (
	oBTLen   = 8 * iota // int64	0	8
	oBTFirst            // int64	8	8
	oBTLast             // int64	16	8
	oBTKD               // int64	24	8
	oBTKX               // int64	32	8
	oBTSzKey            // int64	40	8
	oBTSzVal            // int64	48	8

	szBTree
)

type BTree struct {
	*DB
	Off   int64
	SzKey int64
	SzVal int64
	kd    int
	kx    int
}

func (db *DB) NewBTree(nd, nx int, szKey, szVal int64) (*BTree, error) {
	if nd < 0 || nd > (mathutil.MaxInt-1)/2 ||
		nx < 0 || nx > (mathutil.MaxInt-2)/2 ||
		szKey < 0 || szVal < 0 {
		panic(fmt.Errorf("%T.NewBTree: invalid argument", db))
	}

	if nd == 0 {
		nd = 256 //TODO bench tune
	}
	kd := mathutil.Max(nd/2, 1)
	if nx == 0 {
		nx = 256 //TODO bench tune
	}
	kx := mathutil.Max(nx/2, 2)
	off, err := db.Calloc(szBTree)
	if err != nil {
		return nil, err
	}

	if err := db.w8(off+oBTKD, int64(kd)); err != nil {
		return nil, err
	}

	if err := db.w8(off+oBTKX, int64(kx)); err != nil {
		return nil, err
	}

	if err := db.w8(off+oBTSzKey, szKey); err != nil {
		return nil, err
	}

	if err := db.w8(off+oBTSzVal, szVal); err != nil {
		return nil, err
	}

	return &BTree{DB: db, Off: off, kd: kd, kx: kx}, nil
}

func (db *DB) OpenBTree(off int64) (*BTree, error) {
	n, err := db.r8(off + oBTKD)
	if err != nil {
		return nil, err
	}

	if n < 0 || n > (mathutil.MaxInt-1)/2 {
		return nil, fmt.Errorf("%T.OpenBTree: corrupted database", db)
	}

	kd := int(n)
	if n, err = db.r8(off + oBTKX); err != nil {
		return nil, err
	}

	if n < 0 || n > (mathutil.MaxInt-2)/2 {
		return nil, fmt.Errorf("%T.OpenBTree: corrupted database", db)
	}

	kx := int(n)
	szKey, err := db.r8(off + oBTSzKey)
	if err != nil {
		return nil, err
	}

	szVal, err := db.r8(off + oBTSzVal)
	if err != nil {
		return nil, err
	}

	return &BTree{DB: db, Off: off, kd: int(kd), kx: int(kx), SzKey: szKey, SzVal: szVal}, nil
}

func (t *BTree) setFirst(n int64) error { return t.w8(t.Off+oBTFirst, n) }
func (t *BTree) setLast(n int64) error  { return t.w8(t.Off+oBTLast, n) }
func (t *BTree) setLen(n int64) error   { return t.w8(t.Off+oBTLen, n) }

func (t *BTree) First() (int64, error) { return t.r8(t.Off + oBTFirst) }
func (t *BTree) Last() (int64, error)  { return t.r8(t.Off + oBTLast) }
func (t *BTree) Len() (int64, error)   { return t.r8(t.Off + oBTLen) }
