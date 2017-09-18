// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"
	"math"

	"github.com/cznic/mathutil"
)

var (
	_ btPage = (*btDPage)(nil)
)

const (
	btTagDataPage = iota
	btTagIndexPage
)

const (
	oBTRoot  = 8 * iota // int64	0	8
	oBTLen              // int64	8	8
	oBTFirst            // int64	16	8
	oBTLast             // int64	24	8
	oBTKD               // int64	32	8
	oBTKX               // int64	40	8
	oBTSzKey            // int64	48	8
	oBTSzVal            // int64	56	8

	szBTree
)

type btPage interface {
	clr(func(int64, int64) error) error
	find(func(int64) (int, error)) (int, bool, error)
	len() (int, error)
}

type BTree struct {
	*DB
	Off   int64
	SzKey int64
	SzVal int64
	kd    int
	kx    int
}

func (db *DB) NewBTree(nd, nx int, szKey, szVal int64) (*BTree, error) {
	if nd < 0 || nd > (math.MaxInt32-1)/2 ||
		nx < 0 || nx > (math.MaxInt32-2)/2 ||
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

	return &BTree{DB: db, Off: off, SzKey: szKey, SzVal: szVal, kd: kd, kx: kx}, nil
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

func (t *BTree) openDPage(off int64) (btDPage, error) { return btDPage{t, off}, nil }
func (t *BTree) root() (int64, error)                 { return t.r8(t.Off + oBTRoot) }
func (t *BTree) setFirst(n int64) error               { return t.w8(t.Off+oBTFirst, n) }
func (t *BTree) setLast(n int64) error                { return t.w8(t.Off+oBTLast, n) }
func (t *BTree) setLen(n int64) error                 { return t.w8(t.Off+oBTLen, n) }
func (t *BTree) setRoot(n int64) error                { return t.w8(t.Off+oBTRoot, n) }

func (t *BTree) openPage(off int64) (btPage, error) {
	switch tag, err := t.r4(off); {
	case err != nil:
		return nil, err
	case tag == btTagDataPage:
		p, err := t.openDPage(off)
		if err != nil {
			return nil, err
		}

		return p, nil
	case tag == btTagIndexPage:
		panic("TODO")
	default:
		return nil, fmt.Errorf("%T.clr: corrupted database", t)
	}
}

func (t *BTree) clr(off int64, free func(int64, int64) error) error {
	if off == 0 {
		return nil
	}

	p, err := t.openPage(off)
	if err != nil {
		return err
	}

	switch x := p.(type) {
	case btDPage:
		return x.clr(free)
	default:
		panic(fmt.Errorf("%T.clr: internal error %T", t, x))
	}
}

func (t *BTree) First() (int64, error) { return t.r8(t.Off + oBTFirst) }
func (t *BTree) Last() (int64, error)  { return t.r8(t.Off + oBTLast) }
func (t *BTree) Len() (int64, error)   { return t.r8(t.Off + oBTLen) }

func (t *BTree) Get(cmp func(int64) (int, error)) (int64, bool, error) {
	r, err := t.root()
	if err != nil {
		return 0, false, err
	}

	if r == 0 {
		return 0, false, nil
	}

	q, err := t.openPage(r)
	if err != nil {
		return 0, false, err
	}

	for {
		i, ok, err := q.find(cmp)
		if err != nil {
			return 0, false, err
		}

		if ok {
			switch x := q.(type) {
			case btDPage:
				return x.voff(i), true, nil
			//TODO			case *x:
			//TODO				q = x.x[i+1].ch
			//TODO				continue
			default:
				panic(fmt.Errorf("%T.Get: internal error: %T", t, x))
			}
		}

		//TODO		switch x := q.(type) {
		//TODO		case *x:
		//TODO			q = x.x[i].ch
		//TODO		default:
		//TODO			return
		//TODO		}
	}
}

func (t *BTree) Remove(free func(k, v int64) error) error {
	r, err := t.root()
	if err != nil {
		return err
	}

	if err := t.clr(r, free); err != nil {
		return err
	}

	return t.Free(t.Off)
}

func (t *BTree) Set(cmp func(int64) (int, error)) (int64, int64, error) {
	pi := -1
	r, err := t.root()
	if err != nil {
		return 0, 0, err
	}

	if r == 0 {
		z, err := newBTDPage(t)
		if err != nil {
			return 0, 0, err
		}

		if err := z.insert(0); err != nil {
			return 0, 0, err
		}

		if err := t.setRoot(z.off); err != nil {
			return 0, 0, err
		}

		if err := t.setFirst(z.off); err != nil {
			return 0, 0, err
		}

		if err := t.setLast(z.off); err != nil {
			return 0, 0, err
		}

		return z.koff(0), z.voff(0), nil
	}

	q, err := t.openPage(r)
	if err != nil {
		return 0, 0, err
	}

	for {
		i, ok, err := q.find(cmp)
		if err != nil {
			return 0, 0, err
		}

		if ok {
			_ = pi
			switch x := q.(type) {
			case btDPage:
				return x.koff(i), x.voff(i), nil
			//TODO			case *x:
			//TODO				i++
			//TODO				if x.c > 2*kx {
			//TODO					x, i = t.splitX(p, x, pi, i)
			//TODO				}
			//TODO				pi = i
			//TODO				p = x
			//TODO				q = x.x[i].ch
			//TODO				continue
			//TODO			case *d:
			//TODO				x.d[i].v = v
			default:
				panic(fmt.Errorf("%T.clr: internal error %T", t, x))
			}
		}

		c, err := q.len()
		if err != nil {
			return 0, 0, err
		}

		switch x := q.(type) {
		case btDPage:
			switch {
			case c < 2*t.kd:
				if err := x.insert(i); err != nil {
					return 0, 0, err
				}
			default:
				panic("TODO")
				//TODO t.overflow(p, x, pi, i, k, v)
			}
			return x.koff(i), x.voff(i), nil
		default:
			panic(fmt.Errorf("%T.clr: internal error %T", t, x))
		}
		//TODO		switch x := q.(type) {
		//TODO		case *x:
		//TODO			if x.c > 2*kx {
		//TODO				x, i = t.splitX(p, x, pi, i)
		//TODO			}
		//TODO			pi = i
		//TODO			p = x
		//TODO			q = x.x[i].ch
		//TODO		case *d:
		//TODO			switch {
		//TODO			case x.c < 2*kd:
		//TODO				t.insert(x, i, k, v)
		//TODO			default:
		//TODO				t.overflow(p, x, pi, i, k, v)
		//TODO			}
		//TODO			return
		//TODO		}
	}
}

const (
	oBTDPageTag   = 8 * iota // int32		0	4
	oBTDPageLen              // int32		8	4
	oBTDPagePrev             // int64		16	8
	oBTDPageNext             // int64		24	8
	oBTDPageItems            // [2*kd+1]item	32	(2*kd+1)*(szKey+szVal)
)

type btDPage struct {
	*BTree
	off int64
}

func newBTDPage(t *BTree) (btDPage, error) {
	rq := oBTDPageItems + (2*int64(t.kd)+1)*(t.SzKey+t.SzVal)
	off, err := t.Alloc(rq)
	if err != nil {
		return btDPage{}, err
	}

	r := btDPage{t, off}
	if err := r.setTag(btTagDataPage); err != nil {
		return btDPage{}, err
	}

	if err := r.setLen(0); err != nil {
		return btDPage{}, err
	}

	if err := r.setNext(0); err != nil {
		return btDPage{}, err
	}

	if err := r.setPrev(0); err != nil {
		return btDPage{}, err
	}

	return r, nil
}

func (d *btDPage) koff(i int) int64      { return d.off + oBTDPageItems + int64(i)*(d.SzKey+d.SzVal) }
func (d btDPage) len() (int, error)      { return d.r4(d.off + oBTDPageLen) }
func (d *btDPage) next() (int64, error)  { return d.r8(d.off + oBTDPageNext) }
func (d *btDPage) prev() (int64, error)  { return d.r8(d.off + oBTDPagePrev) }
func (d *btDPage) setLen(n int) error    { return d.w4(d.off+oBTDPageLen, n) }
func (d *btDPage) setNext(n int64) error { return d.w8(d.off+oBTDPageNext, n) }
func (d *btDPage) setPrev(n int64) error { return d.w8(d.off+oBTDPagePrev, n) }
func (d *btDPage) setTag(n int) error    { return d.w4(d.off+oBTDPageTag, n) }
func (d *btDPage) tag() (int, error)     { return d.r4(d.off + oBTDPageTag) }
func (d *btDPage) voff(i int) int64      { return d.koff(i) + d.SzVal }

func (d btDPage) clr(free func(int64, int64) error) error {
	if free != nil {
		c, err := d.len()
		if err != nil {
			return err
		}

		o := d.SzKey + d.SzVal
		koff := d.koff(0)
		voff := d.voff(0)
		for i := 0; i < c; i++ {
			if err := free(koff, voff); err != nil {
				return err
			}

			koff += o
			voff += o
		}
	}
	return d.Free(d.off)
}

func (d btDPage) find(cmp func(off int64) (int, error)) (int, bool, error) {
	h, err := d.len()
	if err != nil {
		return 0, false, err
	}

	var l int
	h--
	for l <= h {
		m := (l + h) >> 1
		switch c, err := cmp(d.koff(m)); {
		case err != nil:
			return 0, false, err
		case c > 0:
			l = m + 1
		case c == 0:
			return m, true, nil
		default:
			h = m - 1
		}
	}
	return l, false, nil
}

func (d *btDPage) insert(i int) error {
	c, err := d.len()
	if err != nil {
		return err
	}

	if i < c {
		panic("TODO")
	}

	if err := d.setLen(c + 1); err != nil {
		return err
	}

	n, err := d.BTree.Len()
	if err != nil {
		return err
	}

	return d.BTree.setLen(n + 1)
}
