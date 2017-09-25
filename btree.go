// Copyright 2014 The b Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE-B file.

// Modifications are
//
// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"fmt"
	"math"

	"github.com/cznic/internal/buffer"
	"github.com/cznic/mathutil"
)

const (
	btND       = 256
	btNX       = 32
	maxCopyBuf = 64 << 20
)

var (
	_ btPage = btDPage{}
	_ btPage = btXPage{}
)

const (
	btTagDataPage = iota
	btTagIndexPage
)

const (
	oBTRoot  = 8 * iota // int64
	oBTLen              // int64
	oBTFirst            // int64
	oBTLast             // int64
	oBTKD               // int64
	oBTKX               // int64
	oBTSzKey            // int64
	oBTSzVal            // int64

	szBTree
)

type btPage interface {
	clr(func(int64, int64) error) error
	find(func(int64) (int, error)) (int, bool, error)
	len() (int, error)
}

// BTree is a B+tree.
type BTree struct {
	*DB
	Off   int64 // Location in the database.
	SzKey int64 // The szKey argument of NewBTree.
	SzVal int64 // The szVal argument of NewBTree.
	kd    int
	kx    int
}

// NewBTree allocates and returns a new, empty BTree or an error, if any.  The
// nd and nx arguments are the desired number of items in a data or index page.
// Passing zero will use default values. The szKey and szVal arguments are the
// sizes of the BTree keys and values.
func (db *DB) NewBTree(nd, nx int, szKey, szVal int64) (*BTree, error) {
	if nd < 0 || nd > (math.MaxInt32-1)/2 ||
		nx < 0 || nx > (math.MaxInt32-2)/2 ||
		szKey < 0 || szVal < 0 {
		panic(fmt.Errorf("%T.NewBTree: invalid argument", db))
	}

	if nd == 0 {
		nd = btND
	}
	kd := mathutil.Max(nd/2, 1)
	if nx == 0 {
		nx = btNX
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

// OpenBTree opend and returns an existing BTree or an error, if any.
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

	return &BTree{DB: db, Off: off, kd: kd, kx: kx, SzKey: szKey, SzVal: szVal}, nil
}

func (t *BTree) first() (int64, error)       { return t.r8(t.Off + oBTFirst) }
func (t *BTree) last() (int64, error)        { return t.r8(t.Off + oBTLast) }
func (t *BTree) openDPage(off int64) btDPage { return btDPage{t, off} }
func (t *BTree) openXPage(off int64) btXPage { return btXPage{t, off} }
func (t *BTree) root() (int64, error)        { return t.r8(t.Off + oBTRoot) }
func (t *BTree) setFirst(n int64) error      { return t.w8(t.Off+oBTFirst, n) }
func (t *BTree) setLast(n int64) error       { return t.w8(t.Off+oBTLast, n) }
func (t *BTree) setLen(n int64) error        { return t.w8(t.Off+oBTLen, n) }
func (t *BTree) setRoot(n int64) error       { return t.w8(t.Off+oBTRoot, n) }

func (t *BTree) clr(off int64, free func(int64, int64) error) error {
	if off == 0 {
		return nil
	}

	p, err := t.openPage(off)
	if err != nil {
		return err
	}

	return p.clr(free)
}

func (t *BTree) newBTXPage(ch0 int64) (r btXPage, err error) {
	r.BTree = t
	if r.off, err = t.Alloc(oBTXPageItems + 16*(2*int64(r.kx)+2)); err != nil {
		return btXPage{}, err
	}

	if err := r.setTag(btTagIndexPage); err != nil {
		return btXPage{}, err
	}

	if ch0 != 0 {
		if err := r.setChild(0, ch0); err != nil {
			return btXPage{}, err
		}
	}

	return r, nil
}

func (t *BTree) openPage(off int64) (btPage, error) {
	switch tag, err := t.r4(off); {
	case err != nil:
		return nil, err
	case tag == btTagDataPage:
		return t.openDPage(off), nil
	case tag == btTagIndexPage:
		return t.openXPage(off), nil
	default:
		return nil, fmt.Errorf("%T.clr: corrupted database", t)
	}
}

// Len returns the number of items i t or an error, if any.
func (t *BTree) Len() (int64, error) { return t.r8(t.Off + oBTLen) } //TODO no error

// Clear deletes all items of t.
//
// The free function may be nil, otherwise it's called with the offsets of the
// key and value of an item that is being deleted from the tree. Both koff and
// voff may be zero when appropriate.
func (t *BTree) Clear(free func(koff, voff int64) error) error {
	r, err := t.root()
	if err != nil {
		return err
	}

	if r == 0 {
		return nil
	}

	p, err := t.openPage(r)
	if err != nil {
		return err
	}

	if err := p.clr(free); err != nil {
		return err
	}

	if err := t.setLen(0); err != nil {
		return err
	}

	if err := t.setFirst(0); err != nil {
		return err
	}

	if err := t.setLast(0); err != nil {
		return err
	}

	return t.setRoot(0)
}

// Delete removes an item from t and returns a boolean value indicating if the
// item was found.
//
// The item is searched for by calling the cmp function that gets the offset of
// a tree key to compare. It returns a positive value if the desired key
// collates after the tree key, a zero if the keys are equal and a negative
// value if the desired key collates before the tree key.
//
// For discussion of the free function see Clear.
func (t *BTree) Delete(cmp func(koff int64) (int, error), free func(koff, voff int64) error) (bool, error) {
	pi := -1
	var p btXPage
	r, err := t.root()
	if err != nil {
		return false, err
	}

	if r == 0 {
		return false, nil
	}

	q, err := t.openPage(r)
	if err != nil {
		return false, err
	}
	for {
		i, ok, err := q.find(cmp)
		if err != nil {
			return false, err
		}

		if ok {
			switch x := q.(type) {
			case btXPage:
				xc, err := x.len()
				if err != nil {
					return false, err
				}

				r, err := t.root()
				if err != nil {
					return false, err
				}

				if xc < t.kx && x.off != r {
					if x, i, err = x.underflow(p, pi, i); err != nil {
						return false, err
					}
				}
				pi = i + 1
				p = x
				ch, err := x.child(pi)
				if err != nil {
					return false, err
				}

				if q, err = t.openPage(ch); err != nil {
					return false, err
				}

				continue
			case btDPage:
				if err := x.extract(i, free); err != nil {
					return false, err
				}

				xc, err := x.len()
				if err != nil {
					return false, err
				}

				if xc >= t.kd {
					return true, nil
				}

				r, err := t.root()
				if err != nil {
					return false, err
				}

				if x.off != r {
					if err := x.underflow(p, pi, free); err != nil {
						return false, err
					}
				} else {
					tc, err := t.Len()
					if err != nil {
						return false, err
					}

					if tc == 0 {
						if err := t.Clear(free); err != nil {
							return false, err
						}
					}
				}
				return true, nil
			}
		}

		switch x := q.(type) {
		case btXPage:
			xc, err := x.len()
			if err != nil {
				return false, err
			}

			r, err := t.root()
			if err != nil {
				return false, err
			}

			if xc < t.kx && x.off != r {
				if x, i, err = x.underflow(p, pi, i); err != nil {
					return false, err
				}
			}
			pi = i
			p = x
			ch, err := x.child(i)
			if err != nil {
				return false, err
			}

			if q, err = t.openPage(ch); err != nil {
				return false, err
			}
		case btDPage:
			return false, nil
		}
	}
}

// Get searches for a key in the tree and returns the offset of its associated
// value and a boolean value indicating success.
//
// For discussion of the cmp function see Delete.
func (t *BTree) Get(cmp func(koff int64) (int, error)) (int64, bool, error) {
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
			case btXPage:
				ch, err := x.child(i + 1)
				if err != nil {
					return 0, false, err
				}

				if q, err = t.openPage(ch); err != nil {
					return 0, false, err
				}

				continue
			}
		}

		switch x := q.(type) {
		case btDPage:
			return 0, false, nil
		case btXPage:
			ch, err := x.child(i)
			if err != nil {
				return 0, false, err
			}

			if q, err = t.openPage(ch); err != nil {
				return 0, false, err
			}
		}
	}
}

// Remove frees all space used by t.
//
// For discussion of the free function see Clear.
func (t *BTree) Remove(free func(koff, voff int64) error) (err error) {
	r, err := t.root()
	if err != nil {
		return err
	}

	if err := t.clr(r, free); err != nil {
		return err
	}

	if err := t.Free(t.Off); err != nil {
		return err
	}

	t.Off = 0
	return nil
}

// Seek searches the tree for a key collating after the key used by the cmp
// function and a boolean value indicating the desired and found keys are
// equal.
//
// For discussion of the cmp function see Delete.
func (t *BTree) Seek(cmp func(int64) (int, error)) (*BTreeCursor, bool, error) {
	r, err := t.root()
	if err != nil {
		return nil, false, err
	}

	if r == 0 {
		return &BTreeCursor{}, false, nil
	}

	q, err := t.openPage(r)
	if err != nil {
		return nil, false, err
	}

	for {
		i, ok, err := q.find(cmp)
		if err != nil {
			return nil, false, err
		}

		if ok {
			switch x := q.(type) {
			case btDPage:
				return x.newEnumerator(i, true), true, nil
			case btXPage:
				ch, err := x.child(i + 1)
				if err != nil {
					return nil, false, err
				}

				if q, err = t.openPage(ch); err != nil {
					return nil, false, err
				}
				continue
			}
		}

		switch x := q.(type) {
		case btDPage:
			return x.newEnumerator(i, false), false, nil
		case btXPage:
			ch, err := x.child(i)
			if err != nil {
				return nil, false, err
			}

			if q, err = t.openPage(ch); err != nil {
				return nil, false, err
			}
		}
	}
}

// SeekFirst returns an Enumerator position on the first item of t or an error,
// if any.
func (t *BTree) SeekFirst() (*BTreeCursor, error) {
	p, err := t.first()
	if err != nil {
		return nil, err
	}

	if p == 0 {
		return &BTreeCursor{}, nil
	}

	return t.openDPage(p).newEnumerator(0, true), nil
}

// SeekLast returns an Enumerator position on the last item of t or an error,
// if any.
func (t *BTree) SeekLast() (*BTreeCursor, error) {
	p, err := t.last()
	if err != nil {
		return nil, err
	}

	if p == 0 {
		return &BTreeCursor{}, nil
	}

	e := t.openDPage(p).newEnumerator(0, true)
	e.i = e.c - 1
	return e, nil
}

// Set adds or overwrites an item in t and returns the offsets if its key and value or an error, if any.
//
// For discussion of the cmp function see Delete.
//
// For discussion of the free function see Clear.
func (t *BTree) Set(cmp func(koff int64) (int, error), free func(koff int64) error) (int64, int64, error) {
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

	var p btXPage
	for {
		i, ok, err := q.find(cmp)
		if err != nil {
			return 0, 0, err
		}

		if ok {
			switch x := q.(type) {
			case btDPage:
				koff := x.koff(i)
				voff := x.voff(i)
				if free != nil {
					if err := free(voff); err != nil {
						return 0, 0, err
					}
				}

				return koff, voff, nil
			case btXPage:
				i++
				c, err := x.len()
				if err != nil {
					return 0, 0, err
				}

				if c > 2*t.kx {
					if x, i, err = x.split(p, pi, i); err != nil {
						return 0, 0, err
					}
				}
				pi = i
				p = x
				ch, err := x.child(i)
				if err != nil {
					return 0, 0, err
				}

				if q, err = t.openPage(ch); err != nil {
					return 0, 0, err
				}

				continue
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
				q, j, err := x.overflow(p, pi, i)
				if err != nil {
					return 0, 0, err
				}

				if q.off != 0 {
					x = q
					i = j
				}
			}
			return x.koff(i), x.voff(i), nil
		case btXPage:
			if c > 2*t.kx {
				if x, i, err = x.split(p, pi, i); err != nil {
					return 0, 0, err
				}
			}
			pi = i
			p = x
			ch, err := x.child(i)
			if err != nil {
				return 0, 0, err
			}

			if q, err = t.openPage(ch); err != nil {
				return 0, 0, err
			}
		}
	}
}

const (
	oBTDPageTag   = 8 * iota // int32
	oBTDPageLen              // int32
	oBTDPagePrev             // int64
	oBTDPageNext             // int64
	oBTDPageItems            // [2*kd+1]struct{[szKey]byte, [szVal]byte}
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

func (d btDPage) koff(i int) int64      { return d.off + oBTDPageItems + int64(i)*(d.SzKey+d.SzVal) }
func (d btDPage) len() (int, error)     { return d.r4(d.off + oBTDPageLen) }
func (d btDPage) next() (int64, error)  { return d.r8(d.off + oBTDPageNext) }
func (d btDPage) prev() (int64, error)  { return d.r8(d.off + oBTDPagePrev) }
func (d btDPage) setLen(n int) error    { return d.w4(d.off+oBTDPageLen, n) }
func (d btDPage) setNext(n int64) error { return d.w8(d.off+oBTDPageNext, n) }
func (d btDPage) setPrev(n int64) error { return d.w8(d.off+oBTDPagePrev, n) }
func (d btDPage) setTag(n int) error    { return d.w4(d.off+oBTDPageTag, n) }
func (d btDPage) voff(i int) int64      { return d.koff(i) + d.SzVal }

func (d btDPage) cat(p btXPage, r btDPage, pi int, free func(int64, int64) error) error {
	rc, err := r.len()
	if err != nil {
		return err
	}

	dc, err := d.len()
	if err != nil {
		return err
	}

	if err := d.mvL(r, dc, rc); err != nil {
		return err
	}

	rn, err := r.next()
	if err != nil {
		return err
	}

	if rn != 0 {
		if err := d.openDPage(rn).setPrev(d.off); err != nil {
			return err
		}
	} else if err := d.setLast(d.off); err != nil {
		return err
	}

	if err := d.setNext(rn); err != nil {
		return err
	}

	if err := d.Free(r.off); err != nil {
		return err
	}

	pc, err := p.len()
	if err != nil {
		return err
	}

	if pc > 1 {
		if err := p.extract(pi); err != nil {
			return err
		}

		return p.setChild(pi, d.off)
	}

	root, err := d.root()
	if err != nil {
		return err
	}

	if err := d.Free(root); err != nil {
		return err
	}

	return d.setRoot(d.off)
}

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
	if d.off == 0 {
		panic(0)
	}
	return d.Free(d.off)
}

func (d btDPage) copy(s btDPage, di, si, n int) error {
	if n <= 0 {
		return nil
	}

	dst := d.koff(di)
	src := s.koff(si)
	var rq int
	var p *[]byte
	var b []byte
	for rem := (d.SzKey + d.SzVal) * int64(n); rem != 0; rem -= int64(rq) {
		if rem <= maxCopyBuf {
			rq = int(rem)
		} else {
			rq = maxCopyBuf
		}

		if p == nil {
			p = buffer.Get(rq)
			b = *p
		}
		if nr, err := s.ReadAt(b[:rq], src); nr != rq {
			if err == nil {
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}

		if nw, err := d.WriteAt(b[:rq], dst); nw != rq {
			if err == nil {
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}
		src += int64(rq)
		dst += int64(rq)
	}
	buffer.Put(p)
	return nil
}

func (d btDPage) extract(i int, free func(int64, int64) error) error {
	c, err := d.len()
	if err != nil {
		return err
	}

	if free != nil {
		if err := free(d.koff(i), d.voff(i)); err != nil {
			return err
		}
	}

	c--
	if err := d.setLen(c); err != nil {
		return err
	}

	if i < c {
		if err := d.copy(d, i, i+1, c-i); err != nil {
			return err
		}
	}
	tc, err := d.Len()
	if err != nil {
		return err
	}

	tc--
	return d.BTree.setLen(tc)
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

func (d btDPage) insert(i int) error {
	c, err := d.len()
	if err != nil {
		return err
	}

	if i < c {
		if err := d.copy(d, i+1, i, c-i); err != nil {
			return err
		}
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

func (d btDPage) newEnumerator(i int, hit bool) *BTreeCursor {
	c, err := d.len()
	return &BTreeCursor{
		btDPage: d,
		c:       c,
		err:     err,
		hit:     hit,
		i:       i,
	}
}

func (d btDPage) mvL(r btDPage, dc, c int) error {
	if err := d.copy(r, dc, 0, c); err != nil {
		return err
	}

	rc, err := r.len()
	if err != nil {
		return err
	}

	if err := r.copy(r, 0, c, rc-c); err != nil {
		return err
	}

	if err := d.setLen(dc + c); err != nil {
		return err
	}

	return r.setLen(rc - c)
}

func (d btDPage) mvR(r btDPage, rc, c int) error {
	if err := r.copy(r, c, 0, rc); err != nil {
		return err
	}

	dc, err := d.len()
	if err != nil {
		return err
	}

	if err := r.copy(d, 0, dc-c, c); err != nil {
		return err
	}
	if err := r.setLen(rc + c); err != nil {
		return err
	}

	return d.setLen(dc - c)
}

func (d btDPage) overflow(p btXPage, pi, i int) (btDPage, int, error) {
	l, r, err := p.siblings(pi)
	if err != nil {
		return btDPage{}, 0, err
	}

	if l.off != 0 {
		c, err := l.len()
		if err != nil {
			return btDPage{}, 0, err
		}

		if c < 2*d.kd && i != 0 {
			if err := l.mvL(d, c, 1); err != nil {
				return btDPage{}, 0, err
			}

			if err := d.insert(i - 1); err != nil {
				return btDPage{}, 0, err
			}

			return d, i - 1, p.setKey(pi-1, d.koff(0))
		}
	}

	if r.off != 0 {
		c, err := r.len()
		if err != nil {
			return btDPage{}, 0, err
		}

		if c < 2*d.kd {
			if i < 2*d.kd {
				if err := d.mvR(r, c, 1); err != nil {
					return btDPage{}, 0, err
				}

				if err := d.insert(i); err != nil {
					return btDPage{}, 0, err
				}

				return btDPage{}, 0, p.setKey(pi, r.koff(0))
			}

			if err := r.insert(0); err != nil {
				return btDPage{}, 0, err
			}

			if err := p.setKey(pi, r.koff(0)); err != nil {
				return btDPage{}, 0, err
			}

			return r, 0, nil
		}
	}

	return d.split(p, pi, i)
}

func (d btDPage) underflow(p btXPage, pi int, free func(int64, int64) error) error {
	l, r, err := p.siblings(pi)
	if err != nil {
		return err
	}

	if l.off != 0 {
		lc, err := l.len()
		if err != nil {
			return err
		}

		qc, err := d.len()
		if err != nil {
			return err
		}

		if lc+qc >= 2*d.kd {
			if err := l.mvR(d, qc, 1); err != nil {
				return err
			}

			return p.setKey(pi-1, d.koff(0))
		}
	}

	if r.off != 0 {
		qc, err := d.len()
		if err != nil {
			return err
		}

		rc, err := r.len()
		if err != nil {
			return err
		}

		if qc+rc >= 2*d.kd {
			if err := d.mvL(r, qc, 1); err != nil {
				return err
			}

			if err := p.setKey(pi, r.koff(0)); err != nil {
				return err
			}

			return nil
		}
	}

	if l.off != 0 {
		return l.cat(p, d, pi-1, free)
	}

	return d.cat(p, r, pi, free)
}

func (d btDPage) split(p btXPage, pi, i int) (q btDPage, j int, err error) {
	var r btDPage
	if r, err = newBTDPage(d.BTree); err != nil {
		return q, j, err
	}

	n, err := d.next()
	if err != nil {
		return q, j, err
	}

	if n != 0 {
		if err := r.setNext(n); err != nil {
			return q, j, err
		}

		if err = d.openDPage(n).setPrev(r.off); err != nil {
			return q, j, err
		}
	} else {
		if err := d.setLast(r.off); err != nil {
			return q, j, err
		}
	}

	if err := d.setNext(r.off); err != nil {
		return q, j, err
	}

	if err := r.setPrev(d.off); err != nil {
		return q, j, err
	}

	if err := r.copy(d, 0, d.kd, 2*d.kd-d.kd); err != nil {
		return q, j, err
	}

	if err := d.setLen(d.kd); err != nil {
		return q, j, err
	}

	if err := r.setLen(d.kd); err != nil {
		return q, j, err
	}

	var done bool
	if i > d.kd {
		done = true
		q = r
		j = i - d.kd
		if err := q.insert(j); err != nil {
			return btDPage{}, 0, err
		}
	}

	if pi >= 0 {
		if err := p.insert(pi, r.koff(0), r.off); err != nil {
			return btDPage{}, 0, err
		}
	} else {
		x, err := d.newBTXPage(d.off)
		if err != nil {
			return btDPage{}, 0, err
		}

		if err := x.insert(0, r.koff(0), r.off); err != nil {
			return btDPage{}, 0, err
		}

		if err := d.setRoot(x.off); err != nil {
			return btDPage{}, 0, err
		}
	}
	if done {
		return q, j, nil
	}

	return btDPage{}, 0, d.insert(i)
}

const (
	oBTXPageTag   = 8 * iota // int32
	oBTXPageLen              // int32
	oBTXPageItems            // [2*kx+2]struct{int64,int64}
)

type btXPage struct {
	*BTree
	off int64
}

func (x btXPage) child(i int) (y int64, yy error) { return x.r8(x.off + oBTXPageItems + int64(i)*16) }
func (x btXPage) item(i int) int64                { return x.off + oBTXPageItems + int64(i)*16 }
func (x btXPage) key(i int) (int64, error)        { return x.r8(x.off + oBTXPageItems + int64(i)*16 + 8) }
func (x btXPage) len() (int, error)               { return x.r4(x.off + oBTXPageLen) }
func (x btXPage) setChild(i int, c int64) error   { return x.w8(x.off+oBTXPageItems+int64(i)*16, c) }
func (x btXPage) setKey(i int, k int64) error     { return x.w8(x.off+oBTXPageItems+int64(i)*16+8, k) }
func (x btXPage) setLen(n int) error              { return x.w4(x.off+oBTXPageLen, n) }
func (x btXPage) setTag(n int) error              { return x.w4(x.off+oBTXPageTag, n) }

func (x btXPage) cat(p btXPage, r btXPage, pi int) error {
	k, err := p.key(pi)
	if err != nil {
		return err
	}

	qc, err := x.len()
	if err != nil {
		return err
	}

	if err := x.setKey(qc, k); err != nil {
		return err
	}

	rc, err := r.len()
	if err != nil {
		return err
	}

	if err := x.copy(r, qc+1, 0, rc); err != nil {
		return err
	}

	qc += rc + 1
	if err := x.setLen(qc); err != nil {
		return err
	}

	ch, err := r.child(rc)
	if err != nil {
		return err
	}

	if err := x.setChild(qc, ch); err != nil {
		return err
	}

	if err := r.Free(r.off); err != nil {
		return err
	}

	pc, err := p.len()
	if err != nil {
		return err
	}

	if pc > 1 {
		pc--
		if err := p.setLen(pc); err != nil {
			return err
		}

		if pi < pc {
			k, err := p.key(pi + 1)
			if err != nil {
				return err
			}

			if err := p.setKey(pi, k); err != nil {
				return err
			}

			if err := p.copy(p, pi+1, pi+2, pc-pi-1); err != nil {
				return err
			}

			ch, err := p.child(pc + 1)
			if err != nil {
				return err
			}

			if err := p.setChild(pc, ch); err != nil {
				return err
			}
		}
		return nil
	}

	proot, err := x.root()
	if err != nil {
		return err
	}

	if err := x.Free(proot); err != nil {
		return err
	}

	return x.setRoot(x.off)
}

func (x btXPage) clr(free func(int64, int64) error) error {
	c, err := x.len()
	if err != nil {
		return err
	}

	for i := 0; i <= c; i++ {
		off, err := x.child(i)
		if err != nil {
			return err
		}

		if off == 0 {
			break
		}

		ch, err := x.openPage(off)
		if err != nil {
			return err
		}

		if err := ch.clr(free); err != nil {
			return err
		}
	}
	return x.Free(x.off)
}

func (x btXPage) copy(s btXPage, di, si, n int) error {
	nb := 16 * n
	p := buffer.Get(nb)
	if nr, err := s.ReadAt(*p, s.item(si)); nr != nb {
		if err == nil {
			panic("internal error")
		}

		buffer.Put(p)
		return err
	}

	if nw, err := x.WriteAt(*p, x.item(di)); nw != nb {
		if err == nil {
			panic("internal error")
		}

		buffer.Put(p)
		return err
	}

	buffer.Put(p)
	return nil
}

func (x btXPage) extract(i int) error {
	xc, err := x.len()
	if err != nil {
		return err
	}

	xc--
	if err = x.setLen(xc); err != nil {
		return err
	}

	if i < xc {
		if err := x.copy(x, i, i+1, xc-i); err != nil {
			return err
		}

		ch, err := x.child(xc + 1)
		if err != nil {
			return err
		}

		if err := x.setChild(xc, ch); err != nil {
			return err
		}
	}

	return nil
}

func (x btXPage) find(cmp func(off int64) (int, error)) (int, bool, error) {
	h, err := x.len()
	if err != nil {
		return 0, false, err
	}

	var l int
	h--
	for l <= h {
		m := (l + h) >> 1
		k, err := x.key(m)
		if err != nil {
			return 0, false, err
		}

		switch c, err := cmp(k); {
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

func (x btXPage) insert(i int, k, ch int64) error {
	c, err := x.len()
	if err != nil {
		return err
	}

	if i < c {
		ch, err := x.child(c)
		if err != nil {
			return err
		}

		if err := x.setChild(c+1, ch); err != nil {
			return err
		}

		if err := x.copy(x, i+2, i+1, c-i-1); err != nil {
			return err
		}

		k, err := x.key(i)
		if err != nil {
			return err
		}

		if err := x.setKey(i+1, k); err != nil {
			return err
		}
	}

	if err := x.setLen(c + 1); err != nil {
		return err
	}

	if err := x.setKey(i, k); err != nil {
		return err
	}

	return x.setChild(i+1, ch)
}

func (x btXPage) siblings(i int) (l, r btDPage, err error) {
	if x.off == 0 {
		return btDPage{}, btDPage{}, nil
	}

	if i >= 0 {
		if i > 0 {
			ch, err := x.child(i - 1)
			if err != nil {
				return l, r, err
			}

			l = x.openDPage(ch)
		}
		c, err := x.len()
		if err != nil {
			return l, r, err
		}

		if i < c {
			ch, err := x.child(i + 1)
			if err != nil {
				return l, r, err
			}

			r = x.openDPage(ch)
		}
	}
	return l, r, nil
}

func (x btXPage) split(p btXPage, pi, i int) (btXPage, int, error) {
	r, err := x.newBTXPage(0)
	if err != nil {
		return btXPage{}, 0, err
	}

	c, err := x.len()
	if err != nil {
		return btXPage{}, 0, err
	}

	if err := r.copy(x, 0, x.kx+1, c-x.kx); err != nil {
		return btXPage{}, 0, err
	}

	if err := x.setLen(x.kx); err != nil {
		return btXPage{}, 0, err
	}

	if err := r.setLen(x.kx); err != nil {
		return btXPage{}, 0, err
	}

	if pi >= 0 {
		k, err := x.key(x.kx)
		if err != nil {
			return btXPage{}, 0, err
		}

		if err := p.insert(pi, k, r.off); err != nil {
			return btXPage{}, 0, err
		}
	} else {
		nx, err := x.newBTXPage(x.off)
		if err != nil {
			return btXPage{}, 0, err
		}

		k, err := x.key(x.kx)
		if err != nil {
			return btXPage{}, 0, err
		}

		if err := nx.insert(0, k, r.off); err != nil {
			return btXPage{}, 0, err
		}

		if err := x.setRoot(nx.off); err != nil {
			return btXPage{}, 0, err
		}
	}

	if i > x.kx {
		x = r
		i -= x.kx + 1
	}

	return x, i, nil
}

func (x btXPage) underflow(p btXPage, pi, i int) (btXPage, int, error) {
	var l, r btXPage
	if pi >= 0 {
		if pi > 0 {
			ch, err := p.child(pi - 1)
			if err != nil {
				return btXPage{}, 0, err
			}

			l = x.openXPage(ch)
		}
		pc, err := p.len()
		if err != nil {
			return btXPage{}, 0, err
		}

		if pi < pc {
			ch, err := p.child(pi + 1)
			if err != nil {
				return btXPage{}, 0, err
			}

			r = x.openXPage(ch)
		}
	}

	var lc int
	var err error
	if l.off != 0 {
		if lc, err = l.len(); err != nil {
			return btXPage{}, 0, err
		}

		if lc > x.kx {
			qc, err := x.len()
			if err != nil {
				return btXPage{}, 0, err
			}

			ch, err := x.child(qc)
			if err != nil {
				return btXPage{}, 0, err
			}

			if x.setChild(qc+1, ch); err != nil {
				return btXPage{}, 0, err
			}

			if err := x.copy(x, 1, 0, qc); err != nil {
				return btXPage{}, 0, err
			}

			if ch, err = l.child(lc); err != nil {
				return btXPage{}, 0, err
			}

			if x.setChild(0, ch); err != nil {
				return btXPage{}, 0, err
			}

			k, err := p.key(pi - 1)
			if err != nil {
				return btXPage{}, 0, err
			}

			if err := x.setKey(0, k); err != nil {
				return btXPage{}, 0, err
			}

			qc++
			if err := x.setLen(qc); err != nil {
				return btXPage{}, 0, err
			}

			i++
			lc--
			if err := l.setLen(lc); err != nil {
				return btXPage{}, 0, err
			}

			if k, err = l.key(lc); err != nil {
				return btXPage{}, 0, err
			}

			if err := p.setKey(pi-1, k); err != nil {
				return btXPage{}, 0, err
			}

			return x, i, nil
		}
	}

	if r.off != 0 {
		rc, err := r.len()
		if err != nil {
			return btXPage{}, 0, err
		}

		if rc > x.kx {
			k, err := p.key(pi)
			if err != nil {
				return btXPage{}, 0, err
			}

			qc, err := x.len()
			if err != nil {
				return btXPage{}, 0, err
			}

			if err := x.setKey(qc, k); err != nil {
				return btXPage{}, 0, err
			}

			qc++
			if err := x.setLen(qc); err != nil {
				return btXPage{}, 0, err
			}

			ch, err := r.child(0)
			if err != nil {
				return btXPage{}, 0, err
			}

			if x.setChild(qc, ch); err != nil {
				return btXPage{}, 0, err
			}

			if k, err = r.key(0); err != nil {
				return btXPage{}, 0, err
			}

			if err := p.setKey(pi, k); err != nil {
				return btXPage{}, 0, err
			}

			if err := r.copy(r, 0, 1, rc-1); err != nil {
				return btXPage{}, 0, err
			}

			rc--
			if err := r.setLen(rc); err != nil {
				return btXPage{}, 0, err
			}

			if ch, err = r.child(rc + 1); err != nil {
				return btXPage{}, 0, err
			}

			if err := r.setChild(rc, ch); err != nil {
				return btXPage{}, 0, err
			}

			return x, i, nil
		}
	}

	if l.off != 0 {
		i += lc + 1
		if err := l.cat(p, x, pi-1); err != nil {
			return btXPage{}, 0, err
		}

		return l, i, nil
	}

	if err := x.cat(p, r, pi); err != nil {
		return btXPage{}, 0, err
	}

	return x, i, nil
}

// BTreeCursor provides enumerating BTree items.
type BTreeCursor struct {
	K int64 // Item key offset. Not valid before calling Next or Prev.
	V int64 // Item value offset. Not valid before calling Next or Prev.
	btDPage
	c        int
	err      error
	hasMoved bool
	hit      bool
	i        int
}

// Err returns the error, if any, that was encountered during iteration.
func (e *BTreeCursor) Err() error { return e.err }

// Next moves the cursor to the next item in the tree and sets the K and V
// fields accordingly. It returns true on success, or false if there is no next
// item or an error happened while moving the cursor. Err should be consulted
// to distinguish between the two cases.
//
// Every use of the K/V fields, even the first one, must be preceded by a call
// to Next or Prev.
func (e *BTreeCursor) Next() bool {
	if e.err != nil || e.off == 0 {
		return false
	}

	if e.hasMoved {
		e.i++
	}

	e.hasMoved = true
	if e.i < e.c {
		e.K = e.koff(e.i)
		e.V = e.K + e.SzKey
		return true
	}

	if e.btDPage.off, e.err = e.btDPage.next(); e.err != nil || e.off == 0 {
		return false
	}

	if e.c, e.err = e.len(); e.err != nil {
		return false
	}

	e.i = 0
	e.K = e.koff(0)
	e.V = e.K + e.SzKey
	return true
}

// Prev moves the cursor to the previous item in the tree and sets the K and V
// fields accordingly. It returns true on success, or false if there is no
// previous item or an error happened while moving the cursor. Err should be
// consulted to distinguish between the two cases.
//
// Every use of the K/V fields, even the first one, must be preceded by a call
// to Next or Prev.
func (e *BTreeCursor) Prev() bool {
	if e.err != nil || e.off == 0 {
		return false
	}

	if e.hasMoved || !e.hit {
		e.i--
	}

	e.hasMoved = true
	if e.i >= 0 {
		e.K = e.koff(e.i)
		e.V = e.K + e.SzKey
		return true
	}

	if e.btDPage.off, e.err = e.btDPage.prev(); e.err != nil || e.off == 0 {
		return false
	}

	if e.c, e.err = e.len(); e.err != nil {
		return false
	}

	e.i = e.c - 1
	e.K = e.koff(e.i)
	e.V = e.K + e.SzKey
	return true
}
