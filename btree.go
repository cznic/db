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

	return &BTree{DB: db, Off: off, kd: kd, kx: kx, SzKey: szKey, SzVal: szVal}, nil
}

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

func (t *BTree) First() (int64, error) { return t.r8(t.Off + oBTFirst) }
func (t *BTree) Last() (int64, error)  { return t.r8(t.Off + oBTLast) }
func (t *BTree) Len() (int64, error)   { return t.r8(t.Off + oBTLen) }

func (t *BTree) Clear(free func(int64, int64) error) error {
	r, err := t.root()
	if err != nil {
		return err
	}

	//TODO	if t.r == nil {
	//TODO		return
	//TODO	}
	if r == 0 {
		return nil
	}

	//TODO	clr(t.r)
	p, err := t.openPage(r)
	if err != nil {
		return err
	}

	if err := p.clr(free); err != nil {
		return err
	}

	//TODO	t.c, t.first, t.last, t.r = 0, nil, nil, nil
	if err := t.setLen(0); err != nil {
		return err
	}

	if err := t.setFirst(0); err != nil {
		return err
	}

	if err := t.setLast(0); err != nil {
		return err
	}

	//TODO	t.ver++
	return t.setRoot(0)
}

func (t *BTree) Delete(cmp func(int64) (int, error), free func(int64, int64) error) (bool, error) {
	//TODO	pi := -1
	pi := -1
	//TODO	var p *x
	var p btXPage
	//TODO	q := t.r
	r, err := t.root()
	if err != nil {
		return false, err
	}

	//TODO	if q == nil {
	//TODO		return false
	//TODO	}
	//TODO
	if r == 0 {
		return false, nil
	}

	q, err := t.openPage(r)
	if err != nil {
		return false, err
	}
	for {
		//TODO		var i int
		//TODO		i, ok = t.find(q, k)
		i, ok, err := q.find(cmp)
		if err != nil {
			return false, err
		}

		if ok {
			//TODO			switch x := q.(type) {
			switch x := q.(type) {
			//TODO			case *x:
			case btXPage:
				xc, err := x.len()
				if err != nil {
					return false, err
				}

				r, err := t.root()
				if err != nil {
					return false, err
				}

				//TODO				if x.c < kx && q != t.r {
				if xc < t.kx && x.off != r {
					dbg("TODO")
					panic("TODO")
					//TODO					x, i = t.underflowX(p, x, pi, i)
					//TODO				}
				}
				//TODO				pi = i + 1
				pi = i + 1
				//TODO				p = x
				p = x
				//TODO				q = x.x[pi].ch
				ch, err := x.child(pi)
				if err != nil {
					return false, err
				}

				if q, err = t.openPage(ch); err != nil {
					return false, err
				}

				//TODO				ok = false
				//TODO				continue
				continue
				//TODO			case *d:
			case btDPage:
				//TODO				t.extract(x, i)
				if err := x.extract(i, free); err != nil {
					return false, err
				}

				xc, err := x.len()
				if err != nil {
					return false, err
				}

				//TODO				if x.c >= kd {
				if xc >= t.kd {
					//TODO					return true
					//TODO				}
					return true, nil
				}

				r, err := t.root()
				if err != nil {
					return false, err
				}

				//TODO
				//TODO				if q != t.r {
				if x.off != r {
					//TODO					t.underflow(p, x, pi)
					if err := x.underflow(p, pi, free); err != nil {
						return false, err
					}
					//TODO				} else if t.c == 0 {
				} else {
					tc, err := t.Len()
					if err != nil {
						return false, err
					}

					if tc == 0 {
						//TODO					t.Clear()
						if err := t.Clear(free); err != nil {
							return false, err
						}
						//TODO				}
					}
				}
				//TODO				return true
				return true, nil
			}
		}

		//TODO		switch x := q.(type) {
		switch x := q.(type) {
		//TODO		case *x:
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
				//TODO				x, i = t.underflowX(p, x, pi, i)
				dbg("TODO")
				panic("TODO")
			}
			//TODO			pi = i
			//TODO			p = x
			//TODO			q = x.x[i].ch
			pi = i
			p = x
			ch, err := x.child(i)
			if err != nil {
				return false, err
			}

			if q, err = t.openPage(ch); err != nil {
				return false, err
			}
			//TODO		case *d:
		case btDPage:
			//TODO			return false
			return false, nil
		}
	}
}

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

func (t *BTree) Remove(free func(k, v int64) error) (err error) {
	r, err := t.root()
	if err != nil {
		return err
	}

	if err := t.clr(r, free); err != nil {
		return err
	}

	return t.Free(t.Off)
}

func (t *BTree) Set(cmp func(int64) (int, error), free func(int64) error) (int64, int64, error) {
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
func (d btDPage) tag() (int, error)     { return d.r4(d.off + oBTDPageTag) }
func (d btDPage) voff(i int) int64      { return d.koff(i) + d.SzVal }

func (d btDPage) cat(p btXPage, r btDPage, pi int, free func(int64, int64) error) error {
	//TODO	t.ver++
	//TODO	q.mvL(r, r.c)
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

	//TODO	if r.n != nil {
	//TODO		r.n.p = q
	//TODO	} else {
	//TODO		t.last = q
	//TODO	}
	rn, err := r.next()
	if err != nil {
		return err
	}

	if rn != 0 {
		dbg("TODO")
		panic("TODO")
	} else if err := d.setLast(d.off); err != nil {
		return err
	}

	//TODO	q.n = r.n
	//TODO	*r = zd
	//TODO	btDPool.Put(r)
	if err := d.setLast(rn); err != nil {
		return err
	}

	if err := d.Free(r.off); err != nil {
		return err
	}

	//TODO	if p.c > 1 {
	//TODO		p.extract(pi)
	//TODO		p.x[pi].ch = q
	//TODO		return
	//TODO	}
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

	//TODO	switch x := t.r.(type) {
	//TODO	case *x:
	//TODO		*x = zx
	//TODO		btXPool.Put(x)
	//TODO	case *d:
	//TODO		*x = zd
	//TODO		btDPool.Put(x)
	//TODO	}
	//TODO	t.r = q
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
	switch nb := (d.SzKey + d.SzVal) * int64(n); {
	case nb > mathutil.MaxInt:
		dbg("TODO")
		panic("TODO")
	default:
		nb := int(nb)
		p := buffer.Get(nb)
		if nr, err := s.ReadAt(*p, s.koff(si)); nr != nb {
			if err == nil {
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}

		if nw, err := d.WriteAt(*p, d.koff(di)); nw != nb {
			if err == nil {
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}

		buffer.Put(p)
	}
	return nil
}

func (d btDPage) extract(i int, free func(int64, int64) error) error {
	//TODO	t.ver++
	//TODO	//r = q.d[i].v // prepared for Extract
	//TODO	q.c--
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

	//TODO	if i < q.c {
	if i < c {
		//TODO		copy(q.d[i:], q.d[i+1:q.c+1])
		if err := d.copy(d, i, i+1, c-i); err != nil {
			return err
		}
		//TODO	}
	}
	//TODO	q.d[q.c] = zde // GC
	//TODO	t.c--
	tc, err := d.Len()
	if err != nil {
		return err
	}

	tc--
	//TODO	return
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
	//TODO	t.ver++
	//TODO	l, r := p.siblings(pi)
	l, r, err := p.siblings(pi)
	if err != nil {
		return err
	}

	//TODO
	//TODO	if l != nil && l.c+q.c >= 2*kd {
	if l.off != 0 {
		//TODO		l.mvR(q, 1)
		//TODO		p.x[pi-1].k = q.d[0].k
		//TODO		return
		//TODO	}
		qc, err := d.len()
		if err != nil {
			return err
		}

		if err := l.mvR(d, qc, 1); err != nil {
			return err
		}

		return p.setKey(pi-1, d.koff(0))
	}

	//TODO	if r != nil && q.c+r.c >= 2*kd {
	if r.off != 0 {
		qc, err := d.len()
		if err != nil {
			return err
		}

		rc, err := d.len()
		if err != nil {
			return err
		}

		if qc+rc >= 2*d.kd {
			dbg("TODO")
			panic("TODO")

			//TODO		q.mvL(r, 1)
			c, err := d.len()
			if err != nil {
				return err
			}

			if err := d.mvL(r, c, 1); err != nil {
				return err
			}

			//TODO		p.x[pi].k = r.d[0].k
			if err := p.setKey(pi, r.koff(0)); err != nil {
				return err
			}

			//TODO		r.d[r.c] = zde // GC
			//TODO		return
			//TODO	}
			return nil
		}
	}

	//TODO	if l != nil {
	//TODO		t.cat(p, l, q, pi-1)
	//TODO		return
	//TODO	}
	if l.off != 0 {
		dbg("TODO")
		panic("TODO")
	}

	//TODO	t.cat(p, q, r, pi)
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
func (x btXPage) tag() (int, error)               { return x.r4(x.off + oBTXPageTag) }

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

	//TODO	q.c--
	//TODO	if i < q.c {
	//TODO		copy(q.x[i:], q.x[i+1:q.c+1])
	//TODO		q.x[q.c].ch = q.x[q.c+1].ch
	//TODO		q.x[q.c].k = zk  // GC
	//TODO		q.x[q.c+1] = zxe // GC
	//TODO	}
	if i < xc {
		dbg("TODO")
		panic("TODO")
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
