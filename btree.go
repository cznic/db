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
			//TODO			case *x:
			//TODO				q = x.x[i+1].ch
			//TODO				continue
			default:
				dbg("TODO")
				panic(fmt.Errorf("%T.Get: internal error: %T", t, x))
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
		default:
			dbg("TODO")
			panic(fmt.Errorf("%T.Get: internal error: %T", t, x))
		}

		//TODO		switch x := q.(type) {
		//TODO		case *x:
		//TODO			q = x.x[i].ch
		//TODO		default:
		//TODO			return
		//TODO		}
	}
}

func (t *BTree) Remove(free func(k, v int64) error) (err error) {
	// dbg("==== %T.Remove(%#x)", t, t.Off)
	// defer func() { dbg("remove: %v", err) }()
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
				if err := free(voff); err != nil {
					return 0, 0, err
				}

				return koff, voff, nil
			case btXPage:
				//TODO			case *x:
				//TODO				i++
				i++
				c, err := x.len()
				if err != nil {
					return 0, 0, err
				}

				//TODO				if x.c > 2*kx {
				//TODO					x, i = t.splitX(p, x, pi, i)
				//TODO				}
				if c > 2*t.kx {
					dbg("TODO")
					panic("TODO")
				}
				//TODO				pi = i
				//TODO				p = x
				pi = i
				p = x
				//TODO				q = x.x[i].ch
				ch, err := x.child(i)
				if err != nil {
					return 0, 0, err
				}

				if q, err = t.openPage(ch); err != nil {
					return 0, 0, err
				}
				//TODO				continue
				continue
			//TODO			case *d:
			//TODO				x.d[i].v = v
			default:
				dbg("TODO")
				panic(fmt.Errorf("%T.Set: internal error %T", t, x))
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
		default:
			dbg("TODO")
			panic(fmt.Errorf("%T.Set: internal error %T", t, x))
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
				dbg("TODO")
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}

		if nw, err := d.WriteAt(*p, d.koff(di)); nw != nb {
			if err == nil {
				dbg("TODO")
				panic("internal error")
			}

			buffer.Put(p)
			return err
		}

		buffer.Put(p)
	}
	return nil
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
	// copy(l.d[l.c:], r.d[:c])
	if err := d.copy(r, c, 0, c); err != nil {
		return err
	}

	// copy(r.d[:], r.d[c:r.c])
	rc, err := r.len()
	if err != nil {
		return err
	}

	if err := r.copy(r, 0, c, rc-c); err != nil {
		return err
	}

	// l.c += c
	if err := d.setLen(dc + c); err != nil {
		return err
	}

	// r.c -= c
	return r.setLen(rc - c)
}

func (d btDPage) mvR(r btDPage, rc, c int) error {
	// copy(r.d[c:], r.d[:r.c])
	if err := r.copy(r, c, 0, rc); err != nil {
		return err
	}

	// copy(r.d[:c], l.d[l.c-c:])
	dc, err := d.len()
	if err != nil {
		return err
	}

	if err := r.copy(d, 0, dc-c, c); err != nil {
		return err
	}
	// r.c += c
	if err := r.setLen(rc + c); err != nil {
		return err
	}

	// l.c -= c
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

		//TODO 	if l != nil && l.c < 2*kd && i != 0 {
		if c < 2*d.kd && i != 0 {
			//TODO 		l.mvL(d, 1)
			if err := l.mvL(d, c, 1); err != nil {
				return btDPage{}, 0, err
			}

			//TODO 		t.insert(d, i-1, k, v)
			if err := d.insert(i - 1); err != nil {
				return btDPage{}, 0, err
			}

			//TODO 		p.x[pi-1].k = d.d[0].k
			return d, i - 1, p.setKey(pi-1, d.koff(0))
		}
	}

	if r.off != 0 {
		c, err := r.len()
		if err != nil {
			return btDPage{}, 0, err
		}

		//TODO 	if r != nil && r.c < 2*kd {
		if c < 2*d.kd {
			if i < 2*d.kd {
				//TODO 			d.mvR(r, 1)
				if err := d.mvR(r, c, 1); err != nil {
					return btDPage{}, 0, err
				}

				//TODO 			t.insert(d, i, k, v)
				if err := d.insert(i); err != nil {
					return btDPage{}, 0, err
				}

				//TODO 			p.x[pi].k = r.d[0].k
				//TODO 			return
				return btDPage{}, 0, p.setKey(pi, r.koff(0))
			}

			dbg("TODO")
			panic("TODO")
			//TODO
			//TODO 		t.insert(r, 0, k, v)
			//TODO 		p.x[pi].k = k
			//TODO 		return
			//TODO 	}
		}
	}

	return d.split(p, pi, i)
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
		//TODO		r.n = d.n
		if err := r.setNext(n); err != nil {
			return q, j, err
		}

		//TODO		r.n.p = r
		if err = d.openDPage(n).setPrev(r.off); err != nil {
			return q, j, err
		}
	} else {
		// t.last = r
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

	//TODO	copy(r.d[:], d.d[kd:2*kd])
	if err := r.copy(d, 0, d.kd, 2*d.kd-d.kd); err != nil {
		return q, j, err
	}

	//TODO	for i := range d.d[kd:] {
	//TODO		d.d[kd+i] = zde
	//TODO	}
	//TODO	d.c = kd
	//TODO	r.c = kd

	if err := d.setLen(d.kd); err != nil {
		return q, j, err
	}

	if err := r.setLen(d.kd); err != nil {
		return q, j, err
	}

	var done bool
	if i > d.kd {
		done = true
		//TODO		t.insert(r, i-kd, k, v)

		q = r
		j = i - d.kd
		if err := q.insert(j); err != nil {
			return btDPage{}, 0, err
		}
	}

	if pi >= 0 {
		//p.insert(pi, r.d[0].k, r)
		if err := p.insert(pi, r.koff(0), r.off); err != nil {
			return btDPage{}, 0, err
		}
	} else {
		//t.r = newX(d).insert(0, r.d[0].k, r)
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
	oBTXPageTag   = 8 * iota // int32		0	4
	oBTXPageLen              // int32		8	4
	oBTXPageItems            // [2*kx+2]item	32	(2*kx+2)*16, item is struct{child, dpage int64}
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
			dbg("TODO")
			panic("internal error")
		}

		buffer.Put(p)
		return err
	}

	if nw, err := x.WriteAt(*p, x.item(di)); nw != nb {
		if err == nil {
			dbg("TODO")
			panic("internal error")
		}

		buffer.Put(p)
		return err
	}

	buffer.Put(p)
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
		// q.x[c+1].ch = q.x[c].ch
		ch, err := x.child(c)
		if err != nil {
			return err
		}

		if err := x.setChild(c+1, ch); err != nil {
			return err
		}

		// copy(q.x[i+2:], q.x[i+1:c])
		if err := x.copy(x, i+2, i+1, c-i-1); err != nil {
			return err
		}

		// q.x[i+1].k = q.x[i].k
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
			//TODO			l = q.x[i-1].ch.(*d)
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
			//TODO			r = q.x[i+1].ch.(*d)
			ch, err := x.child(i + 1)
			if err != nil {
				return l, r, err
			}

			r = x.openDPage(ch)
		}
	}
	return l, r, nil
}

func (q btXPage) split(p btXPage, pi, i int) (btXPage, int, error) {
	//TODO t.ver++
	//TODO r := btXPool.Get().(*x)
	r, err := q.newBTXPage(0)
	if err != nil {
		return btXPage{}, 0, err
	}

	//TODO copy(r.x[:], q.x[kx+1:])
	c, err := q.len()
	if err != nil {
		return btXPage{}, 0, err
	}

	if err := r.copy(q, 0, q.kx+1, c-q.kx-1); err != nil {
		return btXPage{}, 0, err
	}

	//TODO q.c = kx
	if err := q.setLen(q.kx); err != nil {
		return btXPage{}, 0, err
	}

	//TODO r.c = kx
	if err := r.setLen(q.kx); err != nil {
		return btXPage{}, 0, err
	}

	if pi >= 0 {
		//TODO 	p.insert(pi, q.x[kx].k, r)
		dbg("TODO")
		panic("TODO")
	} else {
		//TODO 	t.r = newX(q).insert(0, q.x[kx].k, r)
		nx, err := q.newBTXPage(q.off)
		if err != nil {
			return btXPage{}, 0, err
		}

		k, err := q.key(q.kx)
		if err != nil {
			return btXPage{}, 0, err
		}

		if err := nx.insert(0, k, r.off); err != nil {
			return btXPage{}, 0, err
		}

		if err := q.setRoot(nx.off); err != nil {
			return btXPage{}, 0, err
		}
	}

	//TODO q.x[kx].k = zk
	//TODO for i := range q.x[kx+1:] {
	//TODO 	q.x[kx+i+1] = zxe
	//TODO }
	if i > q.kx {
		//TODO 	q = r
		q = r
		//TODO 	i -= kx + 1
		i -= q.kx + 1
	}

	return q, i, nil
}
