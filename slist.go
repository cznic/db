// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

const (
	oSListNext = 8 * iota // int64		0	8
	oSListData            // [dataSize]byte	8	dataSize
)

// SList is a node of a single linked list.
type SList struct {
	*DB
	Off int64
}

// NewSList returns a newly allocated SList or an error, if any. The datasize
// parameter is the fixed size of data associated with the list node. To
// get/set the node data, use the ReadAt/WriteAt methods of db, using
// SList.DataOff() as the offset. Reading or writing more than datasize data at
// DataOff() is undefined behavior and may irreparably corrupt the database.
//
// The result of NewSList is not a part of any list.
func (db *DB) NewSList(dataSize int64) (SList, error) {
	off, err := db.Alloc(oSListData + dataSize)
	if err != nil {
		return SList{}, err
	}

	r, err := db.OpenSList(off)
	if err != nil {
		return SList{}, err
	}

	return r, r.setNext(0)
}

// OpenSList returns an SList found at offset off.
func (db *DB) OpenSList(off int64) (SList, error) { return SList{db, off}, nil }

// DataOff returns the offset in db at which data of s are located.
func (s SList) DataOff() int64 { return s.Off + oSListData }

// Next returns the offset of the next node of s.
func (s SList) Next() (int64, error) { return s.r8(s.Off + oSListNext) }

func (s SList) setNext(off int64) error { return s.w8(s.Off+oSListNext, off) }

// InsertAfter inserts s after the SList node at off. Node s must not be
// already a part of any list.
func (s SList) InsertAfter(off int64) error {
	n, err := s.OpenSList(off)
	if err != nil {
		return err
	}

	afterNext, err := n.Next()
	if err != nil {
		return err
	}

	if err = n.setNext(s.Off); err != nil {
		return err
	}

	return s.setNext(afterNext)
}

// InsertBefore inserts s before the SList node at off. If the SList node at
// off is linked to from an SList node at prev, the prev argument must reflect
// that, otherwise prev must be zero. Node s must not be already a part of any
// list.
func (s SList) InsertBefore(prev, off int64) error {
	n, err := s.OpenSList(off)
	if err != nil {
		return err
	}

	if prev != 0 {
		n, err := s.OpenSList(prev)
		if err != nil {
			return err
		}

		if err := n.setNext(s.Off); err != nil {
			return err
		}
	}
	return s.setNext(n.Off)
}

// Remove removes s from a list. If s is linked to from an SList node at prev,
// the prev argument must reflect that, otherwise prev must be zero.
func (s SList) Remove(prev int64) error {
	if prev != 0 {
		next, err := s.Next()
		if err != nil {
			return err
		}

		n, err := s.OpenSList(prev)
		if err != nil {
			return err
		}

		if err := n.setNext(next); err != nil {
			return err
		}
	}
	return s.Free(s.Off)
}

// RemoveAll removes all nodes from a list starting at s. If s is linked to
// from an SList node at prev, the prev argument must reflect that, otherwise
// prev must be zero.
func (s SList) RemoveAll(prev int64) error {
	if prev != 0 {
		n, err := s.OpenSList(prev)
		if err != nil {
			return err
		}

		if err := n.setNext(0); err != nil {
			return err
		}
	}
	for s.Off != 0 {
		next, err := s.Next()
		if err != nil {
			return err
		}

		if err := s.Free(s.Off); err != nil {
			return err
		}

		s.Off = next
	}
	return nil
}
