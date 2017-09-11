// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//TODO- whole file

package db

import (
	"errors"
)

var (
	ErrNoNode = errors.New("operation requires existence of a node")
)

type store interface {
	alloc(size int64) (handle int64, err error)      // store owns the uninitialized bytes
	create(b []byte) (handle int64, err error)       // store copies b and owns the copy
	delete(handle int64) error                       // store recycles its copy
	read(handle int64, b []byte) error               // client owns b, must be big enough
	readAt(handle int64, b []byte, off int64) error  //TODO return (int64, error). client owns b
	size(handle int64) (int64, error)                //
	update(handle int64, b []byte) error             // store recycles its previous copy, copies b and owns the copy
	writeAt(handle int64, b []byte, off int64) error // client owns b
}

func n2b(dst []byte, n int64) []byte {
	b := dst[:8]
	for i := range b {
		b[i] = byte(n)
		n >>= 8
	}
	return dst
}

func b2n(b []byte) int64 {
	b = b[:8]
	var n int64
	for i := range b {
		n = n<<8 | int64(b[7-i])
	}
	return n
}

// buffer recycling hook
func cget(n int) []byte {
	return make([]byte, n)
}

// buffer recycling hook
func get(n int) []byte {
	return make([]byte, n)
}

// buffer recycling hook
func put(b []byte) {}

func read8(s store, handle int64, off int64, scratch []byte) (int64, error) {
	if err := s.readAt(handle, scratch[:8], off); err != nil {
		return -1, err
	}

	return b2n(scratch), nil
}

func write8(s store, handle, off, val int64, scratch []byte) error {
	return s.writeAt(handle, n2b(scratch[:8], val), off)
}
