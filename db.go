// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package DB implements some data structures often found in databases.
package db

import (
	"os"
)

type DB interface {
	// Alloc allocates a database block large enough for storing size bytes
	// and returns its offset or an error, if any.
	Alloc(size int64) (int64, error)

	// Calloc is like Alloc but the allocated database block is zeroed up
	// to size.
	Calloc(size int64) (int64, error)

	// Close finishes database use.
	Close() error

	// Free recycles the allocated database block at off.
	Free(off int64) error

	// ReadAt reads len(p) bytes into p starting at offset off in the
	// database. It returns the number of bytes read (0 <= n <= len(p)) and
	// any error encountered.
	//
	// When ReadAt returns n < len(p), it returns a non-nil error
	// explaining why more bytes were not returned.
	//
	// Even if ReadAt returns n < len(p), it may use all of p as scratch
	// space during the call.
	//
	// If the n = len(p) bytes returned by ReadAt are at the end of the
	// database, ReadAt may return either err == EOF or err == nil.
	ReadAt(p []byte, off int64) (n int, err error)

	// Realloc changes the size of the file block allocated at off, which
	// must have been returned from Alloc or Realloc, to size and returns
	// the offset of the relocated file block or an error, if any. The
	// contents will be unchanged in the range from the start of the region
	// up to the minimum of the old and new sizes. Realloc(off, 0) is equal
	// to Free(off). If the file block was moved, a Free(off) is done.
	Realloc(off, size int64) (int64, error)

	// Root returns the offset of the database root object or an error, if
	// any.  It's not an error if a newly created or empty database has no
	// root yet.  The returned offset in that case will be < 0.
	Root() (int64, error)

	// SetRoot sets the offset of the database root object.
	SetRoot(root int) error

	// Stat returns the os.FileInfo structure describing the database. If
	// there is an error, it will be of type *os.PathError.
	Stat() (os.FileInfo, error)

	// Sync commits the current contents of the database to stable storage.
	// Typically, this means flushing the file system's in-memory copy of
	// recently written data to disk.
	Sync() error

	// Truncate changes the size of the database. If there is an error, it
	// will be of type *os.PathError.
	Truncate(int64) error

	// UsableSize reports the size of the database block allocated at off,
	// which must have been returned from Alloc or Realloc. The allocated
	// file block size can be larger than the size originally requested
	// from Alloc or Realloc.
	UsableSize(off int64) (int64, error)

	// WriteAt writes len(p) bytes from p to the database at offset off. It
	// returns the number of bytes written from p (0 <= n <= len(p)) and
	// any error encountered that caused the write to stop early. WriteAt
	// must return a non-nil error if it returns n < len(p).
	WriteAt(p []byte, off int64) (n int, err error)
}
