// Copyright 2017 The DB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

const (
	oBTreeTODO = 8 * iota // int64		0	8
)

// BTree represents a B+tree.
type BTree struct {
	*DB
	Off int64
}
