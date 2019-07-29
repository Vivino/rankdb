package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

// Cache provides a caching interface.
type Cache interface {
	Add(key, value interface{})
	Get(key interface{}) (interface{}, bool)
	Contains(key interface{}) bool
	Remove(key interface{})
}
