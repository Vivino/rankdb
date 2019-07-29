package memstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
)

// NewMemStore returns a new MemBlobStore
func NewMemStore() *MemBlobStore {
	return &MemBlobStore{
		blobs:   make(map[string][]byte),
		reads:   make(map[string]int),
		writes:  make(map[string]int),
		deletes: make(map[string]int),
	}
}

type MemBlobStore struct {
	blobsMu sync.Mutex
	blobs   map[string][]byte
	reads   map[string]int
	writes  map[string]int
	deletes map[string]int
	debug   bool
}

func toKey(set, key string) string {
	return set + "/" + key
}

func splitKey(k string) (set, key string) {
	s := strings.Split(k, "/")
	if len(s) != 2 {
		panic(fmt.Errorf("len != 2, was %d", len(s)))
	}
	return s[0], s[1]
}

func (m *MemBlobStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	if m.debug {
		log.Logger(ctx).Info("Get Blob", "blob_set", set, "blob_key", key)
	}
	m.blobsMu.Lock()
	blob, ok := m.blobs[toKey(set, key)]
	m.reads[toKey(set, key)]++
	m.blobsMu.Unlock()
	if !ok {
		return nil, blobstore.ErrBlobNotFound
	}
	return blob, nil
}

func (m *MemBlobStore) Set(ctx context.Context, set, key string, val []byte) error {
	if m.debug {
		log.Logger(ctx).Info("Set Blob", "blob_set", set, "blob_key", key, "length", len(val))
	}
	if len(val) > blobstore.MaxSize {
		return blobstore.ErrBlobTooBig
	}
	if len(key) < 2 {
		return fmt.Errorf("key too short: %s", key)
	}
	dst := make([]byte, len(val))
	copy(dst, val)
	m.blobsMu.Lock()
	m.blobs[toKey(set, key)] = dst
	m.writes[toKey(set, key)]++
	m.blobsMu.Unlock()
	return nil
}

func (m *MemBlobStore) Delete(ctx context.Context, set, key string) error {
	if m.debug {
		log.Logger(ctx).Info("Delete Blob", "blob_set", set, "blob_key", key)
	}
	m.blobsMu.Lock()
	delete(m.blobs, toKey(set, key))
	m.deletes[toKey(set, key)]++
	m.blobsMu.Unlock()
	return nil
}

func (m *MemBlobStore) Usage(set string) (blobs, size int) {
	m.blobsMu.Lock()
	for k, v := range m.blobs {
		if strings.HasPrefix(k, toKey(set, "")) {
			size += len(v)
			blobs++
		}
	}
	m.blobsMu.Unlock()
	return blobs, size
}

func (m *MemBlobStore) Dir(set string) []string {
	var res []string
	m.blobsMu.Lock()
	for k, v := range m.blobs {
		s, key := splitKey(k)
		if set == s {
			res = append(res, fmt.Sprintf("%s: %d bytes [r:%d, w:%d, d:%d]", key, len(v), m.reads[k], m.writes[k], m.deletes[k]))
		}
	}
	m.blobsMu.Unlock()
	sort.Strings(res)
	return res
}

// Debug enables printing of all operations
func (m *MemBlobStore) Debug(b bool) {
	m.debug = b
}
