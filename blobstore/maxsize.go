package blobstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/Vivino/rankdb/log"
)

// MaxSizeStore will make sure that no blobs are above the specified size.
// The upper storage limit is 65535 * maximum size.
type MaxSizeStore struct {
	store   Store
	maxsize int

	// Cache parts size.
	// No entry means one or unknown number of segments.
	parts   map[string]int
	running map[string]*sync.Mutex
	mu      sync.Mutex
}

// NewMaxSizeStore will split blobs into the specified size.
// Specify the upper byte limit.
// The upper storage limit is 65535 * maximum size.
func NewMaxSizeStore(store Store, maxSize int) (*MaxSizeStore, error) {
	if maxSize < 1024 {
		return nil, fmt.Errorf("max size should be more than 1KB")
	}
	r := MaxSizeStore{
		store:   store,
		maxsize: maxSize,
		parts:   make(map[string]int),
		running: make(map[string]*sync.Mutex),
	}
	return &r, nil
}

func mapKey(set, key string) string {
	return set + `/\` + key
}

func extKey(key string, n int) string {
	return key + "+" + strconv.Itoa(n)
}

func (r *MaxSizeStore) runningOp(set, key string) func() {
	r.mu.Lock()
	mu := r.running[mapKey(set, key)]
	if mu == nil {
		mu = &sync.Mutex{}
		r.running[mapKey(set, key)] = mu
	}
	r.mu.Unlock()
	mu.Lock()
	return mu.Unlock
}

// Get a blob.
func (r *MaxSizeStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	defer r.runningOp(set, key)()
	b, err := r.store.Get(ctx, set, key)
	if err != nil {
		return nil, err
	}
	n, size := binary.Uvarint(b)
	if size < 1 {
		return nil, errors.New("corrupt blob")
	}
	b = b[size:]
	if n == 0 {
		r.mu.Lock()
		delete(r.parts, mapKey(set, key))
		r.mu.Unlock()
		return b, nil
	}
	r.mu.Lock()
	r.parts[mapKey(set, key)] = int(n + 1)
	r.mu.Unlock()
	for i := 0; i < int(n); i++ {
		b2, err := r.store.Get(ctx, set, extKey(key, i))
		if err != nil {
			return nil, err
		}
		b = append(b, b2...)
	}
	return b, nil
}

// Set a blob.
func (r *MaxSizeStore) Set(ctx context.Context, set, key string, b []byte) error {
	defer r.runningOp(set, key)()
	total := (len(b) + binary.MaxVarintLen16 + r.maxsize - 1) / r.maxsize
	if total == 0 {
		panic("incomprehensible")
	}
	if total >= math.MaxUint16 {
		return ErrBlobTooBig
	}
	var buf [binary.MaxVarintLen16]byte
	var bbuf = buf[:]
	s := binary.PutUvarint(bbuf[:], uint64(total-1))

	// delete extra parts if more.
	// Since we are saving it is extremely likely a load has previously been done.
	r.mu.Lock()
	prev := r.parts[mapKey(set, key)]
	r.mu.Unlock()
	for i := total; i < prev; i++ {
		kkey := extKey(key, i-1)
		err := r.store.Delete(ctx, set, kkey)
		if err != nil {
			log.Error(ctx, "Error deleting extra segments", "error", err, "key", kkey, "set", set)
		}
	}

	// Prepend total
	b2 := make([]byte, len(b)+s)
	copy(b2, buf[:s])
	copy(b2[s:], b)

	for i := 0; i < total; i++ {
		kkey := key
		if i > 0 {
			kkey = extKey(key, i-1)
		}
		l := r.maxsize
		if l > len(b2) {
			l = len(b2)
		}
		b = b2[:l]
		err := r.store.Set(ctx, set, kkey, b)
		if err != nil {
			return err
		}
		b2 = b2[l:]
	}
	r.mu.Lock()
	if total == 1 {
		delete(r.parts, mapKey(set, key))
	} else {
		r.parts[mapKey(set, key)] = total
	}
	r.mu.Unlock()
	return nil
}

// Delete a blob.
func (r *MaxSizeStore) Delete(ctx context.Context, set, key string) error {
	defer r.runningOp(set, key)()
	r.mu.Lock()
	n := uint64(r.parts[mapKey(set, key)])
	r.mu.Unlock()

	if n == 0 {
		b, err := r.store.Get(ctx, set, key)
		if err != nil {
			return nil
		}
		var size int
		n, size = binary.Uvarint(b)
		if size < 1 {
			n = 0
		}
		n++
	}
	for i := 0; i < int(n); i++ {
		kkey := key
		if i > 0 {
			kkey = extKey(key, i-1)
		}
		err := r.store.Delete(ctx, set, kkey)
		if err != nil {
			return err
		}
	}
	r.mu.Lock()
	delete(r.parts, mapKey(set, key))
	r.mu.Unlock()
	return nil
}
