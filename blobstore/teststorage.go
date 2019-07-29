package blobstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
)

// TestStorage provides callbacks and interceptors for easy testing of blob store failures.
type TestStorage struct {
	get            func(ctx context.Context, set, key string) ([]byte, error)
	GetCallback    func(set, key string)
	set            func(ctx context.Context, set, key string, val []byte) error
	SetCallback    func(set, key string, b []byte)
	delete         func(ctx context.Context, set, key string) error
	DeleteCallback func(set, key string)
}

// NewTestStore is a store that can be used for testing.
// Without modifications it allows
func NewTestStore(store Store) *TestStorage {
	return &TestStorage{
		get:    store.Get,
		set:    store.Set,
		delete: store.Delete,
	}
}

// Get a blob.
func (t *TestStorage) Get(ctx context.Context, set, key string) ([]byte, error) {
	if t.GetCallback != nil {
		t.GetCallback(set, key)
	}
	return t.get(ctx, set, key)
}

// Set a blob.
func (t *TestStorage) Set(ctx context.Context, set, key string, b []byte) error {
	if t.SetCallback != nil {
		t.SetCallback(set, key, b)
	}
	return t.set(ctx, set, key, b)
}

// Delete a blob.
func (t *TestStorage) Delete(ctx context.Context, set, key string) error {
	if t.DeleteCallback != nil {
		t.DeleteCallback(set, key)
	}
	return t.delete(ctx, set, key)
}

// FailGetKey will call the provided function.
// If nil is returned, the original store is queried,
// otherwise the error is returned.
func (t *TestStorage) FailGet(check func(set, key string) error) {
	up := t.get
	t.get = func(ctx context.Context, set, key string) ([]byte, error) {
		if err := check(set, key); err != nil {
			return nil, err
		}
		return up(ctx, set, key)
	}
}

// FailSet will call the provided function.
// If nil is returned, the original store is queried,
// otherwise the error is returned.
func (t *TestStorage) FailSet(check func(set, key string) error) {
	up := t.set
	t.set = func(ctx context.Context, set, key string, b []byte) error {
		if err := check(set, key); err != nil {
			return err
		}
		return up(ctx, set, key, b)
	}
}

// FailDelete will call the provided function.
// If nil is returned, the original store is queried,
// otherwise the error is returned.
func (t *TestStorage) FailDelete(check func(set, key string) error) {
	up := t.delete
	t.delete = func(ctx context.Context, set, key string) error {
		if err := check(set, key); err != nil {
			return err
		}
		return up(ctx, set, key)
	}
}

// ErrTestIntentional is used for intentional test failures.
var ErrTestIntentional = fmt.Errorf("intentional failure for test")

// TestAlwaysFail will make all calls to blob store fail.
// ErrTestIntentional will be returned.
var TestAlwaysFail = func(set, key string) error {
	return ErrTestIntentional
}

// TestFailAfterN will fail after N calls.
// ErrTestIntentional will be returned.
func TestFailAfterN(n int) func(set, key string) error {
	var mu sync.Mutex
	var cnt int
	return func(set, key string) error {
		mu.Lock()
		now := cnt
		cnt++
		mu.Unlock()
		if now == n {
			return ErrTestIntentional
		}
		return nil
	}
}

// TestFailPct will fail the percentage of calls.
// ErrTestIntentional will be returned.
func TestFailPct(n uint32, seed int64) func(set, key string) error {
	rng := rand.New(rand.NewSource(seed))
	var mu sync.Mutex
	return func(set, key string) error {
		mu.Lock()
		r := rng.Uint32()
		mu.Unlock()
		if r%100 < n {
			return ErrTestIntentional
		}
		return nil
	}
}
