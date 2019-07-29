package blobstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"errors"
)

// Store supplies a blob storage interface.
type Store interface {
	// GetBlob will retrieve a blob from Aerospike.
	// Expects data to have been stored with Set or Create.
	Get(ctx context.Context, set, key string) ([]byte, error)

	// SetTTL will store a byte blob in Aerospike with a given TTL.
	// Update policy is last write wins with no generation checks.
	// To retrieve a blob use GetBlob with same set/key values.
	// If ttl is 0 no expiration will be set.
	// A blob may not be bigger than 1MB.
	Set(ctx context.Context, set, key string, val []byte) error

	// Delete will delete a blob.
	// If the blob could not be deleted an error is returned.
	// If the blob does not exist, the request has no effect.
	Delete(ctx context.Context, set, key string) error
}

type WithSet interface {
	// GetBlob will retrieve a blob from Aerospike.
	// Expects data to have been stored with Set or Create.
	Get(ctx context.Context, key string) ([]byte, error)

	// SetTTL will store a byte blob in Aerospike with a given TTL.
	// Update policy is last write wins with no generation checks.
	// To retrieve a blob use GetBlob with same set/key values.
	// If ttl is 0 no expiration will be set.
	// A blob may not be bigger than 1MB.
	Set(ctx context.Context, key string, val []byte) error

	// Delete will delete a blob.
	// If the blob could not be deleted an error is returned.
	// If the blob does not exist, the request has no effect.
	Delete(ctx context.Context, key string) error

	// Store returns the underlying store.
	Store() Store
}

// StoreWithSet wraps storage with a specific set.
// The returned interface is safe for concurrent use.
func StoreWithSet(s Store, set string) WithSet {
	return storeWithSet{store: s, set: set}
}

type storeWithSet struct {
	store Store
	set   string
}

func (s storeWithSet) Get(ctx context.Context, key string) ([]byte, error) {
	return s.store.Get(ctx, s.set, key)
}

func (s storeWithSet) Set(ctx context.Context, key string, val []byte) error {
	return s.store.Set(ctx, s.set, key, val)
}

func (s storeWithSet) Delete(ctx context.Context, key string) error {
	return s.store.Delete(ctx, s.set, key)
}

func (s storeWithSet) Store() Store {
	return s.store
}

var (
	// ErrBlobNotFound is returned when a blob cannot be found.
	ErrBlobNotFound = errors.New("blob not found")
	// ErrBlobTooBig is returned if a blob is larger than 1MB.
	ErrBlobTooBig = errors.New("blob size exceeded")
	// ErrBlobExists is returned if a blob exists when create is called.
	ErrBlobExists = errors.New("blob already exists")
	// ErrShuttingDown is returned when server is shutting down.
	ErrShuttingDown = errors.New("server is shutting down")
)

const (
	// MaxSize is the maximum blob size.
	MaxSize = 1 << 20
)
