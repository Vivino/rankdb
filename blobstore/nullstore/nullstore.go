package nullstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb/blobstore"
)

// New returns a new NullStore
// By default Save/Delete returns no error and Get returns ErrBlobNotFound.
func New() *NullStore {
	return &NullStore{GetErr: blobstore.ErrBlobNotFound}
}

type NullStore struct {
	GetResult []byte
	GetErr    error
	SetErr    error
	DelErr    error
}

func (m *NullStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	return m.GetResult, m.GetErr
}

func (m *NullStore) Set(ctx context.Context, set, key string, val []byte) error {
	return m.SetErr
}

func (m *NullStore) Delete(ctx context.Context, set, key string) error {
	return m.DelErr
}
