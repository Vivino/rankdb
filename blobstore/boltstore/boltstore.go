package boltstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"go.etcd.io/bbolt"
)

// NewBoltStore returns a new BoltStore.
// If nil options are provided a default is used.
func NewBoltStore(fileName string, opts *bbolt.Options) (*BoltStore, error) {
	if opts == nil {
		opts = &bbolt.Options{Timeout: 10 * time.Second}
	}
	db, err := bbolt.Open(fileName, 0600, opts)
	if err != nil {
		return nil, err
	}

	return &BoltStore{db: db}, nil
}

type BoltStore struct {
	db *bbolt.DB
}

// DB will return the underlying bolt DB.
// This should not be modified once the server has started.
func (b *BoltStore) DB() *bbolt.DB {
	return b.db
}

func (b *BoltStore) Close() error {
	return b.db.Close()
}

func (b *BoltStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	var res []byte
	err := b.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(set))
		if b == nil {
			return blobstore.ErrBlobNotFound
		}
		val := b.Get([]byte(key))
		if val == nil {
			return blobstore.ErrBlobNotFound
		}
		// blob only valid during transaction, so copy.
		res = make([]byte, len(val))
		copy(res, val)
		return nil
	})
	return res, err
}

func (b *BoltStore) Set(ctx context.Context, set, key string, val []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(set))
		if err != nil {
			return err
		}
		err = b.Put([]byte(key), val)
		return err
	})
}

func (b *BoltStore) Create(ctx context.Context, set, key string, val []byte) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(set))
		if err != nil {
			return err
		}
		v := b.Get([]byte(key))
		if v != nil {
			return blobstore.ErrBlobExists
		}
		return b.Put([]byte(key), val)
	})
}

func (b *BoltStore) Delete(ctx context.Context, set, key string) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(set))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}
