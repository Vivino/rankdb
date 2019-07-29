package badgerstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"sync"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
	"github.com/dgraph-io/badger"
)

const queueSize = 100

// New returns a new BadgerStore with the provided options.
func New(opts badger.Options) (*BadgerStore, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &BadgerStore{
		db:    db,
		queue: make(map[string][]byte, queueSize),
	}, nil
}

// BadgerStore returns a store that operate on Badger database.
// We have a short queue to be able to do bulk writes.
type BadgerStore struct {
	db    *badger.DB
	queue map[string][]byte
	mu    sync.Mutex
}

func (b *BadgerStore) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	err := b.flush()
	err2 := b.db.Close()
	if err != nil {
		return err2
	}
	return err
}

func (b *BadgerStore) key(set, key string) []byte {
	return []byte(set + "/" + key)
}

func (b *BadgerStore) keyString(set, key string) string {
	return set + "/" + key
}

func (b *BadgerStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	var res []byte
	// Check queue first
	b.mu.Lock()
	val, ok := b.queue[b.keyString(set, key)]
	b.mu.Unlock()
	if ok {
		res = make([]byte, len(val))
		copy(res, val)
		return res, nil
	}
	// Load from DB.
	err := b.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(b.key(set, key))
		if err != nil {
			return err
		}
		// blob only valid during transaction, so copy.
		res, err = item.ValueCopy(nil)
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = blobstore.ErrBlobNotFound
	}

	return res, err
}

func (b *BadgerStore) flush() error {
	txn := b.db.NewTransaction(true)
	for k, v := range b.queue {
		if err := txn.Set([]byte(k), v); err == badger.ErrTxnTooBig {
			err = txn.Commit()
			txn = b.db.NewTransaction(true)
			err = txn.Set([]byte(k), v)
			if err != nil {
				txn.Discard()
				return err
			}
			delete(b.queue, k)
		} else if err == nil {
			delete(b.queue, k)
		} else {
			txn.Discard()
			return err
		}
	}
	return txn.Commit()
}

func (b *BadgerStore) Set(ctx context.Context, set, key string, val []byte) error {
	cKey := b.keyString(set, key)
	dst := make([]byte, len(val))
	copy(dst, val)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue[cKey] = dst
	if len(b.queue) >= queueSize {
		return b.flush()
	}
	return nil
}

func (b *BadgerStore) Delete(ctx context.Context, set, key string) error {
	b.mu.Lock()
	delete(b.queue, b.keyString(set, key))
	b.mu.Unlock()
	return b.db.Update(func(tx *badger.Txn) error {
		err := tx.Delete(b.key(set, key))
		if err == badger.ErrKeyNotFound {
			err = nil
		}
		return err
	})
}

// BadgerLogger converts a logger on a context to a Badger logger.
// Debug messages are ignored, Info are forwarded and Warning/Errors are written as errors.
func BadgerLogger(ctx context.Context) badger.Logger {
	return logWrapper{log.Logger(ctx)}
}

type logWrapper struct {
	a log.Adapter
}

func (b logWrapper) Errorf(s string, vals ...interface{}) {
	b.a.Error(fmt.Sprintf(s, vals...))
}

func (b logWrapper) Warningf(s string, vals ...interface{}) {
	b.a.Error(fmt.Sprintf(s, vals...))
}

func (b logWrapper) Infof(s string, vals ...interface{}) {
	b.a.Info(fmt.Sprintf(s, vals...))
}

func (b logWrapper) Debugf(string, ...interface{}) {}
