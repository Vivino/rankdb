package blobstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
)

// RetryStore will apply retry functionality with customizable backoff.
type RetryStore struct {
	retryStoreOptions

	store Store
}

type RetryStoreOption func(*retryStoreOptions) error

type retryStoreOptions struct {
	// Back off for each operation.
	get, set, del func() backoff.BackOff

	opTimeout time.Duration

	// No retry on these errors
	permanent map[error]struct{}
}

var defaultBackoff = func() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 20 * time.Millisecond
	return b
}

var defaultRetryStoreOptions = retryStoreOptions{
	get:       defaultBackoff,
	set:       defaultBackoff,
	del:       defaultBackoff,
	opTimeout: time.Second,
	permanent: map[error]struct{}{
		context.Canceled: {},
		ErrBlobNotFound:  {},
		ErrBlobTooBig:    {},
	},
}

// LazySaveOption provides access to LazySave Options that are methods on this struct.
type RetryOpt struct{}

// WithLazySaveOption provides an element to create lazySave parameters.
var WithRetryOpt = RetryOpt{}

// Backoff supplies individual backoff strategies for get, set and delete operations.
// If nil is supplied for any option, that strategy will not be updated.
func (l RetryOpt) Backoff(get, set, del func() backoff.BackOff) RetryStoreOption {
	return func(o *retryStoreOptions) error {
		if get != nil {
			o.get = get
		}
		if set != nil {
			o.set = set
		}
		if del != nil {
			o.del = del
		}
		return nil
	}
}

// PermanentErr will add errors to permanent error table.
func (l RetryOpt) PermanentErr(errs ...error) RetryStoreOption {
	return func(o *retryStoreOptions) error {
		if len(errs) == 0 {
			return nil
		}
		// Copy so we don't modify defaults.
		perm := map[error]struct{}{}
		for k := range o.permanent {
			perm[k] = struct{}{}
		}
		for _, err := range errs {
			if err != nil {
				o.permanent[err] = struct{}{}
			}
		}
		o.permanent = perm
		return nil
	}
}

// OpTimeout will set the context timeout for each operation attempt.
// Setting this to 0 will use parent context deadlines.
// Default is 1 second.
func (l RetryOpt) OpTimeout(duration time.Duration) RetryStoreOption {
	return func(o *retryStoreOptions) error {
		o.opTimeout = duration
		return nil
	}
}

// NewLazySaver will create a new lazy saver backed by the provided store.
func NewRetryStore(store Store, opts ...RetryStoreOption) (*RetryStore, error) {
	r := RetryStore{
		retryStoreOptions: defaultRetryStoreOptions,
		store:             store,
	}

	for _, opt := range opts {
		err := opt(&r.retryStoreOptions)
		if err != nil {
			return nil, err
		}
	}
	return &r, nil
}

// Get a blob.
func (r *RetryStore) Get(ctx context.Context, set, key string) ([]byte, error) {
	bo := r.get()
	bo = backoff.WithContext(bo, ctx)
	var res []byte
	op := func() error {
		lctx := ctx
		if r.opTimeout != 0 {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, r.opTimeout)
			defer cancel()
		}
		b, err := r.store.Get(lctx, set, key)
		if err != nil {
			return r.opErr(ctx, err)
		}
		res = b
		return nil
	}
	return res, backoff.Retry(op, bo)
}

// Set a blob.
func (r *RetryStore) Set(ctx context.Context, set, key string, b []byte) error {
	bo := r.set()
	bo = backoff.WithContext(bo, ctx)
	op := func() error {
		lctx := ctx
		if r.opTimeout != 0 {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, r.opTimeout)
			defer cancel()
		}
		err := r.store.Set(lctx, set, key, b)
		if err != nil {
			return r.opErr(ctx, err)
		}
		return nil
	}
	return backoff.Retry(op, bo)
}

// Delete a blob.
func (r *RetryStore) Delete(ctx context.Context, set, key string) error {
	bo := r.del()
	bo = backoff.WithContext(bo, ctx)
	op := func() error {
		lctx := ctx
		if r.opTimeout != 0 {
			var cancel context.CancelFunc
			lctx, cancel = context.WithTimeout(ctx, r.opTimeout)
			defer cancel()
		}
		err := r.store.Delete(lctx, set, key)
		if err != nil {
			return r.opErr(ctx, err)
		}
		return nil
	}
	return backoff.Retry(op, bo)
}

// opErr checks if an error is permanent or the context is cancelled
// and returns a permanent error if so.
func (r *RetryStore) opErr(ctx context.Context, err error) error {
	if _, ok := r.permanent[err]; ok {
		return backoff.Permanent(err)
	}
	select {
	case <-ctx.Done():
		return backoff.Permanent(err)
	default:
	}
	return err
}
