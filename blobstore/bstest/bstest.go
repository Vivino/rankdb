// Package bstest supplies helpers to test blobstores.
package bstest

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
	"github.com/google/go-cmp/cmp"
)

const (
	// MaxBlobSize is the typical max blob size.
	MaxBlobSize = 34431
)

type Test struct {
	s blobstore.Store
	t *testing.T

	Many, Less, Few int
}

// NewTests will create a test object.
// This allows to run common tests on a blobstore.
func NewTest(s blobstore.Store, t *testing.T) Test {
	return Test{
		s:    s,
		t:    t,
		Many: 10000,
		Less: 1000,
		Few:  100,
	}
}

// All will run all tests as sub-tests.
func (t Test) All(ctx context.Context) {
	tt := t.t
	t.t = nil
	defer func() {
		t.t = tt
	}()
	test := func(name string, fn func(ctx context.Context, tt *testing.T)) {
		tt.Run(name, func(tt *testing.T) {
			logger := testlogger.New(tt)
			ctx := log.WithLogger(ctx, logger)
			fn(ctx, tt)
		})
	}
	test("Basic", t.Basic)
	test("Concurrent", t.Concurrent)
	test("Colliding", t.Colliding)
	test("Overlapping", t.Overlapping)
}

// Basic test basic set/get/delete functionality
func (t Test) Basic(ctx context.Context, tt *testing.T) {
	blobSize := MaxBlobSize
	b := newBlob(blobSize, 0x1337)
	hashOrg := md5.Sum(b)
	set, key := randString(20), randString(20)

	v, err := t.s.Get(ctx, set, key)
	if err != blobstore.ErrBlobNotFound {
		tt.Fatalf("want %v, got %v", blobstore.ErrBlobNotFound, err)
	}
	if len(v) != 0 {
		tt.Fatalf("want %v, got %v", 0, len(v))
	}
	err = t.s.Delete(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}

	// Store blob
	err = t.s.Set(ctx, set, key, b)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	v, err = t.s.Get(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	if len(v) != blobSize {
		tt.Fatalf("want %v, got %v", blobSize, len(v))
	}
	if !bytes.Equal(v, b) {
		tt.Fatalf("stored object was not returned correctly")
	}
	// Modify original slice
	copy(b, b[blobSize/2:])
	gotH := md5.Sum(v)
	if !bytes.Equal(gotH[:], hashOrg[:]) {
		tt.Fatalf("stored object was had not broken reference to input")
	}

	hashOrg = md5.Sum(b)
	err = t.s.Set(ctx, set, key, b)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	v, err = t.s.Get(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	if len(v) != blobSize {
		tt.Fatalf("want %v, got %v", blobSize, len(v))
	}
	if !bytes.Equal(v, b) {
		tt.Fatalf("stored object was not returned correctly")
	}

	// Delete it
	err = t.s.Delete(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	v, err = t.s.Get(ctx, set, key)
	if err != blobstore.ErrBlobNotFound {
		tt.Fatalf("want %v, got %v", blobstore.ErrBlobNotFound, err)
	}
	if len(v) != 0 {
		tt.Fatalf("want %v, got %v", 0, len(v))
	}
	err = t.s.Delete(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
}

// Concurrent runs concurrent get/set/delete operations, but on unique keys/goroutine.
func (t Test) Concurrent(ctx context.Context, tt *testing.T) {
	var wg sync.WaitGroup
	var instances = t.Less
	if testing.Short() {
		instances = t.Few
	}
	test := func(i int) string {
		rng := rand.New(rand.NewSource(int64(i)))
		var blobSize = int(rng.Int63()%MaxBlobSize) + 1
		b := newBlob(blobSize, rng.Int63())
		hashOrg := md5.Sum(b)
		set, key := randStringRng(20, rng.Int63()), randStringRng(20, rng.Int63())

		v, err := t.s.Get(ctx, set, key)
		if err != blobstore.ErrBlobNotFound {
			return fmt.Sprintf("get: want %v, got %v (nothing stored)", blobstore.ErrBlobNotFound, err)
		}
		if len(v) != 0 {
			return fmt.Sprintf("get len: want %v, got %v (nothing stored)", 0, len(v))
		}
		err = t.s.Delete(ctx, set, key)
		if err != nil {
			return fmt.Sprintf("delete: want %v, got %v (nothing stored)", nil, err)
		}

		// Store blob
		err = t.s.Set(ctx, set, key, b)
		if err != nil {
			return fmt.Sprintf("want %v, got %v (initial store)", nil, err)
		}
		v, err = t.s.Get(ctx, set, key)
		if err != nil {
			return fmt.Sprintf("want %v, got %v (initial store)", nil, err)
		}
		if len(v) != blobSize {
			return fmt.Sprintf("want %v, got %v (initial store)", blobSize, len(v))
		}
		if !bytes.Equal(v, b) {
			tt.Log(cmp.Diff(v, b))
			return fmt.Sprintf("stored object was not returned correctly (initial store)")
		}
		// Modify original slice
		copy(b, b[blobSize/2:])
		gotH := md5.Sum(v)
		if !bytes.Equal(gotH[:], hashOrg[:]) {
			return fmt.Sprintf("stored object was had not broken reference to input")
		}

		hashOrg = md5.Sum(b)
		err = t.s.Set(ctx, set, key, b)
		if err != nil {
			return fmt.Sprintf("set err: want %v, got %v (after update)", nil, err)
		}
		v, err = t.s.Get(ctx, set, key)
		if err != nil {
			return fmt.Sprintf("get err: want %v, got %v (after update)", nil, err)
		}
		if len(v) != blobSize {
			return fmt.Sprintf("length: want %v, got %v (after update)", blobSize, len(v))
		}
		if !bytes.Equal(v, b) {
			tt.Log(cmp.Diff(v, b))
			return fmt.Sprintf("compare: stored object was not returned correctly")
		}

		// Delete it
		err = t.s.Delete(ctx, set, key)
		if err != nil {
			return fmt.Sprintf("delete: want %v, got %v", nil, err)
		}
		v, err = t.s.Get(ctx, set, key)
		if err != blobstore.ErrBlobNotFound {
			return fmt.Sprintf("delete, get: want %v, got %v", blobstore.ErrBlobNotFound, err)
		}
		if len(v) != 0 {
			return fmt.Sprintf("delete, len: want %v, got %v", 0, len(v))
		}
		err = t.s.Delete(ctx, set, key)
		if err != nil {
			return fmt.Sprintf("delete 2: want %v, got %v", nil, err)
		}
		return ""
	}

	errs := make(chan string, instances)
	wg.Add(instances)
	for i := 0; i < instances; i++ {
		go func(i int) {
			defer wg.Done()
			e := test(i)
			if e != "" {
				errs <- e
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		tt.Error(e)
	}
}

// Colliding runs concurrent get/set/delete operations, with colliding keys.
// This should usually be run with race detection.
func (t Test) Colliding(ctx context.Context, tt *testing.T) {
	var wg sync.WaitGroup
	var instances = t.Few
	if testing.Short() {
		instances /= 10
	}
	gRNG := rand.New(rand.NewSource(1337))
	set, key := randStringRng(20, gRNG.Int63()), randStringRng(20, gRNG.Int63())
	test := func(i int) {
		rng := rand.New(rand.NewSource(int64(i)))
		var blobSize = int(rng.Int63()%MaxBlobSize) + 1
		b := newBlob(blobSize, rng.Int63())
		t.s.Get(ctx, set, key)
		t.s.Delete(ctx, set, key)
		t.s.Set(ctx, set, key, b)
		t.s.Get(ctx, set, key)
		t.s.Delete(ctx, set, key)
	}

	wg.Add(instances)
	for i := 0; i < instances; i++ {
		go func(i int) {
			defer wg.Done()
			test(i)
		}(i)
	}
	wg.Wait()
	_, err := t.s.Get(ctx, set, key)
	if err != blobstore.ErrBlobNotFound {
		tt.Errorf("want %v, got %v", blobstore.ErrBlobNotFound, nil)
	}
}

// Overlapping runs concurrent get/set/delete operations, with colliding keys.
// Last operation should leave the
// This should usually be run with race detection.
func (t Test) Overlapping(ctx context.Context, tt *testing.T) {
	var wg sync.WaitGroup
	var instances = t.Few
	if testing.Short() {
		instances /= 10
	}
	gRNG := rand.New(rand.NewSource(16775))
	set, key := randStringRng(20, gRNG.Int63()), randStringRng(20, gRNG.Int63())
	test := func(i int) error {
		rng := rand.New(rand.NewSource(int64(i)))
		var blobSize = int(rng.Int63()%MaxBlobSize) + 1

		b, err := t.s.Get(ctx, set, key)
		if err == nil {
			if len(b) == 0 {
				return errors.New("got 0 length blob")
			}
			want := newBlob(len(b), int64(len(b)))
			if !bytes.Equal(b, want) {
				gotH := md5.Sum(b)
				wantH := md5.Sum(want)
				return fmt.Errorf("result mismatch hash %x, want %x", gotH[:], wantH[:])
			}
		}
		t.s.Delete(ctx, set, key)
		b = newBlob(blobSize, int64(blobSize))
		err = t.s.Set(ctx, set, key, b)
		if err != nil {
			return err
		}
		b, err = t.s.Get(ctx, set, key)
		if err == nil {
			if len(b) == 0 {
				return errors.New("got 0 length blob (second try)")
			}
			want := newBlob(len(b), int64(len(b)))
			if !bytes.Equal(b, want) {
				gotH := md5.Sum(b)
				wantH := md5.Sum(want)
				return fmt.Errorf("result mismatch hash %x, want %x (second try)", gotH[:], wantH[:])
			}
		}
		return nil
	}

	errs := make(chan string, instances)
	wg.Add(instances)
	for i := 0; i < instances; i++ {
		go func(i int) {
			defer wg.Done()
			e := test(i)
			if e != nil {
				errs <- e.Error()
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		tt.Error(err)
	}
	b, err := t.s.Get(ctx, set, key)
	if err != nil {
		tt.Fatalf("want %v, got %v", nil, err)
	}
	if len(b) == 0 {
		tt.Fatal("got 0 length blob (at exit)")
	}
	want := newBlob(len(b), int64(len(b)))
	if !bytes.Equal(b, want) {
		gotH := md5.Sum(b)
		wantH := md5.Sum(want)
		tt.Errorf("result mismatch hash %x, want %x (at exit)", gotH[:], wantH[:])
	}
}

func newBlob(size int, seed int64) []byte {
	rng := rand.New(rand.NewSource(seed))
	blob := make([]byte, size)
	_, err := io.ReadAtLeast(rng, blob, size)
	if err != nil {
		panic(err)
	}
	return blob
}

func randString(size int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rng.Int63()%25) + 'a'
	}
	return string(b)
}

func randStringRng(size int, seed int64) string {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rng.Int63()%25) + 'a'
	}
	return string(b)
}
