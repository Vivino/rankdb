package blobstore_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/bstest"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
)

func TestNewMaxSizeStore(t *testing.T) {
	ctx := log.WithLogger(context.Background(), testlogger.New(t))
	testCases := []struct {
		maxSize  int
		dataSize int
	}{
		{
			maxSize:  2048,
			dataSize: 2048 * 10,
		},
		{
			maxSize:  2000,
			dataSize: 2048 * 10,
		},
		{
			maxSize:  2048,
			dataSize: 2047,
		},
		{
			maxSize:  2048,
			dataSize: 2046,
		},
		{
			maxSize:  2048,
			dataSize: 2045,
		},
		{
			maxSize:  2048,
			dataSize: 0,
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("max%d-data%d", test.maxSize, test.dataSize), func(t *testing.T) {
			store := memstore.NewMemStore()
			tester := blobstore.NewTestStore(store)
			var maxSize = test.maxSize
			mss, err := blobstore.NewMaxSizeStore(tester, maxSize)
			if err != nil {
				t.Fatal(err)
			}

			var data = make([]byte, test.dataSize)
			const testSet = "test-set"
			const testKey = "test-key"
			_, _ = rand.Read(data)

			tester.SetCallback = func(set, key string, b []byte) {
				if set != testSet {
					t.Errorf("set mismatch, %q (got) != %q (want)", set, testSet)
				}
				if len(b) > maxSize {
					t.Errorf("size too big, %v (got) > %v (want)", len(b), maxSize)
				}
				if !t.Failed() {
					t.Logf("blob %q/%q %v bytes, ok", set, key, len(b))
				}
			}

			err = mss.Set(ctx, testSet, testKey, data)
			if err != nil {
				t.Fatal(err)
			}

			read, err := mss.Get(ctx, testSet, testKey)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, read) {
				t.Fatal("bytes mismatch")
			}
			t.Log("Read back ok")

			err = mss.Delete(ctx, testSet, testKey)
			if err != nil {
				t.Fatal(err)
			}

			stored := store.Dir(testSet)
			if len(stored) != 0 {
				t.Error("failed to delete elements: ", stored)
			}
			if !t.Failed() {
				t.Log("Items deleted")
			}

			// Test overwriting with shorter.
			err = mss.Set(ctx, testSet, testKey, data)
			if err != nil {
				t.Fatal(err)
			}
			err = mss.Set(ctx, testSet, testKey, data[:len(data)/2])
			if err != nil {
				t.Fatal(err)
			}
			err = mss.Delete(ctx, testSet, testKey)
			if err != nil {
				t.Fatal(err)
			}

			stored = store.Dir(testSet)
			if len(stored) != 0 {
				t.Error("failed to delete elements: ", stored)
			}
			if !t.Failed() {
				t.Log("Items deleted (after smaller)")
			}
		})
	}
}

func TestMaxSizeStoreGen(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	mss, err := blobstore.NewMaxSizeStore(m, bstest.MaxBlobSize/4)
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(mss, t)
	st.All(ctx)
}

func TestMaxSizeStoreLazy(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	mss, err := blobstore.NewMaxSizeStore(m, bstest.MaxBlobSize/4)
	if err != nil {
		t.Fatal(err)
	}
	ls, err := blobstore.NewLazySaver(mss, blobstore.WithLazySaveOption.SaveTimeout(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(ls, t)
	st.All(ctx)
}

func TestMaxSizeStoreRetryErrors(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	ts := blobstore.NewTestStore(m)

	// Fail 15% of the time.
	ts.FailGet(blobstore.TestFailPct(15, 1337))
	ts.FailSet(blobstore.TestFailPct(15, 13370))
	ts.FailDelete(blobstore.TestFailPct(15, 133700))
	rs, err := blobstore.NewRetryStore(ts)
	if err != nil {
		t.Fatal(err)
	}

	mss, err := blobstore.NewMaxSizeStore(rs, bstest.MaxBlobSize/4)
	if err != nil {
		t.Fatal(err)
	}

	st := bstest.NewTest(mss, t)
	st.All(ctx)
}
