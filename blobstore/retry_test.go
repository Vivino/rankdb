package blobstore_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"testing"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/bstest"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
)

func TestRetryStore(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	mss, err := blobstore.NewRetryStore(m)
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(mss, t)
	st.All(ctx)
}

func TestRetryStoreErrors(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	ts := blobstore.NewTestStore(m)

	// Fail 15% of the time.
	ts.FailGet(blobstore.TestFailPct(15, 1337))
	ts.FailSet(blobstore.TestFailPct(15, 13370))
	ts.FailDelete(blobstore.TestFailPct(15, 133700))
	mss, err := blobstore.NewRetryStore(ts)
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(mss, t)
	st.All(ctx)
}
