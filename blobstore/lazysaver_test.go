package blobstore_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/bstest"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
)

func TestLazySaver(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	ls, err := blobstore.NewLazySaver(m,
		blobstore.WithLazySaveOption.Logger(logger),
		blobstore.WithLazySaveOption.SaveTimeout(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(ls, t)
	st.All(ctx)
}

func TestLazySaverContention(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := memstore.NewMemStore()
	ls, err := blobstore.NewLazySaver(m,
		blobstore.WithLazySaveOption.Items(30, 20),
		blobstore.WithLazySaveOption.Savers(1),
		blobstore.WithLazySaveOption.SaveTimeout(time.Minute),
		blobstore.WithLazySaveOption.Logger(logger))
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(ls, t)
	if !testing.Short() {
		st.Few *= 10
		st.Less *= 10
		st.Many *= 10
	}
	st.All(ctx)
}
