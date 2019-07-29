package memstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"testing"

	"github.com/Vivino/rankdb/blobstore/bstest"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
)

func TestNewMemStore(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	m := NewMemStore()
	m.Debug(false)
	st := bstest.NewTest(m, t)
	st.All(ctx)
}
