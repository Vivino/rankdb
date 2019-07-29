package badgerstore

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore/bstest"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
	"github.com/dgraph-io/badger"
)

func randString(size int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(rng.Int63()%25) + 'a'
	}
	return string(b)
}

func TestNew(t *testing.T) {
	t.Parallel()
	logger := testlogger.New(t)
	ctx := log.WithLogger(context.Background(), logger)
	dir := os.TempDir()
	fn := filepath.Join(dir, randString(10)+"-testdb.badger")
	defer os.Remove(fn)
	m, err := New(badger.DefaultOptions(fn))
	if err != nil {
		t.Fatal(err)
	}
	st := bstest.NewTest(m, t)
	st.Many = 10000
	st.Less = 1000
	st.Few = 200
	st.All(ctx)
}
