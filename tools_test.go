package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"testing"
	"time"

	"github.com/Vivino/rankdb"
)

func TestRandString(t *testing.T) {
	saw := make(map[string]struct{})
	for i := 0; i < 50; i++ {
		p := rankdb.RandString(8)
		time.Sleep(time.Millisecond)
		if _, ok := saw[p]; ok {
			t.Fatalf("already saw %q", p)
		}
		saw[p] = struct{}{}
		t.Log(p)
	}
}
