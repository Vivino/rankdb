package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"testing"
	"time"
	"unsafe"

	"github.com/Vivino/rankdb"
)

func TestElement_Size(t *testing.T) {
	var info rankdb.Element
	const infoSize = unsafe.Sizeof(info)
	t.Log(infoSize)
}

func TestElement_Above(t *testing.T) {
	testSet := []struct {
		// a.Above(b)
		a, b rankdb.Element
		want bool
	}{{
		a:    rankdb.Element{Score: 100},
		b:    rankdb.Element{Score: 50},
		want: true,
	}, {
		a:    rankdb.Element{Score: 50},
		b:    rankdb.Element{Score: 100},
		want: false,
	}, {
		a:    rankdb.Element{TieBreaker: 50},
		b:    rankdb.Element{TieBreaker: 100},
		want: false,
	}, {
		a:    rankdb.Element{TieBreaker: 100},
		b:    rankdb.Element{TieBreaker: 50},
		want: true,
	}, {
		a:    rankdb.Element{Updated: uint32(time.Now().Unix())},
		b:    rankdb.Element{Updated: uint32(time.Now().Add(-time.Hour).Unix())},
		want: false,
	}, {
		a:    rankdb.Element{Updated: uint32(time.Now().Add(-time.Hour).Unix())},
		b:    rankdb.Element{Updated: uint32(time.Now().Unix())},
		want: true,
	}, {
		a:    rankdb.Element{ID: 100},
		b:    rankdb.Element{ID: 10},
		want: false,
	}, {
		a:    rankdb.Element{ID: 10},
		b:    rankdb.Element{ID: 100},
		want: true,
	}}
	for i, test := range testSet {
		if got := test.a.Above(test.b); got != test.want {
			t.Errorf("test %d, '%+v' Above '%+v'. Got %t, want %t", i, test.a, test.b, got, test.want)
		} else {
			t.Logf("test %d ok", i)
		}
	}
}
