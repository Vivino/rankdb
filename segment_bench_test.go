package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/memstore"
)

func timeUnix(t time.Time) uint32 {
	return uint32(t.Unix())
}

const (
	benchMaxElements   = 10000
	benchStartElements = 10
	benchMulElements   = 5
)

func randElements(n int, seed ...int64) Elements {
	if n == 0 {
		return Elements{}
	}
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	if len(seed) > 0 {
		rng = rand.New(rand.NewSource(seed[0]))
	}
	res := make(Elements, n)
	for i := range res {
		res[i] = Element{
			Score:      uint64(rng.Uint32()),
			TieBreaker: rng.Uint32(),
			Updated:    timeUnix(time.Now().Add(-time.Millisecond * time.Duration(rng.Int63n(int64(time.Hour)*24*365)))),
			ID:         ElementID(rng.Uint64()),
			Payload:    []byte(`{"value":"` + RandString(5) + `", "type": "user-list"}`),
		}
	}
	return res
}

func randSortedElements(n int, seed ...int64) Elements {
	if n == 0 {
		return Elements{}
	}
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	if len(seed) > 0 {
		rng = rand.New(rand.NewSource(seed[0]))
	}
	res := make(Elements, n)
	for i := range res {
		res[i] = Element{
			Score:      uint64(n - i),
			TieBreaker: rng.Uint32(),
			Updated:    timeUnix(time.Now().Add(-time.Millisecond * time.Duration(rng.Int63n(int64(time.Hour)*24*365)))),
			ID:         ElementID(rng.Uint64()),
			Payload:    nil,
		}
	}
	return res
}

func BenchmarkSegment_SaveElements(b *testing.B) {
	ctx := context.Background()
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := NewElements(randSortedElements(n))
		segs := NewSegments(0, false)
		s := NewSegmentElements(segs, in)
		memStore := memstore.NewMemStore()
		store := blobstore.StoreWithSet(memStore, "test")
		err := s.saveElements(ctx, store, s.elements)
		if err != nil {
			b.Fatal(err)
		}
		pl, err := store.Get(ctx, s.seg.StorageKey())
		if err != nil {
			memStore.Dir("test")
			b.Log(s.seg.StorageKey())
			b.Fatal(err)
		}
		b.Log(n, "Elements. Blob size:", len(pl))
		b.ResetTimer()
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			b.SetBytes(int64(len(pl)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err := s.saveElements(ctx, store, in)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSegment_LoadElements(b *testing.B) {
	ctx := context.Background()
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := NewElements(randElements(n))
		segs := NewSegments(0, false)
		s := NewSegmentElements(segs, in)
		mem := memstore.NewMemStore()
		store := blobstore.StoreWithSet(mem, "test")
		err := s.saveElements(ctx, store, in)
		if err != nil {
			b.Fatal(err)
		}
		pl, err := store.Get(ctx, s.seg.StorageKey())
		if err != nil {
			b.Log(mem.Dir("test"))
			b.Fatal(err)
		}
		b.ResetTimer()
		b.Log(n, "Elements. Blob size:", len(pl))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			b.SetBytes(int64(len(pl)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				e, err := s.seg.loadElements(ctx, store, true)
				if err != nil {
					b.Fatal(err)
				}
				if len(e) != len(in) {
					b.Fatal(len(e), "!=", len(in))
				}
			}
		})
	}
}
