package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/Vivino/rankdb"
)

func TestSegment_Size(t *testing.T) {
	var info rankdb.Segment
	const infoSize = unsafe.Sizeof(info)
	t.Log(infoSize)
}

func TestSegment_Filter(t *testing.T) {
	lst := rankdb.Elements{}
	lst = append(lst, rankdb.Element{Score: 1000, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 900, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 800, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 700, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 500, TieBreaker: 3})
	lst = append(lst, rankdb.Element{Score: 500, TieBreaker: 2})
	lst = append(lst, rankdb.Element{Score: 400, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 300, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 2, TieBreaker: 1})
	lst = append(lst, rankdb.Element{Score: 1, TieBreaker: 1})
	lst.Sort()
	testSet := []struct {
		name string
		seg  rankdb.Segment
		want rankdb.Elements
	}{
		{
			name: "range where tiebreaker is above",
			seg:  rankdb.Segment{Min: 300, Max: 500},
			want: rankdb.Elements{{Score: 400, TieBreaker: 1}, {Score: 300, TieBreaker: 1}},
		},
		{
			name: "range where tiebreaker is equal",
			seg:  rankdb.Segment{Min: 300, Max: 500, MaxTie: 2},
			want: rankdb.Elements{{Score: 500, TieBreaker: 2}, {Score: 400, TieBreaker: 1}, {Score: 300, TieBreaker: 1}},
		},
		{
			name: "range where tiebreaker is only difference",
			seg:  rankdb.Segment{Min: 500, Max: 500, MaxTie: 10},
			want: rankdb.Elements{{Score: 500, TieBreaker: 3}, {Score: 500, TieBreaker: 2}},
		},
		{
			name: "all",
			seg:  rankdb.Segment{Max: math.MaxUint64, MaxTie: math.MaxUint32},
			want: lst,
		},
		{
			name: "none",
			seg:  rankdb.Segment{},
			want: rankdb.Elements{},
		},
		{
			name: "first",
			seg:  rankdb.Segment{Min: 1000, Max: 10000},
			want: rankdb.Elements{{Score: 1000, TieBreaker: 1}},
		},
		{
			name: "last",
			seg:  rankdb.Segment{Min: 1, Max: 2},
			want: rankdb.Elements{{Score: 1, TieBreaker: 1}},
		},
	}

	for i, test := range testSet {
		t.Run(fmt.Sprint(i, "-", test.name), func(t *testing.T) {
			got := test.seg.Filter(lst)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Got %v, want %v", got, test.want)
			}
		})
	}
}

func TestSegment_FilterScoresIdx(t *testing.T) {
	testSet := []struct {
		name                  string
		seg                   rankdb.Segment
		scores                []uint64
		want_start, want_stop int
	}{
		{
			name:       "range where index is in the middle",
			seg:        rankdb.Segment{Min: 300, Max: 500},
			scores:     []uint64{2000, 1000, 500, 400, 300, 100, 60, 0},
			want_start: 2,
			want_stop:  4,
		},
		{
			name:       "range where segment is at beginning",
			seg:        rankdb.Segment{Min: 300, Max: 5000},
			scores:     []uint64{2000, 1000, 500, 400, 300, 100, 60, 0},
			want_start: 0,
			want_stop:  4,
		},
		{
			name:       "range where segment is at the end",
			seg:        rankdb.Segment{Min: 0, Max: 100},
			scores:     []uint64{2000, 1000, 500, 400, 300, 100, 60, 0},
			want_start: 5,
			want_stop:  7,
		},
		{
			name:       "range where none is in range (too big)",
			seg:        rankdb.Segment{Min: 3000, Max: 5000},
			scores:     []uint64{2000, 1000, 500, 400, 300, 100, 60, 0},
			want_start: 0,
			want_stop:  0,
		},
		{
			name:       "range where none is in range (too small)",
			seg:        rankdb.Segment{Min: 0, Max: 40},
			scores:     []uint64{2000, 1000, 500, 400, 300, 100, 60, 50},
			want_start: 0,
			want_stop:  0,
		},
	}

	for i, test := range testSet {
		t.Run(fmt.Sprint(i, "-", test.name), func(t *testing.T) {
			start, stop := test.seg.FilterScoresIdx(test.scores)
			if start != test.want_start {
				t.Errorf("Got start %v, want %v", start, test.want_start)
			}
			if stop != test.want_stop {
				t.Errorf("Got stop %v, want %v", stop, test.want_stop)
			}
		})
	}
}

func BenchmarkSegment_Filter(b *testing.B) {
	for n := benchStartElements * 10; n <= benchMaxElements*10; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		quarter := n / 4
		segs := rankdb.NewSegments(0, false)
		s := rankdb.NewSegment(segs)
		s.Min, s.MinTie = in[3*quarter].Score, in[3*quarter].TieBreaker
		s.Max, s.MaxTie = in[quarter].Score, in[quarter].TieBreaker

		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				seg := s.Filter(in)
				if len(seg) < quarter {
					b.Fatalf("too short, got %v, want at least %v", len(seg), quarter)
				}
				if len(seg) > n-quarter {
					b.Fatalf("too long, got %v, want at least %v", len(seg), n-quarter)
				}
			}
		})
	}
}

/*
func BenchmarkSegment_SaveElements(b *testing.B) {
	ctx := context.Background()
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		segs := rankdb.NewSegments(0, false)
		s := rankdb.NewSegmentElements(segs, in)
		store := blobstore.StoreWithSet(memstore.NewMemStore(), "test")
		pl, err := store.Get(ctx, s.seg.StorageKey())
		if err != nil {
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
		in := rankdb.NewElements(randElements(n))
		segs := rankdb.NewSegments(0, false)
		s := rankdb.NewSegmentElements(segs, in)
		mem := memstore.NewMemStore()
		store := blobstore.StoreWithSet(mem, "test")
		err := s.saveElements(ctx, store, in)
		if err != nil {
			b.Fatal(err)
		}
		pl, err := store.Get(ctx, s.StorageKey())
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
				e, err := s.loadElements(ctx, store)
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
*/
