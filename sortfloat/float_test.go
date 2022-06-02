package sortfloat

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"math"
	"sort"
	"testing"
	"testing/quick"
)

func TestSortableFloat64(t *testing.T) {
	values := []float64{-10, -200, 0, 30, 400, 40, 12e-15, 11e10, -13e-15, -14e15, 11e10 + 1, math.NaN(), 0.2,
		math.Inf(1), math.Inf(-1), math.MaxFloat64, -math.MaxFloat64,
		math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64}
	// Add -0
	values = append(values, math.Float64frombits(0x8000000000000000))
	var tosort []uint64
	for _, v := range values {
		forward := SortableFloat64(v)
		reverse := ReverseFloat64(forward)
		t.Logf("in: %v, fbits:%0x, for: %0x, out: %v, rbits: %0x", v, math.Float64bits(v), forward, reverse, math.Float64bits(reverse))
		tosort = append(tosort, forward)
	}
	sort.SliceStable(tosort, func(i, j int) bool { return tosort[i] < tosort[j] })
	sort.Float64s(values)

	for i, v := range values {
		got := ReverseFloat64(tosort[i])
		if v != got && !(v != v) {
			t.Errorf("Index %d, want %v, got %v", i, v, got)
		} else {
			t.Log(i, "uint:", got, "float:", v)
		}
	}
}

func TestReverseFloat64(t *testing.T) {
	f := func(x float64) bool {
		return ReverseFloat64(SortableFloat64(x)) == x
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 1e5}); err != nil {
		t.Error(err)
	}
	t.Log(math.Float64frombits(0x8000000000000000) > 0)
}
