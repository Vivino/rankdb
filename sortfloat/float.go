package sortfloat

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "math"

// SortableFloat64 converts a float64 to a uint64 that sort in similar fashion to
// float64, including NaN, +-Inf.
// The value can be converted back to the exact float64 value using ReverseFloat64.
func SortableFloat64(a float64) uint64 {
	// Handle NaN (yeah, looks weird)
	if a != a {
		return 0
	}
	// Handle -0 (yeah, looks weird)
	if a == 0 {
		return 0x8000000000000000
	}
	i := math.Float64bits(a)

	if a < 0 {
		i ^= (1 << 64) - 1
	} else {
		// Only flip sign
		i ^= 1 << 63
	}
	return i
}

// ReverseFloat64 converts the uint64 back to the original value. See SortableFloat64.
func ReverseFloat64(a uint64) float64 {
	if a == 0 {
		return math.NaN()
	}
	if (a >> 63) == 0 {
		a ^= (1 << 63) - 1
	}
	a ^= 1 << 63

	return math.Float64frombits(a)
}
