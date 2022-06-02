package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import "sort"

//go:generate msgp $GOFILE

// ListID is the ID of a list.
type ListID string

// ListIDs is a slice of list IDs.
type ListIDs []ListID

// Deduplicate list ids.
// The id list will be sorted as a side effect.
func (ids *ListIDs) Deduplicate() (changed bool) {
	if ids == nil {
		return false
	}
	lst := *ids
	lst.Sort()
	for i := 1; i < len(lst); {
		prev := lst[i-1]
		e := lst[i]
		if prev != e {
			i++
			continue
		}
		changed = true
		// Delete element i
		lst = append(lst[:i], lst[i+1:]...)
		// Re-test at i (don't increment)
	}
	*ids = lst
	return changed
}

// Sort ids.
func (ids ListIDs) Sort() {
	if sort.SliceIsSorted(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	}) {
		return
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
}
