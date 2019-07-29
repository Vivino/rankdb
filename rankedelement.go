package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"encoding/json"
	"sort"
)

// RankedElement is a ranked element.
type RankedElement struct {
	Element
	// FromTop indicates how many items are above this in the list.
	FromTop int
	// FromBottom indicates how many items are below this in the list.
	FromBottom int

	// Above contains elements above the current element.
	// Ends with element just above current.
	Above []Element `json:",omitempty"`

	// Below contains elements below the current element.
	// Starts with element just below current.
	Below []Element `json:",omitempty"`

	// Before contains the element placement before update.
	Before *RankedElement `json:"before,omitempty"`
}

// Rank contains the element rank without neighbors.
//msgp:ignore Rank
type Rank struct {
	FromTop    int
	FromBottom int
	Element    Element
}

// CalculateFromBottom recalculates the FromBottom with the supplied list total.
func (r *RankedElement) CalculateFromBottom(total int) {
	r.FromBottom = total - r.FromTop - 1
}

// String returns a human readble representation of the ranked element.
func (r RankedElement) String() string {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// RankedElements is a collection od ranked elements.
type RankedElements []RankedElement

// Elements returns the underlying elements in the same order as the ranked elements.
func (r RankedElements) Elements() Elements {
	res := make(Elements, len(r))
	for i := range r {
		res[i] = r[i].Element
	}
	return res
}

// CalculateFromBottom recalculates the FromBottom with the supplied list total.
func (r RankedElements) CalculateFromBottom(total int) {
	for i, re := range r {
		re.CalculateFromBottom(total)
		r[i] = re
	}
}

// Offset will add a specific offset to FromTop and subtract it from FromBottom.
func (r RankedElements) Offset(offset int) {
	for i, re := range r {
		re.FromTop += offset
		re.FromBottom -= offset
		r[i] = re
	}
}

// ElementIDs returns the element ids.
func (r RankedElements) IDs() ElementIDs {
	res := make(ElementIDs, len(r))
	for i := range r {
		res[i] = r[i].Element.ID
	}
	return res
}

// Sort will re-sort the elements.
func (r RankedElements) Sort() {
	sort.Slice(r, func(i, j int) bool {
		return r[i].Element.Above(r[j].Element)
	})
}
