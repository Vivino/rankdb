package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

//go:generate msgp $GOFILE

import (
	"encoding/json"
	"math"
)

// ElementID is the ID of an element in a list.
type ElementID uint64

// Element contains information about a single element in a list.
//msgp:tuple Element
type Element struct {
	Score      uint64
	ID         ElementID
	Payload    []byte
	TieBreaker uint32
	Updated    uint32
}

// elementsVersion should be incremented if non-compatible changes are made.
const elementsVersion = 1

// Above returns true if e should be ranked above b.
// If score is equal, the tiebreaker is used, descending.
// If tiebreaker is equal, last update time wins.
// Final tiebreaker is element ID, where lowest ID gets first.
func (e Element) Above(b Element) bool {
	if e.Score != b.Score {
		return e.Score > b.Score
	}
	if e.TieBreaker != b.TieBreaker {
		return e.TieBreaker > b.TieBreaker
	}
	if e.Updated != b.Updated {
		return e.Updated < b.Updated
	}
	return e.ID < b.ID
}

// As above but purely on pointers.
func (e *Element) aboveP(b *Element) bool {
	if e.Score != b.Score {
		return e.Score > b.Score
	}
	if e.TieBreaker != b.TieBreaker {
		return e.TieBreaker > b.TieBreaker
	}
	if e.Updated != b.Updated {
		return e.Updated < b.Updated
	}
	return e.ID < b.ID
}

// String returns a readable string representation of the element.
func (e Element) String() string {
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// IndexElement is an element that is used as an index.
type IndexElement struct {
	Element
}

// AsIndex returns the index element of this element in the given collection.
func (e Element) AsIndex(s SegmentID) IndexElement {
	return IndexElement{Element{ID: e.ID, Score: uint64(e.ID), Updated: e.Updated, TieBreaker: uint32(s)}}
}

// PrevMax will return the maximum just prior to the current element.
func (e Element) PrevMax() (score uint64, tie uint32) {
	if e.Score == 0 && e.TieBreaker == 0 {
		return 0, 0
	}
	if e.TieBreaker == 0 {
		return e.Score - 1, math.MaxUint32
	}
	return e.Score, e.TieBreaker - 1
}

// AboveThis will return the score of the element just above this.
func (e Element) AboveThis() (score uint64, tie uint32) {
	if e.Score == math.MaxUint64 && e.TieBreaker == math.MaxUint32 {
		return math.MaxUint64, math.MaxUint32
	}
	if e.TieBreaker == math.MaxUint32 {
		return e.Score + 1, 0
	}
	return e.Score, e.TieBreaker + 1
}
