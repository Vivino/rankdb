package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

//go:generate msgp $GOFILE

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Vivino/rankdb/blobstore"
)

// SegmentID is the ID of a segment.
// It does not correspond to the placement in the list,
// but is used to reference it.
type SegmentID uint32

//msgp:tuple Segment
// A Segment describes a part of the list.
// All elements in the segment are guaranteed to be within Min/Max values (inclusive).
// A segment way describe a larger range than the elements themselves represent,
// it is up to splitting functions to determine this.
type Segment struct {
	ID             SegmentID // ID of this segment
	Min, Max       uint64    // Min/Max score of the elements in the segment.
	MinTie, MaxTie uint32    // Min/Max Tiebreaker of the elements in the segment.
	N              int
	Updated        int64
	Parent         SegmentsID

	loader *elementLoader
}

// segmentVersion should be incremented if non-compatible changes are made.
const segmentVersion = 2

type elementLoader struct {
	Mu        sync.RWMutex
	LoadingMu sync.Mutex
	Loading   chan struct{}
	Gimme     func(read bool) (Elements, error)
}

// MaxSegment contains the highest score and tiebreaker.
func MaxSegment() *Segment {
	return &Segment{Max: math.MaxUint64, MaxTie: math.MaxUint32, Updated: math.MaxInt64, loader: &elementLoader{}}
}

// NewSegment creates a new, empty segment.
func NewSegment(parent *Segments) *Segment {
	return &Segment{
		ID:      parent.newID(),
		Parent:  parent.ID,
		Updated: time.Now().Unix(),
		loader:  &elementLoader{},
	}
}

// NewSegmentElements creates a segment with a number of elements.
// If elements are provided min/max will be populated with values from slice.
// Elements are not saved to segment.
func NewSegmentElements(parent *Segments, e Elements) *lockedSegment {
	s := Segment{
		ID:      parent.newID(),
		Parent:  parent.ID,
		N:       len(e),
		Updated: time.Now().Unix(),
		loader:  &elementLoader{},
	}
	s.loader.Mu.Lock()
	s.Min, s.Max, s.MinTie, s.MaxTie = e.MinMax()
	ls := lockedSegment{
		seg:      &s,
		elements: e,
		index:    -1,
		readOnly: false,
		unlock:   s.loader.Mu.Unlock,
	}
	return &ls
}

// Filter returns the slice of elements that falls within the current segment.
func (s *Segment) Filter(e Elements) Elements {
	start, end := s.FilterIdx(e)
	return e[start:end]
}

// StorageKey returns the storage key for a segment.
func (s *Segment) StorageKey() string {
	return fmt.Sprintf("%s-%06d", s.Parent, s.ID)
}

// cacheID returns the cache id for a segment.
func (s *Segment) cacheID() string {
	return fmt.Sprintf("%v[%v]", s.ID, s.Parent)
}

// FilterIdx returns the indexes of the start and end of the slice of elements
// that fall within the range of the segment.
func (s *Segment) FilterIdx(e Elements) (start, end int) {
	start = -1
	startE := &Element{Score: s.Max, TieBreaker: s.MaxTie}
	// Find first element not above startE
	for i, elem := range e {
		if !elem.aboveP(startE) {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, 0
	}
	endE := &Element{Score: s.Min, TieBreaker: s.MinTie, Updated: math.MaxUint32}
	// Find first element that is below endE
	end = len(e)
	for i, elem := range e[start:] {
		if !elem.aboveP(endE) {
			end = i + start
			break
		}
	}
	return start, end
}

// FilterScoresIdx returns the indexes of the start and end of the slice of elements
// that fall within the range of the segment.
// The supplied scores must be sorted descending.
func (s *Segment) FilterScoresIdx(scores []uint64) (start, end int) {
	if len(scores) == 0 {
		return 0, 0
	}

	start = -1
	// Find first element not above startE
	for i, elem := range scores {
		if elem <= s.Max {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, 0
	}

	// Find first element that is below endE
	end = len(scores)
	for i, elem := range scores[start:] {
		if elem <= s.Min {
			end = i + start
			break
		}
	}
	return start, end
}

// elementsUpdated updates segment information about the elements
// caller must hold s.emu read lock and Segments write lock lock.
func (s *Segment) elementsUpdated(e Elements) {
	s.N = len(e)
	s.Updated = time.Now().UTC().Unix()
}

// loadElements will load segment elements from cache or storage.
// Caller must hold segment lock.
func (s *Segment) loadElements(ctx context.Context, store blobstore.WithSet, readOnly bool) (e Elements, err error) {
	err = nil
	s.loader.LoadingMu.Lock()

	// Check if someone else is loading.
	if wait := s.loader.Loading; wait != nil {
		gimme := s.loader.Gimme
		s.loader.LoadingMu.Unlock()
		<-wait
		return gimme(readOnly)
		// Someone else is loading
	}
	var forReader Elements
	s.loader.Gimme = func(read bool) (Elements, error) {
		if err != nil {
			return nil, err
		}
		if !readOnly {
			return forReader.Clone(false), err
		}
		return forReader, err
	}
	s.loader.Loading = make(chan struct{})
	s.loader.LoadingMu.Unlock()
	defer func() {
		// Signal others we are loaded.
		s.loader.LoadingMu.Lock()
		close(s.loader.Loading)
		s.loader.Loading = nil
		s.loader.Gimme = nil
		s.loader.LoadingMu.Unlock()
	}()

	b, err := store.Get(ctx, s.StorageKey())
	if err != nil {
		return nil, err
	}
	var v uint8
	if b, v = MsgpGetVersion(b); v != elementsVersion {
		return nil, ErrVersionMismatch
	}
	e = make(Elements, 0, s.N)
	_, err = e.UnmarshalMsg(b)
	forReader = e
	if !readOnly {
		forReader = e.Clone(false)
	}
	return e, err
}

// deleteElements will delete the elements in storage and in memory.
// Must hold write lock.
func (s *Segment) deleteElements(ctx context.Context, store blobstore.WithSet) error {
	return store.Delete(ctx, s.StorageKey())
}

// saveElements will save the elements of a segment if it is loaded.
// Bypasses cache.
// Caller must hold s.emu lock
// Typically you should use (*Segments).replaceSegment(...)
func (s *lockedSegment) saveElements(ctx context.Context, store blobstore.WithSet, e Elements) error {
	if s == nil || s.seg == nil {
		return errors.New("tried to save nil segment")
	}
	wm := NewWriterMsg()
	defer wm.Close()
	err := wm.SetVersion(elementsVersion)
	if err != nil {
		return err
	}

	err = e.EncodeMsg(wm.Writer())
	if err != nil {
		return err
	}
	return store.Set(ctx, s.seg.StorageKey(), wm.Buffer().Bytes())
}

// String returns a human readable representation of the segment.
func (s *Segment) String() string {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}
