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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
)

// SegmentsID is the ID of a collection of segments.
type SegmentsID string

// segmentsVersion should be incremented if non-compatible changes are made.
const segmentsVersion = 2

// SegmentsID returns whether the segment ID is non-zero.
func (s SegmentsID) Unset() bool {
	return s == ""
}

// Segments contains all segments of a list
// and contains and index to quickly look up the index of a segment.
type Segments struct {
	ID           SegmentsID
	Segments     []Segment         // Individual segments
	SegmentsLock sync.RWMutex      `msg:"-" json:"-"` // Global lock for all stats on all segments (above)
	NextID       SegmentID         // Next unused ID
	IsIndex      bool              // These segments represents an index.
	idx          map[SegmentID]int // Segment ID -> Segments Index lookup.
	cache        Cache             // Global segment cache

	cacheHits   uint64
	cacheMisses uint64
}

// NewSegments creates new segments and preallocates space for segments.
// Optionally it will preallocate internal index as well.
func NewSegments(preAlloc int, withIdx bool) *Segments {
	s := Segments{
		ID:       SegmentsID(RandString(8)),
		Segments: make([]Segment, 0, preAlloc),
		NextID:   1,
	}
	if withIdx {
		s.idx = make(map[SegmentID]int, preAlloc)
	}
	return &s
}

// NewSegmentsElements creates segments from slices of elements.
// Each member of the slice will result in one segment.
// The Segment range will cover the entire possible space, so first segment
// will always be from MaxElement, etc.
// If idx is supplied it will be populated with index elements.
// If idx is not supplied it is assumed that this is index segments.
func NewSegmentsElements(ctx context.Context, bs blobstore.WithSet, e []Elements, idx *IndexElements) (*Segments, error) {
	s := &Segments{
		ID:       SegmentsID(RandString(8)),
		Segments: make([]Segment, 0, len(e)),
		NextID:   1,
		IsIndex:  idx == nil,
	}

	next := MaxSegment()
	timestamp := time.Now()

	var ids IndexElements
	if idx != nil {
		ids = *idx
	}

	for i, elems := range e {
		if len(elems) == 0 {
			continue
		}
		e2 := elems.Clone(false)
		ls := NewSegmentElements(s, e2)
		seg := ls.seg
		seg.Max = next.Max
		seg.MaxTie = next.MaxTie
		seg.Updated = timestamp.Unix()

		// Last element covers down to 0.
		if i == len(e)-1 {
			seg.Min = 0
			seg.MinTie = 0
		}
		s.Append(ls)
		if idx != nil {
			ids.Elements = append(ids.Elements, elems.ElementIDs(seg.ID).Elements...)
		}
		err := s.replaceSegment(ctx, bs, ls)
		if err != nil {
			return nil, err
		}
		// Next segment starts just before this the last of this.
		e := elems[len(elems)-1]
		next.Max, next.MaxTie = e.PrevMax()
	}

	// If empty, add one full-range segment.
	if len(s.Segments) == 0 {
		ls := s.newLockedSegment(ctx, bs, MaxSegment())
		err := s.replaceSegment(ctx, bs, ls)
		if err != nil {
			return nil, err
		}
	}
	s.AlignIndex()
	if idx != nil {
		*idx = ids
	}
	return s, nil
}

// Elements returns the number of elements in all segments.
// Caller must hold at least read lock for segment.
func (s *Segments) Elements() int {
	s.SegmentsLock.RLock()
	var n int
	for i := range s.Segments {
		seg := &s.Segments[i]
		n += seg.N
	}
	s.SegmentsLock.RUnlock()
	return n
}

// AlignIndex will align segments to be used for indexes.
// This ensure that segment splits are on score boundaries
// so changes in tiebreaker does not affect segment assignment.
// This should only be used on segment creation and not on segments with content.
func (s *Segments) AlignIndex() {
	if !s.IsIndex {
		return
	}
	for i := range s.Segments {
		seg := s.Segments[i]
		if i > 0 {
			prev := s.Segments[i-1]
			prev.MinTie = 0
			seg.MaxTie = math.MaxUint32
			seg.Max = prev.Min - 1
			s.Segments[i] = seg
			s.Segments[i-1] = prev
		}
	}
}

// Append a segment to the end of segments.
// The locked segment is updated with new index.
func (s *Segments) Append(ls *lockedSegment) {
	s.SegmentsLock.Lock()
	s.Segments = append(s.Segments, *ls.seg)
	// Add to index if we have one.
	idx := len(s.Segments) - 1
	ls.index = idx
	if s.idx != nil {
		s.idx[ls.seg.ID] = idx
	}
	// Increment NextID if we need to.
	if ls.seg.ID >= s.NextID {
		s.NextID = ls.seg.ID + 1
	}
	s.SegmentsLock.Unlock()
}

// elementFullIdx returns full element by index.
// If the segment is unloaded it is loaded and the segments are updated.
// Caller must hold segments lock (to pin index).
// Returned segment.emu (if ok) is locked. If readonly an RLock is used.
func (s *Segments) elementFullIdx(ctx context.Context, bs blobstore.WithSet, i int, readOnly bool) (ls *lockedSegment, err error) {
	// 1. Acquire the lock required
	//  If we have write lock, no-one else has it.
	//  If we have read lock others may have it or be attempting to load it.
	// 2. Ask for segment elements in cache.
	// 3. Check if someone is attempting to load it.
	// 4A. Load it if none are loading it.
	// 4B. Wait for others loading it.
	// 5. Return segment elements with lock.

	const debugLocks = false

	ls = &lockedSegment{index: i, readOnly: readOnly}
	seg := &s.Segments[i]
	lock := &seg.loader.Mu
	if readOnly {
		if debugLocks {
			log.Logger(ctx).Info("Read lock requested", "id", seg.cacheID())
		}
		lock.RLock()
		ls.unlock = lock.RUnlock
		if debugLocks {
			log.Logger(ctx).Info("Read lock acquired", "id", seg.cacheID())
			ls.unlock = func() {
				if lock != &seg.loader.Mu {
					panic("lock changed")
				}
				lock.RUnlock()
				log.Logger(ctx).Info("Read lock released", "id", seg.cacheID())
				ls.elements = nil
			}
		}
	} else {
		if debugLocks {
			log.Logger(ctx).Info("Write lock requested", "id", seg.cacheID())
		}
		lock.Lock()
		ls.unlock = lock.Unlock
		if debugLocks {
			log.Logger(ctx).Info("Write lock acquired", "id", seg.cacheID())
			ls.unlock = func() {
				if lock != &seg.loader.Mu {
					panic("lock changed")
				}
				lock.Unlock()
				log.Logger(ctx).Info("Write lock released", "id", seg.cacheID())
				ls.elements = nil
			}
		}
	}
	ls.seg = seg

	if s.cache != nil {
		v, ok := s.cache.Get(seg.cacheID())
		if ok {
			ls.elements, ok = v.(Elements)
			if ok {
				atomic.AddUint64(&s.cacheHits, 1)
				return ls, nil
			}
		}
		atomic.AddUint64(&s.cacheMisses, 1)
	}

	ls.elements, err = seg.loadElements(ctx, bs, readOnly)
	if err != nil {
		ls.unlock()
		return nil, err
	}
	if s.cache != nil {
		s.cache.Add(seg.cacheID(), ls.elements)
	}
	return ls, nil
}

type lockedSegment struct {
	seg      *Segment
	elements Elements
	index    int
	readOnly bool
	unlock   func()
}

// Read lock must be held and locked segment must be write locked.
func (s *Segments) replaceSegment(ctx context.Context, bs blobstore.WithSet, ls *lockedSegment) error {
	if ls.readOnly {
		panic("replace with read only")
	}
	defer ls.unlock()
	if ls.seg.loader == nil {
		ls.seg.loader = &elementLoader{}
	}

	s.SegmentsLock.Lock()
	ls.seg.elementsUpdated(ls.elements)
	if ls.index >= len(s.Segments) || ls.index < 0 {
		return fmt.Errorf("internal error: locked segment index (%d) out of range (0:%d)", ls.index, len(s.Segments))
	}
	s.Segments[ls.index] = *ls.seg
	s.SegmentsLock.Unlock()

	err := ls.saveElements(ctx, bs, ls.elements)
	if err != nil {
		return err
	}

	if s.cache != nil {
		s.cache.Add(ls.seg.cacheID(), ls.elements)
	}

	ls.elements = nil
	return nil
}

// newLockedSegment creates a new locked segment with the content of the provided segment.
// The segment is added to s, but not saved.
func (s *Segments) newLockedSegment(ctx context.Context, bs blobstore.WithSet, seg *Segment) *lockedSegment {
	seg.ID = s.newID()
	seg.Parent = s.ID
	seg.Updated = time.Now().Unix()
	seg.loader = &elementLoader{}
	seg.loader.Mu.Lock()
	ls := &lockedSegment{
		seg:      seg,
		elements: Elements{},
		index:    -1,
		readOnly: false,
		unlock:   seg.loader.Mu.Unlock,
	}
	s.Append(ls)
	return ls
}

// deleteElements deletes specific elements in segments places at supplied index.
// Optionally returns index elements of deleted items.
// Read lock must be held for segments.
func (s *Segments) DeleteElementsIdx(ctx context.Context, bs blobstore.WithSet, ids IndexElements, withIdx bool) (IndexElements, error) {
	// Collect indexes.
	var idxC = make(chan IndexElements, len(s.Segments))
	var errC = make(chan error)

	// Split elements by segments IDs.
	split := s.splitElementsID(ids)
	var wg sync.WaitGroup
	wg.Add(len(split))

	for segIdx, ids := range split {
		if len(ids) == 0 {
			wg.Done()
			continue
		}
		// Go through each segment that has elements that should be deleted.
		go func(segIdx int, e ElementIDs) {
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, segIdx, false)
			if err != nil {
				errC <- err
				return
			}

			toDelete := e.Map()
			deleted := IndexElements{}
			if withIdx {
				deleted = IndexElements{Elements: make(Elements, 0, len(e))}
			}
			wrtTo := 0
			nDeleted := 0
			for _, elem := range ls.elements {
				if _, ok := toDelete[elem.ID]; !ok {
					ls.elements[wrtTo] = elem
					wrtTo++
					continue
				}
				// Delete it
				nDeleted++
				if withIdx {
					deleted.Elements = append(deleted.Elements, elem.AsIndex(ls.seg.ID).Element)
				}
			}
			if nDeleted != len(toDelete) {
				log.Info(ctx, "Did not find all elements", "left", len(toDelete)-nDeleted)
			}
			ls.elements = ls.elements[:wrtTo]
			err = s.replaceSegment(ctx, bs, ls)
			if err != nil {
				errC <- err
				return
			}
			if withIdx {
				deleted.Sort()
				idxC <- deleted
			}
		}(segIdx, ids)
	}
	err := waitErr(ctx, errC, &wg)
	if err != nil {
		return IndexElements{}, err
	}
	close(idxC)
	var indexes IndexElements
	for idx := range idxC {
		indexes.Merge(idx.Elements, false)
	}
	return indexes, nil
}

// deleteElements deletes specific elements in segments.
// Optionally returns index elements of deleted items.
// Provided elements must be sorted.
func (s *Segments) DeleteElements(ctx context.Context, bs blobstore.WithSet, e Elements, withIdx bool) (IndexElements, error) {
	in := e
	var wg sync.WaitGroup
	wg.Add(len(s.Segments))

	// Collect indexes.
	var idxC = make(chan IndexElements, len(s.Segments))
	var errC = make(chan error)
	s.SegmentsLock.RLock()
	for i := range s.Segments {
		seg := s.Segments[i]
		start, end := seg.FilterIdx(in)
		if start == end {
			wg.Done()
			continue
		}
		// Go through each segment that has elements that should be deleted.
		go func(i int, e Elements) {
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, i, false)
			if err != nil {
				errC <- err
				return
			}
			seg := ls.seg

			toDelete := make(map[ElementID]struct{})
			deleted := IndexElements{Elements: make(Elements, 0)}
			if withIdx {
				deleted = IndexElements{Elements: make(Elements, 0, len(e))}
			}
			for _, elem := range e {
				toDelete[elem.ID] = struct{}{}
			}
			wrtto := 0
			ndeleted := 0
			for _, elem := range ls.elements {
				if _, ok := toDelete[elem.ID]; !ok {
					ls.elements[wrtto] = elem
					wrtto++
					continue
				}
				// Delete it
				ndeleted++
				if withIdx {
					deleted.Elements = append(deleted.Elements, elem.AsIndex(seg.ID).Element)
				}
			}
			if ndeleted != len(toDelete) {
				log.Info(ctx, "Did not find all elements", "left", len(toDelete)-ndeleted)
			}
			ls.elements = ls.elements[:wrtto]
			s.replaceSegment(ctx, bs, ls)
			if withIdx {
				deleted.Sort()
				idxC <- deleted
			}
		}(i, in[start:end])
		in = in[end:]
	}
	s.SegmentsLock.RUnlock()
	err := waitErr(ctx, errC, &wg)
	if err != nil {
		return IndexElements{}, err
	}
	close(idxC)
	var indexes IndexElements
	for idx := range idxC {
		indexes.Merge(idx.Elements, false)
	}
	return indexes, nil
}

// Insert sorted elements into segments.
// Returns indexes of elements if withIdx is true.
// Read lock must be held
func (s *Segments) Insert(ctx context.Context, bs blobstore.WithSet, in Elements, withIdx bool) (IndexElements, error) {
	in.Sort()

	if sanityChecks {
		err := s.VerifyElements(ctx, bs, nil)
		if err != nil {
			log.Error(ctx, "Pre- Validation failed", "err", err)
			return IndexElements{}, err
		}
	}

	// Collect indexes.
	s.SegmentsLock.RLock()
	var wg sync.WaitGroup
	wg.Add(len(s.Segments))
	var idxC = make(chan IndexElements, len(s.Segments))
	var errC = make(chan error)
	for i := range s.Segments {
		seg := &s.Segments[i]
		start, end := seg.FilterIdx(in)
		if start == end {
			wg.Done()
			continue
		}
		if start != 0 {
			log.Error(ctx, "Expected start to be 0", "was", start)
		}
		go func(i int, e Elements) {
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, i, false)
			if err != nil {
				errC <- err
				return
			}
			seg := ls.seg
			if sanityChecks {
				if !sort.SliceIsSorted(e, e.Sorter()) {
					err := fmt.Errorf("%d: incoming was not sorted before merge", seg.ID)
					log.Error(ctx, err.Error())
					errC <- err
					ls.unlock()
					return
				}
				if err := ls.elements.HasDuplicates(); err != nil {
					log.Error(ctx, "Duplicates BEFORE merge", "error", err.Error())
					errC <- err
				}
				if ol := ls.elements.IDs().Overlap(e.IDs()); len(ol) != 0 {
					log.Error(ctx, "Insert overlaps existing", "ids", ol)
				}
			}
			ls.elements.Merge(e, false)
			if sanityChecks {
				if !sort.SliceIsSorted(ls.elements, ls.elements.Sorter()) {
					err := fmt.Errorf("%d: outgoing was not sorted after merge", seg.ID)
					log.Error(ctx, err.Error(), "in_len", len(e), "seg_len", len(ls.elements))
					errC <- err
					ls.unlock()
					return
				}
				if err := ls.elements.HasDuplicates(); err != nil {
					log.Error(ctx, "Duplicates AFTER merge", "error", err.Error())
					errC <- err
					ls.unlock()
					return
				}
			}

			err = s.replaceSegment(ctx, bs, ls)
			if err != nil {
				errC <- err
			}
			if withIdx {
				idxC <- e.ElementIDs(seg.ID)
			}
		}(i, in[start:end])
		in = in[end:]
	}
	s.SegmentsLock.RUnlock()
	err := waitErr(ctx, errC, &wg)
	if err != nil {
		return IndexElements{}, err
	}
	if sanityChecks {
		err = s.VerifyElements(ctx, bs, nil)
		if err != nil {
			log.Error(ctx, "Post Validation failed", "err", err)
			return IndexElements{}, err
		}
	}
	close(idxC)
	var idxs IndexElements
	if withIdx {
		idxs = IndexElements{Elements: make(Elements, 0, len(in))}
		for idx := range idxC {
			idxs.Elements = append(idxs.Elements, idx.Elements...)
		}
		idxs.Sort()
	}
	return idxs, nil
}

// newID returns a new segment ID.
// Caller must hold lock to segments.
func (s *Segments) newID() SegmentID {
	id := s.NextID
	s.NextID++
	return id
}

// reindex will recreate the Segment ID index if needed.
// write lock must be held by caller.
func (s *Segments) reindex() {
	s.SegmentsLock.RLock()
	if len(s.idx) == len(s.Segments) {
		s.SegmentsLock.RUnlock()
		return
	}
	s.SegmentsLock.RUnlock()
	s.SegmentsLock.Lock()
	s.idx = make(map[SegmentID]int, len(s.Segments))
	for i, v := range s.Segments {
		s.idx[v.ID] = i
	}
	s.SegmentsLock.Unlock()
}

// segIdxTop returns the segment index and the offset of the element in the segment.
// Returns -1 if offset is after last element.
// caller must hold read lock.
func (s *Segments) segIdxTop(offset int) (segIdx, elementOffset int) {
	n := 0
	s.SegmentsLock.RLock()
	defer s.SegmentsLock.RUnlock()
	for i := range s.Segments {
		seg := &s.Segments[i]
		if n+seg.N > offset {
			return i, offset - n
		}
		n += seg.N
	}
	return -1, -1
}

// segIdxBottom returns the segment index and the offset of the element in the segment.
// Returns -1 if offset is before first element.
// caller must hold read lock.
func (s *Segments) segIdxBottom(offset int) (segIdx, elementOffset int) {
	n := 0
	s.SegmentsLock.RLock()
	defer s.SegmentsLock.RUnlock()
	for i := len(s.Segments) - 1; i >= 0; i-- {
		seg := &s.Segments[i]
		if n+seg.N > offset {
			return i, seg.N - (offset - n) - 1
		}
		n += seg.N
	}
	return -1, -1
}

// segIdxBottom returns the segment index containing first elements with provided score.
// Returns -1 if offset is before first element.
// caller must hold read lock.
func (s *Segments) segIdxScore(score uint64) (segIdx, above, total int) {
	segIdx = -1
	s.SegmentsLock.RLock()
	defer s.SegmentsLock.RUnlock()
	for i := range s.Segments {
		seg := &s.Segments[i]
		if segIdx < 0 && score >= seg.Min && score <= seg.Max {
			segIdx = i
			above = total
		}
		total += seg.N
	}
	return segIdx, above, total
}

// topOffsets returns the top offset of each segment and total number of elements.
// caller must hold read lock.
func (s *Segments) topOffsets() (elements []int, total int) {
	res := make([]int, len(s.Segments))
	n := 0
	s.SegmentsLock.RLock()
	defer s.SegmentsLock.RUnlock()
	for i := range s.Segments {
		seg := &s.Segments[i]
		res[i] = n
		n += seg.N
	}
	return res, n
}

// Verify segments without validating elements.
func (s *Segments) Verify(ctx context.Context, bs blobstore.WithSet) error {
	if len(s.Segments) == 0 {
		return fmt.Errorf("No segments found.")
	}
	foundSegs := make(map[SegmentID]int, len(s.Segments))
	for i := range s.Segments {
		err := func(i int) error {
			s.SegmentsLock.RLock()
			v := &s.Segments[i]
			s.SegmentsLock.RUnlock()

			prefix := fmt.Sprintf("Segment %v:", v.ID)
			if v.Updated == 0 {
				return fmt.Errorf(prefix + "Updated time not set")
			}
			if idx, ok := foundSegs[v.ID]; ok {
				return fmt.Errorf(prefix+"Duplicate segment ID, at index %d and %d", idx, i)
			}
			foundSegs[v.ID] = i

			// Check relation to previous.
			if i == 0 {
				// First segment should start at max.
				if v.Max != math.MaxUint64 || v.MaxTie != math.MaxUint32 {
					return fmt.Errorf(prefix+"First segment did not start at maximum, was %v, %v", v.Max, v.MaxTie)
				}
			} else {
				s.SegmentsLock.RLock()
				prev := &s.Segments[i-1]
				s.SegmentsLock.RUnlock()
				// Maximum of this should be just after minimum of previous
				wantScore, wantTie := Element{Score: prev.Min, TieBreaker: prev.MinTie}.PrevMax()
				if v.Max != wantScore || v.MaxTie != wantTie {
					return fmt.Errorf(prefix+"Does not line up with previous segment. Want score, tie (%d, %d), got (%d, %d)",
						wantScore, wantTie, v.Max, v.MaxTie)
				}
				if s.IsIndex {
					// MaxTie should be 0 on indexes
					if v.MinTie != 0 {
						return fmt.Errorf(prefix + "Index segment did not end with mintie 0.")
					}
					if v.MaxTie != math.MaxUint32 {
						return fmt.Errorf(prefix + "Index segment did not start with maxtie MAX.")
					}
				}
			}

			if i == len(s.Segments)-1 {
				// Last element should go down to 0
				if v.Min != 0 || v.MinTie != 0 {
					return fmt.Errorf(prefix+"Last segment did not end at 0, was %v, %v", v.Min, v.MinTie)
				}
			}
			return nil
		}(i)
		if err != nil {
			return err
		}
	}
	return nil
}

// VerifyElements verifies elements in segments.
// Supply a non-nil "IDs". It will be populated with the Element IDs found. The supplied map is overwritten.
// Segments must be read locked by callers.
func (s *Segments) VerifyElements(ctx context.Context, bs blobstore.WithSet, IDs *map[ElementID]SegmentID) error {
	var errC = make(chan error)
	var elems = make(map[ElementID]SegmentID, s.Elements())
	if IDs != nil {
		*IDs = elems
	}
	var elemsMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(s.Segments))
	var b = NewBucket(8)
	for i := range s.Segments {
		go func(i int) {
			defer wg.Done()
			defer b.Get().Release()
			ls, err := s.elementFullIdx(ctx, bs, i, true)
			if err != nil {
				errC <- err
				return
			}
			defer ls.unlock()
			seg := ls.seg

			prefix := fmt.Sprintf("Segment ID %v (#%d) Elements:", seg.ID, i)
			if !sort.SliceIsSorted(ls.elements, ls.elements.Sorter()) {
				errC <- fmt.Errorf(prefix + "Not sorted")
			}
			if seg.N != len(ls.elements) {
				errC <- fmt.Errorf(prefix+"Element counter wrong, is %d, should be %d", seg.N, len(ls.elements))
			}
			start, end := seg.FilterIdx(ls.elements)
			if start != 0 {
				log.Info(ctx, "Extra elements before start", "elements", len(ls.elements[start:]), "segment", seg)
				errC <- fmt.Errorf(prefix+"%d Element(s) does not belong at START of segment", start)
			}
			if end != len(ls.elements) {
				log.Info(ctx, "Extra elements after END", "elements", len(ls.elements[end:]), "segment", seg)

				err := fmt.Errorf(prefix+"%d Element(s) does not belong at END of segment", len(ls.elements)-end)
				errC <- err
			}
			if seg.N != len(ls.elements) {
				errC <- fmt.Errorf("Segment length mismatch. N: %v, actual len: %v", seg.N, len(ls.elements))
			}

			// Big lock
			elemsMu.Lock()
			defer elemsMu.Unlock()
			for _, elem := range ls.elements {
				if _, ok := elems[elem.ID]; ok {
					errC <- fmt.Errorf(prefix+"Duplicate element ID: %v", elem.ID)
					return
				}
				if s.IsIndex {
					elems[elem.ID] = SegmentID(elem.TieBreaker)
				} else {
					elems[elem.ID] = seg.ID
				}
				if elem.Updated == 0 {
					errC <- fmt.Errorf(prefix+"Element %d does not have time set", 0)
					return
				}
			}
		}(i)
	}
	err := waitErr(ctx, errC, &wg)
	if err != nil {
		return err
	}
	return nil
}

// ElementIndexAll will return index for all elements in the list.
func (s *Segments) ElementIndexAll(ctx context.Context, bs blobstore.WithSet) (IndexElements, error) {
	// Collect indexes.
	res := IndexElements{Elements: make(Elements, 0, s.Elements())}
	var idxC = make(chan IndexElements, len(s.Segments))
	var errC = make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(s.Segments))
	var b = NewBucket(8)
	for i := range s.Segments {
		go func(i int) {
			defer wg.Done()
			defer b.Get().Release()
			ls, err := s.elementFullIdx(ctx, bs, i, true)
			if err != nil {
				errC <- err
				return
			}
			idxC <- ls.elements.ElementIDs(ls.seg.ID)
			ls.unlock()
		}(i)
	}
	err := waitErr(ctx, errC, &wg)
	if err != nil {
		return IndexElements{}, err
	}
	close(idxC)
	for idx := range idxC {
		res.Elements = append(res.Elements, idx.Elements...)
	}
	res.Sort()
	return res, nil
}

// Save segments to disk.
func (s *Segments) Save(ctx context.Context, store blobstore.WithSet) error {
	wm := NewWriterMsg()
	defer wm.Close()
	err := wm.SetVersion(segmentsVersion)
	if err != nil {
		return err
	}
	s.SegmentsLock.RLock()
	err = s.EncodeMsg(wm.Writer())
	s.SegmentsLock.RUnlock()
	if err != nil {
		return err
	}
	buf := wm.Buffer().Bytes()
	return store.Set(ctx, string(s.ID), buf)
}

// Load will load the segments (but no elements) of the specified ID.
func (s SegmentsID) Load(ctx context.Context, store blobstore.WithSet, cache Cache) (*Segments, error) {
	b, err := store.Get(ctx, string(s))
	if err != nil {
		return nil, err
	}
	var v uint8
	b, v = MsgpGetVersion(b)
	segs := Segments{}
	switch v {
	case 1:
		return nil, errors.New("deprecated v1 found")
	case segmentsVersion:
		b, err = segs.UnmarshalMsg(b)
	default:
		return nil, ErrVersionMismatch
	}
	if err != nil {
		log.Info(ctx, "Got err:", "error", err, "length", len(b))
		return nil, err
	}
	segs.cache = cache
	for i := range segs.Segments {
		segs.Segments[i].loader = &elementLoader{}
	}
	return &segs, nil
}

// Delete will delete the segments representation and elements.
func (s *Segments) Delete(ctx context.Context, store blobstore.WithSet) error {
	var wg sync.WaitGroup
	wg.Add(len(s.Segments))
	for i := range s.Segments {
		elem := &s.Segments[i]
		if s.cache != nil {
			s.cache.Remove(elem.cacheID())
		}
		go func(s *Segment) {
			defer wg.Done()
			err := s.deleteElements(ctx, store)
			if err != nil {
				log.Error(ctx, "Unable to delete segment", "id", s.ID, "error", err)
			}
		}(elem)
	}
	wg.Wait()
	s.Segments = nil
	return store.Delete(ctx, string(s.ID))
}

// topRankOf returns the rank of the top element in segment.
func (s *Segments) topRankOf(segIdx int) (n int) {
	for i := range s.Segments {
		if i == segIdx {
			return n
		}
		n += s.Segments[i].N
	}
	panic("segment index out of bounds")
}

// FindElements returns elements with the supplied indices.
// Not found errors are logged, but not fatal.
// Caller must hold s.scores read lock.
func (s *Segments) FindElements(ctx context.Context, bs blobstore.WithSet, ids IndexElements, radius int) ([]RankedElement, error) {
	if len(ids.Elements) == 0 {
		return nil, nil
	}

	// Split elements by segments IDs.
	split := s.splitElementsID(ids)

	// For each segment look up the collected elements.
	var wg sync.WaitGroup
	var errch = make(chan error, 0)
	var resch = make(chan []RankedElement, len(split))
	wg.Add(len(split))
	offsets, total := s.topOffsets()
	for segIdx, id := range split {
		go func(segIdx int, ids []ElementID) {
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, segIdx, true)
			if err != nil {
				errch <- err
				return
			}
			defer ls.unlock()
			var res []RankedElement
			for _, id := range ids {
				re, err := s.findRanked(ctx, bs, ls, id, offsets[segIdx], radius)
				if err != nil {
					// We ignore not found elements.
					continue
				}
				re.CalculateFromBottom(total)
				res = append(res, *re)
			}
			resch <- res
		}(segIdx, id)
	}
	err := waitErr(ctx, errch, &wg)
	if err != nil {
		return nil, err
	}
	close(resch)
	var res []RankedElement
	for e := range resch {
		res = append(res, e...)
	}
	return res, nil
}

// getElementIndex returns element the element at supplied index in supplied segment.
// Must hold Read Lock
func (s *Segments) getElementIndex(ctx context.Context, bs blobstore.WithSet, segIdx, elementOff, offset, radius int) (*RankedElement, error) {
	ls, err := s.elementFullIdx(ctx, bs, segIdx, true)
	if err != nil {
		return nil, err
	}
	defer ls.unlock()

	if elementOff >= len(ls.elements) {
		elementOff = len(ls.elements) - 1
	}
	re, err := s.findRanked(ctx, bs, ls, ls.elements[elementOff].ID, offset, radius)
	if err != nil {
		return nil, err
	}
	return re, nil
}

// findRanked returns ranked element.
// caller must hold segment read lock and element read lock for supplied segment.
func (s *Segments) findRanked(ctx context.Context, bs blobstore.WithSet, ls *lockedSegment, id ElementID, offset, radius int) (*RankedElement, error) {
	eidx, err := ls.elements.FindIdx(id)
	if err != nil {
		//log.Info(ctx, "Unable to find element", "error", err, "segment_id", seg.ID, "element_id", id, "looking_in", seg)
		log.Info(ctx, "Unable to find element", "error", err, "segment_id", ls.seg.ID, "element_id", id)
		return nil, ErrNotFound
	}
	r := RankedElement{
		Element: ls.elements[eidx],
		FromTop: offset + eidx,
		Above:   make([]Element, 0, radius),
		Below:   make([]Element, 0, radius),
	}

	if radius <= 0 {
		return &r, nil
	}
	segIdx := ls.index

	// Below
	func() {
		bidx, bLS, bsegIdx := eidx, ls, segIdx
		for i := 1; i < radius+1; i++ {
			bidx++
			for bidx == len(bLS.elements) {
				// Load next
				bsegIdx++
				if bsegIdx == len(s.Segments) {
					return
				}
				ls, err := s.elementFullIdx(ctx, bs, bsegIdx, true)
				if err != nil {
					log.Error(ctx, "Unable to load segment", "error", err, "segment_idx", bsegIdx)
					return
				}
				defer ls.unlock()
				bLS = ls
				bidx = 0
			}
			r.Below = append(r.Below, bLS.elements[bidx])
		}
	}()

	// Above
	func() {
		aidx, aLS, asegIdx := eidx, ls, segIdx
		for i := 1; i < radius+1; i++ {
			aidx--
			for aidx < 0 {
				// Load previous
				asegIdx--
				if asegIdx == -1 {
					return
				}
				ls, err := s.elementFullIdx(ctx, bs, asegIdx, true)
				if err != nil {
					log.Error(ctx, "Unable to load segment", "error", err, "segment_idx", asegIdx)
					return
				}
				aLS = ls
				aidx = len(aLS.elements) - 1
				defer ls.unlock()
			}
			r.Above = append(r.Above, aLS.elements[aidx])
		}
	}()

	return &r, nil
}

// getElementsOffset returns elements from a specific segment+element offset.
// If at end of list fewer results may be returned.
// Returned offsets start at beginning of segment.
// Caller must hold read lock to Segments.
func (s *Segments) getElementsFromScore(ctx context.Context, bs blobstore.WithSet, score uint64, segIdx, elements int) (RankedElements, error) {
	ls, err := s.elementFullIdx(ctx, bs, segIdx, true)
	if err != nil {
		return nil, err
	}

	defer ls.unlock()
	seg := ls.seg
	for i, elem := range ls.elements {
		if elem.Score <= score {
			e, err := s.getElementsOffset(ctx, bs, segIdx, i, elements)
			if err != nil {
				return nil, err
			}
			return e.Ranked(i, seg.N), nil
		}
	}

	// Return first of next segment.
	e, err := s.getElementsOffset(ctx, bs, segIdx+1, 0, elements)
	if err != nil {
		return nil, err
	}
	return e.Ranked(seg.N, seg.N), nil
}

// getElementsOffset returns elements from a specific segment+element offset.
// If at end of list fewer results may be returned.
// Caller must hold read lock to Segments.
func (s *Segments) getElementsOffset(ctx context.Context, bs blobstore.WithSet, segIdx, firstElem, elements int) (Elements, error) {
	var res = make(Elements, 0, elements)
	for elements > 0 {
		if segIdx >= len(s.Segments) {
			break
		}
		err := func() error {
			ls, err := s.elementFullIdx(ctx, bs, segIdx, true)
			if err != nil {
				return err
			}
			defer ls.unlock()
			if firstElem >= len(ls.elements) {
				firstElem -= len(ls.elements)
				segIdx++
				return nil
			}
			toAdd := ls.elements[firstElem:]
			if len(toAdd) > elements {
				res = append(res, toAdd[:elements]...)
				elements = 0
				return nil
			}
			res = append(res, toAdd...)
			elements -= len(toAdd)
			segIdx++
			firstElem = 0
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// updateIndexElements will update the elements as index.
// Caller must at least hold s.RLock.
func (s *Segments) updateIndexElements(ctx context.Context, bs blobstore.WithSet, e []indexSegmentUpdate) error {
	if len(e) == 0 {
		return nil
	}
	// Split elements by segments.
	split := s.splitIndexUpdates(ctx, e)
	// For each segment look up the collected elements.
	var wg sync.WaitGroup
	var errch = make(chan error, 0)
	now := uint32(time.Now().Unix())
	wg.Add(len(split))
	for segIdx, elems := range split {
		if len(elems) == 0 {
			wg.Done()
			continue
		}
		go func(segIdx int, in []indexSegmentUpdate) {
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, segIdx, false)
			if err != nil {
				log.Error(ctx, "Unable to load index segment", "error", err, "segment_idx", segIdx)
				errch <- err
				return
			}
			seg := ls.seg
			firstErr := true
			for _, upd := range in {
				idx, err := ls.elements.FindScoreIdx(uint64(upd.ID))
				if err != nil {
					if firstErr {
						log.Error(ctx, "Unable to find index element", "error", err, "segment_id", seg.ID, "element_id", upd.ID, "new_seg_id", upd.NewSegment)
						firstErr = false
					}
					continue
				}
				e := ls.elements[idx]
				e.TieBreaker = uint32(upd.NewSegment)
				e.Updated = now
				ls.elements[idx] = e
			}
			err = s.replaceSegment(ctx, bs, ls)
			if err != nil {
				errch <- err
				return
			}
		}(segIdx, elems)
	}
	err := waitErr(ctx, errch, &wg)
	if err != nil {
		return err
	}
	return nil
}

// getFirstWithScore returns the first elements with a given score or below.
// Concurrency safe
// Results are unsorted.
func (s *Segments) getFirstWithScore(ctx context.Context, bs blobstore.WithSet, scores []uint64) (Elements, error) {
	if len(scores) == 0 {
		return Elements{}, nil
	}

	// Split scores into segments.
	// The find the elements in each segment.
	split := s.splitScores(ctx, scores)
	var wg sync.WaitGroup
	var errch = make(chan error, 0)
	var resch = make(chan Elements, len(split))
	wg.Add(len(split))
	var b = NewBucket(8)
	for _, sm := range split {
		go func(sm segmentMatch) {
			defer b.Get().Release()
			defer wg.Done()
			ls, err := s.elementFullIdx(ctx, bs, sm.segIdx, true)
			if err != nil {
				errch <- err
				return
			}
			res := ls.elements.FirstElementsWithScore(sm.scores)
			ls.unlock()
			resch <- res
		}(sm)
	}
	// Collect and return them.
	err := waitErr(ctx, errch, &wg)
	if err != nil {
		return nil, err
	}
	close(resch)
	var res Elements
	for e := range resch {
		res = append(res, e...)
	}
	return res, nil
}

// segmentMatch contains a segment (list) index position,
// and the scores that are contained within the segment.
type segmentMatch struct {
	segIdx int
	scores []uint64
}

// splitScores will split a slice of scores by segment index.
// Lock must be held. Read is ok.
func (s *Segments) splitScores(ctx context.Context, scores []uint64) []segmentMatch {
	res := make([]segmentMatch, 0)
	if len(scores) == 0 {
		return res
	}
	// Sort incoming slice by score descending
	sort.Slice(scores, func(i, j int) bool {
		return scores[i] > scores[j]
	})
	// Loop through segments and find the ones that match.
	for i := range s.Segments {
		s.SegmentsLock.RLock()
		seg := &s.Segments[i]
		s.SegmentsLock.RUnlock()

		if len(scores) == 0 {
			// On success we should always end up here.
			return res
		}
		if seg.Min <= scores[0] {
			// Add as many as we can.
			ended := false
			for j, v := range scores {
				// First one should not hit
				if seg.Min > v {
					lastIdx := j
					res = append(res, segmentMatch{segIdx: i, scores: scores[:lastIdx]})
					scores = scores[lastIdx:]
					ended = true
					break
				}
			}
			if !ended {
				res = append(res, segmentMatch{segIdx: i, scores: scores})
				scores = scores[0:0]
			}
		}
	}
	if len(scores) > 0 {
		// This should only occur if the list is invalid and doesn't contain
		// segments for all values.
		log.Logger(ctx).Error("Did not reach end", "scores", scores, "segments", s.String())
	}
	return res
}

// splitElementsIdx returns the elements split by segment indexes.
// The elements are re-sliced from supplied elements.
func (s *Segments) splitElementsIdx(elems Elements) map[int]Elements {
	newIdx := make(map[int]Elements)
	for i := range s.Segments {
		seg := &s.Segments[i]
		start, end := seg.FilterIdx(elems)
		if start == end {
			continue
		}
		// Got a segment match.
		newIdx[i] = elems[start:end]
	}
	return newIdx
}

// splitElementsID returns elements by split by segments.
// Read Lock must be held. Preferable s.idx should be populated.
func (s *Segments) splitElementsID(ids IndexElements) map[int][]ElementID {
	s.reindex()
	s.SegmentsLock.RLock()
	defer s.SegmentsLock.RUnlock()
	res := make(map[int][]ElementID)
	for _, elem := range ids.Elements {
		// segment index
		i := s.idx[SegmentID(elem.TieBreaker)]
		res[i] = append(res[i], elem.ID)
	}
	return res
}

// indexSegmentUpdate provides information about a segment update when splitting a segment.
type indexSegmentUpdate struct {
	ID         ElementID
	NewSegment SegmentID
}

// splitElementsID returns elements by split by segments.
// Lock must be held. Read is ok.
func (s *Segments) splitIndexUpdates(ctx context.Context, ids []indexSegmentUpdate) map[int][]indexSegmentUpdate {
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].ID > ids[j].ID
	})
	scores := make([]uint64, len(ids))
	for i := range ids {
		scores[i] = uint64(ids[i].ID)
	}
	match := s.splitScores(ctx, scores)
	res := make(map[int][]indexSegmentUpdate)
	n := 0
	if sanityChecks {
		for _, elem := range match {
			found := ids[n : n+len(elem.scores)]
			for i := range found {
				if uint64(found[i].ID) != elem.scores[i] {
					log.Logger(ctx).Error("score mismatch", "found", found[i].ID, "scores", elem.scores[i])
				}
			}
			n += len(elem.scores)
		}
		n = 0
	}
	for _, elem := range match {
		idx := elem.segIdx
		res[idx] = ids[n : n+len(elem.scores)]
		n += len(elem.scores)
	}
	return res
}

// splitIdx will split a segment with the specified index in two equally sized parts.
// The changed indexes are returned as well as new segment.
// The caller must save the new segment and updated indexes if needed.
// Caller must hold Lock and save changes.
func (s *Segments) splitIdx(ctx context.Context, s1 *lockedSegment) (changed []indexSegmentUpdate, newSegment *lockedSegment) {
	s1.seg.N = len(s1.elements)
	orgLen := s1.seg.N
	nn := s1.elements.SplitSize((s1.seg.N + 1) / 2)
	if len(nn) == 1 {
		// Cannot split segment
		log.Info(ctx, "Could not split segment - likely all scores are tied.", s1.seg.ID, len(s1.elements))
		return nil, nil
	}
	if len(nn) != 2 {
		log.Error(ctx, "internal error: Unexpected segment count", s1.seg.N, s1, len(nn), len(s1.elements))
		return nil, nil
	}
	// Overwrite first part of s1.
	s1.elements = nn[0].Clone(false)
	s1.seg.N = len(s1.elements)

	s2 := NewSegmentElements(s, nn[1].Clone(false))
	// Make sure new segment covers entire range of segment
	s2.seg.Min, s2.seg.MinTie = s1.seg.Min, s1.seg.MinTie
	e := s2.elements[0]
	if s.IsIndex {
		e.TieBreaker = math.MaxUint32
		s2.seg.MaxTie = math.MaxUint32
	}
	s1.seg.Min, s1.seg.MinTie = e.AboveThis()

	s.SegmentsLock.Lock()
	defer s.SegmentsLock.Unlock()
	s.Segments = append(s.Segments, Segment{})

	// Insert after current.
	i := s1.index + 1
	copy(s.Segments[i+1:], s.Segments[i:])
	s.Segments[i] = *s2.seg
	s.idx = nil
	s2.index = i

	changed = make([]indexSegmentUpdate, len(nn[1]))
	for i, e := range nn[1] {
		changed[i] = indexSegmentUpdate{
			ID:         e.ID,
			NewSegment: s2.seg.ID,
		}
	}

	if true {
		log.Info(ctx, "Split elements", "elements", orgLen, "split1_id", s1.seg.ID, "split2_id", s2.seg.ID, "len(s1)", s1.seg.N, "len(s2)", s2.seg.N)
		//fmt.Printf("Split %v into \n%v and \n%v", org, s1, s2)
	}
	return changed, s2
}

// mergeWithNext will merge two segments into one at offset of curr.
// The changed indexes are returned as well as new segment.
// The caller must save the current segment and updated indexes.
// The next segment is released, but elements must be deleted by caller.
func (s *Segments) mergeWithNext(ctx context.Context, cur, next *lockedSegment) (changed []indexSegmentUpdate) {
	s.SegmentsLock.Lock()
	defer s.SegmentsLock.Unlock()
	log.Logger(ctx).Info("want to merge", "cur_id", cur.seg.ID, "cur_idx", cur.index, "next_id", next.seg.ID, "next_idx", next.index)

	s1 := cur.seg
	org := cur.elements

	// Clone s2, since we will be overwriting it in the list.
	s2 := *next.seg
	next.seg = &s2

	changed = make([]indexSegmentUpdate, len(next.elements))
	for j, e := range next.elements {
		changed[j] = indexSegmentUpdate{
			ID:         e.ID,
			NewSegment: s1.ID,
		}
	}

	cur.elements = make(Elements, len(org)+len(next.elements))
	copy(cur.elements, org)
	copy(cur.elements[len(org):], next.elements)

	s1.Min = s2.Min
	s1.MinTie = s2.MinTie

	i := next.index
	s.Segments = s.Segments[:i+copy(s.Segments[i:], s.Segments[i+1:])]
	s1.elementsUpdated(cur.elements)

	log.Info(ctx, "Merging elements", "seg_id", s.ID, "cur_idx", cur.index, "next_idx", next.index, "src1_id", s1.ID, "src2_id", s2.ID, "s1_elems", len(org), "s2_elems", s2.N, "new_elems", s1.N)

	s.idx = nil
	return changed
}

// String returns a human readable representation of the segments.
func (s *Segments) String() string {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}
