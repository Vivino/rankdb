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
	"math/big"
	"math/rand"
	"os"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
)

// List contains the segments representing a list
// as well as the index to look up segments by object ID.
//
//msgp:tuple List
type List struct {
	sync.RWMutex `msg:"-" json:"-"`
	ID           ListID
	Set          string
	Metadata     map[string]string
	SplitSize    int
	MergeSize    int
	LoadIndex    bool
	Scores       SegmentsID
	// segmentsLock is used to obtain access to scores/index segments.
	// This lock should not be grabbed manually, but through loadSegments.
	segmentsLock sync.RWMutex
	scores       *Segments
	Index        SegmentsID
	index        *Segments

	// The update lock can be obtained when you need a consistent view of data in the list.
	// This lock should not be grabbed manually, but through loadSegments.
	updateLock sync.Mutex
	manager    *Manager
	// Lock held while loading segments.
	loadingLock sync.Mutex

	cache Cache
}

// ListOption can be used to specify options when creating a list.
// Use either directly or use WithListOption.
//
//msgp:ignore ListOption
type ListOption func(*listOptions) error

type listOptions struct {
	loadIndex            bool
	mergeSize, splitSize int
	metadata             map[string]string
	populate             []Element
	clone                *List
	cache                Cache
}

// defaultListOptions returns the default list options.
func defaultListOptions() listOptions {
	return listOptions{
		loadIndex: true,
		mergeSize: 500, splitSize: 2000,
		cache: nil,
	}
}

// WithListOption provides an element to create list parameters.
var WithListOption = ListOption(nil)

// LoadIndex will signify that lists indexes should be loaded on startup.
// If not, they are loaded on demand, and reclaimed by the server based on global policy.
// By default indexes are loaded.
func (l ListOption) LoadIndex(b bool) ListOption {
	return func(o *listOptions) error {
		o.loadIndex = b
		return nil
	}
}

// Provide custom split/merge sizes for list.
// Merge must be < split.
func (l ListOption) MergeSplitSize(merge, split int) ListOption {
	return func(o *listOptions) error {
		o.mergeSize = merge
		o.splitSize = split
		if o.mergeSize >= o.splitSize {
			return fmt.Errorf("MergeSplitSize: Merge size (%d) cannot be >= Split size (%d)", o.mergeSize, o.splitSize)
		}
		return nil
	}
}

// Populate will populate the list with supplied elements.
func (l ListOption) Populate(e []Element) ListOption {
	return func(o *listOptions) error {
		o.populate = e
		if o.clone != nil {
			return errors.New("cannot both clone list and populate")
		}
		return nil
	}
}

// Clone will populate the list with the content of another list.
func (l ListOption) Clone(lst *List) ListOption {
	return func(o *listOptions) error {
		o.clone = lst
		if o.populate != nil {
			return errors.New("cannot both clone list and populate")
		}
		return nil
	}
}

// Metadata will set metadata.
func (l ListOption) Metadata(m map[string]string) ListOption {
	return func(o *listOptions) error {
		o.metadata = m
		return nil
	}
}

// Cache will set cache of the list.
func (l ListOption) Cache(cache Cache) ListOption {
	return func(o *listOptions) error {
		o.cache = cache
		return nil
	}
}

// NewList creates a new list.
// A list ID and storage set must be provided.
// Use WithListOption to access additional options.
func NewList(ctx context.Context, id ListID, set string, bs blobstore.Store, opts ...ListOption) (*List, error) {
	ctx = log.WithFn(ctx, "list_id", id)
	options := defaultListOptions()
	for _, opt := range opts {
		err := opt(&options)
		if err != nil {
			return nil, err
		}
	}
	if set == "" {
		return nil, fmt.Errorf("NewList: No list set provided")
	}
	if bs == nil {
		return nil, fmt.Errorf("no storage provided")
	}
	l := List{
		ID:        id,
		Set:       set,
		SplitSize: options.splitSize,
		MergeSize: options.mergeSize,
		LoadIndex: options.loadIndex,
		Metadata:  options.metadata,
		cache:     options.cache,
	}
	if len(options.populate) > 0 {
		e := Elements(options.populate)
		e.UpdateTime(time.Now())
		err := l.Populate(ctx, bs, e)
		if err != nil {
			return nil, err
		}
	}
	if options.clone != nil {
		err := l.cloneElements(ctx, bs, options.clone)
		if err != nil {
			return nil, err
		}
	}
	if l.Scores.Unset() {
		segs, err := l.initEmptyList(ctx, bs, nil)
		if segs != nil {
			segs.unlock()
		}
		if err != nil {
			return nil, err
		}
	}
	l.scores.cache = l.cache
	l.index.cache = l.cache

	return &l, nil
}

// newList creates a new, empty list.
func newList(c Cache, m *Manager) *List {
	return &List{cache: c, manager: m}
}

// initEmptyList will create an empty list.
// The segments are saved, but the list itself is not.
// It is possible to transfer update locks from a previous segment,
// if this is replacing existing segments and you hold the update lock.
// Segments will be returned locked.
func (l *List) initEmptyList(ctx context.Context, bs blobstore.Store, xfer *segments) (*segments, error) {
	// Create empty element list.
	store := blobstore.StoreWithSet(bs, l.Set)
	ctx = log.WithFn(ctx)

	segs := l.newSegments(ctx, 1, true, xfer)
	ls := segs.scores.newLockedSegment(ctx, store, MaxSegment())
	err := segs.scores.replaceSegment(ctx, store, ls)
	if err != nil {
		return segs, err
	}

	// Create empty index.
	ls = segs.index.newLockedSegment(ctx, store, MaxSegment())
	err = segs.index.replaceSegment(ctx, store, ls)
	if err != nil {
		return segs, err
	}

	// Save
	err = l.saveSegments(ctx, bs, segs)
	if err != nil {
		return segs, err
	}
	// Verify what we've done.
	return segs, l.verify(ctx, bs, segs)
}

// listVersion should be incremented if non-compatible changes are made.
const listVersion = 1

// updateSegments will update the segments on the list.
func (l *List) updateSegments(ctx context.Context, s *segments) {
	ctx = log.WithFn(ctx)
	if s.readOnly {
		log.Error(ctx, "Was sent segment for update with readonly segment")
		return
	}
	l.scores = s.scores
	l.index = s.index
	if s.scores != nil {
		l.Scores = s.scores.ID
	} else {
		l.Scores = ""
	}
	if s.index != nil {
		l.Index = s.index.ID
	} else {
		l.Index = ""
	}
}

// loadFlags indicate the intentions of grabbing the segments.
// Only certain combinations of flags are allowed:
// * segsWritable + segsLockUpdates
// * segsReadOnly + segsLockUpdates
// * segsReadOnly + segsAllowUpdates
type loadFlags uint8

const (
	// Make segments writeable.
	// This is only needed when you intend to remove or add segments.
	// It is not required if you only intend to update elements in the segments.
	// If specified segsLockUpdates should also be used.
	segsWritable loadFlags = 1 << iota

	// Opposite of segsWritable. Specified for code clarity.
	segsReadOnly

	// Make segments update locked, meaning you want a consistent view of the elements.
	segsLockUpdates

	// Opposite of segsLockUpdates. Specified for code clarity.
	segsAllowUpdates
)

// segments is a structure that controls access to "scores" and "index" *Segments.
// (*List).loadSegments() or (*List).newSegments()
type segments struct {
	scores, index *Segments
	// Unlock will release all locks.
	unlock       func()
	readOnly     bool
	updateLocked bool
}

// removeUnlock will remove the unlock function and make it print an error
// if unlock is called again.
func (s *segments) removeUnlock(ctx context.Context) {
	s.unlock = func() {
		log.Error(ctx, "INTERNAL ERROR: double unlock of segments. Dumping stack.")
		debug.PrintStack()
	}
}

// loadSegments loads and locks the segments for either readonly or write operations.
// A write locked segment may be returned, even if a readonly was requested.
// Callers may *not* hold list lock while calling.
func (l *List) loadSegments(ctx context.Context, bs blobstore.Store, fl loadFlags) (*segments, error) {
	ctx = log.WithFn(ctx)
	readOnly := fl&segsReadOnly != 0
	updateLock := fl&segsLockUpdates != 0
	if readOnly && fl&segsWritable != 0 {
		return nil, errors.New("internal error: both segsWritable and segsReadOnly specified")
	}
	if updateLock && fl&segsAllowUpdates != 0 {
		return nil, errors.New("internal error: both segsLockUpdates and segsAllowUpdates specified")
	}
	if !readOnly && !updateLock {
		return nil, errors.New("segsWritable without segsLockUpdates requested")
	}
	if updateLock {
		l.updateLock.Lock()
	}
	l.loadingLock.Lock()
	defer l.loadingLock.Unlock()
	var err error
	s := segments{}
	if readOnly {
		l.segmentsLock.RLock()
		s.unlock = func() {
			l.segmentsLock.RUnlock()
			if updateLock {
				l.updateLock.Unlock()
			}
			s.removeUnlock(ctx)
		}
	} else {
		l.segmentsLock.Lock()
		s.unlock = func() {
			l.segmentsLock.Unlock()
			if updateLock {
				l.updateLock.Unlock()
			}
			s.removeUnlock(ctx)
		}
	}
	s.scores = l.scores
	s.index = l.index
	s.readOnly = readOnly
	s.updateLocked = updateLock

	// If everything ok, return
	if s.scores != nil && s.index != nil {
		return &s, nil
	}

	if s.readOnly {
		l.segmentsLock.RUnlock()
		l.segmentsLock.Lock()
		s.unlock = func() {
			l.segmentsLock.Unlock()
			if updateLock {
				l.updateLock.Unlock()
			}
		}
		s.readOnly = false
	}

	if s.scores == nil && !l.Scores.Unset() {
		s.scores, err = l.Scores.Load(ctx, blobstore.StoreWithSet(bs, l.Set), l.cache)
		if err != nil {
			log.Error(ctx, "Error loading scores segments", "error", err, "scores_id", l.Scores, "list_id", l.ID)
			s.unlock()
			return nil, err
		}
	}

	if s.index == nil && !l.Index.Unset() {
		s.index, err = l.Index.Load(ctx, blobstore.StoreWithSet(bs, l.Set), l.cache)
		if err != nil {
			log.Error(ctx, "Error loading index segments", "error", err, "index_id", l.Index, "list_id", l.ID)
			// Ditch index and reindex.
			nindex := NewSegments(0, false)
			nindex.IsIndex = true
			s.index = nindex

			err := l.reindex(ctx, bs, &s)
			if err != nil {
				s.unlock()
				return nil, err
			}
		}
	}
	l.updateSegments(ctx, &s)
	// Return read only if that was what was requested.
	if readOnly {
		l.segmentsLock.Unlock()
		l.segmentsLock.RLock()
		s.unlock = func() {
			l.segmentsLock.RUnlock()
			if updateLock {
				l.updateLock.Unlock()
			}
			s.removeUnlock(ctx)
		}
	}
	return &s, nil
}

// newSegments returns a new set up segments that can be written to.
// withIdx indicates whether an index segment should be allocated.
// The returned segments will be write and update locked.
// It is possible to transfer update locks from a previous segment,
// if this is replacing existing segments and you hold the update lock.
func (l *List) newSegments(ctx context.Context, preAlloc int, withIdx bool, xfer *segments) *segments {
	ctx = log.WithFn(ctx)
	if xfer != nil {
		log.Info(ctx, "replacing segments", "updatelocked", xfer.updateLocked, "readonly", xfer.readOnly)
	}

	// If update locked, we retain the lock.
	if xfer != nil && !xfer.updateLocked {
		log.Error(ctx, "newSegments: previous segment not update locked. Cannot reuse.")
		xfer.unlock()
		xfer = nil
	}
	s := segments{
		scores:       NewSegments(preAlloc, true),
		readOnly:     false,
		updateLocked: true,
	}
	s.unlock = func() {
		l.updateLock.Unlock()
		l.segmentsLock.Unlock()
		s.removeUnlock(ctx)
	}

	if withIdx {
		s.index = NewSegments(preAlloc, false)
		s.index.IsIndex = true
	}

	if xfer != nil {
		if xfer.readOnly {
			// Upgrade to full lock.
			// We know we have update lock.
			l.segmentsLock.RUnlock()
			l.segmentsLock.Lock()
		}
		return &s
	}
	l.updateLock.Lock()
	l.segmentsLock.Lock()
	return &s
}

// saveSegments will update segments on the list and save them.
// This must be used when element counts within one or more segments have changed.
func (l *List) saveSegments(ctx context.Context, bs blobstore.Store, s *segments) error {
	ctx = log.WithFn(ctx)
	if s == nil {
		return errors.New("no segments provided")
	}
	if !s.readOnly {
		l.updateSegments(ctx, s)
	}
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()
	store := blobstore.StoreWithSet(bs, set)
	if s.scores != nil {
		err := s.scores.Save(ctx, store)
		if err != nil {
			return err
		}
	} else {
		log.Info(ctx, "scores not loaded, cannot save")
	}
	if s.index != nil {
		err := s.index.Save(ctx, store)
		if err != nil {
			return err
		}
	} else {
		log.Info(ctx, "index not loaded, cannot save")
	}
	return nil
}

// cloneElements will replace all elements of this list with the elements of the original.
func (l *List) cloneElements(ctx context.Context, bs blobstore.Store, org *List) error {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	oSegs, err := org.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return fmt.Errorf("loading segments: %v", err)
	}
	oScores := oSegs.scores
	defer oSegs.unlock()

	org.RWMutex.RLock()
	oSet := org.Set
	org.RWMutex.RUnlock()

	l.RWMutex.RLock()
	dSet := l.Set
	dMergeSize := l.MergeSize
	dSplitSize := l.SplitSize
	l.RWMutex.RUnlock()

	bsDst := blobstore.StoreWithSet(bs, dSet)
	newSegments := l.newSegments(ctx, len(oScores.Segments), false, nil)
	defer newSegments.unlock()
	dstIdx := IndexElements{Elements: make(Elements, 0, oScores.Elements())}
	for i := range oScores.Segments {
		ls, err := oScores.elementFullIdx(ctx, blobstore.StoreWithSet(bs, oSet), i, true)
		if err != nil {
			return err
		}
		newSeg := Segment{
			ID:     0, // populated by newLockedSegment
			Min:    ls.seg.Min,
			Max:    ls.seg.Max,
			MinTie: ls.seg.MinTie,
			MaxTie: ls.seg.MaxTie,
			N:      len(ls.elements),
			Parent: newSegments.scores.ID,
			loader: &elementLoader{},
		}
		dst := newSegments.scores.newLockedSegment(ctx, bsDst, &newSeg)
		dst.elements = ls.elements.Clone(true)
		ls.unlock()
		dstIdx.Elements = append(dstIdx.Elements, dst.elements.ElementIDs(dst.seg.ID).Elements...)
		if err != nil {
			dst.unlock()
			return err
		}
		err = newSegments.scores.replaceSegment(ctx, bsDst, dst)
		if err != nil {
			log.Error(ctx, err.Error())
		}
	}
	dstIdx.Sort()

	// Add index elements.
	wantSize := (dMergeSize + dSplitSize) / 2
	newIdx, err := NewSegmentsElements(ctx, bsDst, dstIdx.SplitSize(wantSize), nil)
	if err != nil {
		return err
	}
	newSegments.index = newIdx

	// List may have other split/merge settings, check that.
	l.checkSplit(ctx, bs, newSegments)
	return l.saveSegments(ctx, bs, newSegments)
}

// ScoreError is returned when error is related to scores.
type ScoreError error

// IndexError is returned when error is related to index
// and can be fixed by a re-index.
type IndexError error

// Verify a list without loading elements.
func (l *List) Verify(ctx context.Context, bs blobstore.Store) error {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return fmt.Errorf("Loading segments: %v", err)
	}
	defer segs.unlock()
	return l.verify(ctx, bs, segs)
}

// Verify a list without loading elements.
func (l *List) verify(ctx context.Context, bs blobstore.Store, segs *segments) error {
	l.RWMutex.RLock()
	ctx = log.WithFn(ctx, "list_id", l.ID)
	set := l.Set
	lMergeSize := l.MergeSize
	lSplitSize := l.SplitSize
	lScores := l.Scores
	lIndex := l.Index
	l.RWMutex.RUnlock()

	err := func() error {
		l.RWMutex.RLock()
		defer l.RWMutex.RUnlock()
		if l.Scores.Unset() {
			return fmt.Errorf("Score Segments ID not set on list.")
		}
		if l.Index.Unset() {
			return fmt.Errorf("Index Segments ID not set on list.")
		}
		if segs.scores.ID != l.Scores {
			return fmt.Errorf("Scores Segments ID mismatch. List: %v, Segments:%v", l.Scores, l.scores.ID)
		}
		if segs.index.ID != l.Index {
			return IndexError(fmt.Errorf("Index Segments ID mismatch. List: %v, Segments:%v", l.Index, l.index.ID))
		}
		if segs.scores.IsIndex {
			return fmt.Errorf("Score Segments was marked as index.")
		}
		if !segs.index.IsIndex {
			return IndexError(fmt.Errorf("Index Segments was not marked as index."))
		}
		if l.SplitSize < l.MergeSize {
			return fmt.Errorf("SplitSize (%d) < MergeSize (%d)", l.SplitSize, l.MergeSize)
		}
		return nil
	}()
	if err != nil {
		return err
	}

	store := blobstore.StoreWithSet(bs, set)
	err = segs.scores.Verify(ctx, store)
	if err != nil {
		log.Info(ctx, "Scores:", "segments", segs.scores)

		return ScoreError(fmt.Errorf("Verifying Scores: %v", err))
	}
	shouldSplit := false
	psegn := lMergeSize
	for i := range segs.scores.Segments {
		segs.scores.SegmentsLock.RLock()
		seg := &segs.scores.Segments[i]
		segs.scores.SegmentsLock.RUnlock()
		if seg.Parent != lScores {
			return ScoreError(fmt.Errorf("Scores Segment %d Parent ID mismatch. List: %v, Segment:%v", i, lScores, seg.Parent))
		}
		if seg.N > lSplitSize {
			log.Info(ctx, "Score segment should be split", "seg_id", seg.ID, "n_elements", seg.N, "split_size", lSplitSize)
			shouldSplit = true
		}
		if seg.N+psegn < lMergeSize {
			log.Info(ctx, "Score segment should be merged with previous", "seg_id", seg.ID, "n_elements", seg.N+psegn, "merge_size", lMergeSize)
			shouldSplit = true
		}
		psegn = seg.N
	}
	err = segs.index.Verify(ctx, store)
	if err != nil {
		log.Info(ctx, "Index:", "segments", segs.index)
		return IndexError(fmt.Errorf("Verifying Index: %v", err))
	}
	psegn = lMergeSize
	for i := range segs.index.Segments {
		segs.index.SegmentsLock.RLock()
		seg := &l.index.Segments[i]
		segs.index.SegmentsLock.RUnlock()
		if seg.Parent != lIndex {
			return IndexError(fmt.Errorf("Index Segment %d Parent ID mismatch. List: %v, Segment:%v", i, lIndex, seg.Parent))
		}
		if seg.N > lSplitSize {
			shouldSplit = true
			log.Info(ctx, "Index segment should be split", "seg_id", seg.ID)
		}
		if i > 0 && seg.N+psegn < lMergeSize {
			shouldSplit = true
			log.Info(ctx, "Index segment should be merged with previous", "seg_id", seg.ID)
		}
		psegn = seg.N
	}
	if shouldSplit {
		l.requestSplit(ctx)
	}
	return nil
}

// VerifyElements verifies elements in list.
func (l *List) VerifyElements(ctx context.Context, bs blobstore.Store) error {
	l.RWMutex.RLock()
	ctx = log.WithFn(ctx, "list_id", l.ID)
	l.RWMutex.RUnlock()
	// We want a consistent view of elements/indexes, so we need update.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsLockUpdates)
	if err != nil {
		return err
	}
	defer segs.unlock()
	return l.verifyElements(ctx, bs, segs)
}

// VerifyElements verifies elements in list.
// Caller should hold list updatelock for full consistency.
func (l *List) verifyElements(ctx context.Context, bs blobstore.Store, segs *segments) error {
	ctx = log.WithFn(ctx)
	l.RWMutex.RLock()
	store := blobstore.StoreWithSet(bs, l.Set)
	l.RWMutex.RUnlock()
	var ids map[ElementID]SegmentID

	// We do not get the update lock since caller may be holding it.
	err := segs.scores.VerifyElements(ctx, store, &ids)
	if err != nil {
		return ScoreError(fmt.Errorf("Verifying Score Elements: %v", err))
	}
	var idxids map[ElementID]SegmentID
	err = segs.index.VerifyElements(ctx, store, &idxids)
	if err != nil {
		return IndexError(fmt.Errorf("Verifying Index Elements: %v", err))
	}
	for id, segid := range ids {
		if isegid, ok := idxids[id]; !ok {
			return IndexError(fmt.Errorf("Object %v was not indexed", id))
		} else {
			if segid != isegid {
				return IndexError(fmt.Errorf("Object %v is indexed to wrong segment (want:%v != got:%v)", id, segid, isegid))
			}
		}
		delete(idxids, id)
	}
	for id := range idxids {
		return IndexError(fmt.Errorf("Extra object ID %d found", id))
	}
	return nil
}

// Reindex will re-create the list element index.
func (l *List) Reindex(ctx context.Context, bs blobstore.Store) error {
	ctx = log.WithFn(ctx)
	segs, err := l.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
	if err != nil {
		return err
	}
	defer segs.unlock()
	return l.reindex(ctx, bs, segs)
}

// Reindex will re-create the list element index.
func (l *List) reindex(ctx context.Context, bs blobstore.Store, segs *segments) error {
	ctx = log.WithFn(ctx)
	if segs.readOnly {
		return errors.New("reindex: readonly segments given")
	}
	if !segs.updateLocked {
		return errors.New("reindex: segments without update lock given")
	}
	ctx = log.WithFn(ctx, "list_id", l.ID)
	store := blobstore.StoreWithSet(bs, l.Set)
	l.RWMutex.RLock()
	mergeSize := l.MergeSize
	splitSize := l.SplitSize
	l.RWMutex.RUnlock()

	idx, err := segs.scores.ElementIndexAll(ctx, store)
	if err != nil {
		return err
	}
	wantSize := (mergeSize + splitSize) / 2
	newIdx, err := NewSegmentsElements(ctx, store, idx.SplitSize(wantSize), nil)
	if err != nil {
		return err
	}

	// Save index.
	err = newIdx.Save(ctx, store)
	if err != nil {
		log.Error(ctx, "Saving new index:", "err", err)
		return err
	}

	// Delete old
	err = segs.index.Delete(ctx, store)
	if err != nil {
		// We will survive, but want to know
		log.Error(ctx, "Deleting index:", "err", err)
	}

	// Replace
	segs.index = newIdx
	err = l.saveSegments(ctx, bs, segs)
	if err != nil {
		segs.unlock()
		return err
	}

	return nil
}

// VerifyUnlocked validates that all elements of the list can be locked.
// This is only really usable for tests, where list context is controlled.
func (l *List) VerifyUnlocked(ctx context.Context, timeout time.Duration) error {
	var fail = make(chan struct{})
	var ok = make(chan struct{})
	var wg = &sync.WaitGroup{}

	go func() {
		time.Sleep(timeout)
		close(fail)
	}()

	// Check list can be locked.
	testLock(log.Logger(ctx).New("lock", "list"), fail, l, wg)
	go func() {
		wg.Wait()
		close(ok)
	}()

	select {
	case <-fail:
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		time.Sleep(time.Millisecond * 100)
		return errors.New("timeout acquiring locks")
	case <-ok:
	}
	ok = make(chan struct{})

	// We need read lock.
	l.RWMutex.RLock()
	defer l.RWMutex.RUnlock()
	testLock(log.Logger(ctx).New("lock", "list.loadingLock"), fail, &l.loadingLock, wg)
	testLock(log.Logger(ctx).New("lock", "list.segmentsLock"), fail, &l.segmentsLock, wg)
	testLock(log.Logger(ctx).New("lock", "list.updateLock"), fail, &l.updateLock, wg)
	if l.index != nil {
		testLock(log.Logger(ctx).New("lock", "list.index.segmentstats"), fail, &l.index.SegmentsLock, wg)
		for i := range l.index.Segments {
			seg := &l.index.Segments[i]
			testLock(log.Logger(ctx).New("lock", "list.index.loader.Mu", "segment", i, "segment_id", seg.cacheID()), fail, &seg.loader.Mu, wg)
			testLock(log.Logger(ctx).New("lock", "list.index.loader.LoadingMu", "segment", i, "segment_id", seg.cacheID()), fail, &seg.loader.LoadingMu, wg)
		}
	}
	if l.scores != nil {
		testLock(log.Logger(ctx).New("lock", "list.scores.segmentstats"), fail, &l.scores.SegmentsLock, wg)
		for i := range l.scores.Segments {
			seg := &l.scores.Segments[i]
			testLock(log.Logger(ctx).New("lock", "list.scores.loader.Mu", "segment", i, "segment_id", seg.cacheID()), fail, &seg.loader.Mu, wg)
			testLock(log.Logger(ctx).New("lock", "list.scores.loader.LoadingMu", "segment", i, "segment_id", seg.cacheID()), fail, &seg.loader.LoadingMu, wg)
		}
	}
	go func() {
		wg.Wait()
		close(ok)
	}()

	select {
	case <-fail:
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		time.Sleep(time.Millisecond * 100)
		return errors.New("timeout acquiring locks")
	case <-ok:
		log.Info(ctx, "All locks acquired")
		return nil
	}
}

func testLock(log log.Adapter, fail chan struct{}, locker sync.Locker, wg *sync.WaitGroup) {
	wg.Add(1)
	var ok = make(chan struct{})
	go func() {
		locker.Lock()
		close(ok)
		locker.Unlock()
	}()
	go func() {
		select {
		case <-ok:
			wg.Done()
		case <-fail:
			log.Error("Timeout acquiring lock")
		}
	}()
}

// Populate will replace content of list with the supplied elements.
// Elements are assumed to be de-duplicated.
func (l *List) Populate(ctx context.Context, bs blobstore.Store, e Elements) error {
	ctx = log.WithFn(ctx)
	segs, err := l.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
	if err != nil {
		// Log error, but proceed.
		log.Error(ctx, "Populate: error loading existing segments", "error", err.Error())
	}
	segs, err = l.populate(ctx, bs, e, segs)
	if segs != nil {
		segs.unlock()
	}
	return err
}

// Populate will replace content of list with the supplied elements.
// Elements are assumed to be de-duplicated.
// Even if an error is returned, the returned segments must be unlocked of not nil.
func (l *List) populate(ctx context.Context, bs blobstore.Store, e Elements, segs *segments) (*segments, error) {
	ctx = log.WithFn(ctx, "list_id", l.ID)
	store := blobstore.StoreWithSet(bs, l.Set)

	l.RWMutex.RLock()
	hasValid := (!l.Scores.Unset() || !l.Index.Unset()) && segs != nil
	l.RWMutex.RUnlock()
	if hasValid {
		err := l.deleteAll(ctx, bs, segs)
		if err != nil {
			return segs, err
		}
	}
	if len(e) == 0 {
		segs, err := l.initEmptyList(ctx, bs, segs)
		return segs, err
	}

	// Retain full lock while populating.
	l.RWMutex.Lock()

	// Above ~200k we get significant slowdown, we defer these.
	var deferred Elements
	const insertLimit = 100000
	if len(e) > insertLimit && !sort.SliceIsSorted(e, e.Sorter()) {
		// We want the initial list to contain random ordered elements,
		// so the initial segments are split across all of the range.
		// Forcefully shuffle the slice.
		rng := rand.New(rand.NewSource(0xc0cac01a))
		for i := range e {
			j := rng.Intn(i + 1)
			e[i], e[j] = e[j], e[i]
		}
		deferred = e[insertLimit:]
		e = e[:insertLimit]
	}
	e.Sort()

	// Calculate size of each segment
	wantSize := (l.MergeSize + l.SplitSize) / 2
	if len(deferred) > 0 {
		factor := float64(len(deferred)+len(e)) / float64(len(e))
		wantSize = int(float64(wantSize) / factor)
		// Avoid too many merges.
		if wantSize < l.MergeSize/4 {
			wantSize = l.MergeSize / 4
		}
	}

	// Split input
	split := e.SplitSize(wantSize)
	segs = l.newSegments(ctx, 0, false, segs)

	ids := IndexElements{make(Elements, 0, len(e))}
	scores, err := NewSegmentsElements(ctx, store, split, &ids)
	if err != nil {
		l.RWMutex.Unlock()
		return segs, err
	}
	scores.cache = l.cache
	segs.scores = scores

	// Generate Index segments.
	ids.Sort()
	split = ids.SplitSize(wantSize)
	idx, err := NewSegmentsElements(ctx, store, split, nil)
	if err != nil {
		segs.unlock()
		return segs, err
	}
	idx.cache = l.cache
	segs.index = idx
	l.RWMutex.Unlock()

	if len(deferred) > 0 {
		l.updateSegments(ctx, segs)
		err = l.insert(ctx, bs, deferred, segs)
		if err != nil {
			return segs, err
		}
	}
	l.updateSegments(ctx, segs)
	err = l.saveSegments(ctx, bs, segs)
	if err != nil {
		return segs, err
	}
	// verify will queue merge/split if needed.
	return segs, l.verify(ctx, bs, segs)
}

// Len returns the number of elements in the list.
func (l *List) Len(ctx context.Context, bs blobstore.Store) (int, error) {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return 0, err
	}
	defer segs.unlock()
	scores := segs.scores
	index := segs.index
	n := scores.Elements()
	if sanityChecks {
		n2 := index.Elements()
		if n != n2 {
			log.Error(ctx, "Element count mismatch", "scores", n, "index", n2)
			return 0, errors.New("Element count mismatch")
		}
	}
	return n, nil
}

// ListStats provides overall stats for a list
type ListStats struct {
	Elements    int
	Segments    int
	Top         *RankedElement
	Bottom      *RankedElement
	CacheHits   uint64
	CacheMisses uint64
	CachePct    float64
}

// Stats returns general stats about the list.
// If elements is true, top and bottom elements will be loaded.
func (l *List) Stats(ctx context.Context, bs blobstore.Store, elements bool) (*ListStats, error) {
	ctx = log.WithFn(ctx)
	res := ListStats{}

	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	scores := segs.scores
	index := segs.index

	res.Elements = scores.Elements()
	//nolint - why's it okay to use scores before checking if it's nil again?
	res.Segments = len(scores.Segments)
	//nolint - why's it okay to use scores before checking if it's nil again?
	if scores != nil {
		res.CacheHits = l.scores.cacheHits + l.index.cacheHits
		res.CacheMisses = l.scores.cacheMisses + l.index.cacheMisses
	}
	if index != nil {
		res.CacheHits += index.cacheHits
		res.CacheMisses += index.cacheMisses
	}

	if res.CacheMisses+res.CacheHits > 0 {
		res.CachePct = 100 * float64(res.CacheHits) / float64(res.CacheMisses+res.CacheHits)
	}
	if elements {
		es, err := l.GetRankTop(ctx, bs, 0, 1)
		if err == nil && len(es) > 0 {
			top := RankedElement{Element: es[0]}
			top.CalculateFromBottom(res.Elements)
			res.Top = &top
		}
		es, err = l.GetRankBottom(ctx, bs, 0, 1)
		if err == nil && len(es) > 0 {
			bottom := RankedElement{Element: es[0], FromTop: res.Elements - 1}
			res.Bottom = &bottom
		}
	}

	return &res, nil
}

// ReleaseSegments will release all segments from list
// including all elements.
func (l *List) ReleaseSegments(ctx context.Context) {
	ctx = log.WithFn(ctx)
	var gotLoading, gotSegments, cancelled bool
	var done = make(chan struct{})
	var mu sync.Mutex
	l.RWMutex.RLock()
	listID := l.ID
	l.RWMutex.RUnlock()

	// We add a one minute timeout, since we have seen deadlocks here.
	const timeout = time.Minute
	// Execute function with mutex if no cancelled.
	// Returns whether cancelled.
	withMutexCancelled := func(fn func()) bool {
		mu.Lock()
		defer mu.Unlock()
		if !cancelled {
			fn()
		}
		return cancelled
	}

	go func() {
		l.loadingLock.Lock()
		if withMutexCancelled(func() { gotLoading = true }) {
			l.loadingLock.Unlock()
			return
		}
		l.segmentsLock.Lock()
		if withMutexCancelled(func() {
			gotSegments = true
			if cancelled {
				return
			}
			l.scores = nil
			l.index = nil
			l.segmentsLock.Unlock()
			l.loadingLock.Unlock()
			gotSegments = false
			gotLoading = false
			close(done)
		}) {
			l.segmentsLock.Unlock()
			return
		}
	}()
	select {
	case <-time.After(timeout):
		withMutexCancelled(func() {
			log.Error(ctx, "segment release timeout", "gotLoading", gotLoading, "gotSegments", gotSegments, "list_id", listID)
			cancelled = true
			if gotSegments {
				l.segmentsLock.Unlock()
			}
			if gotLoading {
				l.loadingLock.Unlock()
			}
		})
	case <-done:
	}
}

// DeleteAll will delete all stored content of a list.
func (l *List) DeleteAll(ctx context.Context, bs blobstore.Store) error {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
	if err != nil {
		log.Error(ctx, "Error loading segments for delete", "err", err)
		// Ignore, we are deleting anyway.
		return nil
	}
	defer segs.unlock()
	return l.deleteAll(ctx, bs, segs)
}

// deleteAll will delete all stored content of a list.
func (l *List) deleteAll(ctx context.Context, bs blobstore.Store, segs *segments) error {
	ctx = log.WithFn(ctx)
	if segs == nil {
		return errors.New("deleteAll: nil segments provided")
	}
	if segs.readOnly {
		return errors.New("deleteAll: readOnly segments provided")
	}
	if !segs.updateLocked {
		return errors.New("deleteAll: segments without update lock provided")
	}
	l.RWMutex.Lock()
	defer l.RWMutex.Unlock()

	store := blobstore.StoreWithSet(bs, l.Set)
	if segs.scores != nil {
		err := segs.scores.Delete(ctx, store)
		if err != nil {
			log.Error(ctx, "Error Deleting Scores", "error", err)
		}
	}
	if segs.index != nil {
		err := segs.index.Delete(ctx, store)
		if err != nil {
			log.Error(ctx, "Error Deleting Index", "error", err)
		}
	}
	l.scores = nil
	l.index = nil
	l.Index = ""
	l.Scores = ""
	return nil
}

// getIndexElements looks up elements in the index.
// May return less than requested elements if duplicates are provided or elements could not be found.
func (l *List) getIndexElements(ctx context.Context, bs blobstore.Store, ids []ElementID, segs *segments) (IndexElements, error) {
	ctx = log.WithFn(ctx)
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()

	// Look up element ids in index to get their segment.
	var scoreMap = make(map[uint64]struct{}, len(ids))
	var asScore = make([]uint64, 0, len(ids))
	for _, v := range ids {
		if _, ok := scoreMap[uint64(v)]; ok {
			log.Info(ctx, "Removing duplicated requested id", "id", v)
			continue
		}
		asScore = append(asScore, uint64(v))
		scoreMap[uint64(v)] = struct{}{}
	}
	e, err := segs.index.getFirstWithScore(ctx, blobstore.StoreWithSet(bs, set), asScore)
	if err != nil {
		return IndexElements{}, err
	}
	// Check results
	e2 := make(Elements, 0, len(e))
	for _, elem := range e {
		if _, ok := scoreMap[elem.Score]; !ok {
			//log.Info(ctx, "Removing wrong result (requested id not found)", "id", elem.Score)
			continue
		}
		e2 = append(e2, elem)
		if sanityChecks {
			delete(scoreMap, elem.Score)
		}
	}
	if sanityChecks && len(scoreMap) > 0 {
		log.Info(ctx, "Did not find elements", "ids", scoreMap)
	}
	return IndexElements{Elements: e2}, nil
}

// GetElements will look up the provided elements and return them as ranked.
// The returned elements are sorted by rank.
// Elements that are not found are not returned.
func (l *List) GetElements(ctx context.Context, bs blobstore.Store, ids []ElementID, radius int) (RankedElements, error) {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	return l.getElements(ctx, bs, ids, radius, segs)
}

// GetElements will look up the provided elements and return them as ranked.
// The returned elements are sorted by rank.
// Elements that are not found are not returned.
func (l *List) getElements(ctx context.Context, bs blobstore.Store, ids []ElementID, radius int, segs *segments) (RankedElements, error) {
	ctx = log.WithFn(ctx)
	scores := segs.scores
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()

	// Disallow updates to scores until we have found the elements
	e, err := l.getIndexElements(ctx, bs, ids, segs)
	if err != nil {
		return nil, err
	}
	// Find elements in each segment.
	return scores.FindElements(ctx, blobstore.StoreWithSet(bs, set), e, radius)
}

// UpdateElements will update or add elements to the list.
// Elements should be deduplicated when provided.
// Concurrent calls to UpdateElements with the same elements can result in duplicate entries in the list.
// If results are not requested, the returned slice will always be empty.
func (l *List) UpdateElements(ctx context.Context, bs blobstore.Store, elems Elements, radius int, results bool) (RankedElements, error) {
	ctx = log.WithFn(ctx)
	elems.Sort()
	var ids = elems.IDs()
	ids.Sort()

	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsLockUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	scores := segs.scores
	index := segs.index

	l.RWMutex.RLock()
	ctx = log.WithValues(ctx, "list_id", l.ID)
	set := l.Set
	l.RWMutex.RUnlock()

	store := blobstore.StoreWithSet(bs, set)
	scores.reindex()

	// This will give us the index segments in which the existing
	// elements are placed or should be placed.
	segmentsIdx := make(map[int]*lockedSegment)
	// Element to segment index map. This should not change.
	elementsIdx := make(map[ElementID]int)
	// Contains all existing index entries with values prior to update.
	existingIdx := make(map[ElementID]Element)
	// Contains all segments with previous score values.
	segmentScores := make(map[int]*lockedSegment)
	segmentScoresWait := make(map[int]chan struct{})
	// Contains all segments with previous score values.
	oldRank := make(map[ElementID]*RankedElement)

	// used for local locking
	var mu sync.Mutex
	var withLock = func(fn func()) {
		mu.Lock()
		fn()
		mu.Unlock()
	}

	// segmentScoresLoad loads a score by index.
	var segmentScoresLoad func(segIdx int) *lockedSegment
	segmentScoresLoad = func(segIdx int) *lockedSegment {
		mu.Lock()
		ss, ok := segmentScores[segIdx]
		if !ok {
			// Check if we are loading it.
			if ch := segmentScoresWait[segIdx]; ch != nil {
				// Yes, wait for load to finish.
				mu.Unlock()
				<-ch
				return segmentScoresLoad(segIdx)
			}
			ch := make(chan struct{})
			segmentScoresWait[segIdx] = ch
			defer close(ch)
			mu.Unlock()
			var err error
			ss, err = scores.elementFullIdx(ctx, store, segIdx, false)
			if err != nil {
				log.Error(ctx, "Could not load score segment", "error", err.Error())
				mu.Lock()
				delete(segmentScoresWait, segIdx)
				mu.Unlock()
				return nil
			}
			mu.Lock()
			segmentScores[segIdx] = ss
			delete(segmentScoresWait, segIdx)
		}
		mu.Unlock()
		return ss
	}

	// Use if returning early.
	unlockAll := func() {
		for _, ls := range segmentsIdx {
			ls.unlock()
		}
		for _, ls := range segmentScores {
			ls.unlock()
		}
	}

	// Find what information we have on existing items in the index.
	dstIndexes := index.splitScores(ctx, ids.AsScore())
	var wg sync.WaitGroup
	var errs = make(chan error, 1)
	for _, e := range dstIndexes {
		if len(e.scores) == 0 {
			continue
		}
		wg.Add(1)
		go func(e segmentMatch) {
			defer wg.Done()
			ls, err := index.elementFullIdx(ctx, store, e.segIdx, false)
			if err != nil {
				errs <- err
				return
			}
			withLock(func() {
				segmentsIdx[e.segIdx] = ls
			})

			// See which we can actually find.
			found := ls.elements.FirstElementsWithScore(e.scores)
			for _, score := range e.scores {
				eid := ElementID(score)
				withLock(func() {
					elementsIdx[eid] = e.segIdx
				})
				existing, err := found.Find(eid)
				if err != nil {
					// There was no element with that ID.
					continue
				}
				withLock(func() {
					existingIdx[eid] = *existing
				})

				// Load the existing scores segment.
				scores.SegmentsLock.RLock()
				segIdx := scores.idx[SegmentID(existing.TieBreaker)]
				scores.SegmentsLock.RUnlock()
				ss := segmentScoresLoad(segIdx)
				if ss == nil {
					continue
				}
				if results {
					// Record rank of old element.
					rank, err := scores.findRanked(ctx, store, ss, eid, 0, 0)
					if err != nil {
						log.Info(ctx, "Could not find element", "error", err.Error())
						continue
					}
					withLock(func() {
						oldRank[eid] = rank
					})
				}
			}
		}(e)
	}

	err = waitErr(ctx, errs, &wg)
	if err != nil {
		unlockAll()
		return nil, err
	}
	// Split incoming elements to get destination score segments.
	for sidx, elems := range scores.splitElementsIdx(elems) {
		// The element should end up in this segment
		scoreDstSeg := segmentScoresLoad(sidx)
		if scoreDstSeg == nil {
			continue
		}

		// Check if element have changed from another segment
		for _, elem := range elems {
			idx := elem.AsIndex(scoreDstSeg.seg.ID)
			// Check if existing is same as it ends up in.
			existing, ok := existingIdx[elem.ID]
			if !ok {
				// New: Add element.
				_, err = scoreDstSeg.elements.Add(elem)
				if err != nil {
					log.Error(ctx, "Could not add index", "error", err.Error())
					continue
				}

				// Add index entry to element.
				_, err = segmentsIdx[elementsIdx[elem.ID]].elements.Add(idx.Element)
				if err != nil {
					log.Error(ctx, "Could not add index", "error", err.Error())
				}
				continue
			}

			// Existing element: Check if we should update index.
			if existing.TieBreaker == uint32(scoreDstSeg.seg.ID) {
				// We are in same segment, simply update score element.
				_, err = scoreDstSeg.elements.Update(elem)
				if err != nil {
					log.Error(ctx, "Could not update existing score", "error", err.Error())
				}
				continue
			}

			// Add element. It should be new to this segment.
			_, err = scoreDstSeg.elements.Add(elem)
			if err != nil {
				log.Error(ctx, "Could not add new score", "error", err.Error())
				continue
			}

			// Update index to point to new segment.
			_, err = segmentsIdx[elementsIdx[elem.ID]].elements.Update(idx.Element)
			if err != nil {
				log.Error(ctx, "Could not update existing index", "error", err.Error())
				continue
			}

			// Remove from old scores segment
			scores.SegmentsLock.RLock()
			eSid := scores.idx[SegmentID(existing.TieBreaker)]
			scores.SegmentsLock.RUnlock()
			rem := segmentScoresLoad(eSid)
			if rem != nil {
				err := rem.elements.Delete(elem.ID)
				if err != nil {
					log.Info(ctx, "Could not delete existing", "error", err.Error())
				}
			}
		}
	}

	for _, ls := range segmentsIdx {
		err := index.replaceSegment(ctx, store, ls)
		if err != nil {
			log.Error(ctx, "Error saving updated index elements", "error", err.Error())
		}
	}

	for _, ls := range segmentScores {
		err := scores.replaceSegment(ctx, store, ls)
		if err != nil {
			log.Error(ctx, "Error saving updated score elements", "error", err.Error())
		}
	}
	wg.Wait()
	l.checkSanity(ctx, bs, "update", segs)

	// Get updated when everything is said and done.
	updated := []RankedElement{}
	if results {
		updated, err = l.getElements(ctx, bs, ids, radius, segs)
		if err != nil {
			return nil, err
		}
		for i, elem := range updated {
			if before, ok := oldRank[elem.ID]; ok {
				elem.Before = before
			}
			updated[i] = elem
		}
	}

	l.checkSplit(ctx, bs, segs)
	return updated, l.saveSegments(ctx, bs, segs)
}

// deleteElements will delete elements with the supplied ids.
// If no elements are not found, no error is returned.
func (l *List) DeleteElements(ctx context.Context, bs blobstore.Store, ids []ElementID) error {
	ctx = log.WithFn(ctx)
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsLockUpdates)
	if err != nil {
		return err
	}
	defer segs.unlock()
	scores := segs.scores
	index := segs.index

	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()

	// Retain scores lock until index is updated
	err = func() error {
		e, err := l.getIndexElements(ctx, bs, ids, segs)
		if err != nil {
			return err
		}
		store := blobstore.StoreWithSet(bs, set)

		deleted, err := scores.DeleteElementsIdx(log.WithValues(ctx, "onlist", "scores"), store, e, true)
		if err != nil {
			return err
		}
		if len(deleted.Elements) == 0 {
			return nil
		}

		_, err = index.DeleteElements(log.WithValues(ctx, "onlist", "index"), store, deleted.Elements, false)
		if err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return err
	}
	l.checkSplit(ctx, bs, segs)
	return l.saveSegments(ctx, bs, segs)
}

// Insert elements into list.
// Elements are not checked if they exist and will result in duplicates if they do.
// Provided elements are not required to be sorted.
func (l *List) Insert(ctx context.Context, bs blobstore.Store, e Elements) error {
	ctx = log.WithFn(ctx)
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsLockUpdates)
	if err != nil {
		return err
	}
	defer segs.unlock()
	err = l.insert(ctx, bs, e, segs)
	if err != nil {
		return err
	}
	l.checkSplit(ctx, bs, segs)
	return l.saveSegments(ctx, bs, segs)
}

// Insert elements into list.
// Elements are not checked if they exist and will result in duplicates if they do.
// Provided elements are not required to be sorted.
// Does not save segments or check for splits/merges.
// Provided segments must contain update lock.
func (l *List) insert(ctx context.Context, bs blobstore.Store, e Elements, segs *segments) error {
	ctx = log.WithFn(ctx)
	if !segs.updateLocked {
		return errors.New("insert: segments not update locked")
	}
	store := blobstore.StoreWithSet(bs, l.Set)

	// Limit single inserts to inserting max into each segment.
	var wantLimit = (l.SplitSize / 4) * len(l.index.Segments)
	if wantLimit > 100000 {
		wantLimit = 100000
	}
	for len(e) > wantLimit {
		// Calling ourselves, wantLimit should be the same.
		err := l.insert(ctx, bs, e[:wantLimit], segs)
		if err != nil {
			return err
		}
		e = e[wantLimit:]
		if false {
			// Run a list split after each.
			// DISABLED: Causes instability.
			err = l.split(ctx, bs, segs, false)
			if err != nil {
				log.Error(ctx, "insert: splitter returned error", "error", err.Error())
			}
		}
	}
	e.Sort()

	// Retain scores lock until we have updated indexes.
	err := func() error {
		updated, err := segs.scores.Insert(ctx, store, e, true)
		if err != nil {
			return err
		}
		if len(updated.Elements) > len(e) {
			return fmt.Errorf("sanity check 'len(updated) <= len(elements)' failed")
		}
		updated.Sort()
		_, err = segs.index.Insert(ctx, store, updated.Elements, false)
		if err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return err
	}
	return nil
}

func (l *List) String() string {
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// GetRankTop returns a number of elements at a specific offset from the top of the list.
// Elements are returned in descending order.
// First element returned is rank offset+1.
// Requesting offset 0 will start with top ranked element.
// Requesting offset equal to or grater than list length will return ErrOffsetOutOfBounds.
func (l *List) GetRankTop(ctx context.Context, bs blobstore.Store, offset, elements int) (Elements, error) {
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	return l.getRankTop(ctx, bs, offset, elements, segs)
}

// GetRankTop returns a number of elements at a specific offset from the top of the list.
// Elements are returned in descending order.
// First element returned is rank offset+1.
// Requesting offset 0 will start with top ranked element.
// Requesting offset equal to or grater than list length will return ErrOffsetOutOfBounds.
func (l *List) getRankTop(ctx context.Context, bs blobstore.Store, offset, elements int, segs *segments) (Elements, error) {
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()

	segIdx, eoff := segs.scores.segIdxTop(offset)
	if segIdx < 0 {
		return nil, ErrOffsetOutOfBounds
	}
	store := blobstore.StoreWithSet(bs, set)
	return segs.scores.getElementsOffset(ctx, store, segIdx, eoff, elements)
}

// GetRankBottom returns a number of elements at a specific offset from the top of the list.
// Elements are returned in descending order.
// First element returned is (list length) - offset - 1.
// Requesting offset 0 will return the bottom element.
// If offset is outside list range ErrOffsetOutOfBounds is returned.
func (l *List) GetRankBottom(ctx context.Context, bs blobstore.Store, offset, elements int) (Elements, error) {
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	return l.getRankBottom(ctx, bs, offset, elements, segs)
}

// GetRankBottom returns a number of elements at a specific offset from the top of the list.
// Elements are returned in descending order.
// First element returned is (list length) - offset - 1.
// Requesting offset 0 will return the bottom element.
// If offset is outside list range ErrOffsetOutOfBounds is returned.
func (l *List) getRankBottom(ctx context.Context, bs blobstore.Store, offset, elements int, segs *segments) (Elements, error) {
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()
	segIdx, eoff := segs.scores.segIdxBottom(offset)
	if segIdx < 0 {
		return nil, ErrOffsetOutOfBounds
	}
	store := blobstore.StoreWithSet(bs, set)
	return segs.scores.getElementsOffset(ctx, store, segIdx, eoff, elements)
}

// GetRankScoreDesc returns ranked elements starting with first elements at a specific score.
// If the exact score isn't found, the list will start with the following next entry below the score.
// If no scores are at or below the supplied score, an empty array is returned.
// Results are returned in descending order.
func (l *List) GetRankScoreDesc(ctx context.Context, bs blobstore.Store, score uint64, n int) (RankedElements, error) {
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()
	segIdx, above, total := segs.scores.segIdxScore(score)
	// 	log.Info(ctx, "got", "segIdx",segIdx, "above", above, "total", total, "score", score)
	if segIdx == -1 {
		// Shouldn't happen.
		return nil, ErrOffsetOutOfBounds
	}
	store := blobstore.StoreWithSet(bs, set)
	e, err := segs.scores.getElementsFromScore(ctx, store, score, segIdx, n)
	if err != nil {
		return nil, err
	}
	e.CalculateFromBottom(total)
	e.Offset(above)
	return e, nil
}

// GetRankScoreAsc is not implemented.
func (l *List) GetRankScoreAsc(ctx context.Context, bs blobstore.Store, score uint64, n int) (Elements, error) {
	return nil, ErrNotImplemented
}

// GetPercentile returns the element at a given percentile.
// The percentile must be normalized to 0->1.
func (l *List) GetPercentile(ctx context.Context, bs blobstore.Store, percentile float64, radius int) (*RankedElement, error) {
	if percentile > 1 {
		percentile = 1
	}
	if percentile < 0 {
		percentile = 0
	}
	// Ensure segments are loaded.
	segs, err := l.loadSegments(ctx, bs, segsReadOnly|segsAllowUpdates)
	if err != nil {
		return nil, err
	}
	defer segs.unlock()
	l.RWMutex.RLock()
	set := l.Set
	l.RWMutex.RUnlock()

	store := blobstore.StoreWithSet(bs, set)
	segOffs, total := segs.scores.topOffsets()
	var bTotal = &big.Float{}
	bTotal = bTotal.SetInt(big.NewInt(int64(total)))
	bTotal = bTotal.Mul(bTotal, big.NewFloat(percentile))
	wantOff, _ := bTotal.Int64()
	for i, segOff := range segOffs {
		if wantOff < int64(segOff) {
			e, err := segs.scores.getElementIndex(ctx, store, i-1, int(wantOff)-segOffs[i-1], segOffs[i-1], radius)
			if err != nil {
				return nil, err
			}
			e.CalculateFromBottom(total)
			return e, nil
		}
	}
	// Must be last segment.
	segIdx := len(segOffs) - 1
	e, err := segs.scores.getElementIndex(ctx, store, segIdx, int(wantOff)-segOffs[segIdx], segOffs[segIdx], radius)
	if err != nil {
		return nil, err
	}
	e.CalculateFromBottom(total)
	return e, nil
}

func (l *List) checkSanity(ctx context.Context, bs blobstore.Store, after string, segs *segments) {
	if !sanityChecks {
		return
	}
	err := l.verify(ctx, bs, segs)
	if err != nil {
		log.Error(ctx, "checkSanity: Problem with segments", "after", after, "err", err)
	}
	err = l.verifyElements(ctx, bs, segs)
	if err != nil {
		log.Error(ctx, "checkSanity: Problem with elements", "after", after, "err", err)
	}
}

// Repair will repair the list.
// So far this is fairly brutally done by loading all segments and recreating scores and indexes.
func (l *List) Repair(ctx context.Context, bs blobstore.Store, clearIfErr bool) error {
	store := blobstore.StoreWithSet(bs, l.Set)
	stats, _ := l.Stats(ctx, bs, false)

	l.RWMutex.RLock()
	ctx = log.WithValues(ctx, "list_id", l.ID)
	l.RWMutex.RUnlock()

	segs, err := l.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
	if err != nil {
		if (err == blobstore.ErrBlobNotFound && l.scores == nil) || (clearIfErr && l.scores == nil) {
			log.Logger(ctx).Error("Repair: Could not load segments. list cleared", "list_id", l.ID)
			segs, err := l.initEmptyList(ctx, bs, segs)
			if segs != nil {
				segs.unlock()
			}
			return err
		}
		return err
	}
	defer func() {
		if segs != nil {
			segs.unlock()
		}
	}()

	// Now that no-one else is updating, make sure that no-one else reads from the list while we rebuild.
	l.RWMutex.Lock()
	segs.scores.SegmentsLock.Lock()
	segments := segs.scores.Segments
	allElements := make(Elements, 0, stats.Elements)
	for i := range segments {
		seg, err := segs.scores.elementFullIdx(ctx, store, i, true)
		if err != nil {
			log.Error(ctx, "Repair: Error loading elements", "error", err)
			continue
		}

		elems := seg.elements
		if elems.Deduplicate() {
			log.Info(ctx, "Elements de-duplicated", "before_elems", len(seg.elements), "after_elems", len(elems), "segment_id", seg.seg.ID)
		}
		allElements = append(allElements, elems...)
		seg.unlock()
	}
	// Unlock so we can re-populate.
	l.RWMutex.Unlock()
	segs.scores.SegmentsLock.Unlock()

	if allElements.Deduplicate() {
		log.Info(ctx, "Elements deduplicated")
	}
	// segs is unlocked with deferred function.
	segs, err = l.populate(ctx, bs, allElements, segs)
	return err
}

// Version of the list content.
const backupVersion = 1

// Backup serializes all content of a lists and writes it to the provided writer.
func (l *List) Backup(ctx context.Context, bs blobstore.Store, w *WriterMsgp) error {
	ctx = log.WithFn(ctx, "list_id", l.ID)
	// Ensure segments are loaded.
	// Lock updates while saving.
	ssegs, err := l.loadSegments(ctx, bs, segsReadOnly|segsLockUpdates)
	if err != nil {
		return err
	}
	defer ssegs.unlock()
	if err := w.SetVersion(backupVersion); err != nil {
		return err
	}
	l.RWMutex.RLock()
	store := blobstore.StoreWithSet(bs, l.Set)
	scores := ssegs.scores

	// Save list metadata.
	err = l.EncodeMsg(w.Writer())
	l.RWMutex.RUnlock()
	if err != nil {
		return err
	}

	if err := w.SetVersion(segmentsVersion); err != nil {
		return err
	}
	scores.SegmentsLock.RLock()
	err = scores.EncodeMsg(w.Writer())
	if err != nil {
		return err
	}

	segs := len(scores.Segments)
	scores.SegmentsLock.RUnlock()
	if err := w.SetVersion(segmentVersion); err != nil {
		return err
	}

	// Write segment count
	err = w.Writer().WriteInt(segs)
	if err != nil {
		return err
	}

	for i := 0; i < segs; i++ {
		err := func() error {
			ls, err := scores.elementFullIdx(ctx, store, i, true)
			if err != nil {
				return err
			}
			defer ls.unlock()

			// Encode segment
			err = ls.seg.EncodeMsg(w.Writer())
			if err != nil {
				return err
			}
			// Encode elements
			return ls.elements.EncodeMsg(w.Writer())
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

// RestoreList will restore a list.
// If "newID" is provided, the list is saved with its new new and segments are given new ids.
// Otherwise the exact same list is restored.
func RestoreList(ctx context.Context, bs blobstore.Store, r *ReaderMsgp, c Cache, newID *ListID) (*List, error) {
	l := newList(c, nil)
	if r.GetVersion() != backupVersion {
		log.Error(ctx, "RestoreList: backupVersion mismatch")
		return nil, ErrVersionMismatch
	}
	err := l.DecodeMsg(r.Reader())
	if err != nil {
		return nil, err
	}
	if newID != nil {
		l.ID = *newID
	}
	ctx = log.WithFn(ctx, "list_id", l.ID)
	store := blobstore.StoreWithSet(bs, l.Set)

	// Read scores
	s := Segments{}
	if r.GetVersion() != segmentsVersion {
		log.Error(ctx, "RestoreList: segmentsVersion mismatch")
		return nil, ErrVersionMismatch
	}
	err = s.DecodeMsg(r.Reader())
	if err != nil {
		return nil, err
	}
	s.cache = c
	if r.GetVersion() != segmentVersion {
		log.Error(ctx, "RestoreList: segmentVersion mismatch")
		return nil, ErrVersionMismatch
	}

	// Override segments id if new list.
	if newID != nil {
		s.ID = SegmentsID(RandString(8))
		l.Scores = s.ID
		l.Index = ""
	}
	// Read expected segment count
	segs, err := r.Reader().ReadInt()
	if err != nil {
		return nil, err
	}
	s.Segments = make([]Segment, segs)
	for i := range s.Segments {
		var seg Segment
		// First decode segment.
		err = seg.DecodeMsg(r.Reader())
		if err != nil {
			return nil, err
		}
		seg.loader = &elementLoader{}
		// decode elements
		ls := lockedSegment{
			seg:      &seg,
			elements: make(Elements, 0, seg.N),
			index:    i,
			readOnly: false,
			unlock:   func() {},
		}
		err = ls.elements.DecodeMsg(r.Reader())
		if err != nil {
			return nil, err
		}
		// Override segment id
		seg.Parent = s.ID
		err := s.replaceSegment(ctx, store, &ls)
		if err != nil {
			return nil, err
		}
	}
	l.scores = &s

	// Delete index if it exists and create a stub.
	if !l.Index.Unset() {
		index, err := l.Index.Load(ctx, store, l.cache)
		if err == nil {
			_ = index.Delete(ctx, store)
		}
	}
	nindex := NewSegments(0, false)
	nindex.IsIndex = true
	l.index = nindex
	l.Index = nindex.ID

	return l, l.Reindex(ctx, bs)
}

// ForceSplit will force list splitting/Merging.
func (l *List) ForceSplit(ctx context.Context, bs blobstore.Store) error {
	return l.split(ctx, bs, nil, true)
}

// checkSplit will check all segments if they need splitting.
// If true, a request for splitting will be sent.
// Segments must be provided.
func (l *List) checkSplit(ctx context.Context, bs blobstore.Store, segs *segments) {
	if segs == nil || segs.scores == nil || segs.index == nil {
		log.Error(ctx, "checkSplit: nil segments")
		return
	}
	scores := segs.scores
	index := segs.index

	l.RWMutex.RLock()
	splitSize := l.SplitSize
	mergeSize := l.MergeSize
	l.RWMutex.RUnlock()

	// Check if we should split using only read lock.
	shouldSplitFn := func(s *Segments) bool {
		psegn := mergeSize
		s.SegmentsLock.RLock()
		defer s.SegmentsLock.RUnlock()

		for i := range s.Segments {
			seg := &s.Segments[i]
			if seg.N > splitSize {
				return true
			}
			if i > 0 && seg.N+psegn < mergeSize {
				return true
			}
			psegn = seg.N
		}
		return false
	}
	shouldProcess := shouldSplitFn(scores) || shouldSplitFn(index)
	if shouldProcess {
		l.requestSplit(ctx)
	}
}

func (l *List) requestSplit(ctx context.Context) {
	if l.manager == nil {
		log.Error(ctx, "requestSplit: no manager set", "list_id", l.ID)
		return
	}
	select {
	case l.manager.wantSplit <- l:
		log.Info(ctx, "Requesting split")
	default:
		log.Error(ctx, "requestSplit: Unable to request split, queue full")
	}
}

// split will check all segments if they need splitting or merging and do so.
// If no segments are provided, they will be acquired from the list.
// Segments are saved if splitting is done.
func (l *List) split(ctx context.Context, bs blobstore.Store, segs *segments, merge bool) error {
	ctx = log.WithFn(ctx)
	if segs != nil && segs.readOnly {
		return errors.New("split: readonly segments supplied")
	}
	if segs != nil && !segs.updateLocked {
		return errors.New("split: segments supplied without update lock")
	}
	if segs == nil {
		var err error
		segs, err = l.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
		if err != nil {
			return err
		}
		defer segs.unlock()
	}

	l.RWMutex.RLock()
	splitSize := l.SplitSize
	mergeSize := l.MergeSize
	store := blobstore.StoreWithSet(bs, l.Set)
	l.RWMutex.RUnlock()

	log.Info(ctx, "Splitting", "split_size", splitSize, "merge_size", mergeSize)
	defer func() {
		log.Info(ctx, "Splitting done")
	}()
	scores := segs.scores
	index := segs.index

	// Element indexes to be updated.
	var updIdx []indexSegmentUpdate

	// We must retain both lists for update.
	updated := false

	for i := 0; i < len(scores.Segments); i++ {
		seg := &scores.Segments[i]
		if seg.N < splitSize {
			if !merge {
				continue
			}
			// Should not split, but maybe merge.
			err := func() error {
				if i == 0 {
					return nil
				}
				if seg.N+scores.Segments[i-1].N > mergeSize {
					return nil
				}
				// Yes, we should merge with previous.
				ls, err := scores.elementFullIdx(ctx, store, i, false)
				if err != nil {
					return err
				}
				defer ls.unlock()

				lsp, err := scores.elementFullIdx(ctx, store, i-1, false)
				if err != nil {
					return err
				}

				ch := scores.mergeWithNext(ctx, lsp, ls)
				err = scores.replaceSegment(ctx, store, lsp)
				if err != nil {
					return err
				}
				err = ls.seg.deleteElements(ctx, store)
				if err != nil {
					log.Info(ctx, "deleteElements returned error", "error", err.Error())
				}
				updIdx = append(updIdx, ch...)
				updated = true
				// Skip the merged, so we don't have conflicting indices.
				i++
				return nil
			}()
			if err != nil {
				log.Error(ctx, "Score merge returned error", "error", err.Error(), "seg_id", seg.ID, "seg_idx", i)
				return err
			}
			continue
		}
		// Split
		err := func() error {
			ls, err := scores.elementFullIdx(ctx, store, i, false)
			if err != nil {
				return err
			}
			ch, newSeg := scores.splitIdx(ctx, ls)
			if ch == nil && newSeg == nil {
				ls.unlock()
				return nil
			}
			// Save changed segments
			err = scores.replaceSegment(ctx, store, ls)
			if err != nil {
				return err
			}
			err = scores.replaceSegment(ctx, store, newSeg)
			if err != nil {
				return err
			}
			updIdx = append(updIdx, ch...)
			updated = true
			// Skip inserted
			i++
			return nil
		}()
		if err != nil {
			log.Error(ctx, "Score split returned error", "error", err.Error(), "seg_id", seg.ID, "seg_idx", i)
			return err
		}
	}
	if updated {
		// Replace locks.
		scores.idx = nil
	}

	// Update indexes of split/merged elements
	err := index.updateIndexElements(ctx, store, updIdx)
	if err != nil {
		log.Error(ctx, "Index update returned error", "error", err.Error())
		return err
	}
	err = scores.Verify(ctx, store)
	if err != nil {
		log.Error(ctx, "Scores verify error", "segments", segs.scores, "error", err.Error())
	}
	err = index.Verify(ctx, store)
	if err != nil {
		log.Error(ctx, "Index verify error", "segments", segs.index, "error", err.Error())
	}

	// Now check if indexes should be merged/split.
	for i := 0; i < len(index.Segments); i++ {
		seg := &index.Segments[i]
		if seg.N < splitSize {
			// Check if we should merge
			err := func() error {
				if i == 0 {
					return nil
				}
				if seg.N+index.Segments[i-1].N > mergeSize {
					return nil
				}
				// Yes, we should merge with previous.
				ls, err := index.elementFullIdx(ctx, store, i, false)
				if err != nil {
					return err
				}
				defer ls.unlock()

				lsp, err := index.elementFullIdx(ctx, store, i-1, false)
				if err != nil {
					return err
				}

				_ = index.mergeWithNext(ctx, lsp, ls)
				err = index.replaceSegment(ctx, store, lsp)
				if err != nil {
					return err
				}
				err = ls.seg.deleteElements(ctx, store)
				if err != nil {
					log.Info(ctx, "deleteElements returned error", "error", err.Error())
				}

				updated = true
				// Skip merged.
				i++
				return nil
			}()
			if err != nil {
				log.Error(ctx, "Index merge returned error", "error", err.Error(), "seg_id", seg.ID, "seg_idx", i)
				return err
			}
			continue
		}
		// Split segment
		err := func() error {
			ls, err := index.elementFullIdx(ctx, store, i, false)
			if err != nil {
				return err
			}

			// Split segment
			_, newSeg := index.splitIdx(ctx, ls)
			if newSeg == nil {
				ls.unlock()
				return nil
			}

			// Save updated segments.
			err = index.replaceSegment(ctx, store, newSeg)
			if err != nil {
				return err
			}
			err = index.replaceSegment(ctx, store, ls)
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			log.Error(ctx, "Index merge returned error", "error", err.Error(), "seg_id", seg.ID, "seg_idx", i)
			return err
		}
		i++
		updated = true
	}
	if !updated {
		return nil
	}

	// invalidate index
	index.idx = nil

	return l.saveSegments(ctx, bs, segs)
}
