package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/readahead"
	"github.com/tinylib/msgp/msgp"
)

// Lists contains an id indexed
type Lists struct {
	lists *sync.Map

	// Global cache for segments
	cache Cache

	// wantSplit is for lists that requests splitting.
	manager *Manager
}

// ByID returns a list by ID.
// This is an O(1) operation.
func (l Lists) ByID(id ListID) (*List, bool) {
	lst, ok := l.lists.Load(id)
	if !ok {
		return nil, false
	}
	list, ok := lst.(*List)
	if !ok {
		return nil, false
	}
	return list, true
}

// ByIDs returns lists by ID.
// This is an O(n) operation.
// Lists that are not found are returned as nil.
func (l Lists) ByIDs(ids ...ListID) []*List {
	res := make([]*List, len(ids))
	for i, id := range ids {
		lst, ok := l.lists.Load(id)
		if !ok {
			continue
		}
		list, ok := lst.(*List)
		if !ok {
			continue
		}
		res[i] = list
	}
	return res
}

const compressLists = true

type writeCounter struct {
	w io.Writer
	n int64
}

func (w *writeCounter) Write(b []byte) (int, error) {
	w.n += int64(len(b))
	return w.w.Write(b)
}

const (
	compressionBase uint8 = 7
	compressionNone       = compressionBase
	compressionZstd       = 1<<4 | compressionBase
)

// Save the lists.
func (l Lists) Save(ctx context.Context, w io.Writer) error {
	wc := &writeCounter{w: w}
	n := 0
	defer func() {
		log.Info(ctx, "Saved lists", "lists", n, "size", wc.n)
	}()
	wm := NewWriterMsg()
	defer wm.Close()
	wm.ReplaceWriter(wc)

	c := compressionNone
	if compressLists {
		c = compressionZstd
		enc, err := zstd.NewWriter(wc)
		if err != nil {
			return err
		}
		wm.ReplaceWriter(enc)
		defer enc.Close()
	}
	_, err := wc.Write([]byte{c})
	if err != nil {
		return err
	}

	defer wm.Flush()
	err = wm.SetVersion(listVersion)
	if err != nil {
		return err
	}
	msg := wm.Writer()

	// Lists are simply a stream of msgp objects.
	l.lists.Range(func(key, value interface{}) bool {
		list, ok := value.(*List)
		if !ok {
			log.Error(ctx, "unexpected list value type", "actual_type", fmt.Sprintf("%T", value))
			return true
		}
		n++
		list.RWMutex.RLock()
		defer list.RWMutex.RUnlock()
		err = list.EncodeMsg(msg)
		return err == nil
	})
	if err != nil {
		return err
	}
	// Write "true" to end.
	err = msg.WriteBool(true)
	if err != nil {
		return err
	}
	wm.Flush()
	return nil
}

// Load the lists. Overwrites all existing lists.
func (l *Lists) Load(ctx context.Context, bs blobstore.Store, b []byte) error {
	var lazyLoad []func()
	if len(b) == 0 {
		return ErrNotFound
	}
	start := time.Now()
	var rm *ReaderMsgp
	cID := b[0]
	if cID&compressionBase == compressionBase {
		// Skip the byte we just read.
		b = b[1:]
		switch cID {
		case compressionNone:
			rm = NewReaderMsgp(b)
		case compressionZstd:
			dec, err := zstd.NewReader(bytes.NewBuffer(b))
			if err != nil {
				return err
			}
			defer dec.Close()
			rm = NewReaderMsgpReader(dec)
		}
	} else {
		// Fall back... can be removed when we have converted.
		dec := flate.NewReader(bytes.NewBuffer(b))
		defer dec.Close()
		ra := readahead.NewReader(dec)
		defer ra.Close()
		rm = NewReaderMsgpReader(ra)
	}
	if rm == nil {
		return errors.New("nil reader")
	}
	defer rm.Close()
	if rm.GetVersion() != listVersion {
		return ErrVersionMismatch
	}
	n := 0
	l.lists = &sync.Map{}
	msg := rm.Reader()
	t, err := msg.NextType()
	for t != msgp.BoolType && err == nil {
		lst := newList(l.cache, l.manager)
		err = lst.DecodeMsg(msg)
		if err != nil {
			return err
		}
		l.Add(lst)
		n++

		t, err = msg.NextType()
		if lst.LoadIndex {
			lazyLoad = append(lazyLoad, func() {
				ctx := log.WithValues(ctx, "list_id", lst.ID)
				segs, err := lst.loadSegments(ctx, bs, segsWritable|segsLockUpdates)
				if err != nil {
					log.Error(ctx, "Loading segments:", "error", err)
					return
				}
				defer segs.unlock()
				err = lst.verify(ctx, bs, segs)
				if err != nil {
					log.Error(ctx, "Verifying list:", "error", err)
					return
				}
			})
		}
	}
	log.Info(ctx, "Loaded lists", "list count", n, "size", len(b), "duration", time.Since(start))
	if len(lazyLoad) > 0 {
		go func() {
			start := time.Now()
			log.Info(ctx, "Starting loading segments", "list count", len(lazyLoad))
			for _, fn := range lazyLoad {
				fn()
			}
			log.Info(ctx, "Finished loading segments", "list count", len(lazyLoad), "duration", time.Since(start))
		}()
	}
	return err
}

// Exists returns the ids that exist.
func (l *Lists) Exists(ids ...ListID) []ListID {
	var exists []ListID
	for _, id := range ids {
		if _, ok := l.lists.Load(id); ok {
			exists = append(exists, id)
		}
	}
	return exists
}

// Prune will release all in-memory elements and for lists
// that has LoadIndex set to false.
func (l *Lists) Prune(ctx context.Context) error {
	var err error = nil
	l.lists.Range(func(key, value interface{}) bool {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return false
		default:
		}
		list, ok := value.(*List)
		if !ok {
			log.Error(ctx, "Wrong list type:", "type", fmt.Sprintf("%T", value))
			return true
		}
		list.RWMutex.RLock()
		li := list.LoadIndex
		list.RWMutex.RUnlock()
		// If LoadIndex is specified we only prune elements.
		if !li {
			list.ReleaseSegments(ctx)
		}
		return true
	})
	return err
}

// Add or overwrite list.
func (l *Lists) Add(lists ...*List) {
	for _, lst := range lists {
		lst.cache = l.cache
		lst.manager = l.manager
		l.lists.Store(lst.ID, lst)
	}
}

// MatchAll will return all lists where all metadata fields match the provided.
// A numbers of sets can also be provided. All lists from sets are included.
// If q and sets both have 0 entries nothing is returned.
// Results are returned in random order.
func (l *Lists) MatchAll(q map[string]string, sets []string) []ListID {
	var res = make([]ListID, 0)
	if len(q) == 0 && len(sets) == 0 {
		return res
	}
	s := make(map[string]struct{}, len(sets))
	for _, set := range sets {
		s[set] = struct{}{}
	}
	l.lists.Range(func(key, value interface{}) bool {
		list, ok := value.(*List)
		if !ok {
			log.Error(context.Background(), "Wrong list type:", "type", fmt.Sprintf("%T", value))
			return true
		}
		if len(s) > 0 {
			if _, ok := s[list.Set]; ok {
				res = append(res, list.ID)
				return true
			}
		}
		if list.Metadata == nil || len(q) == 0 {
			// No metadata, no match
			return true
		}
		for k, v := range q {
			if list.Metadata[k] != v {
				return true
			}
		}
		// All matched
		res = append(res, list.ID)
		return true
	})
	return res
}

// Delete a list.
// Does not delete underlying data.
func (l *Lists) Delete(id ListID) {
	l.lists.Delete(id)
}

// PageInfo contains information about the number
// of lists/elements before and after the ones in the list.
type PageInfo struct {
	Before, After int
}

// SortedIDsAfter returns a number of lists sorted by ID.
// The first entry returned will be the one following the provided ID.
// Provide an empty ID to get from the first list.
// If n <= 0 an empty list and page info will be returned.
func (l *Lists) SortedIDsAfter(from ListID, n int) ([]*List, PageInfo) {
	var page PageInfo
	if n <= 0 {
		return []*List{}, page
	}
	// We need at least two elements in s
	var cropOne bool
	if n == 1 {
		cropOne = true
		n = 2
	}
	s := make(sort.StringSlice, 0, n)
	var lastElement ListID

	// We do selective insert to avoid sorting the entire list.
	l.lists.Range(func(key, value interface{}) bool {
		if id, ok := key.(ListID); ok {
			// Only append if after from offset.
			if id > from {
				if len(s) == n {
					page.After++
					if id > lastElement {
						return true
					}
					s.Sort()
					// Overwrite biggest.
					lastElement = ListID(s[n-2])
					s[n-1] = string(id)
					if id > lastElement {
						lastElement = id
					}
					return true
				}
				s = append(s, string(id))
				if id > lastElement {
					lastElement = id
				}
			} else {
				page.Before++
			}
		}
		return true
	})
	s.Sort()
	if cropOne {
		if len(s) > 1 {
			s = s[:1]
			page.After++
		}
	}
	res := make([]*List, 0, len(s))
	for len(res) < n && len(s) > 0 {
		lst, ok := l.ByID(ListID(s[0]))
		if ok {
			res = append(res, lst)
		}
		s = s[1:]
	}
	return res, page
}

// All returns all list ids, unsorted.
func (l *Lists) All() ListIDs {
	s := make(ListIDs, 0, 100)

	// We do selective insert to avoid sorting the entire list.
	l.lists.Range(func(key, value interface{}) bool {
		if id, ok := key.(ListID); ok {
			s = append(s, id)
		}
		return true
	})
	return s
}

// SortedIDsBefore returns a number of lists sorted by ID.
// The last entry in the list will be the one preceding the provided ID.
// Provide an empty ID to get from the first list.
// If n <= 0 an empty list and page info will be returned.
func (l *Lists) SortedIDsBefore(to ListID, n int) ([]*List, PageInfo) {
	var page PageInfo
	if n <= 0 {
		return []*List{}, page
	}
	// We need at least two elements in s
	var cropOne bool
	if n == 1 {
		cropOne = true
		n = 2
	}
	s := make(sort.StringSlice, 0, n)
	var firstElement ListID

	// We do selective insert to avoid sorting the entire list.
	l.lists.Range(func(key, value interface{}) bool {
		if id, ok := key.(ListID); ok {
			// Only append if after from offset.
			if id < to {
				if len(s) == n {
					page.Before++
					if id < firstElement {
						return true
					}
					s.Sort()
					// Overwrite smallest
					firstElement = ListID(s[1])
					s[0] = string(id)
					if id < firstElement {
						firstElement = id
					}
					return true
				}
				s = append(s, string(id))
				if id < firstElement {
					firstElement = id
				}
			} else {
				page.After++
			}
		}
		return true
	})
	s.Sort()
	if cropOne {
		if len(s) > 1 {
			page.After++
			s = s[:1]
		}
	}
	res := make([]*List, 0, len(s))
	for len(res) < n && len(s) > 0 {
		lst, ok := l.ByID(ListID(s[0]))
		if ok {
			res = append(res, lst)
		}
		s = s[1:]
	}
	return res, page
}

// ListToListID returns the IDs of the supplied lists.
// Elements that are nil are ignored.
func ListToListID(lists ...*List) []ListID {
	res := make([]ListID, 0, len(lists))
	for _, list := range lists {
		if list != nil {
			res = append(res, list.ID)
			continue
		}
		res = append(res, "<nil>")
	}
	return res
}
