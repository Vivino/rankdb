package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

//go:generate msgp $GOFILE

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Elements is a collection of elements.
// When this type is used elements can be assumed to be sorted.
type Elements []Element

// IndexElements contains elements that are used to index other elements.
type IndexElements struct {
	Elements
}

// ElementIds is a slice of collection ids.
type ElementIDs []ElementID

// NewElements converts an (unsorted) slice of elements into
// a sorted slice of elements.
// Duplicate elements are removed.
func NewElements(e []Element) Elements {
	res := Elements(e)
	// Deduplicate returns sorted results.
	res.Deduplicate()
	return res
}

// Clone all element in list.
// Payloads are optionally cloned.
func (l Elements) Clone(payloads bool) Elements {
	res := make(Elements, len(l))
	copy(res, l)
	if !payloads {
		return res
	}
	for i, e := range res {
		if len(e.Payload) == 0 {
			continue
		}
		p := make([]byte, len(e.Payload))
		copy(p, e.Payload)
		res[i].Payload = p
	}
	return res
}

// HasDuplicates returns true if elements contains duplicates.
func (l *Elements) HasDuplicates() error {
	nids := len(l.ids())
	nelements := len(*l)
	if nids != nelements {
		return fmt.Errorf("duplicate ids. %d unique ids, %d elements", nids, nelements)
	}
	return nil
}

// Deduplicate will remove entries with duplicate Element IDs.
// If duplicates are found, the element with latest update time is kept.
// If update time is equal, the one with the highest score is kept.
// The element list is always re-sorted.
func (l *Elements) Deduplicate() (changed bool) {
	lst := *l
	// We re-sort the slice to avoid allocating a map/copy.
	sort.Slice(lst, lst.IDSorter())
	for i := 1; i < len(lst); {
		prev := lst[i-1]
		e := lst[i]
		if prev.ID != e.ID {
			i++
			continue
		}
		changed = true
		j := i
		// If updated time equal, use highest score
		if prev.Updated == e.Updated {
			if prev.Score > e.Score {
				j = i - 1
			}
		} else {
			// Preserve latest updated.
			if prev.Updated < e.Updated {
				j = i - 1
			}
		}
		// Delete element j
		lst = append(lst[:j], lst[j+1:]...)
		// Re-test at i.
	}
	// Resort
	sort.Slice(lst, lst.Sorter())
	*l = lst
	return changed
}

// Ranked converts elements to ranked elements.
// The offset from the top of the list of the first element
// and total number of elements in the list must be provided.
func (l Elements) Ranked(topOffset, total int) RankedElements {
	res := make(RankedElements, 0, len(l))
	for i, e := range l {
		re := RankedElement{Element: e, FromTop: topOffset + i}
		re.CalculateFromBottom(total)
		res = append(res, re)
	}
	return res
}

// Sort the elements.
// Returns whether a change was made.
func (l *Elements) Sort() (changed bool) {
	lst := *l
	if sort.SliceIsSorted(lst, lst.Sorter()) {
		return false
	}
	if len(lst) < 25000 {
		sort.Slice(lst, lst.Sorter())
		return true
	}
	// Split + sort
	split := l.SplitSize(2500)
	for i, s := range split {
		s.Sort()
		split[i] = s
	}

	// Merge
	for len(split) > 1 {
		var wg sync.WaitGroup
		next := (len(split) + 1) / 2
		wg.Add(next)
		for i := 0; i < next; i++ {
			first := i * 2
			second := i*2 + 1
			if second >= len(split) {
				split[i] = split[first]
				wg.Done()
				continue
			}
			fst := split[first]
			sec := split[second]
			go func(i int, fst, sec Elements) {
				defer wg.Done()
				fst.Merge(sec, true)
				split[i] = fst
			}(i, fst, sec)
		}
		wg.Wait()
		split = split[:next]
	}
	if sanityChecks {
		if !sort.SliceIsSorted(split[0], split[0].Sorter()) {
			panic("wtf?")
		}
	}
	*l = split[0]
	return true
}

// Find element in list.
// Returns ErrNotFound if not found.
func (l Elements) Find(id ElementID) (*Element, error) {
	for i := range l {
		if l[i].ID == id {
			e := l[i]
			return &e, nil
		}
	}
	return nil, ErrNotFound
}

// FindIdx returns index of element.
func (l Elements) FindIdx(id ElementID) (int, error) {
	for i := range l {
		if l[i].ID == id {
			return i, nil
		}
	}
	return 0, ErrNotFound
}

// FindScoreIdx returns index of first element that matches score.
func (l Elements) FindScoreIdx(score uint64) (int, error) {
	for i := range l {
		if l[i].Score == score {
			return i, nil
		}
	}
	return 0, ErrNotFound
}

// Insert element in list.
// Returns index of inserted item.
func (l *Elements) Insert(e Element) int {
	lst := *l
	for i, le := range lst {
		if le.Above(e) {
			continue
		}
		// Element should be placed at i
		lst = append(lst, Element{})
		copy(lst[i+1:], lst[i:])
		lst[i] = e

		*l = lst
		return i
	}
	// Element should be last.
	lst = append(lst, e)
	*l = lst
	return len(lst) - 1
}

// Merge other elements into this list.
// Provided elements must be sorted.
// Provide information on whether l is shared with other slices. Use true if in doubt.
// Does not deduplicate on IDs, use MergeDeduplicate for this (approximately 1 order of magnitude slower).
func (l *Elements) Merge(ins Elements, sliced bool) {
	if len(ins) == 0 {
		return
	}
	lst := *l

	// For big lists, we create a new and selectively insert.
	if sliced || len(ins)+len(lst) > 1000 || len(ins) > 10 || cap(lst) < len(ins)+len(lst) {
		total := len(lst) + len(ins)
		dst := make([]Element, 0, total)
		for len(dst) < total {
			if len(lst) == 0 {
				dst = append(dst, ins...)
				ins = nil
				continue
			}
			if len(ins) == 0 {
				dst = append(dst, lst...)
				lst = nil
				continue
			}
			a, b := &lst[0], &ins[0]
			if a.aboveP(b) {
				dst = append(dst, *a)
				lst = lst[1:]
				continue
			}
			dst = append(dst, *b)
			ins = ins[1:]
			continue
		}
		*l = dst
		return
	}

	nexti := 0
	next := &ins[nexti]
	for i := 0; i < len(lst); {
		if lst[i].aboveP(next) {
			i++
			continue
		}
		lst = append(lst, Element{})
		copy(lst[i+1:], lst[i:])
		lst[i] = *next
		nexti++
		if nexti == len(ins) {
			*l = lst
			return
		}
		next = &ins[nexti]
	}

	// Append remaining elements to end.
	lst = append(lst, ins[nexti:]...)
	*l = lst
}

// ids returns all element ids as map.
func (l Elements) ids() map[ElementID]struct{} {
	m := make(map[ElementID]struct{}, len(l))
	for _, e := range l {
		m[e.ID] = struct{}{}
	}
	return m
}

// MergeDeduplicate will merge other elements into this list.
// IDs are checked for duplicates and inserted elements overwrite existing.
// ins is used, so content is overwritten.
// Each list must be de-duplicated.
// It is not a strict requirement that lists are sorted.
func (l *Elements) MergeDeduplicate(ins Elements) {
	if len(ins) == 0 {
		return
	}
	// Get new ids to be inserted.
	insert := ins.ids()

	// Add all from existing not in insert lst.
	for _, e := range *l {
		if _, ok := insert[e.ID]; !ok {
			ins = append(ins, e)
		}
	}
	// Re-sort.
	sort.Slice(ins, ins.Sorter())
	*l = ins
	return
}

// MinMax returns the minimum and maximum values of the elements.
// If no elements are provided the entire range is returned.
func (l Elements) MinMax() (min, max uint64, minTie, maxTie uint32) {
	if len(l) == 0 {
		return 0, math.MaxUint64, 0, math.MaxUint32
	}

	max, maxTie = l[0].Score, l[0].TieBreaker
	last := l[len(l)-1]
	min, minTie = last.Score, last.TieBreaker
	return
}

// Add element if it does not exist.
// If element exists, it is updated.
func (l *Elements) Add(e Element) (*Rank, error) {
	if e.Updated == 0 {
		e.Updated = uint32(time.Now().Unix())
	}
	_, err := l.FindIdx(e.ID)
	if err == nil {
		return l.Update(e)
	}

	return l.idxRank(l.Insert(e)), nil
}

// idxRank returns the Rank representation of index.
// Callers should verify that idx is valid.
func (l Elements) idxRank(idx int) *Rank {
	if idx >= len(l) {
		panic(fmt.Sprintf("idx(%d) > len(%d)", idx, len(l)))
	}
	if idx < 0 {
		panic("idx < 0")
	}
	return &Rank{FromTop: idx, FromBottom: len(l) - idx - 1}
}

// Update will delete the previous element in the list with the same ID
// and insert the new element.
// Returns ErrNotFound if element could not be found.
func (l *Elements) Update(e Element) (*Rank, error) {
	if e.Updated == 0 {
		e.Updated = uint32(time.Now().Unix())
	}
	err := l.Delete(e.ID)
	if err != nil {
		return nil, err
	}
	return l.idxRank(l.Insert(e)), nil
}

// Delete element from list.
// Returns ErrNotFound if element could not be found.
func (l *Elements) Delete(id ElementID) error {
	lst := *l
	for i, e := range lst {
		if e.ID == id {
			lst = append(lst[:i], lst[i+1:]...)
			*l = lst
			return nil
		}
	}
	return ErrNotFound
}

// SplitSize will split the elements into a slice of elements.
func (l Elements) SplitSize(inEach int) []Elements {
	if len(l) <= inEach {
		return []Elements{l}
	}
	// Rounded up number of new slices
	n := (len(l) + inEach - 1) / inEach
	// Elements in each slice
	elems := (len(l) + n - 1) / n

	start := 0
	res := make([]Elements, 0, n)
	for i := 0; i < n; i++ {
		end := start + elems
		if end > len(l) {
			end = len(l)
		}
		for end > 0 && end < len(l) {
			// We much have a difference in score and tiebreaker to split.
			if l[end-1].Score == l[end].Score && l[end-1].TieBreaker == l[end].TieBreaker {
				end++
				continue
			}
			break
		}
		if start == end {
			break
		}
		res = append(res, l[start:end])
		start = end
	}
	return res
}

// UpdateTime will update time of all elements to the provided time.
func (l *Elements) UpdateTime(t time.Time) {
	lst := *l
	unx := uint32(t.Unix())
	for i := range lst {
		lst[i].Updated = unx
	}
	*l = lst
}

// firstElementsWithScore returns the first element that has each of the
// supplied scores.
// If no element exist with the supplied score, the element below is returned.
// Scores must be sorted descending.
func (e Elements) FirstElementsWithScore(scores []uint64) Elements {
	if len(scores) == 0 {
		return nil
	}
	res := make(Elements, 0, len(scores))
	top := scores[0]
	for i := range e {
		if e[i].Score <= top {
			res = append(res, e[i])
			if len(scores) <= 1 {
				break
			}
			scores = scores[1:]
			top = scores[0]
		}
	}
	return res
}

// minMaxUnsorted provides the smallest and biggest score
// on unsorted elements.
func (l Elements) minMaxUnsorted() (min, max uint64) {
	min = math.MaxUint64
	max = 0
	for _, v := range l {
		if v.Score > max {
			max = v.Score
		}
		if v.Score < min {
			min = v.Score
		}
	}
	return min, max
}

// ElementIDs returns element ids as ranked elements,
// where score is the element id and payload is the segment to which they belong.
func (l Elements) ElementIDs(id SegmentID) IndexElements {
	res := IndexElements{Elements: make(Elements, 0, len(l))}
	for _, elem := range l {
		res.Elements = append(res.Elements, elem.AsIndex(id).Element)
	}
	res.Sort()
	return res
}

// Sorter returns a sorter that will sort the elements by score, descending.
// If score is equal, the tiebreaker is used, descending.
// If tiebreaker is equal, earliest update time wins.
// Final tiebreaker is element ID, where lowest ID gets first.
func (l Elements) Sorter() func(i, j int) bool {
	return func(i, j int) bool {
		a, b := &l[i], &l[j]
		return a.aboveP(b)
	}
}

// IDSorter returns a sorting function that sorts by ID.
func (l Elements) IDSorter() func(i, j int) bool {
	return func(i, j int) bool {
		return l[i].ID < l[j].ID
	}
}

// IDSorter returns a sorting function that sorts by ID.
func (l Elements) IDs() ElementIDs {
	ids := make(ElementIDs, len(l))
	for i, v := range l {
		ids[i] = v.ID
	}
	return ids
}

// String returns a readable string representation of the elements.
func (l Elements) String() string {
	b, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

// Map returns the element ids as a map.
func (e ElementIDs) Map() map[ElementID]struct{} {
	m := make(map[ElementID]struct{}, len(e))
	for _, elem := range e {
		m[elem] = struct{}{}
	}
	return m
}

// Deduplicate and sort the element ids.
func (e *ElementIDs) Deduplicate() {
	lst := *e
	m := make(map[ElementID]struct{}, len(lst))
	for _, elem := range lst {
		m[elem] = struct{}{}
	}
	lst = make(ElementIDs, 0, len(m))
	for k := range m {
		lst = append(lst, k)
	}
	*e = lst
	e.Sort()
}

// Sort the element ids.
func (e *ElementIDs) Sort() {
	lst := *e
	sort.Slice(lst, func(i, j int) bool {
		return lst[i] > lst[j]
	})
	*e = lst
}

// NotIn returns the elements not in b.
func (e ElementIDs) NotIn(b ElementIDs) ElementIDs {
	remain := e.Map()
	for _, id := range b {
		delete(remain, id)
	}
	n := make(ElementIDs, 0, len(remain))
	for k := range remain {
		n = append(n, k)
	}
	return n
}

// Overlap returns the overlap between the Element IDs.
func (e ElementIDs) Overlap(b ElementIDs) ElementIDs {
	em := e.Map()
	res := make(ElementIDs, 0)
	for _, id := range b {
		if _, ok := em[id]; ok {
			res = append(res, id)
		}
	}
	return res
}

// AsScore returns the element ids as slice of uint64.
func (e ElementIDs) AsScore() []uint64 {
	var dst = make([]uint64, len(e))
	for i, id := range e {
		dst[i] = uint64(id)
	}
	return dst
}

// SegmentSorter returns a sorted that orders by segment first and score secondly.
func (e IndexElements) SegmentSorter() func(i, j int) bool {
	return func(i, j int) bool {
		a, b := e.Elements[i], e.Elements[j]
		if a.TieBreaker != b.TieBreaker {
			return a.TieBreaker > b.TieBreaker
		}
		return a.Score > b.Score
	}
}
