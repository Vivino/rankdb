//nolint:errcheck
package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/stretchr/testify/assert"
)

type counter uint64

func (c *counter) next() rankdb.ElementID {
	cnt := *c
	cnt++
	*c = cnt
	return rankdb.ElementID(cnt)
}

func timeUnix(t time.Time) uint32 {
	return uint32(t.Unix())
}

func TestElements_Add(t *testing.T) {
	lst := rankdb.Elements{}
	cnt := counter(1000)
	now := time.Now()
	_, err := lst.Add(rankdb.Element{ID: cnt.next(), Score: 1000})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 100})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 100000})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 10})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 100})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1, Updated: timeUnix(now)})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1, Updated: timeUnix(now.Add(-time.Hour))})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1, Updated: timeUnix(now.Add(time.Hour))})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1, Updated: timeUnix(now)})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next() - 100, Score: 500, TieBreaker: 1, Updated: timeUnix(now)})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next() - 100, Score: 500, TieBreaker: 1, Updated: timeUnix(now)})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next() - 200, Score: 500, TieBreaker: 1, Updated: timeUnix(now)})
	assert.NoError(t, err)
	if len(lst) < 5 {
		t.Fatal("Want elements in list, found ", len(lst))
	}
	if !sort.SliceIsSorted(lst, lst.Sorter()) {
		t.Fatalf("Slice was not sorted: \n%v", lst)
	}
}

func TestElements_Delete(t *testing.T) {
	orig := rankdb.NewElements(randElements(1000))
	t.Run("middle", func(t *testing.T) {
		testElements_DeleteAt(t, 500, orig)
	})
	t.Run("beginning", func(t *testing.T) {
		testElements_DeleteAt(t, 0, orig)
	})
	t.Run("end", func(t *testing.T) {
		testElements_DeleteAt(t, len(orig)-1, orig)
	})
}

func testElements_DeleteAt(t *testing.T, n int, orig rankdb.Elements) {
	testOn := orig.Clone(true)
	assert.NoError(t, testOn.Delete(orig[n].ID))
	if len(orig)-1 != len(testOn) {
		t.Fatal("did not remove element")
	}
	_, err := testOn.Find(orig[n].ID)
	if err != rankdb.ErrNotFound {
		t.Fatal("did not remove element")
	}
	for i, e := range orig[0:n] {
		if !reflect.DeepEqual(testOn[i], e) {
			t.Fatalf("element %d mismatch (before)", i)
		}
	}
	for i, e := range orig[n+1:] {
		if !reflect.DeepEqual(testOn[i+n], e) {
			t.Fatalf("element %d mismatch (after)", i)
		}
	}
}

func TestElements_Deduplicate(t *testing.T) {
	nnow := time.Now()
	now := timeUnix(nnow)
	before := timeUnix(nnow.Add(-time.Hour))
	after := timeUnix(nnow.Add(time.Hour))
	testSet := []struct {
		in   rankdb.Elements
		out  rankdb.Elements
		want bool
	}{{
		in:   []rankdb.Element{{ID: 101, Updated: now}, {ID: 102, Updated: now}, {ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
		want: false,
	}, {
		in:   []rankdb.Element{{ID: 100, Updated: now}, {ID: 102, Updated: now}, {ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 100, Updated: now}, {ID: 102, Updated: now}},
		want: true,
	}, {
		in:   []rankdb.Element{{ID: 102, Updated: now}, {ID: 102, Updated: now}, {ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 100, Updated: now}, {ID: 102, Updated: now}},
		want: true,
	}, {
		in:   []rankdb.Element{{ID: 102, Updated: now}, {ID: 102, Updated: now}, {ID: 102, Updated: now}},
		out:  []rankdb.Element{{ID: 102, Updated: now}},
		want: true,
	}, {
		in:   []rankdb.Element{},
		out:  []rankdb.Element{},
		want: false,
	}, {
		in:   []rankdb.Element{{ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 100, Updated: now}},
		want: false,
	}, {
		in:   []rankdb.Element{{ID: 100, Updated: before}, {ID: 102, Updated: now}, {ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 100, Updated: now}, {ID: 102, Updated: now}},
		want: true,
	}, {
		in:   []rankdb.Element{{ID: 100, Updated: after}, {ID: 102, Updated: now}, {ID: 100, Updated: now}},
		out:  []rankdb.Element{{ID: 102, Updated: now}, {ID: 100, Updated: after}},
		want: true,
	}}

	for i, test := range testSet {
		t.Run(fmt.Sprint("Test", i), func(t *testing.T) {
			cp := test.in.Clone(false)
			got := cp.Deduplicate()
			if !reflect.DeepEqual(cp, test.out) {
				t.Errorf("test %d. Got %v, want %v", i, cp, test.out)
			} else if got != test.want {
				t.Errorf("test %d, '%+v'. Got changed:%t, want %t", i, test.in, got, test.want)
			}
		})
	}
}

func TestElements_Merge(t *testing.T) {
	now := timeUnix(time.Now())
	testSet := []struct {
		a, b rankdb.Elements
		want rankdb.Elements
	}{{
		a:    []rankdb.Element{{ID: 102, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 101, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 101, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 102, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 101, Updated: now}, {ID: 102, Updated: now}},
		b:    []rankdb.Element{{ID: 100, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
		b:    []rankdb.Element{},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{},
		b:    []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{},
		b:    []rankdb.Element{},
		want: []rankdb.Element{},
	}, {
		a:    []rankdb.Element{{ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 100, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 100, Updated: now}},
	}}

	for i, test := range testSet {
		t.Run(fmt.Sprint("Test", i), func(t *testing.T) {
			a := test.a.Clone(true)
			a.Sort()
			b := test.b.Clone(true)
			b.Sort()
			a.Merge(b, false)
			if !reflect.DeepEqual(test.want, a) {
				t.Errorf("test %d. Got %v, want %v", i, a, test.want)
			}
		})
	}
}

func TestElements_MergeBig(t *testing.T) {
	n := 20000
	org := rankdb.NewElements(randElements(n))
	ins := rankdb.NewElements(randElements(n/10, 10))
	out := org
	out.Merge(ins, false)
	if len(out) < len(org)+len(ins)-1 {
		t.Fatalf("length too short %d+%d=%d -> %d", len(org), len(ins), len(org)+len(ins), len(out))
	}
	if !sort.SliceIsSorted(out, out.Sorter()) {
		t.Fatal("Output was not sorted.")
	}
}

func TestElements_MergeSmall(t *testing.T) {
	n := 200
	org := rankdb.NewElements(randElements(n))
	ins := rankdb.NewElements(randElements(n/10, 10))
	out := org
	out.Merge(ins, false)
	if len(out) < len(org)+len(ins)-1 {
		t.Fatalf("length too short %d+%d=%d -> %d", len(org), len(ins), len(org)+len(ins), len(out))
	}
	if !sort.SliceIsSorted(out, out.Sorter()) {
		t.Fatal("Output was not sorted.")
	}
}

func TestElements_MergeDeduplicate(t *testing.T) {
	now := timeUnix(time.Now())
	testSet := []struct {
		a, b rankdb.Elements
		want rankdb.Elements
	}{{
		a:    []rankdb.Element{{ID: 102, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 101, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 101, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 102, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 101, Updated: now}, {ID: 102, Updated: now}},
		b:    []rankdb.Element{{ID: 100, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
		b:    []rankdb.Element{},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{},
		b:    []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
		want: []rankdb.Element{{ID: 100, Updated: now}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{},
		b:    []rankdb.Element{},
		want: []rankdb.Element{},
	}, {
		a:    []rankdb.Element{{ID: 100, Updated: now, Score: 50}},
		b:    []rankdb.Element{{ID: 100, Updated: now, Score: 5}},
		want: []rankdb.Element{{ID: 100, Updated: now, Score: 5}},
	}, {
		a:    []rankdb.Element{{ID: 102, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 101, Updated: now, Score: 5}, {ID: 100, Updated: now, Score: 5}},
		want: []rankdb.Element{{ID: 100, Updated: now, Score: 5}, {ID: 101, Updated: now, Score: 5}, {ID: 102, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 102, Updated: now}, {ID: 101, Updated: now}, {ID: 100, Updated: now}},
		b:    []rankdb.Element{{ID: 102, Updated: now, Score: 5}},
		want: []rankdb.Element{{ID: 102, Updated: now, Score: 5}, {ID: 100, Updated: now}, {ID: 101, Updated: now}},
	}, {
		a:    []rankdb.Element{{ID: 101, Updated: now}, {ID: 100, Updated: now}, {ID: 102, Updated: now}},
		b:    []rankdb.Element{{ID: 100, Updated: now, Score: 5}},
		want: []rankdb.Element{{ID: 100, Updated: now, Score: 5}, {ID: 101, Updated: now}, {ID: 102, Updated: now}},
	}}

	for i, test := range testSet {
		t.Run(fmt.Sprint("Test", i), func(t *testing.T) {
			a := test.a.Clone(true)
			a.Deduplicate()
			b := test.b.Clone(true)
			b.Deduplicate()
			a.MergeDeduplicate(b)
			if !reflect.DeepEqual(test.want, a) {
				t.Errorf("test %d. Got %v, want %v", i, a, test.want)
			}
		})
	}
}

func TestIndexElements_SegmentSorter(t *testing.T) {
	lst := rankdb.Elements{}
	cnt := counter(1000)
	now := timeUnix(time.Now())
	_, err := lst.Add(rankdb.Element{ID: cnt.next(), Score: 1000})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 100})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 100000})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 10})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 100})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next(), Score: 500, TieBreaker: 1})
	assert.NoError(t, err)
	_, err = lst.Add(rankdb.Element{ID: cnt.next() - 200, Score: 500, TieBreaker: 2, Updated: now})
	assert.NoError(t, err)
	idx := rankdb.IndexElements{Elements: lst}
	sort.Slice(idx.Elements, idx.SegmentSorter())
	wantE := []struct {
		id    rankdb.ElementID
		score uint64
		tie   uint32
	}{
		{id: 1005, tie: 100, score: 500},
		{id: 1004, tie: 10, score: 500},
		{id: 807, tie: 2, score: 500},
		{id: 1006, tie: 1, score: 500},
		{id: 1003, tie: 0, score: 100000},
		{id: 1001, tie: 0, score: 1000},
		{id: 1002, tie: 0, score: 100},
	}
	for i := range idx.Elements {
		got := idx.Elements[i]
		want := wantE[i]
		if got.ID != want.id {
			t.Errorf("ID element %d, want %v, got %v", i, want.id, got.ID)
		}
		if got.TieBreaker != want.tie {
			t.Errorf("ID element %d, want %v, got %v", i, want.tie, got.TieBreaker)
		}
		if got.Score != want.score {
			t.Errorf("ID element %d, want %v, got %v", i, want.score, got.Score)
		}
	}
}

const (
	benchMaxElements   = 10000
	benchStartElements = 10
	benchMulElements   = 5
)

// Insert n unsorted elements in list.
func BenchmarkElements_Insert_All(b *testing.B) {
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := randElements(n)
		out := make(rankdb.Elements, 0, n)
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				out = out[:0]
				for _, e := range in {
					out.Insert(e)
				}
			}
		})
	}
}

// Insert a random elements in list of n elements.
func BenchmarkElements_Insert_Single(b *testing.B) {
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		out := make(rankdb.Elements, 0, n)
		onefrom := rankdb.NewElements(randElements(n))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset + setup
				out = out[:n]
				elem := onefrom[rng.Int()%n] // rng + mod not insignificant
				copy(out, in)
				out.Insert(elem)
			}
		})
	}
}

// Insert a random elements in list of n elements.
func BenchmarkElements_Merge(b *testing.B) {
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		ins := rankdb.NewElements(randElements(n, 10))
		out := make(rankdb.Elements, 0, n*2)
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset + setup
				out = out[:n]
				copy(out, in)
				out.Merge(ins, false)
			}
		})
	}
}

// Insert a random elements in list of n elements.
func BenchmarkElements_MergeDeduplicate(b *testing.B) {
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		input := rankdb.NewElements(randElements(n))
		insert := rankdb.NewElements(randElements(n, 10))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset + setup
				a := input.Clone(false)
				bb := insert.Clone(false)
				a.MergeDeduplicate(bb)
				// Allow for unlikely duplicates
				if len(a) <= len(input)+len(insert)-10 {
					b.Fatal("wtf?")
				}
			}
		})
	}
}

// Create list with n unsorted elements.
func BenchmarkElements_NewElements(b *testing.B) {
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := randElements(n)
		out := make(rankdb.Elements, n)
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset (takes extra time)
				copy(out, in)
				_ = rankdb.NewElements(out)
			}
		})
	}
}

// Create list with n unsorted elements.
func BenchmarkElements_Deduplicate(b *testing.B) {
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := randElements(n)
		// Create duplicates
		for i := 0; i < n/benchMulElements; i++ {
			// https://github.com/golang/go/issues/22174
			a := rng.Intn(len(in))
			b := rng.Intn(len(in))
			in[a].ID = in[b].ID
		}
		out := make(rankdb.Elements, n)
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Reset (takes extra time)
				copy(out, in)
				out.Deduplicate()
			}
		})
	}
}

// Find random element in n elements.
func BenchmarkElements_Find(b *testing.B) {
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				needle := in[rng.Int()%n] // rng + mod not insignificant
				in.Find(needle.ID)
			}
		})
	}
}

// Update random element in n elements.
func BenchmarkElements_Update(b *testing.B) {
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				needle := in[rng.Int()%n] // rng + mod not insignificant
				needle.Score += rng.Uint64()
				in.Update(needle)
			}
		})
	}
}

// Delete+Insert random element in n elements.
func BenchmarkElements_DeleteInsert(b *testing.B) {
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	for n := benchStartElements; n <= benchMaxElements; n *= benchMulElements {
		in := rankdb.NewElements(randElements(n))
		b.Run(fmt.Sprint(n, " Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				needle := in[rng.Int()%n] // rng + mod not insignificant
				in.Delete(needle.ID)
				in.Insert(needle)
			}
		})
	}
}

func randElements(n int, seed ...int64) rankdb.Elements {
	if n == 0 {
		return rankdb.Elements{}
	}
	rng := rand.New(rand.NewSource(0x1337beefc0cac01a))
	if len(seed) > 0 {
		rng = rand.New(rand.NewSource(seed[0]))
	}
	res := make(rankdb.Elements, n)
	for i := range res {
		res[i] = rankdb.Element{
			Score:      uint64(rng.Uint32()),
			TieBreaker: rng.Uint32(),
			Updated:    timeUnix(time.Now().Add(-time.Millisecond * time.Duration(rng.Int63n(int64(time.Hour)*24*365)))),
			ID:         rankdb.ElementID(rng.Uint64()),
			Payload:    []byte(`{"value":"` + rankdb.RandString(5) + `", "type": "user-list"}`),
		}
	}
	return res
}
