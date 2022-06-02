//nolint
package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/blobstore/nullstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var defaultMetadataconst = map[string]string{"list": "A test list"}

func TestLists_SortedIDsAfter(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	mgr, err := rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.NewLists(nil)
	if err != nil {
		t.Fatal(err)
	}
	lists := randLists(ctx, store, set, 100, 0, 100, defaultMetadataconst)
	listIDs := rankdb.ListToListID(lists...)
	mgr.Lists.Add(lists...)

	var testSet = []struct {
		name      string
		first     rankdb.ListID
		n         int
		wantStart int
		wantLen   int
	}{
		{
			name:      "from beginning",
			first:     "",
			n:         10,
			wantStart: 0,
			wantLen:   10,
		}, {
			name:      "from first (not inclusive)",
			first:     "test-list-00000",
			n:         10,
			wantStart: 1,
			wantLen:   10,
		}, {
			name:      "first element",
			first:     "test-list-00000",
			n:         1,
			wantStart: 1,
			wantLen:   1,
		}, {
			name:      "second element",
			first:     "test-list-00001",
			n:         2,
			wantStart: 2,
			wantLen:   2,
		}, {
			name:      "after end",
			first:     "xxxxxxxxxx",
			n:         10,
			wantStart: 100,
			wantLen:   0,
		}, {
			name:      "after end - one",
			first:     "xxxxxxxxxx",
			n:         1,
			wantStart: 100,
			wantLen:   0,
		}, {
			name:      "second to last element",
			first:     "test-list-00098",
			n:         10,
			wantStart: 99,
			wantLen:   1,
		}, {
			name:      "last element",
			first:     "test-list-00099",
			n:         10,
			wantStart: 100,
			wantLen:   0,
		}, {
			name:      "last elements",
			first:     "test-list-00095",
			n:         10,
			wantStart: 96,
			wantLen:   4,
		}, {
			name:      "middle elements",
			first:     "test-list-00049",
			n:         10,
			wantStart: 50,
			wantLen:   10,
		}, {
			name:      "zero elements",
			first:     "",
			n:         0,
			wantStart: 0,
			wantLen:   0,
		}, {
			name:      "negative elements",
			first:     "",
			n:         -1,
			wantStart: 0,
			wantLen:   0,
		},
	}
	for i, tst := range testSet {
		t.Run(tst.name, func(t *testing.T) {
			gotlists, page := mgr.Lists.SortedIDsAfter(tst.first, tst.n)
			got := rankdb.ListToListID(gotlists...)
			want := listIDs[tst.wantStart : tst.wantStart+tst.wantLen]
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%v(%d): got  %v\nwant %v", tst.name, i, got, want)
			}
			if tst.n > 0 {
				//t.Logf("%+v", page)
				wantBefore := tst.wantStart
				wantAfter := len(lists) - tst.wantStart - tst.wantLen
				if page.Before != wantBefore {
					t.Errorf("%v(%d): got Before %v - want %v", tst.name, i, page.Before, wantBefore)
				}
				if page.After != wantAfter {
					t.Errorf("%v(%d): got After %v - want %v", tst.name, i, page.After, wantAfter)
				}
			}
		})
	}
}

func TestLists_SortedIDsBefore(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	mgr, err := rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.NewLists(nil)
	if err != nil {
		t.Fatal(err)
	}
	lists := randLists(ctx, store, set, 100, 0, 100, defaultMetadataconst)
	listIDs := rankdb.ListToListID(lists...)
	mgr.Lists.Add(lists...)

	var testSet = []struct {
		name      string
		first     rankdb.ListID
		n         int
		wantStart int
		wantLen   int
	}{
		{
			name:      "from beginning",
			first:     "",
			n:         10,
			wantStart: 0,
			wantLen:   0,
		}, {
			name:      "from first (not inclusive)",
			first:     "test-list-00000",
			n:         10,
			wantStart: 0,
			wantLen:   0,
		}, {
			name:      "first element",
			first:     "test-list-00000",
			n:         1,
			wantStart: 0,
			wantLen:   0,
		}, {
			name:      "second element",
			first:     "test-list-00001",
			n:         2,
			wantStart: 0,
			wantLen:   1,
		}, {
			name:      "after end",
			first:     "xxxxxxxxxx",
			n:         10,
			wantStart: 100 - 10,
			wantLen:   10,
		}, {
			name:      "second to last element",
			first:     "test-list-00098",
			n:         10,
			wantStart: 98 - 10,
			wantLen:   10,
		}, {
			name:      "last element",
			first:     "test-list-00099",
			n:         10,
			wantStart: 99 - 10,
			wantLen:   10,
		}, {
			name:      "last elements",
			first:     "test-list-00095",
			n:         10,
			wantStart: 95 - 10,
			wantLen:   10,
		}, {
			name:      "middle elements",
			first:     "test-list-00049",
			n:         10,
			wantStart: 49 - 10,
			wantLen:   10,
		}, {
			name:      "zero elements",
			first:     "test-list-00049",
			n:         0,
			wantStart: 0,
			wantLen:   0,
		}, {
			name:      "negative elements",
			first:     "test-list-00049",
			n:         -1,
			wantStart: 0,
			wantLen:   0,
		},
	}
	for i, tst := range testSet {
		t.Run(tst.name, func(t *testing.T) {
			gotlists, page := mgr.Lists.SortedIDsBefore(tst.first, tst.n)
			got := rankdb.ListToListID(gotlists...)
			want := listIDs[tst.wantStart : tst.wantStart+tst.wantLen]
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%v(%d): got  %v\nwant %v", tst.name, i, got, want)
			}
			if tst.n > 0 {
				//t.Logf("%+v", page)
				wantBefore := tst.wantStart
				wantAfter := len(lists) - tst.wantStart - tst.wantLen
				if page.Before != wantBefore {
					t.Errorf("%v(%d): got Before %v - want %v", tst.name, i, page.Before, wantBefore)
				}
				if page.After != wantAfter {
					t.Errorf("%v(%d): got After %v - want %v", tst.name, i, page.After, wantAfter)
				}
			}
		})
	}
}

func TestLists_Load(t *testing.T) {
	ctx := testCtx(t)
	store := nullstore.New()
	set := "test-set"
	mgr, err := rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.NewLists(nil)
	if err != nil {
		t.Fatal(err)
	}
	var nlists = 3000
	if testing.Short() {
		nlists /= 10
	}
	lists := randLists(ctx, store, set, nlists, 0, 100, defaultMetadataconst)
	mgr.Lists.Add(lists...)

	var buf bytes.Buffer
	err = mgr.Lists.Save(ctx, &buf)
	if err != nil {
		t.Fatal(err)
	}

	err = mgr.Lists.Load(ctx, store, buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	loaded, _ := mgr.Lists.SortedIDsAfter("", nlists+1)
	if len(loaded) != len(lists) {
		t.Fatalf("loaded list lenght mismatch, want %v, got %v", len(lists), len(loaded))
	}
	for i := range lists {
		got := loaded[i]
		want := lists[i]
		if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(rankdb.List{}), cmpopts.IgnoreUnexported(sync.RWMutex{})) {
			t.Fatal("loaded mismatch:", cmp.Diff(got, want, cmpopts.IgnoreUnexported(rankdb.List{}), cmpopts.IgnoreUnexported(sync.RWMutex{})))
		}
	}
}

func TestLists_Save(t *testing.T) {
	ctx := testCtx(t)
	store := nullstore.New()
	set := "test-set"
	mgr, err := rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.NewLists(nil)
	if err != nil {
		t.Fatal(err)
	}
	var nlists = 2000
	if testing.Short() {
		nlists /= 10
	}
	lists := randLists(ctx, store, set, nlists, 0, 100, defaultMetadataconst)
	mgr.Lists.Add(lists...)

	for i := 0; i < 10; i++ {
		go func(save rankdb.Lists) {
			var buf bytes.Buffer
			err := save.Save(ctx, &buf)
			if err != nil {
				t.Fatal(err)
			}
			var loaded rankdb.Lists
			err = loaded.Load(ctx, store, buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			n, _ := loaded.SortedIDsAfter("", nlists+1)
			if len(n) != len(lists) {
				panic("loaded mismatch")
			}
		}(mgr.Lists)
	}

	loaded, _ := mgr.Lists.SortedIDsAfter("", nlists+1)
	if len(loaded) != len(lists) {
		t.Fatalf("loaded list lenght mismatch, want %v, got %v", len(lists), len(loaded))
	}
	for i := range lists {
		got := loaded[i]
		want := lists[i]
		if !cmp.Equal(got, want, cmpopts.IgnoreUnexported(rankdb.List{}), cmpopts.IgnoreUnexported(sync.RWMutex{})) {
			t.Fatal("loaded mismatch:", cmp.Diff(got, want, cmpopts.IgnoreUnexported(rankdb.List{}), cmpopts.IgnoreUnexported(sync.RWMutex{})))
		}
	}
}

const benchListsStartLists = 5000
const benchListsMaxLists = 2560000
const benchListsMulLists = 8

func BenchmarkLists_SortedIDs(b *testing.B) {
	ctx := context.Background()
	for n := benchListsStartLists; n <= benchListsMaxLists; n *= benchListsMulLists {
		runtime.GC()
		store := nullstore.New()
		set := "test-set"
		mgr, err := rankdb.NewManager(store, set)
		if err != nil {
			b.Fatal(err)
		}
		err = mgr.NewLists(nil)
		if err != nil {
			b.Fatal(err)
		}
		var mu sync.Mutex
		var gor = runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		var ids = make([]rankdb.ListID, 0, n)
		// Read base after making slice ^, so it isn't counted.
		var basems runtime.MemStats
		runtime.ReadMemStats(&basems)

		wg.Add(gor)
		// Constructing lists takes *way* more time.
		for i := 0; i < gor; i++ {
			go func(i int) {
				defer wg.Done()
				per := n / gor
				lists := randLists(ctx, store, set, per, per*i, 0, nil)
				mgr.Lists.Add(lists...)
				lids := rankdb.ListToListID(lists...)
				mu.Lock()
				ids = append(ids, lids...)
				mu.Unlock()
			}(i)
		}
		wg.Wait()
		runtime.GC()
		if n == benchListsMaxLists {
			f, _ := os.Create("heap-profile.prof")
			pprof.WriteHeapProfile(f)
			f.Close()
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		//b.Logf("Mem: %+v", ms)
		b.ReportAllocs()
		b.ResetTimer()
		b.Run(fmt.Sprint(len(ids), "-Lists"), func(b *testing.B) {
			b.Logf("approx memory use: %.1f bytes/list", float64(ms.HeapInuse-basems.HeapInuse)/float64(n))
			for i := 0; i < b.N; i++ {
				from := ids[rand.Intn(len(ids))]
				// Get 50 lists
				const nLists = 50
				_, _ = mgr.Lists.SortedIDsAfter(from, nLists)
			}
		})
	}
}

func BenchmarkLists_Save(b *testing.B) {
	ctx := context.Background()
	for n := benchListsStartLists; n <= benchListsMaxLists; n *= benchListsMulLists {
		store := nullstore.New()
		set := "test-set"
		mgr, err := rankdb.NewManager(store, set)
		if err != nil {
			b.Fatal(err)
		}
		err = mgr.NewLists(nil)
		if err != nil {
			b.Fatal(err)
		}
		var gor = runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		wg.Add(gor)
		// Constructing lists takes *way* more time.
		for i := 0; i < gor; i++ {
			go func(i int) {
				defer wg.Done()
				per := n / gor
				lists := randLists(ctx, store, set, per, per*i, 0, nil)
				mgr.Lists.Add(lists...)
			}(i)
		}
		wg.Wait()
		var buf bytes.Buffer
		err = mgr.Lists.Save(ctx, &buf)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		b.Run(fmt.Sprint(n, "-Lists"), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(buf.Len()))
			for i := 0; i < b.N; i++ {
				_ = mgr.Lists.Save(ctx, ioutil.Discard)
			}
		})
	}
}

func BenchmarkLists_Load(b *testing.B) {
	ctx := context.Background()
	for n := benchListsStartLists; n <= benchListsMaxLists; n *= benchListsMulLists {
		store := nullstore.New()
		set := "test-set"
		mgr, err := rankdb.NewManager(store, set)
		if err != nil {
			b.Fatal(err)
		}
		err = mgr.NewLists(nil)
		if err != nil {
			b.Fatal(err)
		}
		var gor = runtime.GOMAXPROCS(0)
		var wg sync.WaitGroup
		wg.Add(gor)
		// Constructing lists takes *way* more time.
		for i := 0; i < gor; i++ {
			go func(i int) {
				defer wg.Done()
				per := n / gor
				lists := randLists(ctx, store, set, per, per*i, 0, nil)
				mgr.Lists.Add(lists...)
			}(i)
		}
		wg.Wait()
		ctx = log.WithLogger(ctx, testlogger.NewB(b))
		var buf bytes.Buffer
		err = mgr.Lists.Save(ctx, &buf)
		if err != nil {
			b.Fatal(err)
		}

		mgr.NewLists(nil)
		toload := buf.Bytes()
		b.ResetTimer()
		b.Run(fmt.Sprint(n, "-Lists"), func(b *testing.B) {
			ctx := log.WithLogger(ctx, testlogger.NewB(b))
			b.ReportAllocs()
			b.SetBytes(int64(len(toload)))
			for i := 0; i < b.N; i++ {
				err = mgr.Lists.Load(ctx, store, toload)
				if err != nil {
					b.Fatal(err)
				}
				if i == 0 && n == benchListsMaxLists {
					f, _ := os.Create("heap-profile-loaded.prof")
					pprof.WriteHeapProfile(f)
					f.Close()
				}
			}
		})
	}
}

func randLists(ctx context.Context, store blobstore.Store, set string, n, offset int, elements int, metadata map[string]string) []*rankdb.List {
	res := make([]*rankdb.List, n)
	var md map[string]string
	for i := range res {
		if metadata != nil {
			md = make(map[string]string, len(metadata)+2)
			for k, v := range metadata {
				md[k] = v
			}
			md["random"] = rankdb.RandString(5)
			md["n"] = strconv.Itoa(i)
		}
		elems := randElements(elements, int64(i+offset))
		id := fmt.Sprintf("test-list-%05d", i+offset)
		l, err := rankdb.NewList(ctx, rankdb.ListID(id), set, store,
			rankdb.WithListOption.LoadIndex(false),
			rankdb.WithListOption.MergeSplitSize(10, 20),
			rankdb.WithListOption.Metadata(md),
			rankdb.WithListOption.Populate(elems),
			rankdb.WithListOption.Cache(nil),
		)
		if elements == 0 {
			l.ReleaseSegments(ctx)
		}
		if err != nil {
			panic(err)
		}
		res[i] = l
	}
	runtime.GC()
	return res
}
