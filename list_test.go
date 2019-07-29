package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/memstore"
	"github.com/Vivino/rankdb/blobstore/nullstore"
	"github.com/Vivino/rankdb/log"
	"github.com/Vivino/rankdb/log/testlogger"
	"github.com/google/go-cmp/cmp"
	lru "github.com/hashicorp/golang-lru"
)

func TestNewList(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	store.Debug(true)
	set := "test-set"
	l, err := rankdb.NewList(ctx, "test-list", set, store,
		rankdb.WithListOption.LoadIndex(false),
		rankdb.WithListOption.MergeSplitSize(10, 20),
		rankdb.WithListOption.Metadata(map[string]string{"list": "A test list", "hasmetadata": "yes!"}),
	)
	if err != nil {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
		t.Fatal(err)
	}
	if l.LoadIndex {
		t.Error("wanted Loadindex to be false, it was true")
	}
	if l.Metadata == nil {
		t.Fatal("No metadata found :(")
	}
	want := "A test list"
	got := l.Metadata["list"]
	if got != want {
		t.Errorf("want metadata %q, got %q", want, got)
	}
	if l.Scores.Unset() {
		t.Error("Scores unset")
	}
	if l.Index.Unset() {
		t.Error("Index unset")
	}
	if l.SplitSize != 20 {
		t.Error("Splitsize want 20, got ", l.SplitSize)
	}
	if l.MergeSize != 10 {
		t.Error("Mergesize want 10, got", l.MergeSize)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	l.ReleaseSegments(ctx)
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	err = l.Populate(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(l)
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func TestNewList_fail(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	_, err := rankdb.NewList(ctx, "test-list", set, store,
		rankdb.WithListOption.MergeSplitSize(20, 20))
	if err == nil {
		t.Fatal("want error because mergesize > splitsize")
	}

	_, err = rankdb.NewList(ctx, "test-list", "", store)
	if err == nil {
		t.Fatal("want error because set is empty")
	}

	_, err = rankdb.NewList(ctx, "test-list", "test-set", nil)
	if err == nil {
		t.Fatal("want error because storage is nil")
	}

	bs := blobstore.NewTestStore(store)
	bs.FailSet(blobstore.TestAlwaysFail)
	_, err = rankdb.NewList(ctx, "test-list", "test-set", bs)
	if err == nil {
		t.Fatal("want error because storage is returns error")
	}
	if err != blobstore.ErrTestIntentional {
		t.Fatal("want blobstore.ErrTestIntentional, got ", err)
	}
}

func TestNewList_Populate(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	lst := randElements(111)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(20, 40),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !l.LoadIndex {
		t.Error("wanted Loadindex to be true, it was false")
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
	if files != 10 {
		t.Errorf("expected 10 files, got %d", files)
	}
}

func TestNewList_PopulateLazy(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	lazy, err := blobstore.NewLazySaver(store, blobstore.WithLazySaveOption.Logger(log.Logger(ctx)))
	if err != nil {
		t.Fatal(err)
	}
	set := "test-set"
	lst := randElements(111)

	l, err := rankdb.NewList(ctx, "test-list", set, lazy, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(20, 40),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !l.LoadIndex {
		t.Error("wanted Loadindex to be true, it was false")
	}
	err = l.Verify(ctx, lazy)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, lazy)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	lazy.Shutdown()
	lazy, err = blobstore.NewLazySaver(store, blobstore.WithLazySaveOption.Logger(log.Logger(ctx)))
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, lazy)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, lazy)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
	if files != 10 {
		t.Errorf("expected 10 files, got %d", files)
	}
}

func TestNewListClone(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	lst := randElements(613)

	l0, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(20, 40),
		rankdb.WithListOption.Populate(lst),
		rankdb.WithListOption.Cache(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	l, err := rankdb.NewList(ctx, "test-list-clone", set, store, rankdb.WithListOption.LoadIndex(false),
		rankdb.WithListOption.MergeSplitSize(50, 200),
		rankdb.WithListOption.Clone(l0),
		rankdb.WithListOption.Cache(nil),
	)

	// Delete all of original
	err = l0.DeleteAll(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	if l.LoadIndex {
		t.Error("wanted Loadindex to be false, it was true")
	}
	if l.Scores == l0.Scores {
		t.Error("Scores ID should not match")
	}
	if l.Index == l0.Index {
		t.Error("Index ID should not match")
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := l.Stats(ctx, store, true)
	if err != nil {
		t.Error(err)
	}
	t.Log(stats)
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func TestNewList_PopulateBig(t *testing.T) {
	nElems := 250000
	if testing.Short() {
		nElems = 5000
		return
	}
	ctx := testCtx(t)
	mstore := memstore.NewMemStore()
	//store, err := mstore, error(nil)
	store, err := blobstore.NewLazySaver(mstore,
		blobstore.WithLazySaveOption.Verbose(false),
		blobstore.WithLazySaveOption.Logger(log.Logger(ctx)),
		blobstore.WithLazySaveOption.Items(50, 40),
	)
	if err != nil {
		t.Fatal(err)
	}
	set := "test-set"
	lst := randElements(nElems)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(50+nElems/260, 60+nElems/250),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !l.LoadIndex {
		t.Error("wanted Loadindex to be true, it was false")
	}
	stats, _ := l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	lst = randElements(nElems, 0x13371337)
	_, err = l.UpdateElements(ctx, store, lst, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ = l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	err = l.ForceSplit(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.ForceSplit(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ = l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	store.Shutdown()
	files, size := mstore.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func TestNewList_PopulateBigReload(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	var n = 250000
	if testing.Short() {
		n = 5000
	}
	lst := randElements(n)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(50+n/300, 60+n/200),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ := l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	l.ReleaseSegments(ctx)
	lst = randElements(n, 0x13371337)
	_, err = l.UpdateElements(ctx, store, lst, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ = l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	l.ReleaseSegments(ctx)

	err = l.ForceSplit(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	stats, _ = l.Stats(ctx, store, false)
	t.Logf("%+v", *stats)
	l.ReleaseSegments(ctx)
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// store.Shutdown()
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func testCtx(t *testing.T) context.Context {
	return log.WithLogger(context.Background(), testlogger.New(t))
}

func TestNewList_Reindex(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	var n = 500000
	if testing.Short() {
		n = 10000
	}
	lst := randElements(n)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(500, 2000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(l)
	err = l.Reindex(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func TestNewList_ReindexEmpty(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(500, 2000),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("created ok:", l)
	err = l.Reindex(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("after reindex:", l)
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")

	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
}

func TestList_Insert(t *testing.T) {
	var base = 50000
	var insert = 100000
	if testing.Short() {
		base /= 10
		insert /= 100
	}
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	lst := randElements(base, 1337)

	l, err := rankdb.NewList(ctx, "test-list", set, store,
		rankdb.WithListOption.MergeSplitSize(500, 2000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	lst = randElements(insert, 3117)
	lst.Sort()
	err = l.Insert(ctx, store, lst)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	length, err := l.Len(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// -10 for extremely unlikely chance of collision
	if length < base+insert-10 {
		t.Fatalf("len %d, want at least %d", length, base+insert-10)
	}
	t.Log(l, "len:", length)
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func TestList_GetElements(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	var nelements = 50000
	if testing.Short() {
		nelements /= 10
	}
	lst := randSortedElements(nelements)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(1000, 3000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	var loadn = 10000
	if testing.Short() {
		loadn /= 10
	}
	var toload rankdb.ElementIDs
	for i := 0; i < loadn; i++ {
		toload = append(toload, lst[rand.Intn(len(lst))].ID)
	}
	unique := toload.Map()
	start := time.Now()
	e, err := l.GetElements(ctx, store, toload, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Took", time.Since(start))
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	if len(e) != len(unique) {
		t.Fatalf("want %d results, got %d", len(unique), len(e))
	}
	var idx = make(map[rankdb.ElementID]struct{}, len(unique))
	for i := range e {
		idx[e[i].ID] = struct{}{}
	}
	for i := range toload {
		if _, ok := idx[toload[i]]; !ok {
			t.Errorf("Did not find element with id %v", toload[i])
		}
	}
}

func TestList_InsertParallel(t *testing.T) {
	var base = 50000
	var insert = 100000

	if testing.Short() {
		base = 5000
		insert = 1000
	}
	const insertp = 10
	var inEach = insert / insertp

	ctx := testCtx(t)
	mstore := memstore.NewMemStore()
	store, err := blobstore.NewMaxSizeStore(mstore, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	set := "test-set"
	lst := randSortedElements(base, 1337)

	l, err := rankdb.NewList(ctx, "test-list", set, store,
		rankdb.WithListOption.MergeSplitSize(500, 2000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}

	lst = randElements(insert, 3117)
	lst.Sort()
	var wg sync.WaitGroup
	wg.Add(insertp)
	for i := 0; i < insertp; i++ {
		go func(i int) {
			defer wg.Done()
			ins := lst[i*inEach : (i+1)*inEach]
			err := l.Insert(ctx, store, ins)
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	length, err := l.Len(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	// -10 for extremely unlikely chance of collision
	if length < base+insert-10 {
		t.Fatalf("len %d, want at least %d", length, base+insert-10)
	}
	if !t.Failed() {
		t.Log(l, "len:", length)
		t.Log("Blobs stored:\n" + strings.Join(mstore.Dir(set), "\n"))
		files, size := mstore.Usage(set)
		t.Log("Storage used:", size, "bytes in", files, "blobs.")
	}
}

func TestListRepair(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 50000
	var updaten = 1000
	if testing.Short() {
		n /= 10
		updaten /= 10
	}
	lst := randSortedElements(50000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Insert the same items several times.
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		toUpdate = append(toUpdate, lst[rand.Intn(len(lst))])
	}

	for i := 0; i < 5; i++ {
		// Will fail if sanity checks are enabled.
		err := l.Insert(ctx, store, rankdb.NewElements(toUpdate))
		if err != nil {
			// This will fail if sanity checks are enabled:
			t.Fatal(err)
		}
	}
	err = l.VerifyElements(ctx, store)
	if err == nil {
		// This will fail if sanity checks are enabled:
		t.Fatal("test setup failed, want invalid list")
	}
	err = l.Repair(ctx, store, false)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal("want fixed list, got", err)
	}

	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestListBackup(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 50000
	if testing.Short() {
		n /= 10
	}
	lst := randSortedElements(50000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}

	org, err := l.Stats(ctx, store, true)
	if err != nil {
		t.Fatal(err)
	}

	w := rankdb.NewWriterMsg()
	err = l.Backup(ctx, store, w)
	if err != nil {
		t.Fatal(err)
	}

	// Reset store to ensure no leaks
	store = memstore.NewMemStore()

	// Read back from blob.
	b := w.Buffer().Bytes()
	t.Log("List bytes", len(b))
	r := rankdb.NewReaderMsgp(b)
	l2, err := rankdb.RestoreList(ctx, store, r, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = l2.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l2.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	nstat, err := l2.Stats(ctx, store, true)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(org, nstat) {
		t.Fatalf("(org) %+v != (new) %+v", *org, *nstat)
	}
}

func TestListRestore(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 50000
	if testing.Short() {
		n /= 10
	}
	lst := randSortedElements(50000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}

	org, err := l.Stats(ctx, store, true)
	if err != nil {
		t.Fatal(err)
	}

	w := rankdb.NewWriterMsg()
	err = l.Backup(ctx, store, w)
	if err != nil {
		t.Fatal(err)
	}

	// Read back from blob.
	b := w.Buffer().Bytes()
	t.Logf("List bytes: %v, %.02f bytes/element (uncompressed)", len(b), float64(len(b))/float64(n))
	r := rankdb.NewReaderMsgp(b)
	newID := rankdb.ListID("new-test-list")
	l2, err := rankdb.RestoreList(ctx, store, r, nil, &newID)
	if err != nil {
		t.Fatal(err)
	}
	err = l2.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l2.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	nstat, err := l2.Stats(ctx, store, true)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(org, nstat) {
		t.Fatalf("(org) %+v != (new) %+v", *org, *nstat)
	}
	if l2.ID != newID {
		t.Errorf("new id was not set %v != %v", l2.ID, newID)
	}

	// Test that original list is unaffected
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	org2, err := l.Stats(ctx, store, true)
	if err != nil {
		t.Fatal(err)
	}
	if !cmp.Equal(org, org2) {
		t.Fatalf("(org) %+v != (org2) %+v", *org, *org2)
	}

	if l.Index == l2.Index {
		t.Error("indexes id match")
	}
	if l.Scores == l2.Scores {
		t.Error("scores id  match")
	}
}

func TestListUpdateElements(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 50000
	var updaten = 1000
	if testing.Short() {
		n /= 10
		updaten /= 10
	}
	lst := randSortedElements(50000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	toUpdate.Deduplicate()
	toUpdate.Sort()

	e, err := l.UpdateElements(ctx, store, rankdb.NewElements(toUpdate), 0, true)
	if err != nil {
		t.Fatal(err)
	}

	if len(e) != len(toUpdate) {
		t.Fatalf("want %d results, got %d", len(toUpdate), len(e))
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i])
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestListUpdateElementsEmpty(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 5000
	var updaten = 1000
	if testing.Short() {
		n /= 10
		updaten /= 10
	}
	lst := randSortedElements(5000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 3000),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.UpdateElements(ctx, store, lst, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(lst) {
		t.Fatalf("%d (want) != %d (got)", len(lst), len(res))
	}
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	toUpdate.Deduplicate()
	toUpdate.Sort()

	e, err := l.UpdateElements(ctx, store, rankdb.NewElements(toUpdate), 0, true)
	if err != nil {
		t.Fatal(err)
	}

	if len(e) != len(toUpdate) {
		t.Fatalf("want %d results, got %d", len(toUpdate), len(e))
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}

	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i])
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestListForceSplit(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	n := 5000
	var updaten = 1000
	if testing.Short() {
		n /= 10
		updaten /= 10
	}
	lst := randSortedElements(5000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 1500),
	)
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.UpdateElements(ctx, store, lst, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(lst) {
		t.Fatalf("%d (want) != %d (got)", len(lst), len(res))
	}
	err = l.VerifyUnlocked(ctx, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}

	// Run list splitting for some iterations.
	for i := 0; i < 5; i++ {
		err = l.ForceSplit(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.Verify(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.VerifyUnlocked(ctx, time.Second*1)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force to merge.
	l.MergeSize = 3000
	l.SplitSize = 5000
	for i := 0; i < 5; i++ {
		err = l.ForceSplit(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.Verify(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.VerifyUnlocked(ctx, time.Second*1)
		if err != nil {
			t.Fatal(err)
		}
	}
	l.MergeSize = 1000
	l.SplitSize = 2000

	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	toUpdate.Deduplicate()
	toUpdate.Sort()

	e, err := l.UpdateElements(ctx, store, rankdb.NewElements(toUpdate), 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(e) != len(toUpdate) {
		t.Fatalf("want %d results, got %d", len(toUpdate), len(e))
	}
	err = l.VerifyUnlocked(ctx, time.Second*1)
	if err != nil {
		t.Fatal(err)
	}

	// Run list splitting for some iterations.
	for i := 0; i < 5; i++ {
		err = l.ForceSplit(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.Verify(ctx, store)
		if err != nil {
			t.Fatal(err)
		}
		err = l.VerifyUnlocked(ctx, time.Second*1)
		if err != nil {
			t.Fatal(err)
		}
	}

	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i])
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestList_UpdateElementsConcurrentlySmall(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	var nelements = 100000
	if testing.Short() {
		nelements = 10000
	}
	lst := randSortedElements(nelements)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	var updaten = 5000
	if testing.Short() {
		updaten /= 10
	}
	const updatep = 8
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	toUpdate.Deduplicate()
	toUpdate.Sort()

	updaten = len(toUpdate)
	var inEach = updaten / updatep

	var wg sync.WaitGroup
	wg.Add(updatep)
	for i := 0; i < updatep; i++ {
		go func(i int) {
			defer wg.Done()
			upd := toUpdate[i*inEach : (i+1)*inEach]
			ctx := log.WithValues(ctx, "routine", i)
			e, err := l.UpdateElements(ctx, store, rankdb.NewElements(upd), 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(e) != len(upd) {
				t.Fatalf("want %d results, got %d", len(upd), len(e))
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	e, err := l.GetElements(ctx, store, toUpdate.IDs(), 0)
	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	if len(e) != len(toUpdate) {
		t.Fatalf("Did not get all the wanted results: got %d of %d", len(e), len(toUpdate))
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i].ID)
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestList_UpdateElementsConcurrentlySmallOverlapping(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	var nelements = 100000
	if testing.Short() {
		nelements = 10000
	}
	lst := randSortedElements(nelements)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	var updaten = 5000
	if testing.Short() {
		updaten /= 10
	}
	const updatep = 8
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	updaten = len(toUpdate)
	var inEach = updaten / updatep

	var wg sync.WaitGroup
	wg.Add(updatep)
	for i := 0; i < updatep; i++ {
		go func(i int) {
			defer wg.Done()
			upd := toUpdate[i*inEach : (i+1)*inEach].Clone(false)
			upd.Deduplicate()
			ctx := log.WithValues(ctx, "routine", i)
			e, err := l.UpdateElements(ctx, store, rankdb.NewElements(upd), 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(e) != len(upd) {
				t.Fatalf("want %d results, got %d", len(upd), len(e))
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	toUpdate.Deduplicate()
	e, err := l.GetElements(ctx, store, toUpdate.IDs(), 0)
	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	if len(e) != len(toUpdate) {
		t.Fatalf("Did not get all the wanted results: got %d of %d", len(e), len(toUpdate))
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i].ID)
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestList_UpdateElementsConcurrentlyBig(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	var nelements = 200000
	if testing.Short() {
		nelements = 10000
	}
	lst := randSortedElements(nelements)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(1000, 3000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	var updaten = 10000
	if testing.Short() {
		updaten /= 10
	}
	const updatep = 8
	var toUpdate rankdb.Elements
	for i := 0; i < updaten; i++ {
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}
	toUpdate.Deduplicate()
	toUpdate.Sort()

	updaten = len(toUpdate)
	var inEach = updaten / updatep

	var wg sync.WaitGroup
	wg.Add(updatep)
	for i := 0; i < updatep; i++ {
		go func(i int) {
			defer wg.Done()
			upd := toUpdate[i*inEach : (i+1)*inEach]
			ctx := log.WithValues(ctx, "routine", i)
			e, err := l.UpdateElements(ctx, store, rankdb.NewElements(upd), 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(e) != len(upd) {
				t.Fatalf("want %d results, got %d", len(upd), len(e))
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	e, err := l.GetElements(ctx, store, toUpdate.IDs(), 0)
	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	if len(e) != len(toUpdate) {
		t.Fatalf("Did not get all the wanted results: got %d of %d", len(e), len(toUpdate))
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i].ID)
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
	}
}

func TestList_UpdateElementsConcurrentlyBigOverlapping(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	mstore := memstore.NewMemStore()
	store, _ := blobstore.NewMaxSizeStore(mstore, 1<<20)
	set := "test-set"
	var nelements = 100000
	if testing.Short() {
		nelements = 10000
	}
	lst := randSortedElements(nelements)
	cache, _ := lru.NewARC(1000)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(1000, 3000),
		rankdb.WithListOption.Populate(lst),
		rankdb.WithListOption.Cache(cache),
	)
	if err != nil {
		t.Fatal(err)
	}
	var updaten = 30000
	if testing.Short() {
		updaten /= 10
	}
	const updatep = 8
	var toUpdate rankdb.Elements
	toInsert := randElements(updaten/2, 89997)
	for i := 0; i < updaten; i++ {
		if i&1 == 1 {
			toUpdate = append(toUpdate, toInsert[i/2])
			continue
		}
		elem := lst[rand.Intn(len(lst))]
		elem.Score = uint64(i) * 1000
		toUpdate = append(toUpdate, elem)
	}

	updaten = len(toUpdate)
	var inEach = updaten / updatep

	var wg sync.WaitGroup
	wg.Add(updatep)
	for i := 0; i < updatep; i++ {
		go func(i int) {
			defer wg.Done()
			upd := toUpdate[i*inEach : (i+1)*inEach].Clone(false)
			upd.Deduplicate()
			ctx := log.WithValues(ctx, "routine", i)
			e, err := l.UpdateElements(ctx, store, rankdb.NewElements(upd), 0, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(e) != len(upd) {
				t.Fatalf("want %d results, got %d", len(upd), len(e))
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	toUpdate.Deduplicate()
	e, err := l.GetElements(ctx, store, toUpdate.IDs(), 0)
	var idx = make(map[rankdb.ElementID]rankdb.RankedElement, updaten)
	for _, elem := range e {
		idx[elem.ID] = elem
	}
	if len(e) != len(toUpdate) {
		t.Fatalf("Did not get all the wanted results: got %d of %d", len(e), len(toUpdate))
	}
	for i := range toUpdate {
		if _, ok := idx[toUpdate[i].ID]; !ok {
			t.Errorf("Did not find element with id %v", toUpdate[i].ID)
		}
	}
	if !t.Failed() {
		t.Log("Blobs stored:\n" + strings.Join(mstore.Dir(set), "\n"))
	}
}

func TestList_GetElementsCache(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const num = 50000
	lst := randSortedElements(num)
	cache, err := lru.NewARC(num / 100)
	if err != nil {
		t.Fatal(err)
	}

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
		rankdb.WithListOption.Cache(cache),
	)
	if err != nil {
		t.Fatal(err)
	}
	const loadn = 100
	var toload []rankdb.ElementID
	for i := 0; i < loadn; i++ {
		toload = append(toload, lst[rand.Intn(len(lst))].ID)
	}

	for i := 0; i < 10; i++ {
		e, err := l.GetElements(ctx, store, toload, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(e) < loadn-5 {
			t.Fatalf("want approx %d results, got %d", loadn, len(e))
		}
		var idx = make(map[rankdb.ElementID]struct{}, loadn)
		for i := range e {
			idx[e[i].ID] = struct{}{}
		}
		for i := range toload {
			if _, ok := idx[toload[i]]; !ok {
				t.Errorf("Did not find element with id %v", toload[i])
			}
		}
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	if cache.Len() < 10 {
		t.Log("Blobs stored:\n" + strings.Join(store.Dir(set), "\n"))
		t.Fatalf("expected some elements in cache, found only %d", cache.Len())
	}
	t.Log("elements in cache", cache.Len())
}

func TestList_GetElementsLoad(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const num = 5000
	lst := randSortedElements(num)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
		rankdb.WithListOption.Cache(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	const loadn = 1000
	var toload rankdb.ElementIDs
	for i := 0; i < loadn; i++ {
		toload = append(toload, lst[rand.Intn(len(lst))].ID)
	}
	toload.Deduplicate()
	l.ReleaseSegments(ctx)
	e, err := l.GetElements(ctx, store, toload, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(e) < len(toload) {
		t.Fatalf("want %d results, got %d", len(toload), len(e))
	}
	var idx = make(map[rankdb.ElementID]struct{}, loadn)
	for i := range e {
		idx[e[i].ID] = struct{}{}
	}
	for i := range toload {
		if _, ok := idx[toload[i]]; !ok {
			t.Errorf("Did not find element with id %v", toload[i])
		}
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
}

func TestList_GetElements_Rank(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	lst := listWithScores(10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(2, 5),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}

	var toload []rankdb.ElementID
	for _, e := range lst {
		toload = append(toload, e.ID)
	}

	e, err := l.GetElements(ctx, store, toload, 2)
	if err != nil {
		t.Fatal(err)
	}

	if len(e) != len(toload) {
		t.Fatalf("want %d results, got %d", len(toload), len(e))
	}

	type validate struct {
		top    int
		bottom int
		above  []uint64
		below  []uint64
	}

	want := map[uint64]validate{
		500: {top: 0, bottom: 13, above: []uint64(nil), below: []uint64{400, 300}},
		400: {top: 1, bottom: 12, above: []uint64{500}, below: []uint64{300, 200}},
		300: {top: 2, bottom: 11, above: []uint64{400, 500}, below: []uint64{200, 100}},
		200: {top: 3, bottom: 10, above: []uint64{300, 400}, below: []uint64{100, 90}},
		100: {top: 4, bottom: 9, above: []uint64{200, 300}, below: []uint64{90, 80}},
		90:  {top: 5, bottom: 8, above: []uint64{100, 200}, below: []uint64{80, 70}},
		80:  {top: 6, bottom: 7, above: []uint64{90, 100}, below: []uint64{70, 60}},
		70:  {top: 7, bottom: 6, above: []uint64{80, 90}, below: []uint64{60, 50}},
		60:  {top: 8, bottom: 5, above: []uint64{70, 80}, below: []uint64{50, 40}},
		50:  {top: 9, bottom: 4, above: []uint64{60, 70}, below: []uint64{40, 30}},
		40:  {top: 10, bottom: 3, above: []uint64{50, 60}, below: []uint64{30, 20}},
		30:  {top: 11, bottom: 2, above: []uint64{40, 50}, below: []uint64{20, 10}},
		20:  {top: 12, bottom: 1, above: []uint64{30, 40}, below: []uint64{10}},
		10:  {top: 13, bottom: 0, above: []uint64{20, 30}, below: []uint64(nil)},
	}

	for _, tc := range e {
		t.Run(fmt.Sprintf("ElementScore %v:", tc.Score), func(t *testing.T) {
			w := want[tc.Score]
			prefix := fmt.Sprintf("Element with score %v:", tc.Score)
			if w.top != tc.FromTop {
				t.Errorf(prefix+"want FromTop %v, got %v", w.top, tc.FromTop)
			}
			if w.bottom != tc.FromBottom {
				t.Errorf(prefix+"want FromBottom %v, got %v", w.bottom, tc.FromBottom)
			}
			if len(tc.Above) != len(w.above) {
				t.Errorf(prefix+"want %v elements above, got %v", len(w.above), len(tc.Above))
				return
			}
			if len(tc.Below) != len(w.below) {
				t.Errorf(prefix+"want %v elements above, got %v", len(w.below), len(tc.Below))
				return
			}
			for i := range tc.Above {
				if w.above[i] != tc.Above[i].Score {
					t.Errorf(prefix+"want elements score #%v, %v above, got %v", i, w.above[i], tc.Above[i].Score)
				}
			}
			for i := range tc.Below {
				if w.below[i] != tc.Below[i].Score {
					t.Errorf(prefix+"want elements score %v below, got %v", w.below[i], tc.Below[i].Score)
				}
			}
			if !t.Failed() {
				t.Logf("Element with score %v ok", tc.Score)
			}
		})
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		t.Log("Complete result:", e)
	}
}

func TestList_GetRankTop(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const nelems = 50
	var scores []uint64
	for i := 0; i < nelems; i++ {
		scores = append(scores, uint64(i*10))
	}
	lst := listWithScores(scores...)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(2, 5),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < nelems; start++ {
		const elements = 10
		e, err := l.GetRankTop(ctx, store, start, elements)
		if err != nil {
			t.Errorf("offset %d: %v", start, err)
		}
		want := lst[start:]
		if len(want) > elements {
			want = want[:elements]
		}
		if len(e) != len(want) {
			t.Errorf("wrong number of results, want %v, got %v", len(want), len(e))
			continue
		}
		for i := range e {
			if e[i].ID != want[i].ID {
				t.Errorf("element %v was wrong, want id %v, got %v", i, want[i].ID, e[i].ID)
			}
		}
		if t.Failed() {
			t.Log("got", e)
			t.Log("want", want)
			t.Fatal("Stopping at start offset: ", start)
		}
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.GetRankTop(ctx, store, nelems, 5)
	if err != rankdb.ErrOffsetOutOfBounds {
		t.Logf("want %v, got %v", rankdb.ErrOffsetOutOfBounds, err)
	}
}

func TestList_GetRankBottom(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const nelems = 50
	var scores []uint64
	for i := 0; i < nelems; i++ {
		scores = append(scores, uint64(i*10))
	}
	lst := listWithScores(scores...)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(2, 5),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < nelems; start++ {
		const elements = 10
		e, err := l.GetRankBottom(ctx, store, start, elements)
		if err != nil {
			t.Errorf("offset %d: %v", start, err)
		}
		want := lst[nelems-start-1:]
		if len(want) > elements {
			want = want[:elements]
		}
		if len(e) != len(want) {
			t.Errorf("wrong number of results, want %v, got %v", len(want), len(e))
			continue
		}
		for i := range e {
			if e[i].ID != want[i].ID {
				t.Errorf("element %v was wrong, want id %v, got %v", i, want[i].ID, e[i].ID)
			}
		}
		if t.Failed() {
			t.Log("got", e)
			t.Log("want", want)
			t.Fatal("Stopping at start offset: ", start)
		}
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	_, err = l.GetRankTop(ctx, store, nelems, 5)
	if err != rankdb.ErrOffsetOutOfBounds {
		t.Logf("want %v, got %v", rankdb.ErrOffsetOutOfBounds, err)
	}
}

func TestList_GetRankScoreDesc(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const nelems = 50
	var scores []uint64
	for i := 0; i < nelems; i++ {
		scores = append(scores, uint64(i*10+5))
	}
	lst := listWithScores(scores...)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(2, 5),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < nelems; start++ {
		const elements = 10
		e, err := l.GetRankScoreDesc(ctx, store, lst[start].Score, elements)
		if err != nil {
			t.Errorf("offset %d: %v", start, err)
		}
		want := lst[start:]
		if len(want) > elements {
			want = want[:elements]
		}
		if len(e) != len(want) {
			t.Errorf("wrong number of results, want %v, got %v", len(want), len(e))
			continue
		}
		for i := range e {
			gote := e[i]
			wante := want[i]
			if gote.ID != wante.ID {
				t.Errorf("element %v was wrong, want id %v, got %v", i, want[i], e[i])
			}
			if gote.FromTop != i+start {
				t.Errorf("element %v was wrong, want fromtop %v, got %v", i, i+start, gote.FromTop)
			}
			if gote.FromBottom+gote.FromTop != nelems-1 {
				t.Errorf("element %v was wrong, want frombottom %v, got %v", i, nelems-i-start-1, gote.FromBottom)
			}
		}
		if t.Failed() {
			t.Logf("Requested %d elements starting with score %d", elements, lst[start].Score)
			t.Log("got", e)
			t.Log("want", want)
			t.Fatal("Stopping at start offset: ", start)
		}
	}
	e, err := l.GetRankScoreDesc(ctx, store, 0, 10)
	if err != nil {
		t.Fatalf("score 0: %v", err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	if len(e) != 0 {
		t.Error("want 0 elements, got ", e)
	}
}

func TestList_GetRankPercentile(t *testing.T) {
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const nelems = 50
	var scores []uint64
	for i := 0; i < nelems; i++ {
		scores = append(scores, uint64(i*10))
	}
	lst := listWithScores(scores...)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(2, 5),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	for start := 0; start < nelems; start++ {
		const elements = 10
		startP := (float64(start) + 0.49999) / nelems
		e, err := l.GetPercentile(ctx, store, startP, elements)
		if err != nil {
			t.Errorf("offset %d: %v", start, err)
			continue
		}
		want := lst[start+1:]
		got := e.Below
		if len(want) > elements {
			want = want[:elements]
		}
		if len(got) != len(want) {
			t.Log("got", got)
			t.Log("want", want)
			t.Fatalf("wrong number of results, want %v, got %v", len(want), len(got))
			continue
		}

		for i := range got {
			if got[i].ID != want[i].ID {
				t.Errorf("element %v was wrong, want id %v, got %v", i, want[i].ID, got[i].ID)
			}
		}
		want = make(rankdb.Elements, elements)
		if start < elements {
			want = lst[:start]
		} else {
			want = lst[start-elements : start]
		}

		got = e.Above
		if len(got) != len(want) {
			t.Log("got", got)
			t.Log("want", want)
			t.Fatalf("wrong number of results, want %v, got %v", len(want), len(got))
		}

		for i := range got {
			// Above are returned in reverse order.
			wid := len(want) - i - 1
			if got[i].ID != want[wid].ID {
				t.Errorf("element %v was wrong, want id %v, got %v", i, want[wid].ID, got[i].ID)
			}
		}
		wante := lst[start]
		if wante.ID != e.ID {
			t.Errorf("got ID %v, want %v", e.ID, wante.ID)
		}

		if t.Failed() {
			t.Log("got", e)
			t.Log("want", want)
			t.Fatal("Stopping at start offset: ", start)
		}
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
}

func TestList_DeleteElements(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const testsize = 50000
	lst := randElements(testsize)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(100, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	const every = 50
	const deleten = testsize / every
	var todel []rankdb.ElementID
	for i := 0; i < every; i++ {
		todel = append(todel, lst[i*deleten+rand.Intn(deleten)].ID)
	}

	err = l.DeleteElements(ctx, store, todel)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	e, err := l.GetElements(ctx, store, todel, 0)
	t.Log(e)
	if len(e) != 0 {
		t.Fatalf("want %d results, got %d", 0, len(e))
	}
}

func TestList_DeleteElementsParallel(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
		return
	}
	ctx := context.Background()
	ctx = log.WithLogger(ctx, testlogger.New(t))
	store := memstore.NewMemStore()
	set := "test-set"
	const testsize = 100000
	lst := randSortedElements(testsize)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(200, 300),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	const every = 50000
	const deleten = testsize / every
	var todel rankdb.Elements
	for i := 0; i < every; i++ {
		todel = append(todel, lst[i*deleten+rand.Intn(deleten)])
	}
	todel.Deduplicate()
	const parallel = 20
	var inEach = len(todel) / parallel
	t.Log("Deleting", len(todel), "elements")
	var wg sync.WaitGroup
	wg.Add(parallel)
	for i := 0; i < parallel; i++ {
		del := todel[i*inEach : (i+1)*inEach].IDs()
		go func(i int) {
			defer wg.Done()
			err := l.DeleteElements(ctx, store, del)
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()
	err = l.VerifyUnlocked(ctx, time.Second*5)
	if err != nil {
		t.Fatal(err)
	}
	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	// Kill INFO logging, there are a lot of not found messages.
	ctx = log.WithLogger(ctx, log.NullInfo(log.Logger(ctx)))
	e, err := l.GetElements(ctx, store, todel.IDs(), 0)
	t.Log(rankdb.RankedElements(e).IDs())
	if len(e) != 0 {
		t.Fatalf("want %d results, got %d", 0, len(e))
	}
}

func TestNewList_BigDeleteAll(t *testing.T) {
	ctx := testCtx(t)
	memstore := memstore.NewMemStore()
	store, err := memstore, error(nil)
	if err != nil {
		t.Fatal(err)
	}
	set := "test-set"
	var n = 50000
	if testing.Short() {
		n /= 100
	}
	lst := randSortedElements(n)

	l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
		rankdb.WithListOption.MergeSplitSize(500, 2000),
		rankdb.WithListOption.Populate(lst),
	)
	if err != nil {
		t.Fatal(err)
	}
	l.ReleaseSegments(ctx)

	err = l.Verify(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.VerifyElements(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	err = l.DeleteAll(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	files, size := memstore.Usage(set)
	if files > 0 {
		t.Error("Everything should be gone, blobs stored:\n" + strings.Join(memstore.Dir(set), "\n"))
	}
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
}

func listWithScores(scores ...uint64) rankdb.Elements {
	res := make(rankdb.Elements, len(scores))
	now := uint32(time.Now().Unix())
	for i := range res {
		res[i] = rankdb.Element{
			Score:      uint64(scores[i]),
			TieBreaker: 0,
			Updated:    now,
			ID:         rankdb.ElementID(i + 1),
		}
	}
	res.Sort()
	return res
}

const benchListStartElements = 5000
const benchListMaxElements = 2560000 * 2
const benchListMulElements = 2

func BenchmarkNewList(b *testing.B) {
	ctx := context.Background()
	set := "test-set"
	for n := benchListStartElements; n <= benchListMaxElements; n *= benchListMulElements {
		lst := randElements(n)
		b.ResetTimer()
		b.Run(fmt.Sprint(n, "-Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := nullstore.New()
				l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
					rankdb.WithListOption.MergeSplitSize(500, 2000),
					rankdb.WithListOption.Populate(lst),
				)
				if i == 0 && n == benchListMaxElements {
					lst = nil
					runtime.GC()
					f, _ := os.Create("heap-profile-newlist.prof")
					pprof.WriteHeapProfile(f)
					f.Close()
				}
				if err != nil {
					b.Fatal(err)
				}
				_ = l.ID
			}
		})
	}
}

func BenchmarkNewList_Sorted(b *testing.B) {
	ctx := context.Background()
	set := "test-set"
	for n := benchListStartElements; n <= benchListMaxElements; n *= benchListMulElements {
		lst := randSortedElements(n)
		if false {
			pl, _ := json.MarshalIndent(lst, "", "\t")
			rd := bufio.NewReader(bytes.NewBuffer(pl))
			b2 := &bytes.Buffer{}
			b2.WriteString(`{
  "id": "list-` + fmt.Sprint(n) + `-elements",
  "load_index": true,
  "merge_size": 500,
  "metadata": {
    "country": "dk",
    "game": "2"
  },
  "populate": `)
			for {
				ln, err := rd.ReadString('\n')
				if err != nil {
					break
				}
				if strings.HasPrefix(ln, `		"TieBreaker":`) {
					ln = strings.TrimSuffix(ln, ",\n") + "\n"
				}
				if strings.HasPrefix(ln, `		"Updated":`) {
					ln = ""
				}
				if strings.HasPrefix(ln, `		"Payload":`) {
					ln = ""
				}
				if ln != "" {
					b2.WriteString(ln)
				}
			}
			b2.WriteString(`],
  "set": "storage-set",
  "split_size": 2000
}`)
			_ = ioutil.WriteFile(fmt.Sprintf("../%d-elements.json", n), b2.Bytes(), os.ModePerm)
		}
		b.ResetTimer()
		b.Run(fmt.Sprint(n, "Elements"), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				store := memstore.NewMemStore()
				l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
					rankdb.WithListOption.MergeSplitSize(500, 2000),
					rankdb.WithListOption.Populate(lst),
				)
				if i == 0 && n == benchListMaxElements {
					lst = nil
					runtime.GC()
					f, _ := os.Create("heap-profile-newlist-sorted.prof")
					pprof.WriteHeapProfile(f)
					f.Close()
				}
				if err != nil {
					b.Fatal(err)
				}
				_ = l.ID
			}
		})
	}
}

func BenchmarkList_GetElements(b *testing.B) {
	set := "test-set"
	for n := 500000; n <= benchListMaxElements; n *= benchListMulElements {
		lst := randSortedElements(n)
		lst.Sort()
		b.Run(fmt.Sprint(n, "-Elements"), func(b *testing.B) {
			ctx := log.WithLogger(context.Background(), testlogger.NewB(b))
			store := memstore.NewMemStore()
			l, err := rankdb.NewList(ctx, "test-list", set, store, rankdb.WithListOption.LoadIndex(true),
				rankdb.WithListOption.MergeSplitSize(500, 2000),
				rankdb.WithListOption.Populate(lst),
			)
			if err != nil {
				b.Fatal(err)
			}
			if got, err := l.Len(ctx, store); got != n {
				if err != nil {
					b.Fatal(err)
				}
				b.Fatalf("Length mismatch got %d != %d (want)", got, n)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				find := []rankdb.ElementID{lst[rand.Intn(len(lst))].ID}
				e, err := l.GetElements(ctx, store, find, 0)
				if err != nil {
					b.Fatal(err)
				}
				if len(e) != 1 {
					b.Error(find[0], " was not returned")
					continue
				}
				if e[0].ID != find[0] {
					b.Error("wrong element", e[0].ID, "!=", find[0])
				}
			}
		})
	}
}
