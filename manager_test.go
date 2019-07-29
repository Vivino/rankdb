package rankdb_test

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"strings"
	"testing"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/blobstore/memstore"
)

func TestManager_StartIntervalSaver(t *testing.T) {
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
	lists := randLists(ctx, store, set, 100, 0, 1, defaultMetadataconst)
	want := rankdb.ListIDs(rankdb.ListToListID(lists...))
	mgr.Lists.Add(lists...)
	sd := make(chan chan struct{})

	// Start saver
	mgr.StartIntervalSaver(ctx, time.Hour, sd)

	// Send shutdown
	sdd := make(chan struct{})
	sd <- sdd

	// Wait for lists to be saved
	<-sdd

	mgr, err = rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.LoadLists(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	got := mgr.Lists.All()
	got.Deduplicate()
	got.Sort()
	if len(want) != len(got) {
		t.Fatalf("list length, got %d want %d", len(got), len(want))
	}
	want.Sort()
	for i, w := range want {
		if w != got[i] {
			t.Errorf("entry %d: got id %q, want %q", i, string(got[i]), string(w))
		}
	}

	// Test main set
	files, size := store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
	if files != 401 {
		t.Errorf("expected 401 files, got %d", files)
	}

}

func TestManager_SaveLists(t *testing.T) {
	ctx := testCtx(t)
	store := memstore.NewMemStore()
	set := "test-set"
	backupSet := "backup"
	mgr, err := rankdb.NewManager(store, set)
	if err != nil {
		t.Fatal(err)
	}
	mgr.Backup = blobstore.StoreWithSet(store, backupSet)
	err = mgr.NewLists(nil)
	if err != nil {
		t.Fatal(err)
	}
	lists := randLists(ctx, store, set, 100, 0, 1, defaultMetadataconst)
	want := rankdb.ListIDs(rankdb.ListToListID(lists...))
	mgr.Lists.Add(lists...)

	err = mgr.SaveLists(ctx, true)
	if err != nil {
		t.Fatal(err)
	}

	// Test main list storage
	mgr, err = rankdb.NewManager(store, set)
	mgr.Backup = blobstore.StoreWithSet(store, backupSet)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.LoadLists(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	got := mgr.Lists.All()
	got.Deduplicate()
	got.Sort()
	if len(want) != len(got) {
		t.Fatalf("list length, got %d want %d", len(got), len(want))
	}
	want.Sort()
	for i, w := range want {
		if w != got[i] {
			t.Errorf("entry %d: got id %q, want %q", i, string(got[i]), string(w))
		}
	}

	// We cannot access listStorageKey, so we put it in directly.
	err = store.Delete(ctx, set, "lists")
	if err != nil {
		t.Fatal(err)
	}

	// Test main list backup
	mgr, err = rankdb.NewManager(store, set)
	mgr.Backup = blobstore.StoreWithSet(store, backupSet)
	if err != nil {
		t.Fatal(err)
	}
	err = mgr.LoadLists(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	got = mgr.Lists.All()
	got.Deduplicate()
	got.Sort()
	if len(want) != len(got) {
		t.Fatalf("list length, got %d want %d", len(got), len(want))
	}
	want.Sort()
	for i, w := range want {
		if w != got[i] {
			t.Errorf("entry %d: got id %q, want %q", i, string(got[i]), string(w))
		}
	}
	t.Log("Blobs stored:\n" + strings.Join(store.Dir(backupSet), "\n"))
	files, size := store.Usage(backupSet)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
	if files != 1 {
		t.Errorf("expected 1 file, got %d", files)
	}

	// Test main set
	files, size = store.Usage(set)
	t.Log("Storage used:", size, "bytes in", files, "blobs.")
	if files != 400 {
		t.Errorf("expected 400 files, got %d", files)
	}
}
