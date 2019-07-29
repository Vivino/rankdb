package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vivino/rankdb/api/client"
	"github.com/google/go-cmp/cmp"
)

func TestMultiListController_Create(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	lists := make([]string, 15)
	if testing.Short() {
		lists = make([]string, 5)
	}
	for i := range lists {
		lists[i] = t.Name() + fmt.Sprint(i)
		createList(t, lists[i], 100)
	}

	payload := client.ListPayloadQL{
		AllInSets:     nil,
		Lists:         lists,
		MatchMetadata: nil,
		Payload: []*client.Element{
			{
				ID:         10,
				Payload:    []byte(`{"custom":"payload"}`),
				Score:      1010,
				TieBreaker: uint32p(100),
			},
			{
				ID:         20,
				Payload:    []byte(`{"custom":"payload2"}`),
				Score:      2020,
				TieBreaker: uint32p(200),
			},
		},
	}

	oneResult := client.RankdbOperationSuccess{
		Results: []*client.RankdbElementTiny{
			{
				FromBottom: 1,
				FromTop:    100,
				ID:         payload.Payload[1].ID,
				Payload:    payload.Payload[1].Payload,
				Score:      payload.Payload[1].Score,
			},
			{
				FromBottom: 0,
				FromTop:    101,
				ID:         payload.Payload[0].ID,
				Payload:    payload.Payload[0].Payload,
				Score:      payload.Payload[0].Score,
			},
		},
	}

	want := client.RankdbResultlist{
		Success: make(map[string]*client.RankdbOperationSuccess, len(lists)),
	}
	for _, v := range lists {
		want.Success[v] = &oneResult
	}

	// Attempt with no credentials
	resp, err := tClient.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct:
	resp, err = tClientUpdate.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err := tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}

	// Check conflicts.
	resp, err = tClientUpdate.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if len(got.Errors) != len(lists) {
		t.Logf("%#v", *got)
		t.Fatal("expected:", len(lists), "errors, got:", len(got.Errors))
	}

	// Delete elements.
	resp, err = tClientDelete.DeleteMultilist(ctx, client.DeleteMultilistPath("10"), nil, nil, lists, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	resp, err = tClientDelete.DeleteMultilist(ctx, client.DeleteMultilistPath("20"), nil, nil, lists, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	listAcc := strings.Join(lists, ",")
	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:update", idToString(200), nil, nil)
	resp, err = tCustom.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:update", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:update", stringp("10,20"), &listAcc, nil)
	resp, err = tCustom.CreateMultilist(ctx, client.CreateMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestMultiListController_Put(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	lists := make([]string, 15)
	if testing.Short() {
		lists = make([]string, 5)
	}
	for i := range lists {
		lists[i] = t.Name() + fmt.Sprint(i)
		createList(t, lists[i], 100)
	}

	payload := client.ListPayloadQL{
		AllInSets:     nil,
		Lists:         lists,
		MatchMetadata: nil,
		Payload: []*client.Element{
			{
				ID:         10,
				Payload:    []byte(`{"custom":"payload"}`),
				Score:      1010,
				TieBreaker: uint32p(100),
			},
			{
				ID:         20,
				Payload:    []byte(`{"custom":"payload2"}`),
				Score:      2020,
				TieBreaker: uint32p(200),
			},
		},
	}

	oneResult := client.RankdbOperationSuccess{
		Results: []*client.RankdbElementTiny{
			{
				FromBottom: 1,
				FromTop:    100,
				ID:         payload.Payload[1].ID,
				Payload:    payload.Payload[1].Payload,
				Score:      payload.Payload[1].Score,
			},
			{
				FromBottom: 0,
				FromTop:    101,
				ID:         payload.Payload[0].ID,
				Payload:    payload.Payload[0].Payload,
				Score:      payload.Payload[0].Score,
			},
		},
	}

	want := client.RankdbResultlist{
		Success: make(map[string]*client.RankdbOperationSuccess, len(lists)),
	}
	for _, v := range lists {
		want.Success[v] = &oneResult
	}

	// Attempt with no credentials
	resp, err := tClient.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct:
	resp, err = tClientUpdate.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err := tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}

	// Check updates.
	payload.Payload[0].Score += 100
	payload.Payload[1].Score += 100
	oneResult.Results[0].Score += 100
	oneResult.Results[1].Score += 100

	resp, err = tClientUpdate.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}

	listAcc := strings.Join(lists, ",")
	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:update", idToString(200), nil, nil)
	resp, err = tCustom.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:update", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:update", stringp("10,20"), &listAcc, nil)
	resp, err = tCustom.PutMultilist(ctx, client.PutMultilistPath(), &payload, nil, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbResultlist(resp)
	fatalErr(t, err)
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}
}

// This will test both backup and restore.
func TestMultilistController_Backup(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	elems := 10000
	if testing.Short() {
		elems = 200
	}
	createList(t, t.Name(), elems)
	createList(t, t.Name()+"-2", elems)
	dir, err := ioutil.TempDir("", t.Name())
	fileName := filepath.Join(dir, t.Name()+".bin")
	payload := client.MultiListBackup{
		Destination: &client.BackupDestination{
			Path:               &fileName,
			ServerListIDPrefix: nil,
			ServerListIDSuffix: nil,
			Type:               "file",
		},
		Lists: &client.ListQL{
			AllInSets:     nil,
			Lists:         []string{t.Name(), t.Name() + "-2"},
			MatchMetadata: nil,
		},
	}

	// Check access
	resp, err := tClient.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientRead.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientDelete.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientUpdate.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)

	// Correct
	resp, err = tClientManage.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusCreated)
	defer os.RemoveAll(dir)
	backup, err := tClient.DecodeRankdbCallback(resp)
	fatalErr(t, err)
	var got *client.RankdbBackupStatus

	// Wait for actual completion
	for {
		time.Sleep(10 * time.Millisecond)
		resp, err = tClientManage.StatusBackup(ctx, client.StatusBackupPath(backup.ID))
		fatalErr(t, err)
		expectCode(t, resp, http.StatusOK)
		got, err = tClientManage.DecodeRankdbBackupStatus(resp)
		fatalErr(t, err)
		if got.Done {
			break
		}
	}
	want := client.RankdbBackupStatus{
		Cancelled: false,
		Custom:    nil,
		Done:      true,
		Errors:    nil,
		Finished:  got.Finished,
		Lists:     2,
		ListsDone: 2,
		Size:      got.Size,
		Started:   got.Started,
		Storage:   "*file.File",
		URI:       backup.CallbackURL,
	}
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}

	// Restore it.
	wantR := client.RankdbRestoreresult{Errors: nil, Restored: 2, Skipped: 0}
	resp, err = tClientManage.RestoreMultilist(ctx, client.RestoreMultilistPath(), nil, stringp("pre-"), stringp("-post"), stringp("file"), &fileName)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusOK)
	gotR, err := tClient.DecodeRankdbRestoreresult(resp)
	fatalErr(t, err)
	if !cmp.Equal(*gotR, wantR) {
		t.Logf("%#v", *gotR)
		t.Fatal(cmp.Diff(wantR, *gotR))
	}

	// Do basic check
	resp, err = tClientRead.GetLists(ctx, client.GetListsPath("pre-"+t.Name()+"-2-post"), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	lst, err := tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)
	wantL := client.RankdbRanklistFull{
		AvgSegmentElements: 0,
		BottomElement:      nil,
		CacheHits:          1,
		CacheMisses:        1,
		CachePercent:       50,
		Elements:           elems,
		ID:                 "pre-" + t.Name() + "-2-post",
		LoadIndex:          true,
		MergeSize:          1000,
		Metadata:           map[string]string{"test": "value"},
		Segments:           0,
		Set:                "test-set",
		SplitSize:          2000,
	}
	// Remove potentially unstable values:
	lst.CacheHits = wantL.CacheHits
	lst.CacheMisses = wantL.CacheMisses
	lst.CachePercent = wantL.CachePercent
	lst.AvgSegmentElements = wantL.AvgSegmentElements
	lst.Segments = wantL.Segments
	if !cmp.Equal(wantL, *lst) {
		t.Fatal(cmp.Diff(wantL, *lst))
	}
}

// Actual restoration tested in TestMultilistController_Backup.
func TestMultilistController_Restore(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	fileName := "doesnt-matter"
	resp, err := tClient.RestoreMultilist(ctx, client.RestoreMultilistPath(), nil, stringp("pre-"), stringp("-post"), stringp("file"), &fileName)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientRead.RestoreMultilist(ctx, client.RestoreMultilistPath(), nil, stringp("pre-"), stringp("-post"), stringp("file"), &fileName)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientDelete.RestoreMultilist(ctx, client.RestoreMultilistPath(), nil, stringp("pre-"), stringp("-post"), stringp("file"), &fileName)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientUpdate.RestoreMultilist(ctx, client.RestoreMultilistPath(), nil, stringp("pre-"), stringp("-post"), stringp("file"), &fileName)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
}
