package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vivino/rankdb/api/client"
	"github.com/google/go-cmp/cmp"
)

// This will do more than just testing this endpoint.
func TestBackupController_Status(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	elems := 1000
	if testing.Short() {
		elems = 50
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

	resp, err := tClientManage.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusCreated)
	defer os.RemoveAll(dir)
	backup, err := tClient.DecodeRankdbCallback(resp)
	fatalErr(t, err)
	var got *client.RankdbBackupStatus

	// Test wrong requests
	resp, err = tClient.StatusBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientRead.StatusBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientUpdate.StatusBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientDelete.StatusBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientManage.StatusBackup(ctx, client.StatusBackupPath("wrong ID"))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusNotFound)

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
}

func TestBackupController_Delete(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	elems := 10000
	if testing.Short() {
		elems = 500
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

	resp, err := tClientManage.BackupMultilist(ctx, client.BackupMultilistPath(), &payload, contentDefault)
	fatalErr(t, err)
	expectCode(t, resp, http.StatusCreated)
	defer os.RemoveAll(dir)
	backup, err := tClient.DecodeRankdbCallback(resp)
	fatalErr(t, err)
	var got *client.RankdbBackupStatus

	// Do the actual cancellation first.
	// We are racing the actual backup,
	// it will only become cancelled if we get there before it completes.
	resp, err = tClientManage.DeleteBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusNoContent)

	// Test wrong requests
	resp, err = tClient.DeleteBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientRead.DeleteBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientUpdate.DeleteBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientDelete.DeleteBackup(ctx, client.StatusBackupPath(backup.ID))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusUnauthorized)
	resp, err = tClientManage.DeleteBackup(ctx, client.StatusBackupPath("wrong ID"))
	fatalErr(t, err)
	expectCode(t, resp, http.StatusNotFound)

	// Wait for actual completion.
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
		Cancelled: true,
		Custom:    nil,
		Done:      true,
		Errors:    nil,
		Finished:  got.Finished,
		Lists:     2,
		ListsDone: got.ListsDone,
		Size:      got.Size,
		Started:   got.Started,
		Storage:   "*file.File",
		URI:       backup.CallbackURL,
	}
	if !got.Cancelled {
		t.Log("Backup was not actually cancelled, backup probably finished before.")
		got.Cancelled = true
	}
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}
}
