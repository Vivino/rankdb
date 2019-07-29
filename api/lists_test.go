package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/Vivino/rankdb/api/client"
	"github.com/google/go-cmp/cmp"
)

func TestListsController_Create(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	r := client.RankList{
		ID:        t.Name(),
		LoadIndex: true,
		MergeSize: 100,
		Metadata:  map[string]string{"test": "value"},
		Populate: []*client.Element{
			&clientElement1,
		},
		Set:       "test-set",
		SplitSize: 200,
	}
	// Create a list with no credentials
	resp, err := tClient.CreateLists(ctx, client.CreateListsPath(), &r, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Create a list with proper credentials
	resp, err = tClientManage.CreateLists(ctx, client.CreateListsPath(), &r, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err := tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)

	// What we expect
	want := client.RankdbRanklistFull{
		AvgSegmentElements: 1,
		BottomElement:      clientRankdbElement(t.Name(), got.BottomElement.UpdatedAt),
		CacheHits:          0,
		CacheMisses:        0,
		CachePercent:       0,
		Elements:           1,
		ID:                 t.Name(),
		LoadIndex:          r.LoadIndex,
		MergeSize:          r.MergeSize,
		Metadata: map[string]string{
			"test": "value",
		},
		Segments:   1,
		Set:        r.Set,
		SplitSize:  r.SplitSize,
		TopElement: clientRankdbElement(t.Name(), got.BottomElement.UpdatedAt),
	}
	if !cmp.Equal(want, *got) {
		t.Fatal(cmp.Diff(want, *got))
	}

	// Creating another list without replace should return conflict.
	resp, err = tClientManage.CreateLists(ctx, client.CreateListsPath(), &r, boolp(false), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusConflict)()

	// However, if we allow to replace, it should be replaced.
	// We try invalid value first.
	r.MergeSize = 500
	want.MergeSize = 500
	resp, err = tClientManage.CreateLists(ctx, client.CreateListsPath(), &r, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// Check if we still have the old list
	resp, err = tClientRead.GetLists(ctx, client.GetListsPath(t.Name()), boolp(false))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Now we should be back to acceptable params.
	r.SplitSize = 1000
	want.SplitSize = 1000
	resp, err = tClientManage.CreateLists(ctx, client.CreateListsPath(), &r, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)
	want.TopElement = clientRankdbElement(t.Name(), got.BottomElement.UpdatedAt)
	want.BottomElement = clientRankdbElement(t.Name(), got.BottomElement.UpdatedAt)
	if !cmp.Equal(want, *got) {
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestListsController_Delete(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to delete a list with no credentials
	resp, err := tClient.DeleteLists(ctx, client.DeleteListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.DeleteLists(ctx, client.DeleteListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.DeleteLists(ctx, client.DeleteListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.DeleteLists(ctx, client.DeleteListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Delete a list that does not exist.
	resp, err = tClientManage.DeleteLists(ctx, client.DeleteListsPath("non-existing-list"))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// Correctly delete the list, check if we can read it.
	resp, err = tClientRead.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	resp, err = tClientManage.DeleteLists(ctx, client.DeleteListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// Check if actually deleted.
	resp, err = tClientRead.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()
}

func TestListsController_Get(t *testing.T) {
	//t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to get a list with no/wrong credentials
	resp, err := tClient.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientManage.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	tCustom := customTokenClient(ctx, t, "api:read", nil, stringp("another-list"), nil)
	resp, err = tCustom.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test valid access
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp(t.Name()), nil)
	resp, err = tCustom.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	resp, err = tClientRead.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	lst, err := tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)
	want := client.RankdbRanklistFull{
		AvgSegmentElements: 100,
		BottomElement:      nil,
		CacheHits:          1,
		CacheMisses:        1,
		CachePercent:       50,
		Elements:           100,
		ID:                 t.Name(),
		LoadIndex:          true,
		MergeSize:          1000,
		Metadata:           map[string]string{"test": "value"},
		Segments:           1,
		Set:                "test-set",
		SplitSize:          2000,
	}
	// Remove potentially unstable values:
	lst.CacheHits = want.CacheHits
	lst.CacheMisses = want.CacheMisses
	lst.CachePercent = want.CachePercent
	if !cmp.Equal(want, *lst) {
		t.Fatal(cmp.Diff(want, *lst))
	}
}

func TestListsController_GetPercentile(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to get a list with no/wrong credentials
	resp, err := tClient.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientManage.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Invalid values
	resp, err = tClientRead.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("abc"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("-0.001"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("100.001"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(-5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// Has access to another list
	tCustom := customTokenClient(ctx, t, "api:read", nil, stringp("another-list"), nil)
	resp, err = tCustom.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test valid access
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp(t.Name()), nil)
	resp, err = tCustom.GetPercentileLists(ctx, client.GetPercentileListsPath(t.Name()), stringp("50"), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tCustom.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	t.Logf("%#v", *got)

	if len(got.Neighbors.Above) != 5 {
		t.Fatal("Expected 5 above")
	}
	if len(got.Neighbors.Below) != 5 {
		t.Fatal("Expected 5 below")
	}
	if got.FromBottom != 49 {
		t.Fatal("Want FromBottom 49, got", got.FromBottom)
	}
	if got.FromTop != 50 {
		t.Fatal("Want FromTop 50, got", got.FromTop)
	}
	if got.ListID != t.Name() {
		t.Fatal("Want list id", t.Name(), "got", got.ListID)
	}
}

func TestListsController_GetRange(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to get a list with no/wrong credentials
	resp, err := tClient.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientManage.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	tCustom := customTokenClient(ctx, t, "api:read", nil, stringp("another-list"), nil)
	resp, err = tCustom.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test invalid values
	resp, err = tClientRead.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(-200), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(10), intp(-5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), intp(20), intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	resp, err = tClientRead.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(500000), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Test valid access
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp(t.Name()), nil)
	resp, err = tCustom.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), nil, intp(50), intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tCustom.DecodeRankdbElementCollection(resp)
	fatalErr(t, err)

	if len(got) != 5 {
		t.Fatal("Expected 5 above")
	}
	first := got[0]
	if first.FromTop != 50 {
		t.Fatal("Want FromTop 50, got", first.FromTop)
	}
	if first.FromBottom != 49 {
		t.Fatal("Want FromBottom 49, got", first.FromBottom)
	}
	if first.ListID != t.Name() {
		t.Fatal("Want list id", t.Name(), "got", first.ListID)
	}

	resp, err = tCustom.GetRangeLists(ctx, client.GetRangeListsPath(t.Name()), intp(49), nil, intp(5))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tCustom.DecodeRankdbElementCollection(resp)
	fatalErr(t, err)

	if len(got) != 5 {
		t.Fatal("Expected 5 above")
	}
	first = got[0]
	if first.FromTop != 50 {
		t.Fatal("Want FromTop 50, got", first.FromTop)
	}
	if first.FromBottom != 49 {
		t.Fatal("Want FromBottom 49, got", first.FromBottom)
	}
	if first.ListID != t.Name() {
		t.Fatal("Want list id", t.Name(), "got", first.ListID)
	}

}

func TestListsController_GetAll(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	for i := 0; i < 100; i++ {
		createList(t, fmt.Sprintf("%s-%02d", t.Name(), i), 0)
	}

	// Attempt to verify a list with no/wrong credentials
	resp, err := tClient.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()), nil, intp(25))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()), nil, intp(25))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()), nil, intp(25))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()), nil, intp(25))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Verify a list that does not exist.
	resp, err = tClientManage.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()), nil, intp(25))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbListsresult(resp)
	fatalErr(t, err)
	if len(got.Lists) != 25 {
		t.Fatal("Expected 25 lists, got ", len(got.Lists))
	}
	for i, l := range got.Lists {
		want := client.RankdbRanklist{
			ID:        fmt.Sprintf("%s-%02d", t.Name(), i),
			LoadIndex: true,
			MergeSize: 1000,
			Metadata:  map[string]string{"test": "value"},
			Set:       "test-set",
			SplitSize: 2000,
		}
		if !cmp.Equal(*l, want) {
			t.Fatal(cmp.Diff(want, *l))
		}
	}
	if got.ListsAfter < 75 {
		t.Fatal("Wanted >=75 lists after, got", got.ListsAfter)
	}
	resp, err = tClientManage.GetAllLists(ctx, client.GetAllListsPath(), stringp(t.Name()+"-49"), nil, intp(15))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbListsresult(resp)
	fatalErr(t, err)
	if len(got.Lists) != 15 {
		t.Fatal("Expected 15 lists, got ", len(got.Lists))
	}
	for i, l := range got.Lists {
		want := client.RankdbRanklist{
			ID:        fmt.Sprintf("%s-%02d", t.Name(), i+50),
			LoadIndex: true,
			MergeSize: 1000,
			Metadata:  map[string]string{"test": "value"},
			Set:       "test-set",
			SplitSize: 2000,
		}
		if !cmp.Equal(*l, want) {
			t.Fatal(cmp.Diff(want, *l))
		}
	}
	if got.ListsAfter < 35 {
		t.Fatal("Wanted >=35 lists after, got", got.ListsAfter)
	}
	if got.ListsBefore < 50 {
		t.Fatal("Wanted <=50 lists before, got", got.ListsBefore)
	}
}

func TestListsController_Verify(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to verify a list with no/wrong credentials
	resp, err := tClient.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Verify a list that does not exist.
	resp, err = tClientManage.VerifyLists(ctx, client.VerifyListsPath("non-existing-list"), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Correctly verify the list, check if we can read it.
	resp, err = tClientManage.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Try with params, just to test if they work.
	resp, err = tClientManage.VerifyLists(ctx, client.VerifyListsPath(t.Name()), boolp(true), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	resp, err = tClientManage.VerifyLists(ctx, client.VerifyListsPath(t.Name()), nil, boolp(true))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
}

func TestListsController_Reindex(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to verify a list with no/wrong credentials
	resp, err := tClient.ReindexLists(ctx, client.ReindexListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.ReindexLists(ctx, client.ReindexListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.ReindexLists(ctx, client.ReindexListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.ReindexLists(ctx, client.ReindexListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test a list that does not exist.
	resp, err = tClientManage.ReindexLists(ctx, client.ReindexListsPath("non-existing-list"))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Correctly access the list,
	resp, err = tClientManage.ReindexLists(ctx, client.ReindexListsPath(t.Name()))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
}

func TestListsController_Repair(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	// Attempt to verify a list with no/wrong credentials
	resp, err := tClient.RepairLists(ctx, client.RepairListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.RepairLists(ctx, client.RepairListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.RepairLists(ctx, client.RepairListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.RepairLists(ctx, client.RepairListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test a list that does not exist.
	resp, err = tClientManage.RepairLists(ctx, client.RepairListsPath("non-existing-list"), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Correctly access the list,
	resp, err = tClientManage.RepairLists(ctx, client.RepairListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	resp, err = tClientManage.RepairLists(ctx, client.RepairListsPath(t.Name()), boolp(true))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
}

func TestListsController_Clone(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)
	createList(t, t.Name()+"-existing", 10)

	r := client.RankList{
		ID:        t.Name() + "-cloned",
		LoadIndex: true,
		MergeSize: 100,
		Metadata:  map[string]string{"test": "value", "cloned": "yes"},
		Populate: []*client.Element{
			&clientElement1,
		},
		Set:       "test-set",
		SplitSize: 200,
	}

	// Attempt to verify a list with no/wrong credentials
	resp, err := tClient.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientRead.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientUpdate.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()
	resp, err = tClientDelete.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Test a list that does not exist.
	resp, err = tClientManage.CloneLists(ctx, client.CloneListsPath("non-existing-list"), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Clone the list, but it should not have a payload.
	resp, err = tClientManage.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// Remove payload
	r.Populate = nil

	// Clone the list to a name that exists.
	r.ID = t.Name() + "-existing"
	resp, err = tClientManage.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusConflict)()

	// Use all correct params.
	r.ID = t.Name() + "-cloned"
	resp, err = tClientManage.CloneLists(ctx, client.CloneListsPath(t.Name()), &r, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	resp, err = tClientRead.GetLists(ctx, client.GetListsPath(t.Name()), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	lst, err := tClient.DecodeRankdbRanklistFull(resp)
	fatalErr(t, err)
	want := client.RankdbRanklistFull{
		AvgSegmentElements: 100,
		BottomElement:      nil,
		CacheHits:          1,
		CacheMisses:        1,
		CachePercent:       50,
		Elements:           100,
		ID:                 t.Name(),
		LoadIndex:          true,
		MergeSize:          1000,
		Metadata:           map[string]string{"test": "value"},
		Segments:           1,
		Set:                "test-set",
		SplitSize:          2000,
	}
	// Remove potentially unstable values:
	lst.CacheHits = want.CacheHits
	lst.CacheMisses = want.CacheMisses
	lst.CachePercent = want.CachePercent
	if !cmp.Equal(want, *lst) {
		t.Fatal(cmp.Diff(want, *lst))
	}
}
