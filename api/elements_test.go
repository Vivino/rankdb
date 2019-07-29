package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Vivino/rankdb/api/client"
	"github.com/google/go-cmp/cmp"
)

func TestElementsController_Create(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	payload := client.Element{
		ID:         10,
		Payload:    []byte(`{"custom":"payload"}`),
		Score:      1010,
		TieBreaker: uint32p(100),
	}
	want := client.RankdbElementFull{
		FromBottom:      0,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: nil,
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	// Attempt with no credentials
	resp, err := tClient.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath("wrong-list-name"), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Correct:
	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err := tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Log(string(got.Payload))
		t.Fatal(cmp.Diff(want, *got))
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("updated_at not set")
	}

	// Check conflict.
	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusConflict)()

	// Delete it.
	resp, err = tClientDelete.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), fmt.Sprint(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:update", idToString(200), nil, nil)
	resp, err = tCustom.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:update", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:update", idToString(payload.ID), stringp(t.Name()), nil)
	resp, err = tCustom.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)

	// Add a neighbor
	payload.ID = 20
	payload.Score = 2020
	payload.TieBreaker = uint32p(300)

	want = client.RankdbElementFull{
		FromBottom:      1,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: client.RankdbElementCollection{
				// This is our previous result.
				&client.RankdbElement{
					FromTop:    1,
					ID:         want.ID,
					ListID:     want.ListID,
					Payload:    want.Payload,
					Score:      want.Score,
					TieBreaker: want.TieBreaker,
					UpdatedAt:  got.UpdatedAt,
				},
			},
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", got.Neighbors.Below)
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestElementsController_Put(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	payload := client.Element{
		ID:         10,
		Payload:    []byte(`{"custom":"payload"}`),
		Score:      1010,
		TieBreaker: uint32p(10),
	}
	want := client.RankdbElementFull{
		FromBottom:      0,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: nil,
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	// Attempt with no credentials
	resp, err := tClient.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath("not-existing-list", *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()
	//ID mismatch
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID * 2)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()
	// Junk ID
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), "junk-id"), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// Correct:
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err := tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Log(string(got.Payload))
		t.Fatal(cmp.Diff(want, *got))
	}

	// Check updates.
	payload.Score *= 2
	want.Score *= 2
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Log(string(got.Payload))
		t.Fatal(cmp.Diff(want, *got))
	}

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:update", idToString(200), nil, nil)
	resp, err = tCustom.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:update", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:update", idToString(payload.ID), stringp(t.Name()), nil)
	resp, err = tCustom.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)

	// Add a neighbor
	payload.ID = 20
	payload.Score = 2020
	payload.TieBreaker = uint32p(300)

	want = client.RankdbElementFull{
		FromBottom:      1,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: client.RankdbElementCollection{
				// This is our previus result.
				&client.RankdbElement{
					FromTop:    1,
					ID:         want.ID,
					ListID:     want.ListID,
					Payload:    want.Payload,
					Score:      want.Score,
					TieBreaker: want.TieBreaker,
					UpdatedAt:  got.UpdatedAt,
				},
			},
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", got.Neighbors.Below)
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestElementsController_Delete(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	payload := client.Element{
		ID:         10,
		Payload:    []byte(`{"custom":"payload"}`),
		Score:      1010,
		TieBreaker: uint32p(10),
	}

	// Create it
	resp, err := tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Attempt with no credentials
	resp, err = tClient.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with update credentials
	resp, err = tClientUpdate.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientDelete.DeleteElements(ctx, client.DeleteElementsPath("non-existing-list", *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	//ID mismatch
	resp, err = tClientDelete.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID * 2)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// Junk ID
	resp, err = tClientDelete.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), "junk-id"))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// OK
	resp, err = tClientDelete.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// It should no longer be there
	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Create it
	resp, err = tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:delete", idToString(200), nil, nil)
	resp, err = tCustom.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:delete", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:delete", idToString(payload.ID), stringp(t.Name()), nil)
	resp, err = tCustom.DeleteElements(ctx, client.DeleteElementsPath(t.Name(), *idToString(payload.ID)))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()
}

func TestElementsController_Get(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	payload := client.Element{
		ID:         10,
		Payload:    []byte(`{"custom":"payload"}`),
		Score:      1010,
		TieBreaker: uint32p(10),
	}

	want := client.RankdbElementFull{
		FromBottom:      0,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: nil,
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	// Create it
	resp, err := tClientUpdate.PutElements(ctx, client.PutElementsPath(t.Name(), *idToString(payload.ID)), &payload, nil, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Attempt with no credentials
	resp, err = tClient.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with update credentials
	resp, err = tClientUpdate.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath("non-existing-list", *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	//ID mismatch
	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID * 2)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Junk ID
	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath(t.Name(), "junk-id"), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusBadRequest)()

	// OK
	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:read", idToString(200), nil, nil)
	resp, err = tCustom.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:read", idToString(payload.ID), stringp(t.Name()), nil)
	resp, err = tCustom.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), nil)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Log(string(got.Payload))
		t.Fatal(cmp.Diff(want, *got))
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("updated_at not set")
	}
	// Add a neighbor
	payload.ID = 20
	payload.Score = 2020
	payload.TieBreaker = uint32p(300)

	want = client.RankdbElementFull{
		FromBottom:      1,
		FromTop:         0,
		ID:              payload.ID,
		ListID:          t.Name(),
		LocalFromBottom: nil,
		LocalFromTop:    nil,
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: nil,
			Below: client.RankdbElementCollection{
				// This is our previus result.
				&client.RankdbElement{
					FromTop:    1,
					ID:         want.ID,
					ListID:     want.ListID,
					Payload:    want.Payload,
					Score:      want.Score,
					TieBreaker: want.TieBreaker,
					UpdatedAt:  got.UpdatedAt,
				},
			},
		},
		Payload:    payload.Payload,
		Score:      payload.Score,
		TieBreaker: *payload.TieBreaker,
		UpdatedAt:  time.Time{},
	}

	resp, err = tClientUpdate.CreateElements(ctx, client.CreateElementsPath(t.Name()), &payload, intp(2), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	resp, err = tClientRead.GetElements(ctx, client.GetElementsPath(t.Name(), *idToString(payload.ID)), intp(2))
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	want.UpdatedAt = got.UpdatedAt
	if !cmp.Equal(*got, want) {
		t.Log(string(got.Payload))
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestElementsController_PutMulti(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	payload := client.PutMultiElementsPayload{
		&client.Element{
			ID:         10,
			Payload:    []byte(`{"custom":"payload"}`),
			Score:      1010,
			TieBreaker: uint32p(10),
		},
		&client.Element{
			ID:         20,
			Payload:    []byte(`{"custom":"payload2"}`),
			Score:      2020,
			TieBreaker: uint32p(20),
		},
		&client.Element{
			ID:         30,
			Payload:    []byte(`{"custom":"payload3"}`),
			Score:      3030,
			TieBreaker: uint32p(30),
		},
	}

	// Scores are increasing, so we should get them back in reverse order.
	want := client.RankdbMultielement{
		Found: []*client.RankdbElement{
			{
				FromBottom: 2,
				FromTop:    0,
				ID:         payload[2].ID,
				ListID:     t.Name(),
				Payload:    payload[2].Payload,
				Score:      payload[2].Score,
				TieBreaker: *payload[2].TieBreaker,
				UpdatedAt:  time.Time{},
			},
			{
				FromBottom: 1,
				FromTop:    1,
				ID:         payload[1].ID,
				ListID:     t.Name(),
				Payload:    payload[1].Payload,
				Score:      payload[1].Score,
				TieBreaker: *payload[1].TieBreaker,
				UpdatedAt:  time.Time{},
			},
			{
				FromBottom: 0,
				FromTop:    2,
				ID:         payload[0].ID,
				ListID:     t.Name(),
				Payload:    payload[0].Payload,
				Score:      payload[0].Score,
				TieBreaker: *payload[0].TieBreaker,
				UpdatedAt:  time.Time{},
			},
		},
		NotFound: nil,
	}

	// Attempt with no credentials
	resp, err := tClient.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath("non-existing-list"), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// OK
	resp, err = tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:update", idToString(200), nil, nil)
	resp, err = tCustom.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:update", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:update", stringp("10,20,30"), stringp(t.Name()), nil)
	resp, err = tCustom.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbMultielement(resp)
	fatalErr(t, err)
	for i, v := range got.Found {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Found[i].UpdatedAt = time.Time{}
	}
	if !cmp.Equal(*got, want) {
		t.Fatal(cmp.Diff(want, *got))
	}

	// We should be able to update
	for i := range payload {
		payload[i].Score++
	}
	for i := range want.Found {
		want.Found[i].Score++
	}

	resp, err = tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), payload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbMultielement(resp)
	fatalErr(t, err)
	for i, v := range got.Found {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Found[i].UpdatedAt = time.Time{}
	}
	if !cmp.Equal(*got, want) {
		t.Fatal(cmp.Diff(want, *got))
	}
}

func TestElementsController_GetMulti(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	putPayload := client.PutMultiElementsPayload{
		&client.Element{
			ID:         10,
			Payload:    []byte(`{"custom":"payload"}`),
			Score:      1010,
			TieBreaker: uint32p(10),
		},
		&client.Element{
			ID:         20,
			Payload:    []byte(`{"custom":"payload2"}`),
			Score:      2020,
			TieBreaker: uint32p(20),
		},
		&client.Element{
			ID:         30,
			Payload:    []byte(`{"custom":"payload3"}`),
			Score:      3030,
			TieBreaker: uint32p(30),
		},
	}

	// Scores are increasing, so we should get them back in reverse order.
	want := client.RankdbMultielement{
		Found: []*client.RankdbElement{
			{
				FromBottom: 2,
				FromTop:    0,
				ID:         putPayload[2].ID,
				ListID:     t.Name(),
				Payload:    putPayload[2].Payload,
				Score:      putPayload[2].Score,
				TieBreaker: *putPayload[2].TieBreaker,
				UpdatedAt:  time.Time{},
			},
			{
				FromBottom: 0,
				FromTop:    2,
				ID:         putPayload[0].ID,
				ListID:     t.Name(),
				Payload:    putPayload[0].Payload,
				Score:      putPayload[0].Score,
				TieBreaker: *putPayload[0].TieBreaker,
				UpdatedAt:  time.Time{},
			},
		},
		NotFound: []uint64{100, 200},
	}

	// Create them
	resp, err := tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), putPayload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	payload := &client.MultiElement{
		ElementIds: []uint64{10, 30, 200, 100},
	}
	// Attempt with no credentials
	resp, err = tClient.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with update credentials
	resp, err = tClientUpdate.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientRead.GetMultiElements(ctx, client.GetMultiElementsPath("non-existing-list"), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// OK
	resp, err = tClientRead.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:read", idToString(200), nil, nil)
	resp, err = tCustom.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:read", stringp("10,30,100,200"), stringp(t.Name()), nil)
	resp, err = tCustom.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbMultielement(resp)
	fatalErr(t, err)
	for i, v := range got.Found {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Found[i].UpdatedAt = time.Time{}
	}
	if !cmp.Equal(*got, want) {
		t.Fatal(cmp.Diff(want, *got))
	}

	// Don't request anything
	payload.ElementIds = payload.ElementIds[:0]
	resp, err = tCustom.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payload, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()
	got, err = tClient.DecodeRankdbMultielement(resp)
	fatalErr(t, err)
	if n := len(got.Found) + len(got.NotFound); n != 0 {
		t.Fatal("want no elements, got ", n)
	}
}

func TestElementsController_DeleteMulti(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 0)

	putPayload := client.PutMultiElementsPayload{
		&client.Element{
			ID:         10,
			Payload:    []byte(`{"custom":"payload"}`),
			Score:      1010,
			TieBreaker: uint32p(10),
		},
		&client.Element{
			ID:         20,
			Payload:    []byte(`{"custom":"payload2"}`),
			Score:      2020,
			TieBreaker: uint32p(20),
		},
		&client.Element{
			ID:         30,
			Payload:    []byte(`{"custom":"payload3"}`),
			Score:      3030,
			TieBreaker: uint32p(30),
		},
	}

	// Scores are increasing, so we should get them back in reverse order.
	want := client.RankdbMultielement{
		Found: []*client.RankdbElement{
			{
				FromBottom: 0,
				FromTop:    0,
				ID:         putPayload[1].ID,
				ListID:     t.Name(),
				Payload:    putPayload[1].Payload,
				Score:      putPayload[1].Score,
				TieBreaker: *putPayload[1].TieBreaker,
				UpdatedAt:  time.Time{},
			},
		},
		NotFound: []uint64{10, 30},
	}

	// Create them
	resp, err := tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), putPayload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	payload := []string{"10", "30"}

	// Attempt with no credentials
	resp, err = tClient.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with read credentials
	resp, err = tClientRead.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with update credentials
	resp, err = tClientUpdate.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientDelete.DeleteMultiElements(ctx, client.DeleteMultiElementsPath("non-existing-list"), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:delete", idToString(200), nil, nil)
	resp, err = tCustom.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:delete", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:delete", stringp("10,30"), stringp(t.Name()), nil)
	resp, err = tCustom.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()

	// OK
	payloadRead := &client.MultiElement{
		ElementIds: []uint64{10, 20, 30},
	}
	resp, err = tClientRead.GetMultiElements(ctx, client.GetMultiElementsPath(t.Name()), payloadRead, contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbMultielement(resp)
	fatalErr(t, err)
	for i, v := range got.Found {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Found[i].UpdatedAt = time.Time{}
	}
	if !cmp.Equal(*got, want) {
		t.Fatal(cmp.Diff(want, *got))
	}

	// Also OK
	resp, err = tClientDelete.DeleteMultiElements(ctx, client.DeleteMultiElementsPath(t.Name()), payload)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNoContent)()
}

func TestElementsController_GetAround(t *testing.T) {
	t.Parallel()
	ctx := testCtx(t)
	createList(t, t.Name(), 100)

	putPayload := client.PutMultiElementsPayload{
		&client.Element{
			ID:         10,
			Payload:    []byte(`{"custom":"payload"}`),
			Score:      1010,
			TieBreaker: uint32p(10),
		},
		&client.Element{
			ID:         20,
			Payload:    []byte(`{"custom":"payload2"}`),
			Score:      2020,
			TieBreaker: uint32p(20),
		},
		&client.Element{
			ID:         30,
			Payload:    []byte(`{"custom":"payload3"}`),
			Score:      3030,
			TieBreaker: uint32p(30),
		},
	}

	// Scores are increasing, so we should get them back in reverse order.
	want := client.RankdbElementFull{
		FromBottom:      1,
		FromTop:         101,
		ID:              20,
		ListID:          t.Name(),
		LocalFromBottom: intp(1),
		LocalFromTop:    intp(1),
		Neighbors: &struct {
			Above client.RankdbElementCollection `form:"above,omitempty" json:"above,omitempty" xml:"above,omitempty"`
			Below client.RankdbElementCollection `form:"below,omitempty" json:"below,omitempty" xml:"below,omitempty"`
		}{
			Above: []*client.RankdbElement{{
				FromBottom: 2,
				FromTop:    100,
				ID:         putPayload[2].ID,
				ListID:     t.Name(),
				Payload:    putPayload[2].Payload,
				Score:      putPayload[2].Score,
				TieBreaker: *putPayload[2].TieBreaker,
			}},

			Below: []*client.RankdbElement{{
				FromBottom: 0,
				FromTop:    102,
				ID:         putPayload[0].ID,
				ListID:     t.Name(),
				Payload:    putPayload[0].Payload,
				Score:      putPayload[0].Score,
				TieBreaker: *putPayload[0].TieBreaker,
			}},
		},
		Payload:    putPayload[1].Payload,
		Score:      putPayload[1].Score,
		TieBreaker: *putPayload[1].TieBreaker,
		UpdatedAt:  time.Time{},
	}

	// Create them
	resp, err := tClientUpdate.PutMultiElements(ctx, client.PutMultiElementsPath(t.Name()), putPayload, boolp(true), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	payload := client.MultiElement{
		ElementIds: []uint64{30, 10},
	}
	// Attempt with no credentials
	resp, err = tClient.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with delete credentials
	resp, err = tClientDelete.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Attempt with update credentials
	resp, err = tClientUpdate.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// 404
	resp, err = tClientRead.GetAroundElements(ctx, client.GetAroundElementsPath("non-existing-list", "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()
	resp, err = tClientRead.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "200"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusNotFound)()

	// OK
	resp, err = tClientRead.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	// Custom clients
	// Wrong id
	tCustom := customTokenClient(ctx, t, "api:read", idToString(200), nil, nil)
	resp, err = tCustom.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Wrong list
	tCustom = customTokenClient(ctx, t, "api:read", nil, stringp("some-other-list"), nil)
	resp, err = tCustom.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusUnauthorized)()

	// Correct
	tCustom = customTokenClient(ctx, t, "api:read", stringp("10,20,30"), stringp(t.Name()), nil)
	resp, err = tCustom.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(5), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err := tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	if got.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt not set")
	}
	got.UpdatedAt = time.Time{}
	for i, v := range got.Neighbors.Below {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Neighbors.Below[i].UpdatedAt = time.Time{}
	}
	for i, v := range got.Neighbors.Above {
		if v.UpdatedAt.IsZero() {
			t.Fatal("updated_at not set")
		}
		got.Neighbors.Above[i].UpdatedAt = time.Time{}
	}
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}

	// range 0
	want.Neighbors.Above = nil
	want.Neighbors.Below = nil
	resp, err = tCustom.GetAroundElements(ctx, client.GetAroundElementsPath(t.Name(), "20"), &payload, intp(0), contentDefault)
	fatalErr(t, err)
	defer expectCode(t, resp, http.StatusOK)()

	got, err = tClient.DecodeRankdbElementFull(resp)
	fatalErr(t, err)
	if got.UpdatedAt.IsZero() {
		t.Fatal("UpdatedAt not set")
	}
	got.UpdatedAt = time.Time{}
	if !cmp.Equal(*got, want) {
		t.Logf("%#v", *got)
		t.Fatal(cmp.Diff(want, *got))
	}
}
