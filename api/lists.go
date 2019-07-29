package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"strconv"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/api/morph"
	"github.com/goadesign/goa"
)

// ListsController implements the lists resource.
type ListsController struct {
	*goa.Controller
}

// NewListsController creates a lists controller.
func NewListsController(service *goa.Service) *ListsController {
	return &ListsController{Controller: service.NewController("ListsController")}
}

// Create runs the create action.
func (c *ListsController) Create(ctx *app.CreateListsContext) error {
	// ListsController_Create: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}
	var oldList *rankdb.List
	if lst, ok := db.Lists.ByID(rankdb.ListID(ctx.Payload.ID)); ok {
		if !ctx.Replace {
			return ctx.Conflict(ErrConflict("list already exists", "list_id", ctx.Payload.ID))
		}
		oldList = lst
	}

	var opts []rankdb.ListOption
	opts = append(opts, rankdb.WithListOption.Cache(cache))
	opts = append(opts, rankdb.WithListOption.Metadata(ctx.Payload.Metadata))
	opts = append(opts, rankdb.WithListOption.MergeSplitSize(ctx.Payload.MergeSize, ctx.Payload.SplitSize))
	opts = append(opts, rankdb.WithListOption.LoadIndex(ctx.Payload.LoadIndex))
	if ctx.Payload.Populate != nil && len(ctx.Payload.Populate) > 0 {
		elems := morph.ApiElements{In: ctx.Payload.Populate}.Elements()
		elems.UpdateTime(time.Now())
		elems.Deduplicate()
		opts = append(opts, rankdb.WithListOption.Populate(elems))
		// Free memory
		ctx.Payload.Populate = nil
	}

	// Create List
	list, err := rankdb.NewList(
		ctx,
		rankdb.ListID(ctx.Payload.ID),
		ctx.Payload.Set,
		db.Storage,
		opts...,
	)
	if err != nil {
		return ctx.BadRequest(err)
	}
	if oldList != nil {
		err := oldList.DeleteAll(ctx, db.Storage)
		if err != nil {
			return err
		}
	}

	// Add to manager
	db.Lists.Add(list)

	// ListsController_Create: end_implement
	return ctx.OKFull(morph.List{In: list}.Full(ctx, db.Storage, true))

}

// Delete runs the delete action.
func (c *ListsController) Delete(ctx *app.DeleteListsContext) error {
	// ListsController_Delete: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	err := db.DeleteList(ctx, rankdb.ListID(ctx.ListID))
	if err != nil {
		return err
	}
	// ListsController_Delete: end_implement
	return ctx.NoContent()
}

// Get runs the get action.
func (c *ListsController) Get(ctx *app.GetListsContext) error {
	// ListsController_Get: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}

	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	// ListsController_Get: end_implement

	return ctx.OKFull(morph.List{In: lst}.Full(ctx, db.Storage, ctx.TopBottom))
}

// GetPercentile runs the get-percentile action.
func (c *ListsController) GetPercentile(ctx *app.GetPercentileListsContext) error {
	// ListsController_GetPercentile: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}

	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	percentile := float64(50.0)
	if fromTop, err2 := strconv.ParseFloat(ctx.FromTop, 64); err2 == nil {
		percentile = fromTop
	} else {
		return ctx.BadRequest(goa.InvalidParamTypeError("from_top", ctx.FromTop, "number"))
	}
	if percentile < 0 {
		return ctx.BadRequest(goa.InvalidRangeError(`from_top`, ctx.FromTop, 0, true))
	}
	if percentile > 100 {
		return ctx.BadRequest(goa.InvalidRangeError(`from_top`, ctx.FromTop, 100, false))
	}
	re, err := lst.GetPercentile(ctx, db.Storage, percentile/100, ctx.Range)
	if err != nil {
		return err
	}
	if re == nil {
		return ctx.NotFound(goa.ErrNotFound("no_element", "list_id", listID))
	}

	// ListsController_GetPercentile: end_implement
	return ctx.OKFull(morph.RankedElement{In: re}.Full(listID))
}

// GetRange runs the get-range action.
func (c *ListsController) GetRange(ctx *app.GetRangeListsContext) error {
	// ListsController_GetRange: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}

	if ctx.FromTop == nil && ctx.FromBottom == nil {
		return ctx.BadRequest(goa.ErrBadRequest("neither from_top nor from_bottom specified"))
	}
	if ctx.FromTop != nil && ctx.FromBottom != nil {
		return ctx.BadRequest(goa.ErrBadRequest("both from_top and from_bottom specified"))
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	total, err := lst.Len(ctx, db.Storage)
	if err != nil {
		return err
	}
	var elems rankdb.Elements
	var res app.RankdbElementCollection
	if ctx.FromTop != nil {
		fromTop := *ctx.FromTop
		elems, err = lst.GetRankTop(ctx, db.Storage, fromTop, ctx.Limit)
		res = morph.Elements{In: elems}.ToRanked(fromTop, total-fromTop-1, listID)
	} else {
		fromBottom := *ctx.FromBottom
		elems, err = lst.GetRankBottom(ctx, db.Storage, *ctx.FromBottom, ctx.Limit)
		res = morph.Elements{In: elems}.ToRanked(total-fromBottom-1, fromBottom, listID)
	}
	if err != nil {
		switch err {
		case rankdb.ErrOffsetOutOfBounds:
			return ctx.NotFound(goa.ErrNotFound(err.Error(), "list_id", listID))
		}
		return err
	}
	if res == nil {
		return ctx.NotFound(goa.ErrNotFound("no_element", "list_id", listID))
	}

	// ListsController_GetRange: end_implement
	return ctx.OK(res)
}

// GetAll runs the get_all action.
func (c *ListsController) GetAll(ctx *app.GetAllListsContext) error {
	// ListsController_GetAll: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	var lists []*rankdb.List
	var page rankdb.PageInfo
	if ctx.BeforeID != "" {
		lists, page = db.Lists.SortedIDsBefore(rankdb.ListID(ctx.BeforeID), ctx.Limit)
	} else {
		lists, page = db.Lists.SortedIDsAfter(rankdb.ListID(ctx.AfterID), ctx.Limit)
	}
	res := app.RankdbListsresult{ListsBefore: page.Before, ListsAfter: page.After}
	res.Lists = make(app.RankdbRanklistCollection, 0, len(lists))
	for _, list := range lists {
		res.Lists = append(res.Lists, morph.List{In: list}.Default())
	}
	// ListsController_GetAll: end_implement
	return ctx.OK(&res)
}

// Reindex runs the reindex action.
func (c *ListsController) Reindex(ctx *app.ReindexListsContext) error {
	// ListsController_Reindex: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	err := lst.Reindex(ctx, db.Storage)
	if err != nil {
		return err
	}
	// ListsController_Reindex: end_implement
	return ctx.OKFull(morph.List{In: lst}.Full(ctx, db.Storage, true))
}

// Reindex runs the reindex action.
func (c *ListsController) Repair(ctx *app.RepairListsContext) error {
	// ListsController_Repair: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return ctx.OK(&app.RankdbListopresult{Error: errAsStringP(err)})
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	err := lst.Repair(ctx, db.Storage, ctx.Clear)
	if err != nil {
		return ctx.OK(&app.RankdbListopresult{Error: errAsStringP(err)})
	}

	// ListsController_Repair: end_implement
	return ctx.OK(&app.RankdbListopresult{List: morph.List{In: lst}.Default()})
}

func errAsStringP(err error) *string {
	if err == nil {
		return nil
	}
	e := err.Error()
	return &e
}

// Verify runs the verify action.
func (c *ListsController) Verify(ctx *app.VerifyListsContext) error {
	// ListsController_Verify: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	err := lst.Verify(ctx, db.Storage)
	if err == nil {
		err = lst.VerifyElements(ctx, db.Storage)
	}
	if err == nil {
		return ctx.OK(&app.RankdbListopresult{List: morph.List{In: lst}.Default()})
	}
	if ctx.Repair {
		err = lst.Repair(ctx, db.Storage, ctx.Clear)
		if err != nil {
			return ctx.OK(&app.RankdbListopresult{Error: errAsStringP(err)})
		}
	} else {
		return ctx.OK(&app.RankdbListopresult{Error: errAsStringP(err)})
	}
	// ListsController_Verify: end_implement
	return ctx.OK(&app.RankdbListopresult{List: morph.List{In: lst}.Default()})
}

// Clone runs the clone action.
func (c *ListsController) Clone(ctx *app.CloneListsContext) error {
	// ListsController_Create: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	orgID := rankdb.ListID(ctx.ListID)
	org, ok := db.Lists.ByID(orgID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", orgID))
	}
	if len(db.Lists.Exists(rankdb.ListID(ctx.Payload.ID))) > 0 {
		return ctx.Conflict(ErrConflict("list already exists", "list_id", ctx.Payload.ID))
	}

	opts := []rankdb.ListOption{}
	opts = append(opts, rankdb.WithListOption.Cache(cache))
	opts = append(opts, rankdb.WithListOption.Metadata(ctx.Payload.Metadata))
	opts = append(opts, rankdb.WithListOption.MergeSplitSize(ctx.Payload.MergeSize, ctx.Payload.SplitSize))
	opts = append(opts, rankdb.WithListOption.Clone(org))
	opts = append(opts, rankdb.WithListOption.LoadIndex(ctx.Payload.LoadIndex))

	if ctx.Payload.Populate != nil {
		return ctx.BadRequest(goa.ErrBadRequest("cloned lists cannot contain payload", "list_id", ctx.Payload.ID))
	}

	// Create List
	list, err := rankdb.NewList(
		ctx,
		rankdb.ListID(ctx.Payload.ID),
		ctx.Payload.Set,
		db.Storage,
		opts...,
	)
	if err != nil {
		return err
	}

	// Add to manager
	db.Lists.Add(list)

	// ListsController_Create: end_implement
	return ctx.OKFull(morph.List{In: list}.Full(ctx, db.Storage, true))

}
