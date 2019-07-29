package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"fmt"
	"sort"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/api/morph"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
)

// ElementsController implements the list-elements resource.
type ElementsController struct {
	*goa.Controller
}

// NewElementsController creates a list-elements controller.
func NewElementsController(service *goa.Service) *ElementsController {
	return &ElementsController{Controller: service.NewController("ElementsController")}
}

// Create runs the create action.
func (c *ElementsController) Create(ctx *app.CreateElementsContext) error {
	// ListElementsController_Create: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}
	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	eid := morph.NewApiElementIDs(ctx.Payload.ID).ElementIDs()
	if err := HasAccessToElement(ctx, eid...); err != nil {
		return ctx.Unauthorized(err)
	}

	elems, err := lst.GetElements(ctx, db.Storage, eid, ctx.Range)
	if err != nil {
		return err
	}
	if len(elems) > 0 {
		return ctx.Conflict(ErrConflict("element already exists", "element_id", ctx.Payload.ID))
	}

	element := morph.NewApiElements(ctx.Payload).Elements()
	element.UpdateTime(time.Now())
	err = lst.Insert(ctx, db.Storage, element)
	if err != nil {
		return err
	}

	elems, err = lst.GetElements(ctx, db.Storage, eid, ctx.Range)
	if err != nil {
		return err
	}
	if len(elems) != 1 {
		return ctx.NotFound(goa.ErrNotFound("not_found", "element_id", ctx.Payload.ID))
	}

	// ListElementsController_Create: end_implement
	res := morph.RankedElement{In: &elems[0]}.Full(listID)
	return ctx.OKFull(res)
}

// Delete runs the delete action.
func (c *ElementsController) Delete(ctx *app.DeleteElementsContext) error {
	// ListElementsController_Delete: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	ids, err := morph.NewApiElementIDsString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	lIDs := ids.ElementIDs()
	if err := HasAccessToElement(ctx, lIDs...); err != nil {
		return ctx.Unauthorized(err)
	}

	err = lst.DeleteElements(ctx, db.Storage, lIDs)
	if err != nil {
		return err
	}
	// ListElementsController_Delete: end_implement
	return ctx.NoContent()
}

// DeleteMulti runs the delete action.
func (c *ElementsController) DeleteMulti(ctx *app.DeleteMultiElementsContext) error {
	// ListElementsController_Delete: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}
	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	ids, err := morph.NewApiElementIDsString(ctx.ElementIds...)
	if err != nil {
		return ctx.BadRequest(err)
	}
	lIDs := ids.ElementIDs()
	if err := HasAccessToElement(ctx, lIDs...); err != nil {
		return ctx.Unauthorized(err)
	}

	err = lst.DeleteElements(ctx, db.Storage, lIDs)
	if err != nil {
		return err
	}
	// ListElementsController_Delete: end_implement
	return ctx.NoContent()
}

// Get runs the get action.
func (c *ElementsController) Get(ctx *app.GetElementsContext) error {
	// ListElementsController_Get: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	ids, err := morph.NewApiElementIDsString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	if err := HasAccessToElement(ctx, ids.ElementIDs()...); err != nil {
		return err
	}
	re, err := lst.GetElements(ctx, db.Storage, ids.ElementIDs(), ctx.Range)
	if err != nil {
		return err
	}
	if len(re) == 0 {
		return ctx.NotFound(goa.ErrNotFound("not_found", "element_id", ctx.ElementID))
	}
	// ListElementsController_Get: end_implement
	res := morph.RankedElement{In: &re[0]}.Full(listID)
	return ctx.OKFull(res)
}

// GetMulti runs the get-multi action.
func (c *ElementsController) GetMulti(ctx *app.GetMultiElementsContext) error {
	// ListElementsController_GetMulti: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	ids := morph.ApiElementIDs{In: ctx.Payload.ElementIds}.ElementIDs()
	ids.Deduplicate()
	if len(ids) == 0 {
		return ctx.OK(&app.RankdbMultielement{})
	}
	if err := HasAccessToElement(ctx, ids...); err != nil {
		return ctx.Unauthorized(err)
	}
	re, err := lst.GetElements(ctx, db.Storage, ids, 0)
	if err != nil {
		return err
	}

	var notFound []uint64
	if len(re) != len(ids) {
		notFound = morph.ElementIDs{In: ids.NotIn(re.IDs())}.Array()
		// Sort returned ids for consistent results.
		sort.Slice(notFound, func(i, j int) bool {
			return notFound[i] < notFound[j]
		})
	}
	re.Sort()
	// ListElementsController_GetMulti: end_implement
	res := &app.RankdbMultielement{
		Found:    morph.RankedElements{In: re}.Default(listID),
		NotFound: notFound,
	}
	return ctx.OK(res)
}

// GetNear runs the get-near action.
func (c *ElementsController) GetAround(ctx *app.GetAroundElementsContext) error {
	// ListElementsController_GetMulti: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	id, err := morph.NewApiElementIDString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	ids := morph.ApiElementIDs{In: ctx.Payload.ElementIds}.ElementIDs()
	ids = append(ids, id.ElementID())
	ids.Deduplicate()
	if len(ids) == 0 {
		return ctx.NotFound(goa.ErrNotFound("not_found", "element_ids", ids))
	}
	if err := HasAccessToElement(ctx, ids...); err != nil {
		return ctx.Unauthorized(err)
	}
	re, err := lst.GetElements(ctx, db.Storage, ids, 0)
	if err != nil {
		return err
	}
	re.Sort()
	found := -1
	for i, elem := range re {
		if elem.ID == id.ElementID() {
			found = i
			break
		}
	}
	if found == -1 {
		return ctx.NotFound(goa.ErrNotFound("not_found", "element_id", ctx.ElementID))
	}
	center := re[found]
	below := re[found+1:]
	if len(below) > ctx.Range {
		below = below[:ctx.Range]
	}
	above := re[:found]
	if len(above) > ctx.Range {
		above = above[len(above)-ctx.Range:]
	}
	log.Info(ctx, "Found", "req_ids", len(ids), "found_ids", len(re), "found_idx", found, "above", len(above), "below", len(below))
	res := morph.RankedElement{In: &center}.FullExt(listID, below, above)
	res.LocalFromTop = &found
	bot := len(re) - found - 1
	res.LocalFromBottom = &bot
	return ctx.OKFull(res)
}

// Put runs the put action.
func (c *ElementsController) Put(ctx *app.PutElementsContext) error {
	// ListElementsController_Put: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	ids, err := morph.NewApiElementIDsString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	if ids.In[0] != ctx.Payload.ID {
		return ctx.BadRequest(fmt.Errorf("id mismatch. url: %v; payload: %v", ids.In[0], ctx.Payload.ID))
	}
	if err := HasAccessToElement(ctx, ids.ElementIDs()...); err != nil {
		return ctx.Unauthorized(err)
	}
	elems := morph.NewApiElements(ctx.Payload).Elements()
	elems.UpdateTime(time.Now())
	re, err := lst.UpdateElements(ctx, db.Storage, elems, ctx.Range, true)
	if err != nil {
		return err
	}
	if len(re) == 0 {
		// Not found is weird...
		return ctx.NotFound(goa.ErrNotFound("not_found", "element_id", ctx.ElementID))
	}

	// ListElementsController_Put: end_implement
	res := morph.RankedElement{In: &re[0]}.FullUpdate(listID)
	return ctx.OKFullUpdate(res)
}

// Put runs the put action.
func (c *ElementsController) PutMulti(ctx *app.PutMultiElementsContext) error {
	// ListElementsController_PutMulti: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	listID := rankdb.ListID(ctx.ListID)
	if err := HasAccessToList(ctx, listID); err != nil {
		return ctx.Unauthorized(err)
	}
	lst, ok := db.Lists.ByID(listID)
	if !ok {
		return ctx.NotFound(goa.ErrNotFound("not_found", "list_id", ctx.ListID))
	}
	elems := morph.ApiElements{In: ctx.Payload}.Elements()
	elems.UpdateTime(time.Now())
	elems.Deduplicate()

	if err := HasAccessToElement(ctx, elems.IDs()...); err != nil {
		return ctx.Unauthorized(err)
	}
	re, err := lst.UpdateElements(ctx, db.Storage, elems, 0, ctx.Results)
	if err != nil {
		return err
	}
	re.Sort()

	res := &app.RankdbMultielement{
		Found: morph.RankedElements{In: re}.Default(listID),
	}

	// ListElementsController_PutMulti: end_implement
	return ctx.OK(res)
}
