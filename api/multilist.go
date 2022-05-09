package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/api/morph"
	"github.com/Vivino/rankdb/backup"
	"github.com/Vivino/rankdb/backup/file"
	"github.com/Vivino/rankdb/backup/reader"
	"github.com/Vivino/rankdb/backup/s3"
	"github.com/Vivino/rankdb/backup/server"
	"github.com/Vivino/rankdb/log"
	"github.com/goadesign/goa"
)

// MultilistController implements the multilist-elements resource.
type MultilistController struct {
	*goa.Controller
}

// NewMultilistElementsController creates a multilist-elements controller.
func NewMultilistElementsController(service *goa.Service) *MultilistController {
	return &MultilistController{Controller: service.NewController("MultilistController")}
}

// Create runs the create action.
func (c *MultilistController) Create(ctx *app.CreateMultilistContext) error {
	// MultilistElementsController_Create: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Payload.Lists}.IDs()
	if len(ctx.Payload.MatchMetadata) > 0 {
		lists = append(lists, db.Lists.MatchAll(ctx.Payload.MatchMetadata, ctx.Payload.AllInSets)...)
	}
	lists.Deduplicate()
	if err := HasAccessToList(ctx, lists...); err != nil {
		return ctx.Unauthorized(err)
	}

	in := morph.ApiElements{In: ctx.Payload.Payload}.Elements()
	in.Deduplicate()
	in.UpdateTime(time.Now())
	if err := HasAccessToElement(ctx, in.IDs()...); err != nil {
		return ctx.Unauthorized(err)
	}

	// Execute
	res := onEveryList(ctx, lists, 16, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		elems, err := lst.GetElements(ctx, db.Storage, in.IDs(), 0)
		if err != nil {
			return nil, err
		}
		if len(elems) > 0 {
			return nil, ErrConflict("element(s) already exists", "element_ids", elems.IDs())
		}

		err = lst.Insert(ctx, db.Storage, in)
		if err != nil {
			return nil, err
		}
		if !ctx.Results {
			return &app.RankdbOperationSuccess{}, nil
		}
		elems, err = lst.GetElements(ctx, db.Storage, in.IDs(), 0)
		if err != nil {
			return nil, err
		}

		return &app.RankdbOperationSuccess{Results: morph.RankedElements{In: elems}.Tiny()}, nil
	})
	if ctx.ErrorsOnly {
		res.Success = nil
	}
	return ctx.OK(res)
}

// Delete runs the delete action.
func (c *MultilistController) Delete(ctx *app.DeleteMultilistContext) error {
	// MultilistElementsController_Delete: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Lists}.IDs()
	var match map[string]string
	if ctx.MatchMetadata != nil && len(*ctx.MatchMetadata) > 0 {
		err := json.Unmarshal([]byte(*ctx.MatchMetadata), &match)
		if err != nil {
			return ctx.BadRequest(err)
		}
	}
	lists = append(lists, db.Lists.MatchAll(match, ctx.AllInSets)...)
	lists.Deduplicate()
	if err := HasAccessToList(ctx, lists...); err != nil {
		return ctx.Unauthorized(err)
	}

	elems, err := morph.NewApiElementIDsString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	ids := elems.ElementIDs()
	if err := HasAccessToElement(ctx, ids...); err != nil {
		return ctx.Unauthorized(err)
	}

	// Execute
	res := onEveryList(ctx, lists, 32, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		err := lst.DeleteElements(ctx, db.Storage, ids)
		if err != nil {
			return nil, err
		}
		return &app.RankdbOperationSuccess{}, nil
	})
	if ctx.ErrorsOnly {
		res.Success = nil
	}
	// MultilistElementsController_Delete: end_implement
	return ctx.OK(res)
}

// Get runs the get action.
func (c *MultilistController) Get(ctx *app.GetMultilistContext) error {
	// MultilistElementsController_Get: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)

	lists := morph.APIListIDs{In: ctx.Lists}.IDs()
	var match map[string]string
	if ctx.MatchMetadata != nil && len(*ctx.MatchMetadata) > 0 {
		err := json.Unmarshal([]byte(*ctx.MatchMetadata), &match)
		if err != nil {
			return ctx.BadRequest(err)
		}
	}
	lists = append(lists, db.Lists.MatchAll(match, ctx.AllInSets)...)
	lists.Deduplicate()
	if err := HasAccessToList(ctx, lists...); err != nil {
		return ctx.Unauthorized(err)
	}

	elems, err := morph.NewApiElementIDsString(ctx.ElementID)
	if err != nil {
		return ctx.BadRequest(err)
	}
	ids := elems.ElementIDs()
	if err := HasAccessToElement(ctx, ids...); err != nil {
		return ctx.Unauthorized(err)
	}

	// Execute
	res := onEveryList(ctx, lists, 64, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		elems, err := lst.GetElements(ctx, db.Storage, ids, 0)
		if err != nil {
			return nil, err
		}

		return &app.RankdbOperationSuccess{Results: morph.RankedElements{In: elems}.Tiny()}, nil
	})

	// MultilistElementsController_Get: end_implement
	return ctx.OK(res)
}

// Put runs the put action.
func (c *MultilistController) Put(ctx *app.PutMultilistContext) error {
	// MultilistElementsController_Put: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Payload.Lists}.IDs()
	if len(ctx.Payload.MatchMetadata) > 0 {
		lists = append(lists, db.Lists.MatchAll(ctx.Payload.MatchMetadata, ctx.Payload.AllInSets)...)
	}
	lists.Deduplicate()
	if err := HasAccessToList(ctx, lists...); err != nil {
		return ctx.Unauthorized(err)
	}

	in := morph.ApiElements{In: ctx.Payload.Payload}.Elements()
	in.UpdateTime(time.Now())
	in.Deduplicate()
	if err := HasAccessToElement(ctx, in.IDs()...); err != nil {
		return ctx.Unauthorized(err)
	}

	// Execute

	res := onEveryList(ctx, lists, 32, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		elems, err := lst.UpdateElements(ctx, db.Storage, in, 0, ctx.Results)
		if err != nil {
			return nil, err
		}

		return &app.RankdbOperationSuccess{Results: morph.RankedElements{In: elems}.Tiny()}, nil
	})
	if ctx.ErrorsOnly {
		res.Success = nil
	}

	// MultilistElementsController_Put: end_implement
	return ctx.OK(res)
}

// Create runs the create action.
func (c *MultilistController) Verify(ctx *app.VerifyMultilistContext) error {
	// MultilistElementsController_Verify: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Payload.Lists}.IDs()
	if len(ctx.Payload.MatchMetadata) > 0 {
		lists = append(lists, db.Lists.MatchAll(ctx.Payload.MatchMetadata, ctx.Payload.AllInSets)...)
	}
	lists.Deduplicate()
	if len(ctx.Payload.Lists)+len(ctx.Payload.MatchMetadata) == 0 {
		lists = db.Lists.All()
	}

	// Execute
	limit := 8
	if ctx.Repair {
		limit = 2
	}
	res := onEveryList(ctx, lists, limit, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		err := lst.Verify(ctx, db.Storage)
		if err != nil && ctx.Repair {
			log.Info(ctx, "Repairing list because of error", "error", err, "list_id", lst.ID)
			err = lst.Repair(ctx, db.Storage, ctx.Clear)
			if err != nil {
				return nil, err
			}
			err = lst.Verify(ctx, db.Storage)
		}
		if err != nil {
			return nil, err
		}
		if !ctx.Elements {
			return &app.RankdbOperationSuccess{}, nil
		}
		err = lst.VerifyElements(ctx, db.Storage)
		if err != nil && ctx.Repair {
			log.Info(ctx, "Repairing list because of error", "error", err, "list_id", lst.ID)
			err = lst.Repair(ctx, db.Storage, ctx.Clear)
			if err != nil {
				return nil, err
			}
			err = lst.VerifyElements(ctx, db.Storage)
		}
		if err != nil {
			return nil, err
		}
		return &app.RankdbOperationSuccess{}, nil
	})
	if ctx.ErrorsOnly {
		res.Success = nil
	}

	// MultilistElementsController_Verify: end_implement
	return ctx.OK(res)
}

// Reindex runs the reindex action.
func (c *MultilistController) Reindex(ctx *app.ReindexMultilistContext) error {
	// MultilistElementsController_Reindex: start_implement
	ctx.Context = CancelWithRequest(ctx.Context, ctx.Request)
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Payload.Lists}.IDs()
	if len(ctx.Payload.MatchMetadata) > 0 {
		lists = append(lists, db.Lists.MatchAll(ctx.Payload.MatchMetadata, ctx.Payload.AllInSets)...)
	}
	lists.Deduplicate()
	if len(ctx.Payload.Lists)+len(ctx.Payload.MatchMetadata) == 0 {
		lists = db.Lists.All()
	}

	// Execute
	res := onEveryList(ctx, lists, 1, func(lst *rankdb.List) (*app.RankdbOperationSuccess, error) {
		err := lst.Reindex(ctx, db.Storage)
		if err != nil {
			return nil, err
		}
		return &app.RankdbOperationSuccess{}, nil
	})
	if ctx.ErrorsOnly {
		res.Success = nil
	}

	// MultilistElementsController_Verify: end_implement
	return ctx.OK(res)
}

// Create runs the create action.
func (c *MultilistController) Backup(ctx *app.BackupMultilistContext) error {
	// MultilistElementsController_Backup: start_implement
	if done, err := UpdateRequest(ctx); err != nil {
		return err
	} else {
		defer done()
	}

	// Collect list ids.
	lists := morph.APIListIDs{In: ctx.Payload.Lists.Lists}.IDs()
	if len(ctx.Payload.Lists.MatchMetadata) > 0 {
		lists = append(lists, db.Lists.MatchAll(ctx.Payload.Lists.MatchMetadata, ctx.Payload.Lists.AllInSets)...)
	}
	lists.Deduplicate()
	if len(ctx.Payload.Lists.Lists)+len(ctx.Payload.Lists.MatchMetadata) == 0 {
		lists = db.Lists.All()
	}

	var dst backup.Saver
	var errCh chan error
	switch ctx.Payload.Destination.Type {
	case "file":
		if ctx.Payload.Destination.Path == nil || *ctx.Payload.Destination.Path == "" {
			return goa.ErrBadRequest("path must be specified for file backups")
		}
		dst = file.New(*ctx.Payload.Destination.Path)
	case "download":
		xfer := reader.NewReader()
		bi := startBackup(lists, xfer, nil)
		ctx.ResponseData.Header().Set("Content-Type", "application/octet-stream")
		ctx.ResponseData.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.bin"`, bi.ID))
		ctx.ResponseData.Header().Set("X-Backup-ID", bi.ID)
		ctx.ResponseData.Header().Set("Transfer-Encoding", "chunked")
		ctx.ResponseData.Header().Set("X-Content-Type-Options", "nosniff")
		ctx.ResponseData.WriteHeader(200)
		stream := xfer.Output()
		defer stream.Close()
		n, err := io.CopyBuffer(ctx.ResponseData.ResponseWriter, stream, make([]byte, 64<<10))
		log.Info(ctx, "Finished sending backup data", "bytes", n, "error", err)
		return err
	case "s3":
		if awsSession == nil {
			return goa.ErrBadRequest("AWS not configured on server")
		}
		if ctx.Payload.Destination.Path == nil || *ctx.Payload.Destination.Path == "" {
			return goa.ErrBadRequest("path must be specified for s3 backups")
		}
		u, err := url.Parse(*ctx.Payload.Destination.Path)
		if err != nil {
			return goa.ErrBadRequest("unable to parse s3 url: " + err.Error() + ". Path should be s3://bucket/path/file.bin")
		}
		s3Client := s3.New(u.Path, u.Host, awsSession)
		errCh = s3Client.ResultErr
		dst = s3Client
	case "server":
		if ctx.Payload.Destination.Path == nil || *ctx.Payload.Destination.Path == "" {
			return goa.ErrBadRequest("path must be specified for server backups")
		}
		srv, err := server.NewRankDB(ctx, *ctx.Payload.Destination.Path)
		if err != nil {
			return goa.ErrBadRequest(err)
		}
		srv.ListIDPrefix = ctx.Payload.Destination.ServerListIDPrefix
		srv.ListIDSuffix = ctx.Payload.Destination.ServerListIDSuffix
		dst = srv
	default:
		return goa.ErrBadRequest("unknown destination type")
	}

	// MultilistElementsController_Backup: end_implement
	return ctx.Created(startBackup(lists, dst, errCh))
}

// Create runs the create action.
func (c *MultilistController) Restore(ctx *app.RestoreMultilistContext) error {
	// MultilistElementsController_Backup: start_implement
	done, err := UpdateRequest(ctx)
	if err != nil {
		return err
	}
	defer done()
	defer ctx.Body.Close()

	info := backup.RestoreInfo{
		Source:  backup.WrapReader{ReadCloser: ctx.Body},
		DB:      db,
		Replace: !ctx.Keep,
		Cache:   cache,
	}
	if ctx.ListIDPrefix != nil {
		info.ListIDPrefix = *ctx.ListIDPrefix
	}
	if ctx.ListIDSuffix != nil {
		info.ListIDSuffix = *ctx.ListIDSuffix
	}
	if ctx.Src != "" {
		_, _ = io.Copy(ioutil.Discard, ctx.Body)
		ctx.Body.Close()
		switch {
		case strings.HasPrefix(ctx.Src, "s3://"):
			if awsSession == nil {
				return goa.ErrBadRequest("AWS not configured on server")
			}
			u, err := url.Parse(ctx.Src)
			if err != nil {
				return goa.ErrBadRequest("unable to parse s3 url: " + err.Error())
			}
			info.Source = s3.New(u.Path, u.Host, awsSession)
		default:
			info.Source = backup.HTTPLoader{URL: ctx.Src, Client: http.DefaultClient}
		}
	}
	if ctx.SrcFile != "" {
		_, _ = io.Copy(ioutil.Discard, ctx.Body)
		ctx.Body.Close()
		f, err := os.Open(ctx.SrcFile)
		if err != nil {
			log.Error(ctx, "Error opening file data", "error", err)
			return goa.ErrBadRequest(err)
		}
		info.Source = backup.WrapReader{ReadCloser: f}
	}
	res, err := info.Restore(bgCtx)
	if err != nil {
		log.Error(ctx, "Error restoring data", "error", err)
		return goa.ErrBadRequest(err)
	}
	log.Info(ctx, "Finished restoring lists")
	// MultilistElementsController_Backup: end_implement
	return ctx.OK(morph.RestoreResult{In: res}.Default())
}

// onEveryList applies a function to all lists represented by the sent ids.
func onEveryList(ctx context.Context, lists rankdb.ListIDs, limit int, fn func(l *rankdb.List) (*app.RankdbOperationSuccess, error)) *app.RankdbResultlist {
	var wg sync.WaitGroup
	wg.Add(len(lists))
	var mu sync.Mutex
	var results = app.RankdbResultlist{
		Success: make(map[string]*app.RankdbOperationSuccess, len(lists)),
		Errors:  make(map[string]string, 0),
	}
	var tokens = make(chan struct{}, limit)
	for i := 0; i < limit; i++ {
		tokens <- struct{}{}
	}
	for i, id := range lists {
		if err := ctx.Err(); err != nil {
			return nil
		}
		if limit > 0 {
			<-tokens
		}
		go func(i int, id rankdb.ListID) {
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					results.Errors[string(id)] = fmt.Sprint(r)
					fmt.Printf("%v: %s\n", r, string(debug.Stack()))
					mu.Unlock()
				}
				if limit > 0 {
					tokens <- struct{}{}
				}
				wg.Done()
			}()
			lst, ok := db.Lists.ByID(id)
			if !ok {
				mu.Lock()
				results.Errors[string(id)] = rankdb.ErrNotFound.Error()
				mu.Unlock()
				return
			}
			res, err := fn(lst)
			if err != nil {
				mu.Lock()
				results.Errors[string(id)] = err.Error()
				mu.Unlock()
				return
			}
			mu.Lock()
			results.Success[string(id)] = res
			mu.Unlock()
		}(i, id)
	}
	wg.Wait()
	return &results
}
