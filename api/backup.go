package api

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/api/client"
	"github.com/Vivino/rankdb/api/morph"
	"github.com/Vivino/rankdb/backup"
	"github.com/goadesign/goa"
)

// BackupController implements the backup resource.
type BackupController struct {
	*goa.Controller
}

// NewBackupController creates a backup controller.
func NewBackupController(service *goa.Service) *BackupController {
	return &BackupController{Controller: service.NewController("BackupController")}
}

// Delete runs the delete action.
func (c *BackupController) Delete(ctx *app.DeleteBackupContext) error {
	// BackupController_Delete: start_implement

	id := backup.ID(ctx.BackupID)
	status := id.Status()
	if status == nil {
		return goa.ErrNotFound("not_found", "backup_id", ctx.BackupID)
	}
	id.Cancel()
	// BackupController_Delete: end_implement
	return ctx.NoContent()
}

// Status runs the status action.
func (c *BackupController) Status(ctx *app.StatusBackupContext) error {
	// BackupController_Status: start_implement

	id := backup.ID(ctx.BackupID)
	status := id.Status()
	if status == nil {
		return goa.ErrNotFound("not_found", "backup_id", ctx.BackupID)
	}

	// BackupController_Status: end_implement
	return ctx.OK(morph.BackupStatus{ID: id, In: status}.Default())
}

// startBackup will start a background job to back up specified lists.
// Provides a callback id for retrieving status.
// errCh can be used for sending delayed error. Send nil if nothing.
func startBackup(lists rankdb.ListIDs, dst backup.Saver, errCh <-chan error) *app.RankdbCallback {
	res := app.RankdbCallback{}
	req := backup.Request{
		DB:    db,
		Dst:   dst,
		Lists: lists,
		ErrCh: errCh,
	}

	id := req.Start(bgCtx)
	res.ID = id.String()
	res.CallbackURL = client.StatusBackupPath(id.String())
	return &res
}
