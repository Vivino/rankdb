package backup

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"context"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/log"
	"github.com/klauspost/compress/zstd"
)

// RestoreInfo contains restoration info.
type RestoreInfo struct {
	// Source to read the data.
	Source Loader
	// Destinatio DB where lists should be stored.
	DB *rankdb.Manager
	// Replace existing lists.
	Replace bool
	// Cache assiciated with the DB.
	Cache rankdb.Cache

	// ListIDPrefix contains a prefix to apply to all lists.
	ListIDPrefix string
	// ListIDPrefix contains a suffix to apply to all lists.
	ListIDSuffix string
}

// RestoreResult is the result of a restore operation that completed.
type RestoreResult struct {
	Restored int
	Skipped  int
	Failed   map[string]string
}

// Restore a backup set.
// 'ri' should contain all needed information.
func (ri *RestoreInfo) Restore(ctx context.Context) (*RestoreResult, error) {
	var res RestoreResult
	r, err := ri.Source.Load(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	db := ri.DB
	msgr := rankdb.NewReaderMsgpReader(zr)
	msg := msgr.Reader()
	defer msgr.Close()
	for {
		// Boolean indicates if there are more lists.
		more, err := msg.ReadBool()
		if err != nil {
			return nil, err
		}
		if !more {
			log.Info(ctx, "Reached end of backup set")
			break
		}
		v := msgr.GetVersion()
		if v != backupListVersion {
			log.Error(ctx, "backupListVersion mismatch", "got_v", v, "want_v", backupListVersion, "err", err)
			return nil, rankdb.ErrVersionMismatch
		}
		listID, err := msg.ReadString()
		if err != nil {
			return nil, err
		}
		dstLstID := rankdb.ListID(ri.ListIDPrefix + listID + ri.ListIDSuffix)
		log.Info(ctx, "Restoring list", "list_id", listID, "dest_list_id", dstLstID)

		b, err := msg.ReadBytes(nil)
		if err != nil {
			return nil, err
		}
		if _, ok := ri.DB.Lists.ByID(dstLstID); ok {
			if !ri.Replace {
				res.Skipped++
				// We have already consumed the body, so we can safely continue.
				continue
			}
			// List will be shortly unavailable.
			err := ri.DB.DeleteList(ctx, dstLstID)
			if err != nil {
				log.Error(ctx, "Error Deleting List", "list_id", dstLstID, "error", err.Error())
				// Restore it anyway.
			}
		}

		lst, err := rankdb.RestoreList(ctx, db.Storage, rankdb.NewReaderMsgp(b), ri.Cache, &dstLstID)
		if err != nil {
			res.Failed[string(dstLstID)] = err.Error()
			log.Error(ctx, "Error Restoring List", "list_id", dstLstID, "error", err.Error())
			continue
		}
		db.Lists.Add(lst)
		res.Restored++
	}
	log.Info(ctx, "Restore ended", "error", err, "result", res)
	return &res, nil
}
