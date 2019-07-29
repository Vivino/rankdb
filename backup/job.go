package backup

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/Vivino/rankdb"
	"github.com/Vivino/rankdb/log"
	"github.com/klauspost/compress/zstd"
	shutdown "github.com/klauspost/shutdown2"
)

var (
	// running backup jobs and a mutex for the list.
	running   = make(map[ID]*jobState)
	runningMu sync.Mutex
)

// ID of a backup
type ID string

// Status returns the status of the jobState with the id.
// Returns nil if the jobState cannot be found.
func (id ID) Status() *Status {
	runningMu.Lock()
	job, ok := running[id]
	runningMu.Unlock()
	if !ok {
		return nil
	}
	job.mu.Lock()
	status := job.status
	job.mu.Unlock()
	return &status
}

// Status returns the status of the jobState with the id.
// Returns nil if the jobState cannot be found.
func (id ID) Cancel() {
	runningMu.Lock()
	job, ok := running[id]
	runningMu.Unlock()
	if !ok {
		return
	}
	job.cancel()
	return
}

func (id ID) String() string {
	return string(id)
}

// Request is a request to start a backup on certain lists on a DB to a Saver.
type Request struct {
	DB     *rankdb.Manager
	Cancel context.CancelFunc
	Dst    Saver
	Lists  rankdb.ListIDs
	ErrCh  <-chan error
}

// jobState contains the internal state of a job.
type jobState struct {
	id     ID
	db     *rankdb.Manager
	cancel context.CancelFunc
	dst    Saver
	status Status
	mu     sync.Mutex
}

type Status struct {
	// Custom information provided by backup
	Custom map[string]string
	// URI of backed up content. Used for restore.
	Done bool
	// Failed operations, indexed by list IDs
	Errors map[string]string
	// Number of lists to be backed up
	Lists int
	// Number of lists done.
	ListsDone int
	// Size of stored data
	Size int64
	// Storage used for backup
	Storage string
	// Successful operations, list IDs
	Success []string

	Cancelled bool
	Started   time.Time
	Finished  *time.Time
}

// writeCounter counts the number of written bytes written to 'w'.
// The count 'n' can be accessed safely when holding the lock.
type writeCounter struct {
	w  io.Writer
	n  int64
	mu sync.Mutex
}

// Write and count the number of bytes written.
func (w *writeCounter) Write(p []byte) (n int, err error) {
	n, err = w.w.Write(p)
	w.mu.Lock()
	w.n += int64(n)
	w.mu.Unlock()
	return n, err
}

// backupLists backups elements
// Only one list will be processing at the time.
func (req Request) Start(bg context.Context) ID {
	// Use chose background for backups.
	ctx, cancel := context.WithCancel(bg)
	job := jobState{
		id:     ID(rankdb.RandString(10)),
		db:     req.DB,
		cancel: cancel,
		dst:    req.Dst,
		status: Status{
			Custom:    make(map[string]string),
			Done:      false,
			Errors:    make(map[string]string),
			Lists:     len(req.Lists),
			Size:      0,
			Storage:   fmt.Sprintf("%T", req.Dst),
			Success:   make([]string, 0, len(req.Lists)),
			Cancelled: false,
			Started:   time.Now(),
			Finished:  nil,
		},
	}
	jobID := job.id
	ctx = log.WithValues(ctx, "backup_id", job.id)
	defer shutdown.FirstFn(job.cancel, "Cancel Backup").Cancel()
	var wc writeCounter

	// Sort lists, for a bit better compression
	sort.Slice(req.Lists, func(i, j int) bool {
		return req.Lists[i] < req.Lists[j]
	})

	runningMu.Lock()
	running[job.id] = &job
	runningMu.Unlock()

	// Start backup
	go func() {
		// unlock and set status when done.
		defer func() {
			if req.ErrCh != nil {
				err := <-req.ErrCh
				if err != nil {
					job.mu.Lock()
					job.status.Errors["delayed_error"] = err.Error()
					job.mu.Unlock()
				}
			}
			wc.mu.Lock()
			n := wc.n
			wc.mu.Unlock()
			job.mu.Lock()
			job.status.Done = true
			job.status.Size = n
			now := time.Now()
			job.status.Finished = &now
			job.cancel()
			job.mu.Unlock()
		}()

		msg := rankdb.NewWriterMsg()
		dst, err := job.dst.Save(ctx)
		if err != nil {
			job.status.Errors["saver"] = err.Error()
			return
		}
		defer dst.Close()
		wc.w = dst
		zw, err := zstd.NewWriter(&wc)
		if err != nil {
			job.status.Errors["compress"] = err.Error()
			return
		}
		defer zw.Close()
		msg.ReplaceWriter(zw)
		defer msg.Close()
		db := job.db

		// The stream is just a collection of lists one after another.
		// Each one starts with a boolean (true)
		// So when a false is encountered it is the end of the stream.
		for _, id := range req.Lists {
			select {
			case <-ctx.Done():
				job.mu.Lock()
				job.status.Cancelled = true
				job.mu.Unlock()
				return
			default:
			}
			lst, ok := db.Lists.ByID(id)
			if !ok {
				job.mu.Lock()
				job.status.Errors[string(id)] = rankdb.ErrNotFound.Error()
				job.mu.Unlock()
				continue
			}
			err := backupList(ctx, db, lst, msg)
			if err != nil {
				log.Error(ctx, "List backup failed. Skipping to next.", "error", err.Error(), "list_id", lst.ID)
				job.mu.Lock()
				job.status.Errors[string(id)] = err.Error()
				job.mu.Unlock()
				continue
			}
			wc.mu.Lock()
			written := wc.n
			wc.mu.Unlock()
			job.mu.Lock()
			job.status.Success = append(job.status.Success, string(id))
			job.status.ListsDone++
			job.status.Size = written
			job.mu.Unlock()
		}
		err = msg.Writer().WriteBool(false)
		if err != nil {
			job.mu.Lock()
			job.status.Errors["finalizing"] = err.Error()
			job.mu.Unlock()
		}
		msg.Flush()
	}()

	log.Info(ctx, "Backup started")
	return jobID
}

const backupListVersion = 1

func backupList(ctx context.Context, db *rankdb.Manager, lst *rankdb.List, writer *rankdb.WriterMsgp) error {
	// Backup to buffer
	t := time.Now()
	var buf bytes.Buffer
	msg := rankdb.NewWriterMsg()
	msg.ReplaceWriter(&buf)

	err := lst.Backup(ctx, db.Storage, msg)
	if err != nil {
		return err
	}
	// Flush writer
	msg.Flush()

	// ok, add record.
	err = writer.Writer().WriteBool(true)
	if err != nil {
		return err
	}
	err = writer.SetVersion(backupListVersion)
	if err != nil {
		return err
	}
	err = writer.Writer().WriteString(string(lst.ID))
	if err != nil {
		return err
	}
	payload := buf.Bytes()
	log.Info(ctx, "Backed up list.", "payload_bytes", len(payload), "list_id", lst.ID, "elapsed", time.Since(t))
	return writer.Writer().WriteBytes(payload)
}
