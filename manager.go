package rankdb

// Copyright 2019 Vivino. All rights reserved
//
// See LICENSE file for license details

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/Vivino/rankdb/blobstore"
	"github.com/Vivino/rankdb/log"
)

type Manager struct {
	Storage     blobstore.Store
	Backup      blobstore.WithSet
	Set         string
	Lists       Lists
	BackupEvery int

	wantSplit   chan *List
	wantRelease chan *List
}

// NewManager will create a new manager that manages the lists of the server.
func NewManager(store blobstore.Store, set string) (*Manager, error) {
	if set == "" {
		return nil, ErrEmptySet
	}
	return &Manager{Storage: store,
		Set:         set,
		wantSplit:   make(chan *List, 1000),
		wantRelease: make(chan *List, 10),
	}, nil
}

const (
	listStorageKey       = "lists"
	backupListStorageKey = "lists-backup"
)

// NewLists will initialize an empty lists set and attach the cache.
func (m *Manager) NewLists(cache Cache) error {
	m.Lists.cache = cache
	m.Lists.lists = &sync.Map{}
	if m.wantSplit == nil {
		m.wantSplit = make(chan *List, 1000)
	}
	m.Lists.manager = m
	return nil
}

// StartIntervalSaver will start a saver that will save the lists at fixed intervals.
// If the interval is 0 the lists are never saved.
// The lists are saved on shutdown. The provided channel is closed when save has finished.
func (m *Manager) StartIntervalSaver(ctx context.Context, every time.Duration, shutdown chan chan struct{}) {
	ctx = log.WithValues(ctx, "service", "Manager:IntervalSaver")
	if every == 0 {
		every = time.Hour * time.Millisecond
	}
	var lastBackup int
	go func() {
		ticker := time.NewTicker(every)
		log.Info(ctx, "Starting", "interval", every)
		defer log.Info(ctx, "Stopped")
		for {
			select {
			case <-ticker.C:
				t := time.Now()
				log.Info(ctx, "Saving lists")
				var backup = m.Backup != nil && m.BackupEvery > 0 && lastBackup >= m.BackupEvery
				if backup {
					lastBackup = 0
				}
				err := m.SaveLists(ctx, backup)
				if err != nil {
					log.Error(ctx, "Unable to save lists", "error", err)
				}
				log.Info(ctx, "Saving done", "duration", time.Since(t))
			case v := <-shutdown:
				log.Info(ctx, "Saving lists (shutdown)")
				err := m.SaveLists(ctx, false)
				if err != nil {
					log.Error(ctx, "Unable to save lists", "error", err)
				}
				log.Info(ctx, "Saving done (shutdown)")
				close(v)
				return
			}
			lastBackup++
		}
	}()
}

// StartListPruner will start an async list pruner that will unload
// elements and segments at regular intervals.
// If the interval provided is 0 the lists are never pruned.
func (m *Manager) StartListPruner(ctx context.Context, every time.Duration, shutdown chan chan struct{}) {
	ctx = log.WithValues(ctx, "service", "Manager:ListPruner")
	if every == 0 {
		go func() {
			v := <-shutdown
			close(v)
		}()
		return
	}
	go func() {
		ticker := time.NewTicker(every)
		log.Info(ctx, "Starting", "interval", every)
		for {
			select {
			case <-ticker.C:
				t := time.Now()
				log.Info(ctx, "Pruning lists")
				err := m.Lists.Prune(ctx)
				if err != nil {
					log.Info(ctx, "Unable to prune lists", "error", err)
				}
				log.Info(ctx, "Pruning done", "duration", time.Since(t))
			case v := <-shutdown:
				close(v)
				log.Info(ctx, "Stopped")
				return
			}
		}
	}()
}

// StartListSplitter will start an async list splitter that will split lists
// that requests so.
func (m *Manager) StartListSplitter(ctx context.Context, store blobstore.Store, shutdown chan chan struct{}) {
	ctx = log.WithValues(ctx, "service", "Manager:ListSplitter")
	go func() {
		log.Info(ctx, "Starting")
		defer log.Info(ctx, "Stopped")
		for {
			select {
			case list := <-m.wantSplit:
				t := time.Now()
				list.RWMutex.RLock()
				ctx2 := log.WithValues(ctx, "list_id", list.ID)
				list.RWMutex.RUnlock()
				log.Info(ctx2, "Splitting/Merging list")
				err := list.split(ctx2, store, nil, true)
				if err != nil {
					log.Error(ctx2, "Unable to split list", "error", err)
				}
				log.Info(ctx2, "Splitting done", "duration", time.Since(t))
			case v := <-shutdown:
				close(v)
				return
			}
		}
	}()
}

// LoadLists will load lists from storage and prepare them for queries.
func (m *Manager) LoadLists(ctx context.Context, cache Cache) error {
	m.Lists.cache = cache
	m.Lists.manager = m
	blob, err := m.Storage.Get(ctx, m.Set, listStorageKey)
	if err == nil {
		err = m.Lists.Load(ctx, m.Storage, blob)
		if err == nil || m.Backup == nil {
			return err
		}
	}

	if m.Backup == nil {
		log.Error(ctx, "Unable to load master list, and no backup", "error", err.Error())
		return err
	}
	// Load backup.
	log.Error(ctx, "Unable to load master list, loading from backup", "error", err.Error())
	blob, err = m.Backup.Get(ctx, backupListStorageKey)
	if err != nil {
		return err
	}
	return m.Lists.Load(ctx, m.Storage, blob)
}

// SaveLists explicitly saves all lists.
func (m *Manager) SaveLists(ctx context.Context, backup bool) error {
	var buf bytes.Buffer
	err := m.Lists.Save(ctx, &buf)
	if err != nil {
		return err
	}
	if backup {
		// If storing backup, do that first.
		err := m.Backup.Set(ctx, backupListStorageKey, buf.Bytes())
		if err != nil {
			log.Error(ctx, "Unable to save backup")
		}
	}
	return m.Storage.Set(ctx, m.Set, listStorageKey, buf.Bytes())
}

// DeleteList will delete a list and all data associated.
func (m *Manager) DeleteList(ctx context.Context, id ListID) error {
	lst, ok := m.Lists.ByID(id)
	if !ok {
		return nil
	}
	m.Lists.Delete(id)
	return lst.DeleteAll(ctx, m.Storage)
}
