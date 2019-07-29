package morph

import (
	"github.com/Vivino/rankdb/api/app"
	"github.com/Vivino/rankdb/api/client"
	"github.com/Vivino/rankdb/backup"
)

type BackupStatus struct {
	ID backup.ID
	In *backup.Status
}

func (b BackupStatus) Tiny() *app.RankdbBackupStatusTiny {
	if b.In == nil {
		return nil
	}
	return &app.RankdbBackupStatusTiny{
		Cancelled: b.In.Cancelled,
		Done:      b.In.Done,
		Finished:  b.In.Finished,
		Lists:     b.In.Lists,
		ListsDone: b.In.ListsDone,
		Size:      b.In.Size,
		Started:   b.In.Started,
		URI:       client.StatusBackupPath(string(b.ID)),
	}
}

func (b BackupStatus) Default() *app.RankdbBackupStatus {
	if b.In == nil {
		return nil
	}
	return &app.RankdbBackupStatus{
		Cancelled: b.In.Cancelled,
		Custom:    b.In.Custom,
		Done:      b.In.Done,
		Errors:    b.In.Errors,
		Finished:  b.In.Finished,
		Lists:     b.In.Lists,
		ListsDone: b.In.ListsDone,
		Size:      b.In.Size,
		Started:   b.In.Started,
		Storage:   b.In.Storage,
		URI:       client.StatusBackupPath(string(b.ID)),
	}
}

func (b BackupStatus) Full() *app.RankdbBackupStatusFull {
	if b.In == nil {
		return nil
	}
	return &app.RankdbBackupStatusFull{
		Cancelled: b.In.Cancelled,
		Custom:    b.In.Custom,
		Done:      b.In.Done,
		Errors:    b.In.Errors,
		Finished:  b.In.Finished,
		Lists:     b.In.Lists,
		ListsDone: b.In.ListsDone,
		Size:      b.In.Size,
		Started:   b.In.Started,
		Storage:   b.In.Storage,
		Success:   b.In.Success,
		URI:       client.StatusBackupPath(string(b.ID)),
	}
}

type RestoreResult struct {
	In *backup.RestoreResult
}

func (r RestoreResult) Default() *app.RankdbRestoreresult {
	if r.In == nil {
		return nil
	}
	return &app.RankdbRestoreresult{
		Errors:   r.In.Failed,
		Restored: r.In.Restored,
		Skipped:  r.In.Skipped,
	}
}
