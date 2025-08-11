package zfsbackrest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/fsm"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/oklog/ulid/v2"
	"github.com/sourcegraph/conc/pool"
)

type BackupState string
type BackupAction string

const (
	BackupStateInitial               BackupState = "initial"
	BackupStateGotParent             BackupState = "got_parent"
	BackupStateCreatedSnapshot       BackupState = "created_snapshot"
	BackupStateCreatedBackupManifest BackupState = "created_backup_manifest"
	BackupStateAddedOrphan           BackupState = "added_orphan"
	BackupStateUploadedSnapshot      BackupState = "uploaded_snapshot"
	BackupStateUpdatedStore          BackupState = "updated_store"
	BackupStateCompleted             BackupState = "completed"
)

type BackupFSMData struct {
	Dataset      string
	BackupID     ulid.ULID
	BackupType   repository.BackupType
	ParentBackup *repository.Backup
	Manifest     *repository.Backup
	SnapshotSize int64
}

func (r *Runner) BackupAllManaged(ctx context.Context, concurrency *config.UploadConcurrency, typ repository.BackupType) error {
	datasets := r.Store.ManagedDatasets
	slog.Info("Backing up managed datasets", "datasets", datasets)
	return r.BackupConcurrent(ctx, concurrency, typ, datasets...)
}

func (r *Runner) BackupConcurrent(
	ctx context.Context,
	concurrency *config.UploadConcurrency,
	typ repository.BackupType,
	datasets ...string,
) error {
	slog.Debug("Creating backup FSMs", "datasets", datasets)
	fsms := make([]*fsm.FSM[BackupState, BackupAction, BackupFSMData], len(datasets))
	for i, dataset := range datasets {
		var err error
		fsms[i], err = r.createBackupFSM(ctx, typ, dataset)
		if err != nil {
			slog.Error("Failed to create backup FSM", "dataset", dataset, "error", err)
			return fmt.Errorf("failed to create backup FSM: %w", err)
		}
	}

	// By this step, we ensured that all datasets exist.

	// We run everything sequentially, other than uploads, which are concurrent.
	slog.Debug("Running backup FSMs sequentially",
		"datasets", datasets,
		"actions", []BackupAction{"get_parent", "create_snapshot", "create_backup_manifest", "add_orphan"})
	for _, fsm := range fsms {
		err := fsm.RunSequence(ctx,
			"get_parent",
			"create_snapshot",
			"create_backup_manifest",
			"add_orphan",
		)
		if err != nil {
			slog.Error("Failed to run backup FSM", "dataset", fsm.CurrentState().Data.Dataset, "error", err)
			return fmt.Errorf("failed to run backup FSM for dataset %s: %w", fsm.CurrentState().Data.Dataset, err)
		}
	}

	maxConcurrency := 0
	switch typ {
	case repository.BackupTypeFull:
		maxConcurrency = concurrency.Full
	case repository.BackupTypeDiff:
		maxConcurrency = concurrency.Diff
	case repository.BackupTypeIncr:
		maxConcurrency = concurrency.Incr
	}

	// Upload concurrently.
	slog.Info("Uploading snapshots concurrently", "max_concurrency", maxConcurrency)
	pool := pool.New().WithMaxGoroutines(maxConcurrency).WithErrors().WithContext(ctx)
	for _, fsm := range fsms {
		fsm := fsm
		pool.Go(func(ctx context.Context) error {
			return fsm.Run(ctx, "upload_snapshot")
		})
	}

	err := pool.Wait()
	if err != nil {
		slog.Error("Failed to upload snapshots", "error", err)
		return fmt.Errorf("failed to upload snapshots: %w", err)
	}

	// Update store and complete.
	slog.Debug("Running backup FSMs sequentially", "actions", []BackupAction{"update_store", "complete"})
	for _, fsm := range fsms {
		err := fsm.RunSequence(ctx,
			"update_store",
			"complete",
		)
		if err != nil {
			slog.Error("Failed to run backup FSM", "dataset", fsm.CurrentState().Data.Dataset, "error", err)
			return fmt.Errorf("failed to run backup FSM for dataset %s: %w", fsm.CurrentState().Data.Dataset, err)
		}
	}

	slog.Info("Concurrent backup completed")
	return nil
}

func (r *Runner) createBackupFSM(ctx context.Context, typ repository.BackupType, dataset string) (*fsm.FSM[BackupState, BackupAction, BackupFSMData], error) {
	id := ulid.Make()
	slog.Debug("Creating backup FSM", "type", typ, "dataset", dataset, "id", id)

	// Fast fail if dataset does not exist.
	exists, err := r.ZFS.DatasetExists(ctx, dataset)
	if err != nil {
		slog.Error("Failed to check if dataset exists", "dataset", dataset, "error", err)
		return nil, fmt.Errorf("failed to check if dataset exists: %w", err)
	}

	if !exists {
		slog.Error("Dataset does not exist", "dataset", dataset)
		return nil, fmt.Errorf("dataset does not exist: %s", dataset)
	}

	fsm := fsm.NewFSM(
		"backup",
		fsm.State[BackupState, BackupFSMData]{
			ID: BackupStateInitial,
			Data: &BackupFSMData{
				Dataset:      dataset,
				BackupID:     id,
				BackupType:   typ,
				ParentBackup: nil,
			},
		},
		map[BackupAction]fsm.Transition[BackupState, BackupFSMData]{
			"get_parent": {
				From: BackupStateInitial,
				To:   BackupStateGotParent,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Getting parent backup", "dataset", data.Dataset, "backup_type", data.BackupType)

					parent, err := r.Store.Backups.GetParent(data.Dataset, data.BackupType)
					if err != nil {
						slog.Error("Failed to get parent backup", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to get parent backup: %w", err))
					}

					slog.Debug("Parent backup", "parent", parent)
					data.ParentBackup = parent

					if parent == nil {
						slog.Debug("No parent backup needed, skipping snapshot check", "dataset", data.Dataset)
						return nil
					}

					slog.Debug("Checking if snapshot for parent exists", "dataset", data.Dataset, "parent", parent)
					exists, err := r.ZFS.SnapshotExists(ctx, data.Dataset, parent.ID)
					if err != nil {
						slog.Error("Failed to check if snapshot exists", "error", err)
						return fmt.Errorf("failed to check if snapshot exists: %w", err)
					}

					if !exists {
						slog.Debug("Snapshot for parent does not exist, creating snapshot", "dataset", data.Dataset, "parent", parent)
						return fsm.NewUnrecoverableError(fmt.Errorf("snapshot for parent does not exist"))
					}

					return nil
				},
			},
			"create_snapshot": {
				From: BackupStateGotParent,
				To:   BackupStateCreatedSnapshot,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Creating snapshot", "dataset", data.Dataset)

					// Skip if snapshot already exists.
					exists, err := r.ZFS.SnapshotExists(ctx, data.Dataset, data.BackupID)
					if err != nil {
						slog.Error("Failed to check if snapshot exists", "error", err)
						return fmt.Errorf("failed to check if snapshot exists: %w", err)
					}
					if exists {
						slog.Debug("Snapshot already exists, skipping creation (idempotency)", "dataset", data.Dataset, "backup", data.BackupID)
						return nil
					}

					err = r.ZFS.CreateSnapshot(ctx, data.Dataset, data.BackupID)
					if err != nil {
						slog.Error("Failed to create snapshot", "error", err)
						return fmt.Errorf("failed to create snapshot: %w", err)
					}

					return nil
				},
			},
			"create_backup_manifest": {
				From: BackupStateCreatedSnapshot,
				To:   BackupStateCreatedBackupManifest,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Creating backup manifest", "dataset", data.Dataset)

					manifest := repository.Backup{
						ID:        data.BackupID,
						Type:      data.BackupType,
						CreatedAt: time.Now(),
						Dataset:   data.Dataset,
					}

					// Sanity checks.
					if data.BackupType == repository.BackupTypeFull && data.ParentBackup != nil {
						return fsm.NewUnrecoverableError(fmt.Errorf("sanity check failed: full backup cannot have a parent backup"))
					}
					if data.BackupType == repository.BackupTypeDiff && data.ParentBackup == nil {
						return fsm.NewUnrecoverableError(fmt.Errorf("sanity check failed: diff backup must have a parent backup"))
					}
					if data.BackupType == repository.BackupTypeIncr && data.ParentBackup == nil {
						return fsm.NewUnrecoverableError(fmt.Errorf("sanity check failed: incremental backup must have a parent backup"))
					}

					if data.ParentBackup != nil {
						manifest.DependsOn = &data.ParentBackup.ID
					}

					data.Manifest = &manifest
					slog.Info("Created backup manifest", "manifest", data.Manifest)

					return nil
				},
			},
			"add_orphan": {
				From: BackupStateCreatedBackupManifest,
				To:   BackupStateAddedOrphan,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Adding orphan", "dataset", data.Dataset, "backup", data.Manifest)

					err := r.Store.AddOrphan(ctx, *data.Manifest, repository.OrphanReasonUncommitted)
					if err != nil {
						slog.Error("Failed to add orphan", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to add orphan: %w", err))
					}

					slog.Debug("Saving store", "store", r.Store)
					if err := r.Store.Save(ctx, r.Storage); err != nil {
						slog.Error("Failed to save store", "error", err)
						return fmt.Errorf("failed to save store: %w", err)
					}

					return nil
				},
			},
			"upload_snapshot": {
				From: BackupStateAddedOrphan,
				To:   BackupStateUploadedSnapshot,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Uploading snapshot", "dataset", data.Dataset)

					writeStream, err := r.Storage.OpenSnapshotWriteStream(
						ctx,
						data.Dataset,
						data.Manifest.ID.String(),
						-1,
						r.Encryption,
					)
					if err != nil {
						slog.Error("Failed to open snapshot write stream", "error", err)
						return fmt.Errorf("failed to open snapshot write stream: %w", err)
					}

					var parentID *ulid.ULID
					if data.ParentBackup != nil {
						parentID = &data.ParentBackup.ID
					}

					size, err := r.ZFS.SendSnapshot(ctx, data.Dataset, data.Manifest.ID, parentID, writeStream)
					if err != nil {
						slog.Error("Failed to send snapshot", "error", err)
						return fmt.Errorf("failed to send snapshot: %w", err)
					}

					data.SnapshotSize = size

					return nil
				},
			},
			"update_store": {
				From: BackupStateUploadedSnapshot,
				To:   BackupStateUpdatedStore,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Debug("Updating store", "dataset", data.Dataset)

					// Remove orphan.
					slog.Debug("Removing orphan", "backup", data.Manifest)
					err := r.Store.RemoveOrphan(ctx, *data.Manifest)
					if err != nil {
						slog.Error("Failed to remove orphan", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to remove orphan: %w", err))
					}

					// Update manifest with the snapshot size.
					data.Manifest.Size = data.SnapshotSize

					// Add backup.
					slog.Debug("Adding backup", "backup", data.Manifest)
					err = r.Store.AddBackup(ctx, *data.Manifest)
					if err != nil {
						slog.Error("Failed to add backup", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to add backup: %w", err))
					}

					// Save.
					slog.Debug("Saving store", "store", r.Store)
					if err := r.Store.Save(ctx, r.Storage); err != nil {
						slog.Error("Failed to save store", "error", err)
						return fmt.Errorf("failed to save store: %w", err)
					}

					return nil
				},
			},
			"complete": {
				From: BackupStateUpdatedStore,
				To:   BackupStateCompleted,
				Run: func(ctx context.Context, data *BackupFSMData) error {
					slog.Info("Backup completed", "dataset", data.Dataset, "backup", data.Manifest)
					return nil
				},
			},
		},
		fsm.RetryExponentialBackoffConfig{
			MaxRetries:     3,
			WaitIncrements: 1 * time.Second,
			MaxWait:        10 * time.Second,
		},
	)

	return fsm, nil
}
