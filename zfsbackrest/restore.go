package zfsbackrest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gargakshit/zfsbackrest/fsm"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/util"
	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/oklog/ulid/v2"
)

func (r *Runner) GetLatestRestoreBackupID(ctx context.Context, dataset string) (ulid.ULID, error) {
	var latestRestorableBackup *repository.Backup
	for _, backup := range r.Store.Backups {
		if backup.Dataset == dataset &&
			(latestRestorableBackup == nil || backup.CreatedAt.After(latestRestorableBackup.CreatedAt)) {
			latestRestorableBackup = backup
		}
	}

	if latestRestorableBackup == nil {
		return ulid.ULID{}, fmt.Errorf("no restorable backup found for dataset %s", dataset)
	}

	return latestRestorableBackup.ID, nil
}

type RestoreState string
type RestoreAction string

const (
	RestoreStateInitial              RestoreState = "initial"
	RestoreStateParentSnapshotExists RestoreState = "parent_snapshot_exists"
	RestoreStateRestored             RestoreState = "restored"
	RestoreStateCompleted            RestoreState = "completed"
)

type RestoreFSMData struct {
	DestinationDataset string
	Backup             *repository.Backup
}

// RestoreRecursive restores a backup and all its dependencies recursively.
func (r *Runner) RestoreRecursive(ctx context.Context, destinationDataset string, backupID ulid.ULID) error {
	slog.Debug("Restoring recursively", "destination-dataset", destinationDataset, "backup-id", backupID)

	backup, ok := r.Store.Backups[backupID]
	if !ok {
		slog.Error("Backup not found", "backup-id", backupID)
		return fmt.Errorf("backup %s not found", backupID)
	}

	if backup.DependsOn != nil {
		slog.Debug("Parent backup found. Restoring parent first.", "destination-dataset", destinationDataset, "backup", backup)
		err := r.RestoreRecursive(ctx, destinationDataset, *backup.DependsOn)
		if err != nil {
			slog.Error("Failed to restore parent", "error", err)
			return fmt.Errorf("failed to restore parent: %w", err)
		}

		slog.Debug("Parent backup restored", "destination-dataset", destinationDataset, "backup", backup)
	}

	slog.Debug("Restoring backup", "destination-dataset", destinationDataset, "backup", backup)
	return r.Restore(ctx, destinationDataset, backupID)
}

func (r *Runner) Restore(ctx context.Context, destinationDataset string, backupID ulid.ULID) error {
	slog.Info("Restoring", "destination-dataset", destinationDataset, "backup-id", backupID)

	fsm, err := r.createRestoreFSM(destinationDataset, backupID)
	if err != nil {
		slog.Error("Failed to create restore FSM", "error", err)
		return fmt.Errorf("failed to create restore FSM: %w", err)
	}

	slog.Debug("Running restore FSM",
		"destination-dataset", destinationDataset,
		"backup-id", backupID,
		"sequence", []RestoreAction{"check_parent_snapshot", "restore", "complete"},
	)
	return fsm.RunSequence(ctx, "check_parent_snapshot", "restore", "complete")
}

func (r *Runner) createRestoreFSM(destinationDataset string, backupID ulid.ULID) (*fsm.FSM[RestoreState, RestoreAction, RestoreFSMData], error) {
	slog.Debug("Creating restore FSM", "destination-dataset", destinationDataset, "backup-id", backupID)

	backup, ok := r.Store.Backups[backupID]
	if !ok {
		slog.Error("Backup not found", "backup-id", backupID)
		return nil, fmt.Errorf("backup %s not found", backupID)
	}

	data := RestoreFSMData{
		DestinationDataset: destinationDataset,
		Backup:             backup,
	}

	return fsm.NewFSM(
		"restore",
		fsm.State[RestoreState, RestoreFSMData]{
			ID:   RestoreStateInitial,
			Data: &data,
		},
		map[RestoreAction]fsm.Transition[RestoreState, RestoreFSMData]{
			"check_parent_snapshot": {
				From: RestoreStateInitial,
				To:   RestoreStateParentSnapshotExists,
				Run: func(ctx context.Context, data *RestoreFSMData) error {
					slog.Debug("Checking if parent snapshot exists", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
					if data.Backup.DependsOn == nil {
						slog.Debug("No parent backup needed.", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
						return nil
					}

					parentBackupID := data.Backup.DependsOn
					exists, err := r.ZFS.SnapshotExists(ctx, data.DestinationDataset, *parentBackupID)
					if err != nil {
						slog.Error("Failed to check if parent snapshot exists", "error", err)
						return fmt.Errorf("failed to check if parent snapshot exists: %w", err)
					}

					if !exists {
						slog.Error("Parent snapshot does not exist. Can't restore.", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
						return fsm.NewUnrecoverableError(fmt.Errorf("parent snapshot does not exist"))
					}

					slog.Debug("Parent snapshot exists", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
					return nil
				},
			},
			"restore": {
				From: RestoreStateParentSnapshotExists,
				To:   RestoreStateRestored,
				Run: func(ctx context.Context, data *RestoreFSMData) error {
					slog.Debug("Restoring snapshot", "destination-dataset", data.DestinationDataset, "backup", data.Backup)

					slog.Debug("Opening snapshot read stream", "dataset", data.Backup.Dataset, "snapshot", data.Backup.ID.String())
					reader, err := r.Storage.OpenSnapshotReadStream(ctx, data.Backup.Dataset, data.Backup.ID.String(), r.Encryption)
					if err != nil {
						slog.Error("Failed to open snapshot read stream", "error", err)
						return fmt.Errorf("failed to open snapshot read stream: %w", err)
					}

					wrappedReader := util.NewLoggedReader("restore", reader, 1*time.Second, data.Backup.Size)

					slog.Debug("Starting ZFS recv", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
					err = r.ZFS.Recv(ctx, data.DestinationDataset, data.Backup.ID, wrappedReader, zfs.RecvOptions{KeepUnmounted: true})
					if err != nil {
						slog.Error("Failed to receive snapshot", "error", err)
						return fmt.Errorf("failed to receive snapshot: %w", err)
					}

					slog.Debug("Snapshot restored", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
					return nil
				},
			},
			"complete": {
				From: RestoreStateRestored,
				To:   RestoreStateCompleted,
				Run: func(ctx context.Context, data *RestoreFSMData) error {
					slog.Info("Restore completed", "destination-dataset", data.DestinationDataset, "backup", data.Backup)
					return nil
				},
			},
		},
		fsm.RetryExponentialBackoffConfig{
			MaxRetries:     5,
			WaitIncrements: 2 * time.Second,
			MaxWait:        10 * time.Second,
		},
	), nil
}
