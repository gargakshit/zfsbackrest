package zfsbackrest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/fsm"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/oklog/ulid/v2"
)

type DeleteState string
type DeleteAction string

type DeleteFSMData struct {
	Dataset string
	Backup  *repository.Backup
	Orphan  bool
}

const (
	DeleteStateInitial               DeleteState = "initial"
	DeleteStatePrerequisitesVerified DeleteState = "prerequisites_verified"
	DeleteStateOrphaned              DeleteState = "orphaned"
	DeleteStateRemoteRemoved         DeleteState = "remote_removed"
	DeleteStateUpdatedStore          DeleteState = "updated_store"
	DeleteStateReleasedSnapshot      DeleteState = "released_snapshot"
	DeleteStateLocalRemoved          DeleteState = "local_removed"
	DeleteStateCompleted             DeleteState = "completed"
)

type DeleteOpts struct {
	SkipPrerequisitesVerification bool
	SkipOrphaning                 bool
	SkipLocalSnapshotRemoval      bool
	SkipRemoteSnapshotRemoval     bool
	DryRun                        bool
}

func (r *Runner) DeleteAllOrphans(ctx context.Context, opts DeleteOpts) error {
	slog.Debug("Deleting all orphans", "opts", opts)

	opts.SkipOrphaning = true

	for _, orphan := range r.Store.Orphans {
		slog.Debug("Deleting orphan", "orphan", orphan.Backup.ID)
		err := r.Delete(ctx, orphan.Backup.Dataset, orphan.Backup.ID, opts)
		if err != nil {
			return fmt.Errorf("failed to delete orphan: %w", err)
		}
	}

	return nil
}

func (r *Runner) DeleteAllExpired(ctx context.Context, opts DeleteOpts, expiry *config.Expiry) error {
	slog.Debug("Deleting all expired backups", "opts", opts)

	for _, dataset := range r.Store.ManagedDatasets {
		slog.Debug("Deleting expired backups for dataset", "dataset", dataset)
		err := r.DeleteExpired(ctx, dataset, opts, expiry)
		if err != nil {
			return fmt.Errorf("failed to delete expired backups for dataset %s: %w", dataset, err)
		}
	}

	return nil
}

func (r *Runner) DeleteExpired(ctx context.Context, dataset string, opts DeleteOpts, expiry *config.Expiry) error {
	slog.Debug("Deleting expired backups", "dataset", dataset, "opts", opts)

	expired, err := r.Store.Backups.ExpiredBackupsForDataset(dataset, expiry)
	if err != nil {
		return fmt.Errorf("failed to get expired backups: %w", err)
	}

	if len(expired) == 0 {
		slog.Info("No expired backups found", "dataset", dataset)
		return nil
	}

	slog.Debug("Deleting expired backups", "dataset", dataset, "count", len(expired))

	sorted := make([]*repository.Backup, 0, len(expired))
	for _, backup := range expired {
		sorted = append(sorted, backup)
	}

	// Sorting by ULID will ensure the children are deleted first, from newest to
	// oldest. This is important because the children may depend on the parent.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID.Compare(sorted[j].ID) > 0
	})

	slog.Debug("Sorted expired backups", "dataset", dataset, "sorted", sorted)

	for _, backup := range sorted {
		slog.Debug("Deleting expired backup", "dataset", dataset, "id", backup.ID)
		err := r.Delete(ctx, dataset, backup.ID, opts)
		if err != nil {
			return fmt.Errorf("failed to delete expired backup: %w", err)
		}
	}

	return nil
}

// DeleteRecursive deletes a backup and all its children.
func (r *Runner) DeleteRecursive(ctx context.Context, dataset string, id ulid.ULID, opts DeleteOpts) error {
	slog.Debug("Deleting backup recursively", "dataset", dataset, "id", id, "opts", opts)

	children := r.Store.Backups.GetChildren(id)
	for _, child := range children {
		slog.Debug("Deleting child", "dataset", dataset, "id", child)
		err := r.DeleteRecursive(ctx, dataset, child.ID, opts)
		if err != nil {
			return fmt.Errorf("failed to delete child: %w", err)
		}
	}

	slog.Debug("Deleting backup", "dataset", dataset, "id", id, "opts", opts)
	err := r.Delete(ctx, dataset, id, opts)
	if err != nil {
		return fmt.Errorf("failed to delete backup: %w", err)
	}

	return nil
}

// Delete deletes a backup.
func (r *Runner) Delete(ctx context.Context, dataset string, id ulid.ULID, opts DeleteOpts) error {
	slog.Debug("Deleting backup", "dataset", dataset, "id", id, "opts", opts)

	fsm, err := r.createDeleteFSM(dataset, id)
	if err != nil {
		return fmt.Errorf("failed to create delete FSM: %w", err)
	}

	action_sequence := []DeleteAction{}
	if opts.SkipPrerequisitesVerification {
		action_sequence = append(action_sequence, "force_skip_prerequisites")
	} else {
		action_sequence = append(action_sequence, "verify_prerequisites")
	}

	if opts.DryRun {
		action_sequence = append(action_sequence, "dry_run")
		slog.Debug("Running FSM action sequence", "action_sequence", action_sequence)
		return fsm.RunSequence(ctx, action_sequence...)
	}

	if opts.SkipOrphaning {
		action_sequence = append(action_sequence, "do_not_orphan")
	} else {
		action_sequence = append(action_sequence, "orphan")
	}

	if opts.SkipRemoteSnapshotRemoval {
		action_sequence = append(action_sequence, "skip_remove_remote")
	} else {
		action_sequence = append(action_sequence, "remove_remote")
	}

	action_sequence = append(action_sequence, "update_store")

	if opts.SkipLocalSnapshotRemoval {
		action_sequence = append(action_sequence, "skip_local_removal")
	} else {
		action_sequence = append(action_sequence, "release_snapshot")
		action_sequence = append(action_sequence, "remove_local")
	}

	action_sequence = append(action_sequence, "complete")
	slog.Debug("Running FSM action sequence", "action_sequence", action_sequence)

	return fsm.RunSequence(ctx, action_sequence...)
}

func (r *Runner) createDeleteFSM(dataset string, id ulid.ULID) (*fsm.FSM[DeleteState, DeleteAction, DeleteFSMData], error) {
	slog.Debug("Creating delete FSM", "dataset", dataset, "id", id)

	isOrphan := false
	backup, ok := r.Store.Backups[id]
	if !ok {
		orphan, ok := r.Store.Orphans[id]
		if !ok {
			slog.Error("Backup not found", "id", id)
			return nil, fmt.Errorf("backup not found for dataset %s: %s", dataset, id)
		}

		backup = &orphan.Backup
		isOrphan = true
	}

	if backup.Dataset != dataset {
		slog.Error("Backup dataset mismatch", "backup", backup.ID, "dataset", backup.Dataset, "expected", dataset)
		return nil, fmt.Errorf("backup dataset mismatch for dataset %s: %s", dataset, id)
	}

	return fsm.NewFSM(
		"delete",
		fsm.State[DeleteState, DeleteFSMData]{
			ID: DeleteStateInitial,
			Data: &DeleteFSMData{
				Dataset: dataset,
				Backup:  backup,
				Orphan:  isOrphan,
			},
		},
		map[DeleteAction]fsm.Transition[DeleteState, DeleteFSMData]{
			"verify_prerequisites": {
				From: DeleteStateInitial,
				To:   DeleteStatePrerequisitesVerified,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Verifying prerequisites", "dataset", data.Dataset, "backup", data.Backup.ID)

					if data.Orphan {
						slog.Debug("Skipping prerequisites verification for orphan", "dataset", data.Dataset, "backup", data.Backup.ID)
						return nil
					}

					// Check if the backup has dependent backups.
					slog.Debug("Getting children of backup", "backup", data.Backup.ID)

					children := r.Store.Backups.GetChildren(data.Backup.ID)
					if len(children) > 0 {
						slog.Error("Backup has dependent backups", "dataset", data.Dataset, "backup", data.Backup.ID, "children", children)
						return fsm.NewUnrecoverableError(fmt.Errorf("backup has dependent backups: %s", data.Backup.ID))
					}

					slog.Debug("Prerequisites verified", "dataset", data.Dataset, "backup", data.Backup.ID)

					return nil
				},
			},
			"dry_run": {
				From: DeleteStatePrerequisitesVerified,
				To:   DeleteStateCompleted,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Warn("Dry run. Skipping mutating anything.", "dataset", data.Dataset, "backup", data.Backup.ID)
					return nil
				},
			},
			"force_skip_prerequisites": {
				From: DeleteStateInitial,
				To:   DeleteStatePrerequisitesVerified,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Warn("Skipping prerequisites verification", "dataset", data.Dataset, "backup", data.Backup.ID)
					return nil
				},
			},
			"do_not_orphan": {
				From: DeleteStatePrerequisitesVerified,
				To:   DeleteStateOrphaned,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Warn("Skipping orphaning backup. This should ONLY be logged when you're deleting an orphan. Use with caution.",
						"dataset", data.Dataset,
						"backup", data.Backup.ID,
					)
					return nil
				},
			},
			"orphan": {
				From: DeleteStatePrerequisitesVerified,
				To:   DeleteStateOrphaned,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Orphaning backup", "dataset", data.Dataset, "backup", data.Backup.ID)

					slog.Debug("Removing backup from store", "backup", data.Backup.ID)
					err := r.Store.Backups.RemoveBackup(data.Backup.ID)
					if err != nil {
						slog.Error("Failed to remove backup", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to remove backup: %w", err))
					}

					slog.Debug("Adding backup to orphaned store", "backup", data.Backup.ID)
					err = r.Store.AddOrphan(ctx, *data.Backup, repository.OrphanReasonStartedDeletion)
					if err != nil {
						slog.Error("Failed to add backup to orphaned store", "error", err)
						return fsm.NewUnrecoverableError(fmt.Errorf("failed to add backup to orphaned store: %w", err))
					}

					// Save the store.
					slog.Debug("Saving store", "backup", data.Backup.ID)
					err = r.Store.Save(ctx, r.Storage)
					if err != nil {
						slog.Error("Failed to save store", "error", err)
						return fmt.Errorf("failed to save store: %w", err)
					}

					slog.Debug("Backup orphaned", "dataset", data.Dataset, "backup", data.Backup.ID)

					return nil
				},
			},
			"skip_remove_remote": {
				From: DeleteStateOrphaned,
				To:   DeleteStateRemoteRemoved,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Warn("Skipping removal of backup from remote store", "dataset", data.Dataset, "backup", data.Backup.ID)
					return nil
				},
			},
			"remove_remote": {
				From: DeleteStateOrphaned,
				To:   DeleteStateRemoteRemoved,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Removing backup from remote", "dataset", data.Dataset, "backup", data.Backup.ID)

					err := r.Storage.DeleteSnapshot(ctx, data.Dataset, data.Backup.ID.String())
					if err != nil {
						slog.Error("Failed to delete backup from remote store", "error", err)
						return fmt.Errorf("failed to delete backup from remote store: %w", err)
					}

					slog.Debug("Snapshot removed from remote store", "dataset", data.Dataset, "backup", data.Backup.ID)

					return nil
				},
			},
			"update_store": {
				From: DeleteStateRemoteRemoved,
				To:   DeleteStateUpdatedStore,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Updating store", "dataset", data.Dataset, "backup", data.Backup.ID)

					// Remove orphaned backup from the store.
					slog.Debug("Removing orphaned backup from store", "backup", data.Backup.ID)
					err := r.Store.RemoveOrphan(ctx, *data.Backup)
					if err != nil {
						slog.Error("Failed to remove orphaned backup from store", "error", err)
						return fmt.Errorf("failed to remove orphaned backup from store: %w", err)
					}

					// Save the store.
					slog.Debug("Saving store", "backup", data.Backup.ID)
					err = r.Store.Save(ctx, r.Storage)
					if err != nil {
						slog.Error("Failed to save store", "error", err)
						return fmt.Errorf("failed to save store: %w", err)
					}

					slog.Debug("Store updated", "dataset", data.Dataset, "backup", data.Backup.ID)

					return nil
				},
			},
			"skip_local_removal": {
				From: DeleteStateUpdatedStore,
				To:   DeleteStateLocalRemoved,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Warn("Skipping local snapshot removal", "dataset", data.Dataset, "backup", data.Backup.ID)
					return nil
				},
			},
			"release_snapshot": {
				From: DeleteStateUpdatedStore,
				To:   DeleteStateReleasedSnapshot,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Releasing snapshot", "dataset", data.Dataset, "backup", data.Backup.ID)

					err := r.ZFS.ReleaseSnapshot(ctx, true, data.Dataset, data.Backup.ID)
					if err != nil {
						// Short circuit for incremental backups.
						if data.Backup.Type == repository.BackupTypeIncr {
							slog.Debug("Skipping snapshot release for incremental backup", "dataset", data.Dataset, "backup", data.Backup.ID)
							return nil
						}

						slog.Warn("Failed to release snapshot. Its non-fatal.", "error", err)
						return nil
					}

					slog.Debug("Snapshot released", "dataset", data.Dataset, "backup", data.Backup.ID)

					return nil
				},
			},
			"remove_local": {
				From: DeleteStateReleasedSnapshot,
				To:   DeleteStateLocalRemoved,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Debug("Removing local snapshot", "dataset", data.Dataset, "backup", data.Backup.ID)

					err := r.ZFS.DeleteSnapshot(ctx, data.Dataset, data.Backup.ID)
					if err != nil {
						slog.Error("Failed to destroy snapshot", "error", err)
						return fmt.Errorf("failed to destroy snapshot: %w", err)
					}

					slog.Debug("Snapshot destroyed", "dataset", data.Dataset, "backup", data.Backup.ID)
					return nil
				},
			},
			"complete": {
				From: DeleteStateLocalRemoved,
				To:   DeleteStateCompleted,
				Run: func(ctx context.Context, data *DeleteFSMData) error {
					slog.Info("Deletion completed", "dataset", data.Dataset, "backup", data.Backup.ID)
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
