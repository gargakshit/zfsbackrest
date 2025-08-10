package zfs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"

	"github.com/oklog/ulid/v2"
)

func snapshotName(dataset string, id ulid.ULID) string {
	return fmt.Sprintf("%s@zfsbackrest-%s", dataset, id.String())
}

func (z *ZFS) CreateSnapshot(ctx context.Context, dataset string, id ulid.ULID) error {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, "snapshot", snapshotName(dataset, id))
	if err != nil {
		slog.Error("Failed to create ZFS snapshot", "dataset", dataset, "id", id, "error", err, "stdout", string(stdout))
		return fmt.Errorf("failed to create ZFS snapshot: %w", err)
	}

	slog.Debug("ZFS snapshot created", "dataset", dataset, "id", id, "stdout", string(stdout))

	return nil
}

func (z *ZFS) DeleteSnapshot(ctx context.Context, dataset string, id ulid.ULID) error {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, "destroy", snapshotName(dataset, id))
	if err != nil {
		slog.Error("Failed to delete ZFS snapshot", "dataset", dataset, "id", id, "error", err, "stdout", string(stdout))
		return fmt.Errorf("failed to delete ZFS snapshot: %w", err)
	}

	slog.Debug("ZFS snapshot deleted", "dataset", dataset, "id", id, "stdout", string(stdout))

	return nil
}

func (z *ZFS) SnapshotExists(ctx context.Context, dataset string, id ulid.ULID) (bool, error) {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, "list", "-t", "snapshot", snapshotName(dataset, id))
	if err != nil {
		// Returns 1 if snapshot does not exist.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if exitErr.ExitCode() == 1 {
				return false, nil
			}
		}

		slog.Error("Failed to check if ZFS snapshot exists", "dataset", dataset, "id", id, "error", err, "stdout", string(stdout))
		return false, fmt.Errorf("failed to check if ZFS snapshot exists: %w", err)
	}

	slog.Debug("ZFS snapshot exists", "dataset", dataset, "id", id, "stdout", string(stdout))
	return true, nil
}
