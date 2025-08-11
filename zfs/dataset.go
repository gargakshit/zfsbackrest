package zfs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
)

func (z *ZFS) DatasetExists(ctx context.Context, dataset string) (bool, error) {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, true, "list", "-H", "-t", "filesystem", "-o", "name", dataset)
	if err != nil {
		// Returns 1 if dataset does not exist.
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			if exitErr.ExitCode() == 1 {
				return false, nil
			}
		}

		slog.Error("Failed to check if ZFS dataset exists", "dataset", dataset, "error", err, "stdout", string(stdout))
		return false, fmt.Errorf("failed to check if ZFS dataset exists: %w", err)
	}

	slog.Debug("ZFS dataset exists", "dataset", dataset, "stdout", string(stdout))
	return true, nil
}
