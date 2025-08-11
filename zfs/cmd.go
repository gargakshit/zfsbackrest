package zfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
)

// runZFSCmdWithStdoutCapture runs a zfs command and returns the output.
func runZFSCmdWithStdoutCapture(ctx context.Context, ignoreErrorCode1 bool, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "zfs", args...)
	slog.Debug("Running zfs command", "zfs", "zfs", "args", args)

	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		printError := true
		if errors.As(err, &exitErr) {
			if ignoreErrorCode1 && exitErr.ExitCode() == 1 {
				printError = false
			}
		}

		if printError {
			slog.Error("Failed to run zfs command", "error", err)
		}

		return nil, fmt.Errorf("failed to run zfs command: %w", err)
	}

	slog.Debug("ZFS command output", "zfs", "zfs", "args", args, "output", string(output))

	return output, nil
}

// runZFSCmdWithStreaming runs a zfs command and returns the stdout and stderr.
func runZFSCmdWithStreaming(ctx context.Context, args ...string) (io.ReadCloser, io.ReadCloser, error) {
	cmd := exec.CommandContext(ctx, "zfs", args...)
	slog.Debug("Running zfs command", "zfs", "zfs", "args", args)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		slog.Error("Failed to get stdout pipe", "error", err)
		return nil, nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		slog.Error("Failed to get stderr pipe", "error", err)
		return nil, nil, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		slog.Error("Failed to start zfs command", "error", err)
		return nil, nil, fmt.Errorf("failed to start zfs command: %w", err)
	}

	return stdout, stderr, nil
}
