package zfs

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/oklog/ulid/v2"
)

type RecvOptions struct {
	KeepUnmounted bool
}

func (z *ZFS) Recv(ctx context.Context, dataset string, id ulid.ULID, reader io.Reader, opts RecvOptions) error {
	slog.Debug("Receiving snapshot", "dataset", dataset, "id", id)
	snap := snapshotName(dataset, id)

	args := []string{"recv", snap}
	if opts.KeepUnmounted {
		args = append(args, "-u")
	}

	stdout, err := runZFSCmdWithStdinStreaming(ctx, reader, args...)
	if err != nil {
		slog.Error("Failed to receive snapshot", "error", err)
		return fmt.Errorf("failed to receive snapshot: %w", err)
	}

	slog.Debug("Received snapshot", "dataset", dataset, "id", id, "stdout", string(stdout))

	return nil
}
