package zfs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/gargakshit/zfsbackrest/util"
	"github.com/oklog/ulid/v2"
)

func (z *ZFS) SendSnapshot(
	ctx context.Context,
	dataset string,
	id ulid.ULID,
	from *ulid.ULID,
	writeStream io.WriteCloser,
) error {
	slog.Debug("Sending snapshot", "dataset", dataset, "id", id, "from", from)

	snap := snapshotName(dataset, id)

	extraArgs := []string{}
	if from != nil {
		extraArgs = append(extraArgs, "-I", snapshotName(dataset, *from))
	}

	stdout, stderr, err := runZFSCmdWithStreaming(ctx,
		append([]string{"send", "-LPc", snap}, extraArgs...)...,
	)
	if err != nil {
		slog.Error("Failed to send snapshot", "error", err)
		return fmt.Errorf("failed to send snapshot: %w", err)
	}

	slog.Debug("Reading snapshot size from stderr")
	size, err := getSnapshotSizeFromSendStderrReader(stderr)
	if err != nil {
		slog.Error("Failed to get snapshot size", "error", err)
		return fmt.Errorf("failed to get snapshot size: %w", err)
	}

	slog.Debug("Snapshot size", "size", size)

	wrappedWriteStream := util.NewLoggedWriter(writeStream, 5*time.Second, size)
	n, err := io.CopyN(wrappedWriteStream, stdout, size)
	if err != nil && err != io.EOF {
		slog.Error("Failed to copy snapshot", "error", err)
		return fmt.Errorf("failed to copy snapshot: %w", err)
	}

	err = writeStream.Close()
	if err != nil {
		slog.Error("Failed to close write stream", "error", err)
		return fmt.Errorf("failed to close write stream: %w", err)
	}

	if n != size {
		slog.Error("Failed to copy snapshot", "expected", size, "actual", n)
		return fmt.Errorf("failed to copy snapshot: expected %d bytes, got %d", size, n)
	}

	slog.Debug("Snapshot copied", "size", size)

	return nil
}

func getSnapshotSizeFromSendStderrReader(stderr io.Reader) (int64, error) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "size\t") {
			size, err := strconv.ParseInt(strings.TrimPrefix(line, "size\t"), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("failed to parse size: %w", err)
			}
			return size, nil
		}
	}

	return 0, scanner.Err()
}
