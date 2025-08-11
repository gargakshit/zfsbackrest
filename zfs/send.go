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

// SendSnapshot sends a snapshot to the write stream. The write stream is
// expected to be a WriteCloser that will be closed when the snapshot is fully
// sent.
func (z *ZFS) SendSnapshot(
	ctx context.Context,
	dataset string,
	id ulid.ULID,
	from *ulid.ULID,
	writeStream io.WriteCloser,
) (int64, error) {
	slog.Debug("Sending snapshot", "dataset", dataset, "id", id, "from", from)

	snap := snapshotName(dataset, id)

	extraArgs := []string{}
	if from != nil {
		extraArgs = append(extraArgs, "-i", snapshotName(dataset, *from))
	}

	stdout, stderr, err := runZFSCmdWithStreaming(ctx,
		append([]string{"send", "-LPpc", snap}, extraArgs...)...,
	)
	if err != nil {
		slog.Error("Failed to send snapshot", "error", err)
		return 0, fmt.Errorf("failed to send snapshot: %w", err)
	}

	slog.Debug("Reading snapshot size from stderr")
	size, err := getSnapshotSizeFromSendStderrReader(stderr)
	if err != nil {
		slog.Error("Failed to get snapshot size", "error", err)
		return 0, fmt.Errorf("failed to get snapshot size: %w", err)
	}

	slog.Debug("Snapshot size", "size", size)

	wrappedWriteStream := util.NewLoggedWriter(snap, writeStream, 5*time.Second, size)

	// We could've used io.CopyN and specified the size, but the size `zfs send`
	// returns is not indicative of the actual size of the stream. It doesn't
	// account for the headers, footers, checksums, etc.
	// Not sure how secure this is :(
	n, err := io.Copy(wrappedWriteStream, stdout)
	if err != nil && err != io.EOF {
		slog.Error("Failed to copy snapshot", "error", err)
		return 0, fmt.Errorf("failed to copy snapshot: %w", err)
	}

	err = writeStream.Close()
	if err != nil {
		slog.Error("Failed to close write stream", "error", err)
		return 0, fmt.Errorf("failed to close write stream: %w", err)
	}

	if n < size {
		slog.Error("Failed to copy snapshot", "snapshot", snap, "expected", size, "actual", n)
		return 0, fmt.Errorf("failed to copy snapshot %s: expected %d bytes, got %d", snap, size, n)
	}

	slog.Debug("Snapshot copied", "size", size)

	return n, nil
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
