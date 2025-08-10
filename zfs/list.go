package zfs

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/gobwas/glob"
)

func (z *ZFS) ListSnapshots(ctx context.Context, dataset string) ([]string, error) {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, "list", "-H", "-t", "snapshot", "-o", "name", dataset)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(stdout), "\n")
	snapshots := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			slog.Warn("Empty line in zfs snapshot list", "dataset", dataset)
			continue
		}

		snapshots = append(snapshots, line)
	}

	slog.Debug("ZFS snapshot list", "dataset", dataset, "snapshots", snapshots)

	return snapshots, nil
}

func (z *ZFS) ListDatasets(ctx context.Context) ([]string, error) {
	stdout, err := runZFSCmdWithStdoutCapture(ctx, "list", "-H", "-t", "filesystem", "-o", "name")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(stdout)), "\n")
	datasets := make([]string, 0, len(lines))
	for _, line := range lines {
		if line == "" {
			slog.Warn("Empty line in zfs dataset list")
			continue
		}

		datasets = append(datasets, line)
	}

	slog.Debug("ZFS dataset list", "datasets", datasets)

	return datasets, nil
}

func (z *ZFS) ListDatasetsWithGlobs(ctx context.Context, globs ...string) ([]string, error) {
	datasets, err := z.ListDatasets(ctx)
	if err != nil {
		slog.Error("Failed to list datasets", "error", err)
		return nil, err
	}

	matchedDatasets := make(map[string]struct{})

	for _, pattern := range globs {
		g, err := glob.Compile(pattern)
		if err != nil {
			slog.Error("Failed to compile glob pattern", "pattern", pattern, "error", err)
			return nil, fmt.Errorf("failed to compile glob pattern %s: %w", pattern, err)
		}

		for _, dataset := range datasets {
			if g.Match(dataset) {
				matchedDatasets[dataset] = struct{}{}
			}
		}
	}

	matchedDatasetsList := make([]string, 0, len(matchedDatasets))
	for dataset := range matchedDatasets {
		matchedDatasetsList = append(matchedDatasetsList, dataset)
	}

	slog.Debug("ZFS dataset list with globs", "globs", globs, "matchedDatasets", matchedDatasetsList)

	return matchedDatasetsList, nil
}
