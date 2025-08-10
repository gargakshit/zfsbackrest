package zfsbackrest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/storage"
	"github.com/gargakshit/zfsbackrest/zfs"
)

type Runner struct {
	Config     *config.Config
	ZFS        *zfs.ZFS
	Store      *repository.Store
	Storage    storage.StrongStore
	Encryption encryption.Encryption
}

func NewRunnerFromExistingRepository(ctx context.Context, config *config.Config) (*Runner, error) {
	slog.Debug("Creating runner", "config", config)

	zfs, err := zfs.New()
	if err != nil {
		slog.Error("Failed to create ZFS client", "error", err)
		return nil, fmt.Errorf("failed to create ZFS client: %w", err)
	}

	storage, err := storage.NewS3StrongStorage(ctx, &config.Repository.S3)
	if err != nil {
		slog.Error("Failed to create S3 storage", "error", err)
		return nil, fmt.Errorf("failed to create S3 storage: %w", err)
	}

	store, err := repository.LoadStore(ctx, storage)
	if err != nil {
		slog.Error("Failed to load store content", "error", err)
		return nil, fmt.Errorf("failed to load store content: %w", err)
	}

	encryption, err := encryption.NewAge(&store.Encryption.Age)
	if err != nil {
		slog.Error("Failed to create encryption", "error", err)
		return nil, fmt.Errorf("failed to create encryption: %w", err)
	}

	return &Runner{
		Config:     config,
		ZFS:        zfs,
		Store:      store,
		Storage:    storage,
		Encryption: encryption,
	}, nil
}

func NewRunnerWithNewRepository(ctx context.Context, config *config.Config, encryptionConfig config.Encryption) (*Runner, error) {
	slog.Debug("Creating runner with new repository", "config", config, "encryption", encryptionConfig)

	zfs, err := zfs.New()
	if err != nil {
		slog.Error("Failed to create ZFS client", "error", err)
		return nil, fmt.Errorf("failed to create ZFS client: %w", err)
	}

	managedDatasets, err := zfs.ListDatasetsWithGlobs(ctx, config.Repository.IncludedDatasets...)
	if err != nil {
		slog.Error("Failed to get managed datasets", "error", err)
		return nil, fmt.Errorf("failed to get managed datasets: %w", err)
	}

	slog.Debug("Managed datasets", "datasets", managedDatasets)

	store := &repository.Store{
		Version:         1,
		CreatedAt:       time.Now(),
		Backups:         repository.Backups{},
		Orphans:         repository.Orphans{},
		Encryption:      encryptionConfig,
		ManagedDatasets: managedDatasets,
	}

	storage, err := storage.NewS3StrongStorage(ctx, &config.Repository.S3)
	if err != nil {
		slog.Error("Failed to create S3 storage", "error", err)
		return nil, fmt.Errorf("failed to create S3 storage: %w", err)
	}

	slog.Debug("Saving store content",
		"store", store,
		"endpoint", config.Repository.S3.Endpoint,
		"bucket", config.Repository.S3.Bucket,
	)

	if err := store.Save(ctx, storage); err != nil {
		slog.Error("Failed to save store content", "error", err)
		return nil, fmt.Errorf("failed to save store content: %w", err)
	}

	encryption, err := encryption.NewAge(&store.Encryption.Age)
	if err != nil {
		slog.Error("Failed to create encryption", "error", err)
		return nil, fmt.Errorf("failed to create encryption: %w", err)
	}

	return &Runner{
		Config:     config,
		ZFS:        zfs,
		Store:      store,
		Storage:    storage,
		Encryption: encryption,
	}, nil
}
