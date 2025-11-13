package zfsbackrest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/storage"
	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/manifoldco/promptui"
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
	
	cfgDatasets, err := zfs.ListDatasetsWithGlobs(ctx, config.Repository.IncludedDatasets...)
	if err != nil {
		slog.Error("Failed to get managed datasets", "error", err)
		return nil, fmt.Errorf("failed to get managed datasets: %w", err)
	}
	
	if diff := diffManagedDatasets(store.ManagedDatasets, cfgDatasets); diff != nil {
		fmt.Printf("%s! Included datasets have changed. ", color.HiRedString("WARNING"))
		red := color.New(color.FgRed)
		green := color.New(color.FgGreen)
		
		fmt.Println("Dataset actions are indicated with the following symbols:")
		fmt.Printf("  %s remove\n", red.Sprint("-"))
		fmt.Printf("  %s add\n\n", green.Sprint("+"))
		
		fmt.Println("zfsbackterm will perform the following actions:")
		
		for _, r := range diff.Removed {
			red.Printf("  - %s\n", r)
		}
		
		for _, a := range diff.Added {
			green.Printf("  + %s\n", a)
		}
		
		fmt.Println(color.New(color.Bold).Sprintf("\nPlan: %s", color.New(color.Faint).Sprintf("%d to add, %d to remove.\n", len(diff.Added), len(diff.Removed))))
		
		prompt := promptui.Prompt {
			Label: "Accept Changes",
			IsConfirm: true,
			Default: "n",
		}
		
		res, err := prompt.Run()
		if err != nil && !errors.Is(err, promptui.ErrAbort) {
			return nil, fmt.Errorf("failed to accept changes: %w", err)
		}
		
		if strings.ToLower(res) == "y" {
			store.ManagedDatasets = cfgDatasets
			if err := store.Save(ctx, storage); err != nil {
				slog.Error("Failed to save store content", "error", err)
				return nil, fmt.Errorf("failed to save store content: %w", err)
			}
		} else if errors.Is(err, promptui.ErrAbort) {
			fmt.Println("Changes rejected.")
			prompt = promptui.Prompt{
				Label: "Continue backup with current configuration",
				IsConfirm: true,
				Default: "y",
			}
			_, err := prompt.Run()
			if err != nil && !errors.Is(err, promptui.ErrAbort) {
				return nil, fmt.Errorf("failed to proceed with current configuration: %w", err)
			}
			
			if errors.Is(err, promptui.ErrAbort) {
				fmt.Println("Backup aborted. Exiting...")
				os.Exit(0)
			}
		}
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
