package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/internal/util"
	"github.com/gargakshit/zfsbackrest/internal/zfsbackrest"
	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

var ageIdentityFile string
var restoreDataset string
var restoreBackupID string
var restoreDatasetTo string

var restoreGuard *util.CommandGuard

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a backup to a dataset",
	Long:  `Restore a backup to a dataset.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		restoreGuard, err = util.NewCommandGuard(util.CommandGuardOpts{
			NeedsRoot:       true,
			NeedsGlobalLock: true,
		})
		if err != nil {
			slog.Error("Failed to initialize command guard", "error", err)
			return fmt.Errorf("failed to initialize command guard: %w", err)
		}

		return nil
	},
	PostRunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Running post-run hook")
		return restoreGuard.OnExit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Restoring backup",
			"age-identity-file", ageIdentityFile,
			"dataset", restoreDataset,
			"backup-id", restoreBackupID,
			"dataset-to", restoreDatasetTo,
		)

		if ageIdentityFile == "" {
			return fmt.Errorf("age identity file is required. Please use --age-identity-file to specify the age identity file")
		}

		if restoreDataset == "" {
			return fmt.Errorf("dataset is required. Please use --dataset to specify the dataset to restore")
		}

		if restoreDatasetTo == "" {
			return fmt.Errorf("dataset-to is required. Please use --dataset-to to specify the dataset to restore to")
		}

		slog.Debug("Reading age identity file", "age-identity-file", ageIdentityFile)
		identity, err := os.ReadFile(ageIdentityFile)
		if err != nil {
			return fmt.Errorf("failed to read age identity file: %w", err)
		}

		slog.Debug("Creating runner from existing repository", "config", cfg)
		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}
		slog.Debug("Runner created", "runner", runner)

		slog.Debug("Creating encryption instance from age identity file", "age-identity-file", ageIdentityFile)
		encryption, err := encryption.NewAgeFromIdentity(string(identity), &runner.Store.Encryption.Age)
		if err != nil {
			return fmt.Errorf("failed to create encryption instance: %w", err)
		}
		slog.Debug("Swapping encryption instance with decryption capabilities")
		runner.Encryption = encryption

		var backupID ulid.ULID

		if restoreBackupID == "" {
			backupID, err = runner.GetLatestRestoreBackupID(cmd.Context(), restoreDataset)
			if err != nil {
				return fmt.Errorf("failed to get latest restore backup ID: %w", err)
			}
		} else {
			backupID, err = ulid.Parse(restoreBackupID)
			if err != nil {
				return fmt.Errorf("failed to parse backup ID: %w", err)
			}
		}

		slog.Info("Restoring backup", "backup-id", backupID, "source-dataset", restoreDataset, "destination-dataset", restoreDatasetTo)

		err = runner.RestoreRecursive(cmd.Context(), restoreDatasetTo, backupID)
		if err != nil {
			return fmt.Errorf("failed to restore backup: %w", err)
		}

		slog.Info("Backup restored", "backup-id", backupID, "source-dataset", restoreDataset, "destination-dataset", restoreDatasetTo)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().StringVarP(&ageIdentityFile, "age-identity-file", "i", "", "Path to the age identity file")
	restoreCmd.Flags().StringVarP(&restoreDataset, "src-dataset", "s", "", "Source dataset to restore. Doesn't necessarily need to exist locally.")
	restoreCmd.Flags().StringVarP(&restoreBackupID, "backup-id", "b", "", "Backup ID to restore (restores the latest backup by default)")
	restoreCmd.Flags().StringVarP(&restoreDatasetTo, "dst-dataset", "d", "", "Destination dataset to restore to. Will error if the dataset already exists.")
}
