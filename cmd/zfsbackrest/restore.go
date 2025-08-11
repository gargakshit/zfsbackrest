package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

var ageIdentityFile string
var restoreDataset string
var restoreBackupID string
var restoreDatasetTo string

var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a backup to a dataset",
	Long:  `Restore a backup to a dataset.`,
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

		return nil
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().StringVarP(&ageIdentityFile, "age-identity-file", "i", "", "Path to the age identity file")
	restoreCmd.Flags().StringVarP(&restoreDataset, "dataset", "d", "", "Dataset to restore")
	restoreCmd.Flags().StringVarP(&restoreBackupID, "backup-id", "b", "", "Backup ID to restore (restores the latest backup by default)")
	restoreCmd.Flags().StringVarP(&restoreDatasetTo, "dataset-to", "o", "", "Dataset to restore to")
}
