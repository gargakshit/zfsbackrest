package main

import (
	"fmt"
	"log/slog"

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

		slog.Debug("Creating runner from existing repository", "config", cfg)
		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}
		slog.Debug("Runner created", "runner", runner)

		_ = runner

		return nil
	},
}

func init() {
	rootCmd.AddCommand(restoreCmd)

	restoreCmd.Flags().StringVar(&ageIdentityFile, "age-identity-file", "", "Path to the age identity file")
	restoreCmd.Flags().StringVar(&restoreDataset, "dataset", "", "Dataset to restore")
	restoreCmd.Flags().StringVar(&restoreBackupID, "backup-id", "", "Backup ID to restore (restores the latest backup by default)")
	restoreCmd.Flags().StringVar(&restoreDatasetTo, "dataset-to", "", "Dataset to restore to")
}
