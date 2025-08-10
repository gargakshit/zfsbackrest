package main

import (
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

var backupType string

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Start a backup",
	Long:  `Start a backup.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateBackupType(backupType); err != nil {
			return fmt.Errorf("invalid backup type: %w", err)
		}

		slog.Info("Starting backup", "type", backupType)

		slog.Debug("Creating runner from existing repository", "config", cfg)
		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}

		err = runner.Backup(cmd.Context(), repository.BackupType(backupType), "storage/projects/zfsbackrest-test")
		if err != nil {
			return fmt.Errorf("failed to backup: %w", err)
		}

		return nil
	},
}

func validateBackupType(backupType string) error {
	switch backupType {
	case "full", "diff", "incr":
		return nil
	default:
		return fmt.Errorf("invalid backup type: %s", backupType)
	}
}

func init() {
	rootCmd.AddCommand(backupCmd)
	backupCmd.Flags().StringVar(&backupType, "type", "full", "The type of backup to start. Valid values are: full, diff, incr.")
}
