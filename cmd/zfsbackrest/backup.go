package main

import (
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/util"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

var backupType string

var backupGuard *util.CommandGuard

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Start a backup",
	Long:  `Start a backup.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Running pre-run hook")

		var err error
		backupGuard, err = util.NewCommandGuard(util.CommandGuardOpts{
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
		return backupGuard.OnExit()
	},
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

		err = runner.BackupAllManaged(cmd.Context(), &cfg.UploadConcurrency, repository.BackupType(backupType))
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
