package main

import (
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/util"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

// actions
var cleanupOrphans bool
var cleanupExpired bool

// options
var cleanupDryRun bool
var cleanupSkipPrerequisitesVerification bool
var cleanupSkipOrphaning bool
var cleanupSkipLocalSnapshotRemoval bool
var cleanupSkipRemoteSnapshotRemoval bool

var cleanupGuard *util.CommandGuard

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup backups",
	Long:  `Cleanup backups.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		cleanupGuard, err = util.NewCommandGuard(util.CommandGuardOpts{
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
		return cleanupGuard.OnExit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Cleanup command", "dry-run", cleanupDryRun, "orphans", cleanupOrphans)

		if cleanupDryRun {
			slog.Info("Dry run enabled, no backups will be deleted. Set --dry-run=false to actually delete backups.")
		}

		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}

		opts := zfsbackrest.DeleteOpts{
			SkipPrerequisitesVerification: cleanupSkipPrerequisitesVerification,
			SkipOrphaning:                 cleanupSkipOrphaning,
			SkipLocalSnapshotRemoval:      cleanupSkipLocalSnapshotRemoval,
			SkipRemoteSnapshotRemoval:     cleanupSkipRemoteSnapshotRemoval,
			DryRun:                        cleanupDryRun,
		}

		if cleanupOrphans {
			slog.Info("Deleting orphans")
			err := runner.DeleteAllOrphans(cmd.Context(), opts)
			if err != nil {
				return fmt.Errorf("failed to delete orphans: %w", err)
			}
		}

		if cleanupExpired {
			slog.Info("Deleting expired backups", "expiry", cfg.Repository.Expiry)
			err := runner.DeleteAllExpired(cmd.Context(), opts, &cfg.Repository.Expiry)
			if err != nil {
				return fmt.Errorf("failed to delete expired backups: %w", err)
			}
		}

		if !cleanupOrphans && !cleanupExpired {
			slog.Error("No action specified. Please specify at least one action.")
			return cmd.Help()
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(cleanupCmd)

	cleanupCmd.Flags().BoolVar(&cleanupDryRun, "dry-run", true, "Dry run")
	cleanupCmd.Flags().BoolVar(&cleanupOrphans, "orphans", false, "Cleanup orphans")
	cleanupCmd.Flags().BoolVar(&cleanupSkipPrerequisitesVerification, "skip-prerequisites-verification", false, "Skip prerequisites verification")
	cleanupCmd.Flags().BoolVar(&cleanupSkipOrphaning, "skip-orphaning", false, "Skip orphaning")
	cleanupCmd.Flags().BoolVar(&cleanupSkipLocalSnapshotRemoval, "skip-local-snapshot-removal", false, "Skip local snapshot removal")
	cleanupCmd.Flags().BoolVar(&cleanupSkipRemoteSnapshotRemoval, "skip-remote-snapshot-removal", false, "Skip remote snapshot removal")
	cleanupCmd.Flags().BoolVar(&cleanupExpired, "expired", false, "Cleanup expired backups")
}
