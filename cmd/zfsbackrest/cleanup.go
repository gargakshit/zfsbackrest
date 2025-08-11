package main

import (
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

var cleanupDryRun bool
var cleanupOrphans bool

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup expired backups",
	Long:  `Cleanup expired backups.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Cleanup command", "dry-run", cleanupDryRun, "orphans", cleanupOrphans)

		if cleanupDryRun {
			slog.Info("Dry run enabled, no backups will be deleted. Set --dry-run=false to actually delete backups.")
		}

		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}

		_ = runner

		return cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(cleanupCmd)

	cleanupCmd.Flags().BoolVar(&cleanupDryRun, "dry-run", true, "Dry run")
	cleanupCmd.Flags().BoolVar(&cleanupOrphans, "orphans", false, "Cleanup orphans")
}
