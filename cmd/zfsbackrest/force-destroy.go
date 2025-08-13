package main

import (
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/internal/util"
	"github.com/gargakshit/zfsbackrest/internal/zfsbackrest"
	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

var forceDestroyYes bool
var forceDestroySnapshotID string
var forceDestroyDatasetName string

// options
var forceDestroySkipPrerequisitesVerification bool
var forceDestroySkipOrphaning bool
var forceDestroySkipLocalSnapshotRemoval bool
var forceDestroySkipRemoteSnapshotRemoval bool

var forceDestroyGuard *util.CommandGuard

var forceDestroyCmd = &cobra.Command{
	Use:   "force-destroy",
	Short: "Force destroy a snapshot",
	Long: `Force destroy a snapshot. This is a dangerous operation and should only be used
if you know what you are doing. It will destroy the snapshot and all of its
children. Any options must be used with extreme caution.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		forceDestroyGuard, err = util.NewCommandGuard(util.CommandGuardOpts{
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
		return forceDestroyGuard.OnExit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug(
			"Starting a force destroy",
			"yes", forceDestroyYes,
			"snapshot-id", forceDestroySnapshotID,
			"dataset-name", forceDestroyDatasetName,
		)

		if forceDestroySnapshotID == "" {
			slog.Error("Force destroy requires --snapshot-id flag. That snapshot and all of its children will be destroyed.")
			return cmd.Help()
		}

		if forceDestroyDatasetName == "" {
			slog.Error("Force destroy requires --dataset-name flag.")
			return cmd.Help()
		}

		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			slog.Error("Failed to create runner", "error", err)
			return err
		}

		snapshotID, err := ulid.ParseStrict(forceDestroySnapshotID)
		if err != nil {
			slog.Error("Failed to parse snapshot ID", "error", err)
			return err
		}

		backup, ok := runner.Store.Backups[snapshotID]
		if !ok {
			slog.Error("Backup not found", "id", snapshotID)
			return fmt.Errorf("backup not found")
		}

		if backup.Dataset != forceDestroyDatasetName {
			slog.Error("Backup dataset does not match", "expected", backup.Dataset, "actual", forceDestroyDatasetName)
			return fmt.Errorf("backup dataset does not match")
		}

		children := runner.Store.Backups.GetAllChildren(snapshotID)
		slog.Debug("Found children", "children", len(children))

		slog.Warn("Snapshot will be destroyed", "id", snapshotID)
		for _, child := range children {
			slog.Warn("Snapshot will be destroyed", "id", child.ID)
		}

		if !forceDestroyYes {
			slog.Error("Force destroy requires --yes flag")
			return cmd.Help()
		}

		err = runner.DeleteRecursive(cmd.Context(), forceDestroyDatasetName, snapshotID, zfsbackrest.DeleteOpts{
			SkipPrerequisitesVerification: forceDestroySkipPrerequisitesVerification,
			SkipOrphaning:                 forceDestroySkipOrphaning,
			SkipLocalSnapshotRemoval:      forceDestroySkipLocalSnapshotRemoval,
			SkipRemoteSnapshotRemoval:     forceDestroySkipRemoteSnapshotRemoval,
		})
		if err != nil {
			slog.Error("Failed to delete snapshot", "error", err)
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(forceDestroyCmd)

	forceDestroyCmd.Flags().BoolVarP(&forceDestroyYes, "yes", "y", false, "Force destroy the snapshot")
	forceDestroyCmd.Flags().StringVarP(&forceDestroySnapshotID, "snapshot-id", "i", "", "Snapshot ID to destroy")
	forceDestroyCmd.Flags().StringVarP(&forceDestroyDatasetName, "dataset-name", "d", "", "Dataset name to destroy")
	forceDestroyCmd.Flags().BoolVarP(&forceDestroySkipPrerequisitesVerification, "skip-prerequisites-verification", "s", false, "Skip prerequisites verification")
	forceDestroyCmd.Flags().BoolVarP(&forceDestroySkipOrphaning, "skip-orphaning", "o", false, "Skip orphaning.")
	forceDestroyCmd.Flags().BoolVarP(&forceDestroySkipLocalSnapshotRemoval, "skip-local-snapshot-removal", "l", false, "Skip removing local snapshot")
	forceDestroyCmd.Flags().BoolVarP(&forceDestroySkipRemoteSnapshotRemoval, "skip-remote-snapshot-removal", "r", false, "Skip removing remote snapshot")
}
