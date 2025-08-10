package main

import (
	"log/slog"

	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/oklog/ulid/v2"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Snapshot commands",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

var createSnapshotCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a snapshot",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		zfs, err := zfs.New()
		if err != nil {
			return err
		}

		id := ulid.Make()
		slog.Info("Creating snapshot", "dataset", args[0], "id", id)

		return zfs.CreateSnapshot(cmd.Context(), args[0], id)
	},
}

var deleteSnapshotCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a snapshot",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		zfs, err := zfs.New()
		if err != nil {
			return err
		}

		id, err := ulid.ParseStrict(args[1])
		if err != nil {
			return err
		}

		slog.Info("Deleting snapshot", "dataset", args[0], "id", id)

		return zfs.DeleteSnapshot(cmd.Context(), args[0], id)
	},
}

func init() {
	rootCmd.AddCommand(snapshotCmd)
	snapshotCmd.AddCommand(createSnapshotCmd)
	snapshotCmd.AddCommand(deleteSnapshotCmd)
}
