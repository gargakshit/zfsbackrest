package main

import (
	"fmt"

	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/spf13/cobra"
)

var listSnapshotsCmd = &cobra.Command{
	Use:   "list-snapshots",
	Short: "List snapshots",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dataset := args[0]

		zfs, err := zfs.New()
		if err != nil {
			return err
		}

		snapshots, err := zfs.ListSnapshots(cmd.Context(), dataset)
		if err != nil {
			return err
		}

		fmt.Println(snapshots)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(listSnapshotsCmd)
}
