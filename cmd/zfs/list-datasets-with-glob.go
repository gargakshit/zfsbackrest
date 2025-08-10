package main

import (
	"fmt"

	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/spf13/cobra"
)

var listDatasetsWithGlobCmd = &cobra.Command{
	Use:   "list-datasets-with-glob",
	Short: "List datasets with glob",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		zfs, err := zfs.New()
		if err != nil {
			return err
		}

		datasets, err := zfs.ListDatasetsWithGlobs(cmd.Context(), args...)
		if err != nil {
			return err
		}

		fmt.Println(datasets)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(listDatasetsWithGlobCmd)
}
