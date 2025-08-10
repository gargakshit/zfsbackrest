package main

import (
	"fmt"

	"github.com/gargakshit/zfsbackrest/zfs"
	"github.com/spf13/cobra"
)

var listDatasetsCmd = &cobra.Command{
	Use:   "list-datasets",
	Short: "List datasets",
	RunE: func(cmd *cobra.Command, args []string) error {
		zfs, err := zfs.New()
		if err != nil {
			return err
		}

		datasets, err := zfs.ListDatasets(cmd.Context())
		if err != nil {
			return err
		}

		fmt.Println(datasets)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(listDatasetsCmd)
}
