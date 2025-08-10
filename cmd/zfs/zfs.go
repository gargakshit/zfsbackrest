package main

import (
	"log/slog"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "zfs",
	Short: "ZFS commands used for debugging the `zfs` package",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func main() {
	setSlog(slog.LevelDebug)
	rootCmd.Execute()
}
