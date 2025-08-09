package main

import (
	"log/slog"

	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new backup repository",
	Long:  `Initialize a new backup repository.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Info("Initializing ZFS backup repository...")

		return cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}
