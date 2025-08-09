package main

import "github.com/spf13/cobra"

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup old backups",
	Long:  `Cleanup old backups.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(cleanupCmd)
}
