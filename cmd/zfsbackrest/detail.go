package main

import "github.com/spf13/cobra"

var detailCmd = &cobra.Command{
	Use:   "detail",
	Short: "Show details about a backup repository",
	Long:  `Show details about a backup repository.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(detailCmd)
}
