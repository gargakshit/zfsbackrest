package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

var jsonDetail bool
var detailCmd = &cobra.Command{
	Use:   "detail",
	Short: "Show details about a backup repository",
	Long:  `Show details about a backup repository.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		slog.Debug("Showing details about backup repository")

		slog.Debug("Creating runner from existing repository", "config", cfg)
		runner, err := zfsbackrest.NewRunnerFromExistingRepository(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}

		_ = runner

		return nil
	},
}

func init() {
	rootCmd.AddCommand(detailCmd)

	isTerminal := isatty.IsTerminal(os.Stdout.Fd())
	detailCmd.Flags().BoolVar(&jsonDetail, "json", !isTerminal, "Output in JSON format")
}
