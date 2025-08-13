package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/internal/util"
	"github.com/gargakshit/zfsbackrest/internal/zfsbackrest"
	"github.com/spf13/cobra"
)

var ageRecipientPublicKey string

var initGuard *util.CommandGuard

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new backup repository",
	Long:  `Initialize a new backup repository.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		var err error
		initGuard, err = util.NewCommandGuard(util.CommandGuardOpts{
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
		return initGuard.OnExit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		if ageRecipientPublicKey == "" {
			return fmt.Errorf("age recipient public key is required")
		}

		slog.Info("Initializing ZFS backup repository...")

		err := encryption.ValidateRecipientPublicKey(ageRecipientPublicKey)
		if err != nil {
			return fmt.Errorf("invalid age recipient public key: %w", err)
		}

		slog.Debug("Creating runner with new repository", "ageRecipientPublicKey", ageRecipientPublicKey)

		_, err = zfsbackrest.NewRunnerWithNewRepository(context.Background(), cfg, config.Encryption{
			Age: config.Age{
				RecipientPublicKey: ageRecipientPublicKey,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create runner: %w", err)
		}

		slog.Info("Repository initialized successfully")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}

func init() {
	initCmd.Flags().StringVar(&ageRecipientPublicKey, "age-recipient-public-key", "", "The public key to use for age encryption")
}
