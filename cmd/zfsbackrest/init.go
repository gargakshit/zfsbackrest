package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/spf13/cobra"
)

var ageRecipientPublicKey string

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a new backup repository",
	Long:  `Initialize a new backup repository.`,
	RunE: func(cmd *cobra.Command, args []string) error {
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

		return cmd.Help()
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}

func init() {
	initCmd.Flags().StringVar(&ageRecipientPublicKey, "age-recipient-public-key", "", "The public key to use for age encryption")
}
