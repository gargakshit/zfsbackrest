package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configFile string
var cfg *config.Config

var (
	version = "0.1.0"
	commit  = "unknown"
	date    = "unknown"
)

var rootCmd = &cobra.Command{
	Use:     "zfsbackrest",
	Short:   "ZFS Backup and Restore Tool",
	Long:    `zfsbackrest is a tool for backing up and restoring ZFS filesystems.`,
	Version: fmt.Sprintf("%s+%s %s", version, commit, date),
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		v := viper.New()
		var err error
		cfg, err = config.LoadConfig(v, configFile)
		if err != nil {
			slog.Error("Failed to load config", "error", err)
			return err
		}

		if cfg.Debug {
			setSlog(slog.LevelDebug)
		} else {
			setSlog(slog.LevelInfo)
		}

		slog.Debug("Using log level debug with the config file", "file", configFile)
		slog.Debug("using config", "config", cfg)

		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(
		&configFile,
		"config", "c",
		"/etc/zfsbackrest.toml",
		"path for the config file",
	)
}

var softExit = false

func main() {
	setSlog(slog.LevelInfo) // set the log level to info by default

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		for range signals {
			if !softExit {
				slog.Warn("Received signal to terminate, will exit after the current operation. Use Ctrl+C again to force exit.")
				softExit = true
			} else {
				slog.Error("Force exiting. You may have unfinished operations.")
				cancel()
				os.Exit(1)
			}
		}
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
