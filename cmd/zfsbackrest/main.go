package main

import (
	"log/slog"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configFile string
var cfg *config.Config
var version = "0.1.0"

var rootCmd = &cobra.Command{
	Use:     "zfsbackrest",
	Short:   "ZFS Backup and Restore Tool",
	Long:    `zfsbackrest is a tool for backing up and restoring ZFS filesystems.`,
	Version: version,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		v := viper.New()
		var err error
		cfg, err = config.LoadConfig(v, configFile)
		if err != nil {
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

func main() {
	rootCmd.Execute()
}
