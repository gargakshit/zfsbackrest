package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/repository"
	"github.com/gargakshit/zfsbackrest/zfsbackrest"
	"github.com/mattn/go-isatty"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
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

		store := runner.Store
		if jsonDetail {
			return json.NewEncoder(os.Stdout).Encode(store)
		}

		if err := renderStoreInfo(store); err != nil {
			return err
		}

		if err := renderManagedDatasets(store); err != nil {
			return err
		}

		if err := renderBackupsTable(store, cfg); err != nil {
			return err
		}

		if err := renderOrphansTable(store); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(detailCmd)

	isTerminal := isatty.IsTerminal(os.Stdout.Fd())
	detailCmd.Flags().BoolVar(&jsonDetail, "json", !isTerminal, "Output in JSON format")
}

func renderStoreInfo(store *repository.Store) error {
	color.New(color.Bold).Add(color.Underline).Fprintf(os.Stdout, "Store Info\n")

	totalStorage := int64(0)
	for _, b := range store.Backups {
		totalStorage += b.Size
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{
		"Version",
		"Created At",
		"Backups",
		"Orphans",
		"Total Storage Used",
		"Age public key",
	})

	table.Append([]string{
		fmt.Sprintf("%d", store.Version),
		store.CreatedAt.Format(time.RFC1123),
		fmt.Sprintf("%d", len(store.Backups)),
		fmt.Sprintf("%d", len(store.Orphans)),
		humanize.Bytes(uint64(totalStorage)),
		store.Encryption.Age.RecipientPublicKey,
	})

	table.Render()

	return nil
}

func renderManagedDatasets(store *repository.Store) error {
	color.New(color.Bold).Add(color.Underline).Fprintf(os.Stdout, "Managed Datasets\n")

	storageUsedByDataset := make(map[string]int64)

	completedFullBackupsByDataset := make(map[string]int)
	completedDiffBackupsByDataset := make(map[string]int)
	completedIncrementalBackupsByDataset := make(map[string]int)

	lastBackupByDataset := make(map[string]time.Time)

	for _, b := range store.Backups {
		storageUsedByDataset[b.Dataset] += b.Size

		switch b.Type {
		case repository.BackupTypeFull:
			completedFullBackupsByDataset[b.Dataset]++
		case repository.BackupTypeDiff:
			completedDiffBackupsByDataset[b.Dataset]++
		case repository.BackupTypeIncr:
			completedIncrementalBackupsByDataset[b.Dataset]++
		}

		if b.CreatedAt.After(lastBackupByDataset[b.Dataset]) {
			lastBackupByDataset[b.Dataset] = b.CreatedAt
		}
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Dataset", "Storage Used", "Full Backups", "Diff Backups", "Incremental Backups", "Last Backup"})
	for _, d := range store.ManagedDatasets {
		table.Append([]string{
			d,
			humanize.Bytes(uint64(storageUsedByDataset[d])),
			fmt.Sprintf("%d", completedFullBackupsByDataset[d]),
			fmt.Sprintf("%d", completedDiffBackupsByDataset[d]),
			fmt.Sprintf("%d", completedIncrementalBackupsByDataset[d]),
			lastBackupByDataset[d].Format(time.RFC1123),
		})
	}

	table.Render()

	return nil
}

func renderBackupsTable(store *repository.Store, cfg *config.Config) error {
	// Convert map to slice and sort by Dataset, then ID
	var backupsSlice []*repository.Backup
	for _, b := range store.Backups {
		backupsSlice = append(backupsSlice, b)
	}

	sort.Slice(backupsSlice, func(i, j int) bool {
		if backupsSlice[i].Dataset == backupsSlice[j].Dataset {
			return backupsSlice[i].ID.Compare(backupsSlice[j].ID) < 0
		}

		return backupsSlice[i].Dataset < backupsSlice[j].Dataset
	})

	color.New(color.Bold).Add(color.Underline).Fprintf(os.Stdout, "Backups\n")

	table := tablewriter.NewWriter(os.Stdout).
		Options(tablewriter.WithTrimSpace(tw.Off))
	table.Header([]string{"Dataset", "Backup ID", "Backup Type", "Depends On", "Created At", "Size", "Expires In"})

	for _, b := range backupsSlice {
		dependsOn := ""
		if b.DependsOn != nil {
			dependsOn = b.DependsOn.String()
		}

		padding := ""
		switch b.Type {
		case repository.BackupTypeDiff:
			padding = "  "
		case repository.BackupTypeIncr:
			padding = "    "
		}

		timeTillExpiry, err := store.Backups.TimeTillExpiry(b.ID, &cfg.Repository.Expiry)
		if err != nil {
			return fmt.Errorf("failed to calculate time till expiry: %w", err)
		}

		table.Append([]string{
			padding + b.Dataset,
			b.ID.String(),
			string(b.Type),
			dependsOn,
			b.CreatedAt.Format(time.RFC1123),
			humanize.Bytes(uint64(b.Size)),
			humanize.Time(time.Now().Add(timeTillExpiry)),
		})
	}

	table.Render()

	return nil
}

func renderOrphansTable(store *repository.Store) error {
	if len(store.Orphans) == 0 {
		return nil
	}

	color.New(color.Bold).Add(color.Underline).Fprintf(os.Stdout, "Orphaned Backups\n")

	var orphansSlice []*repository.Orphan
	for _, o := range store.Orphans {
		orphansSlice = append(orphansSlice, o)
	}

	sort.Slice(orphansSlice, func(i, j int) bool {
		return orphansSlice[i].Backup.ID.Compare(orphansSlice[j].Backup.ID) < 0
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.Header([]string{"Dataset", "Backup ID", "Backup Type", "Depends On", "Created At", "Size", "Reason"})
	for _, o := range orphansSlice {
		dependsOn := ""
		if o.Backup.DependsOn != nil {
			dependsOn = o.Backup.DependsOn.String()
		}

		table.Append([]string{
			o.Backup.Dataset,
			o.Backup.ID.String(),
			string(o.Backup.Type),
			dependsOn,
			o.Backup.CreatedAt.Format(time.RFC1123),
			humanize.Bytes(uint64(o.Backup.Size)),
			string(o.Reason),
		})
	}

	table.Render()

	return nil
}
