package repository

import (
	"errors"
	"log/slog"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
)

// All application flows should use FSMs and should be idempotent.

// Backup flow:
// 1. Create a backup manifest (struct Backup) by finding the latest "parent backup".
//   - Nothing for full backups.
//   - Latest full for diff backups.
//   - Latest diff for incremental backups.
// 2. Commit the store.
// 3. Write that manifest to the orphans.
// 4. Push the snapshot to s3.
// 5. Push the snapshot checksum to s3.
// 6. Move that backup out of orphans to actual backups.
// 7. Commit the store.

// Expiry flow:
// 1. Iterate over all backups.
// 2. Check if the backup is expired.
// 3. If it is, move it to the orphan list.
// 4. Commit the store.
// 5. Delete those backups from the underlying storage.
// 6. Remove them from the orphans list.
// 7. Commit the store.

// Full Reconciliation flow:
// 1. Load the store.
// 2. Validate the store.
// 3. Delete the backups from the orphan list that are "safe to delete".
// 4. Commit the store.
// 5. Start the expiry sequence.

// Store is the main struct that contains the backups and orphans.
// It is made to be stored in a single file, usually on the same filesystem as
// the zfsbackrest repository.
type Store struct {
	Version    int               `json:"version"`
	CreatedAt  time.Time         `json:"created_at"`
	Backups    Backups           `json:"backups"`
	Orphans    Orphans           `json:"orphans"`
	Encryption config.Encryption `json:"encryption"`
}

var (
	ErrInvalidStoreVersion  = errors.New("invalid store version")
	ErrStoreCreatedInFuture = errors.New("store created in the future")
	ErrBackupInOrphan       = errors.New("backup is in orphan list")
	ErrBackupValidation     = errors.New("backup validation failed")
)

func (s *Store) Validate() error {
	if s.Version != 1 {
		slog.Error("Invalid store version", "version", s.Version)
		return ErrInvalidStoreVersion
	}

	if s.CreatedAt.After(time.Now()) {
		slog.Error("Store created in the future", "created_at", s.CreatedAt)
		return ErrStoreCreatedInFuture
	}

	// Check if backups and orphans have the same ID.
	for id := range s.Orphans {
		if _, ok := s.Backups[id]; ok {
			slog.Error("Backup is in both backups and orphans. Your backup store is not consistent.", "backup", id)
			return ErrBackupInOrphan
		}
	}

	// Validate backups.
	for id := range s.Backups {
		if err := s.Backups.Validate(id); err != nil {
			return errors.Join(ErrBackupValidation, err)
		}
	}

	return nil
}
