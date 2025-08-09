package repository

import (
	"errors"
	"log/slog"
	"time"
)

// Store is the main struct that contains the backups and orphans.
// It is made to be stored in a single file, usually on the same filesystem as
// the zfsbackrest repository.
type Store struct {
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	Backups   Backups   `json:"backups"`
	Orphans   Orphans   `json:"orphans"`
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
