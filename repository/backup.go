package repository

import (
	"bytes"
	"errors"
	"log/slog"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/oklog/ulid/v2"
)

type BackupType string

const (
	BackupTypeFull BackupType = "full"
	BackupTypeDiff BackupType = "diff"
	BackupTypeIncr BackupType = "incr"
)

type Backups map[ulid.ULID]*Backup

type Backup struct {
	ID        ulid.ULID  `json:"id"`
	Type      BackupType `json:"type"`
	CreatedAt time.Time  `json:"created_at"`
	DependsOn *ulid.ULID `json:"depends_on"`
	Dataset   string     `json:"dataset"`
}

// Error variables for backup validation
var (
	ErrBackupCreatedInFuture   = errors.New("backup created in the future")
	ErrFullBackupHasParent     = errors.New("full backup depends on a parent backup")
	ErrDiffBackupNoParent      = errors.New("diff backup does not depend on a parent backup")
	ErrDiffBackupParentNotFull = errors.New("diff backup depends on a parent backup that is not a full backup")
	ErrIncrBackupNoParent      = errors.New("incremental backup does not depend on a parent backup")
	ErrIncrBackupParentNotDiff = errors.New("incremental backup depends on a parent backup that is not a diff backup")
	ErrUnknownBackupType       = errors.New("unknown backup type")
	ErrBackupIDMismatch        = errors.New("backup ID mismatch")
	ErrParentBackupNotFound    = errors.New("parent backup not found")
)

// Validate validates the backup identified by id and its parent chain.
func (bs Backups) Validate(id ulid.ULID) error {
	slog.Debug("Validating backup", "backup", id)

	b, ok := bs[id]
	if !ok {
		slog.Error("Backup validation failed", "backup", id, "error", ErrParentBackupNotFound.Error())
		return ErrParentBackupNotFound
	}

	if !bytes.Equal(b.ID[:], id[:]) {
		slog.Error("Backup validation failed", "backup", id, "error", ErrBackupIDMismatch.Error())
		return ErrBackupIDMismatch
	}

	if b.CreatedAt.After(time.Now()) {
		slog.Error("Backup validation failed", "backup", b.ID, "error", ErrBackupCreatedInFuture.Error())
		return ErrBackupCreatedInFuture
	}

	switch b.Type {
	case BackupTypeFull:
		if b.DependsOn != nil {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrFullBackupHasParent.Error())
			return ErrFullBackupHasParent
		}
		return nil

	case BackupTypeDiff:
		if b.DependsOn == nil {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrDiffBackupNoParent.Error())
			return ErrDiffBackupNoParent
		}

		parentID := *b.DependsOn
		parentBackup, ok := bs[parentID]
		if !ok {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrParentBackupNotFound.Error())
			return ErrParentBackupNotFound
		}

		if parentBackup.Type != BackupTypeFull {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrDiffBackupParentNotFull.Error())
			return ErrDiffBackupParentNotFull
		}

		return bs.Validate(parentID)

	case BackupTypeIncr:
		if b.DependsOn == nil {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrIncrBackupNoParent.Error())
			return ErrIncrBackupNoParent
		}

		parentID := *b.DependsOn
		parentBackup, ok := bs[parentID]
		if !ok {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrParentBackupNotFound.Error())
			return ErrParentBackupNotFound
		}

		if parentBackup.Type != BackupTypeDiff {
			slog.Error("Backup validation failed", "backup", b.ID, "error", ErrIncrBackupParentNotDiff.Error())
			return ErrIncrBackupParentNotDiff
		}

		return bs.Validate(parentID)

	default:
		slog.Error("Backup validation failed", "backup", b.ID, "error", ErrUnknownBackupType.Error())
		return ErrUnknownBackupType
	}
}

// Expired returns true if the backup is expired.
// Backups expire when their time is lapsed, or when their parent is expired.
func (bs Backups) Expired(id ulid.ULID, expiry *config.Expiry) (bool, error) {
	slog.Debug("Checking if backup is expired", "backup", id)

	if err := bs.Validate(id); err != nil {
		return false, err
	}

	b := bs[id]
	switch b.Type {
	case BackupTypeFull:
		return b.CreatedAt.Before(time.Now().Add(-expiry.Full)), nil

	case BackupTypeDiff:
		parentExpired, err := bs.Expired(*b.DependsOn, expiry)
		if err != nil {
			return false, err
		}

		return b.CreatedAt.Before(time.Now().Add(-expiry.Diff)) || parentExpired, nil

	case BackupTypeIncr:
		parentExpired, err := bs.Expired(*b.DependsOn, expiry)
		if err != nil {
			return false, err
		}

		return b.CreatedAt.Before(time.Now().Add(-expiry.Incr)) || parentExpired, nil

	default:
		return false, ErrUnknownBackupType
	}
}

// LatestFull returns the latest full backup.
func (bs Backups) LatestFull() *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeFull {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}

// LatestDiff returns the latest diff backup.
func (bs Backups) LatestDiff() *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeDiff {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}

// LatestIncr returns the latest incremental backup.
func (bs Backups) LatestIncr() *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeIncr {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}
