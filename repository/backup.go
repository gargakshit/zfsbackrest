package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/google/go-cmp/cmp"
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
	Size      int64      `json:"size"`
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

func (bs Backups) ExpiredBackupsForDataset(dataset string, expiry *config.Expiry) (Backups, error) {
	slog.Debug("Getting expired backups for dataset", "dataset", dataset)

	expired := make(Backups)
	for _, b := range bs {
		if b.Dataset == dataset {
			didExpire, err := bs.Expired(b.ID, expiry)
			if err != nil {
				return nil, err
			}

			if didExpire {
				expired[b.ID] = b
			}
		}
	}

	return expired, nil
}

func (bs Backups) TimeTillExpiry(id ulid.ULID, expiry *config.Expiry) (time.Duration, error) {
	slog.Debug("Calculating time till expiry", "backup", id)

	if err := bs.Validate(id); err != nil {
		return 0, err
	}

	// get the backup
	b := bs[id]

	switch b.Type {
	case BackupTypeFull:
		return time.Until(b.CreatedAt.Add(expiry.Full)), nil

	case BackupTypeDiff:
		parentExpiry, err := bs.TimeTillExpiry(*b.DependsOn, expiry)
		if err != nil {
			return 0, err
		}

		myExpiry := time.Until(b.CreatedAt.Add(expiry.Diff))
		if myExpiry < parentExpiry {
			return myExpiry, nil
		}

		return parentExpiry, nil

	case BackupTypeIncr:
		parentExpiry, err := bs.TimeTillExpiry(*b.DependsOn, expiry)
		if err != nil {
			return 0, err
		}

		myExpiry := time.Until(b.CreatedAt.Add(expiry.Incr))
		if myExpiry < parentExpiry {
			return myExpiry, nil
		}

		return parentExpiry, nil

	default:
		return 0, ErrUnknownBackupType
	}
}

// LatestFull returns the latest full backup.
func (bs Backups) LatestFull(dataset string) *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeFull && b.Dataset == dataset {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}

// LatestDiff returns the latest diff backup.
func (bs Backups) LatestDiff(dataset string) *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeDiff && b.Dataset == dataset {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}

// LatestIncr returns the latest incremental backup.
func (bs Backups) LatestIncr(dataset string) *Backup {
	var backup *Backup
	for _, b := range bs {
		if b.Type == BackupTypeIncr && b.Dataset == dataset {
			if backup == nil || backup.CreatedAt.Before(b.CreatedAt) {
				backup = b
			}
		}
	}

	return backup
}

func (bs Backups) GetParent(dataset string, typ BackupType) (*Backup, error) {
	switch typ {
	case BackupTypeFull:
		slog.Debug("Parent not needed for full backup", "dataset", dataset)
		return nil, nil

	case BackupTypeDiff:
		slog.Debug("Getting parent for diff backup (full backup)", "dataset", dataset)
		latestFull := bs.LatestFull(dataset)
		if latestFull == nil {
			slog.Error("Parent not found for diff backup", "dataset", dataset, "error", ErrParentBackupNotFound.Error())
			return nil, ErrParentBackupNotFound
		}

		return latestFull, nil

	case BackupTypeIncr:
		slog.Debug("Getting parent for incr backup (diff backup)", "dataset", dataset)
		latestDiff := bs.LatestDiff(dataset)
		if latestDiff == nil {
			slog.Error("Parent not found for incr backup", "dataset", dataset, "error", ErrParentBackupNotFound.Error())
			return nil, ErrParentBackupNotFound
		}

		return latestDiff, nil
	}

	return nil, ErrUnknownBackupType
}

func (bs Backups) GetChildren(id ulid.ULID) Backups {
	slog.Debug("Getting children of backup", "backup", id)

	// Check if backup exists in the first place.
	if _, ok := bs[id]; !ok {
		slog.Error("Backup not found", "backup", id)
		return nil
	}

	// Short circuit for incrementals.
	if bs[id].Type == BackupTypeIncr {
		slog.Debug("Skipping children for incremental backup", "backup", id)
		return nil
	}

	children := make(Backups)
	for _, b := range bs {
		if b.DependsOn != nil && *b.DependsOn == id {
			children[b.ID] = b
		}
	}

	slog.Debug("Found children", "children", len(children))

	return children
}

func (bs Backups) GetAllChildren(id ulid.ULID) Backups {
	slog.Debug("Getting all children of backup", "backup", id)

	// Check if backup exists in the first place.
	if _, ok := bs[id]; !ok {
		slog.Error("Backup not found", "backup", id)
		return nil
	}

	// Short circuit for incrementals.
	if bs[id].Type == BackupTypeIncr {
		slog.Debug("Skipping children for incremental backup", "backup", id)
		return nil
	}

	children := make(Backups)
	for _, b := range bs {
		if b.DependsOn != nil && *b.DependsOn == id {
			children[b.ID] = b
		}
	}

	for _, child := range children {
		maps.Copy(children, bs.GetAllChildren(child.ID))
	}

	slog.Debug("Found children", "children", len(children))

	return children
}

func (bs Backups) RemoveBackup(id ulid.ULID) error {
	slog.Debug("Removing backup", "backup", id)

	if _, ok := bs[id]; !ok {
		slog.Error("Backup not found", "backup", id)
		return fmt.Errorf("backup not found: %s", id)
	}

	delete(bs, id)

	return nil
}

func (s *Store) AddBackup(ctx context.Context, backup Backup) error {
	if existingBackup, ok := s.Backups[backup.ID]; ok {
		if cmp.Equal(existingBackup, &backup) {
			slog.Debug("Backup already exists, skipping addition (idempotency)", "backup", backup.ID)
			return nil
		}

		return fmt.Errorf("backup %s already exists", backup.ID)
	}

	s.Backups[backup.ID] = &backup

	return nil
}
