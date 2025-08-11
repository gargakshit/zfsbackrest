package repository

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
)

type OrphanReason string

const (
	OrphanReasonUncommitted     OrphanReason = "uncommitted"
	OrphanReasonStartedDeletion OrphanReason = "started_deletion"
)

type Orphans map[ulid.ULID]*Orphan

type Orphan struct {
	Backup Backup       `json:"backup"`
	Reason OrphanReason `json:"reason"`
}

func (o *Orphan) SafeToDelete() bool {
	return o.Reason == OrphanReasonUncommitted
}

func (s *Store) AddOrphan(ctx context.Context, backup Backup, reason OrphanReason) error {
	orphan := &Orphan{
		Backup: backup,
		Reason: reason,
	}

	if existingOrphan, ok := s.Orphans[backup.ID]; ok {
		if cmp.Equal(existingOrphan, orphan) {
			slog.Debug("Orphan already exists, skipping addition (idempotency)", "backup", backup.ID)
			return nil
		}

		return fmt.Errorf("backup %s is already an orphan", backup.ID)
	}

	s.Orphans[backup.ID] = orphan

	return nil
}

func (s *Store) RemoveOrphan(ctx context.Context, backup Backup) error {
	if _, ok := s.Orphans[backup.ID]; !ok {
		slog.Error("Orphan not found, skipping removal", "backup", backup.ID)
		return fmt.Errorf("orphan not found")
	}

	delete(s.Orphans, backup.ID)
	slog.Debug("Removed orphan", "backup", backup.ID)

	return nil
}
