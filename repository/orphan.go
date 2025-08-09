package repository

import "github.com/oklog/ulid/v2"

type OrphanReason string

const (
	OrphanReasonUncommitted     OrphanReason = "uncommitted"
	OrphanReasonStartedDeletion OrphanReason = "started_deletion"
)

type Orphans map[ulid.ULID]Orphan

type Orphan struct {
	Backup Backup       `json:"backup"`
	Reason OrphanReason `json:"reason"`
}

func (o *Orphan) SafeToDelete() bool {
	return o.Reason == OrphanReasonUncommitted
}
