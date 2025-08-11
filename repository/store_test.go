package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func TestStoreValidate(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)

	// helpers
	newID := func() ulid.ULID { return ulid.Make() }
	full := func(id ulid.ULID, ts time.Time, parent *ulid.ULID) *Backup {
		return &Backup{ID: id, Type: BackupTypeFull, CreatedAt: ts, DependsOn: parent}
	}
	diff := func(id ulid.ULID, ts time.Time, parent *ulid.ULID) *Backup {
		return &Backup{ID: id, Type: BackupTypeDiff, CreatedAt: ts, DependsOn: parent}
	}
	incr := func(id ulid.ULID, ts time.Time, parent *ulid.ULID) *Backup {
		return &Backup{ID: id, Type: BackupTypeIncr, CreatedAt: ts, DependsOn: parent}
	}

	tests := []struct {
		name    string
		build   func() Store
		wantErr error
		alsoIs  error // optional nested error to check via errors.Is
	}{
		{
			name: "invalid version -> ErrInvalidStoreVersion",
			build: func() Store {
				return Store{Version: 2, CreatedAt: now, Backups: Backups{}, Orphans: Orphans{}}
			},
			wantErr: ErrInvalidStoreVersion,
		},
		{
			name: "created in future -> ErrStoreCreatedInFuture",
			build: func() Store {
				return Store{Version: 1, CreatedAt: future, Backups: Backups{}, Orphans: Orphans{}}
			},
			wantErr: ErrStoreCreatedInFuture,
		},
		{
			name: "backup id overlaps orphan id -> ErrBackupInOrphan",
			build: func() Store {
				id := newID()
				b := full(id, past, nil)
				return Store{
					Version:   1,
					CreatedAt: now,
					Backups:   Backups{id: b},
					Orphans:   Orphans{id: &Orphan{Backup: *b}},
				}
			},
			wantErr: ErrBackupInOrphan,
		},
		{
			name: "backup validate fails (diff no parent) -> ErrBackupValidation + ErrDiffBackupNoParent",
			build: func() Store {
				id := newID()
				b := diff(id, past, nil)
				return Store{Version: 1, CreatedAt: now, Backups: Backups{id: b}, Orphans: Orphans{}}
			},
			wantErr: ErrBackupValidation,
			alsoIs:  ErrDiffBackupNoParent,
		},
		{
			name: "backup validate fails (missing parent) -> ErrBackupValidation + ErrParentBackupNotFound",
			build: func() Store {
				id := newID()
				missing := newID()
				b := diff(id, past, &missing)
				return Store{Version: 1, CreatedAt: now, Backups: Backups{id: b}, Orphans: Orphans{}}
			},
			wantErr: ErrBackupValidation,
			alsoIs:  ErrParentBackupNotFound,
		},
		{
			name: "valid store: empty",
			build: func() Store {
				return Store{Version: 1, CreatedAt: now, Backups: Backups{}, Orphans: Orphans{}}
			},
			wantErr: nil,
		},
		{
			name: "valid store: one full backup",
			build: func() Store {
				id := newID()
				b := full(id, past, nil)
				return Store{Version: 1, CreatedAt: now, Backups: Backups{id: b}, Orphans: Orphans{}}
			},
			wantErr: nil,
		},
		{
			name: "valid store: incr -> diff -> full chain",
			build: func() Store {
				fid := newID()
				did := newID()
				iid := newID()
				fb := full(fid, past, nil)
				db := diff(did, past, &fid)
				ib := incr(iid, past, &did)
				return Store{Version: 1, CreatedAt: now, Backups: Backups{fid: fb, did: db, iid: ib}, Orphans: Orphans{}}
			},
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			s := tc.build()
			err := s.Validate()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
			if tc.alsoIs != nil && !errors.Is(err, tc.alsoIs) {
				t.Fatalf("expected wrapped error %v, got %v", tc.alsoIs, err)
			}
		})
	}
}
