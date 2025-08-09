package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/oklog/ulid/v2"
)

func TestBackupValidate(t *testing.T) {
	now := time.Now()
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)

	// helper to make ULIDs quickly, content not important for ordering
	newID := func() ulid.ULID { return ulid.Make() }

	tests := []struct {
		name    string
		setup   func() (Backups, ulid.ULID)
		wantErr error
	}{
		{
			name: "full: valid (no parent)",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				bs := Backups{
					id: {ID: id, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: nil,
		},
		{
			name: "full: has parent -> ErrFullBackupHasParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeFull, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrFullBackupHasParent,
		},
		{
			name: "full: created in future -> ErrBackupCreatedInFuture",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				bs := Backups{
					id: {ID: id, Type: BackupTypeFull, CreatedAt: future},
				}
				return bs, id
			},
			wantErr: ErrBackupCreatedInFuture,
		},
		{
			name: "diff: no parent -> ErrDiffBackupNoParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				bs := Backups{
					id: {ID: id, Type: BackupTypeDiff, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrDiffBackupNoParent,
		},
		{
			name: "diff: parent not found -> ErrParentBackupNotFound",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				missing := newID()
				bs := Backups{
					id: {ID: id, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &missing},
				}
				return bs, id
			},
			wantErr: ErrParentBackupNotFound,
		},
		{
			name: "diff: parent not full -> ErrDiffBackupParentNotFull",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeDiff, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrDiffBackupParentNotFull,
		},
		{
			name: "diff: parent full and valid -> ok",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: nil,
		},
		{
			name: "diff: parent full created in future -> ErrBackupCreatedInFuture",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: future},
				}
				return bs, id
			},
			wantErr: ErrBackupCreatedInFuture,
		},
		{
			name: "diff: parent full but that full has parent -> ErrFullBackupHasParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				gparent := newID()
				bs := Backups{
					id:      {ID: id, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &parent},
					parent:  {ID: parent, Type: BackupTypeFull, CreatedAt: past, DependsOn: &gparent},
					gparent: {ID: gparent, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrFullBackupHasParent,
		},
		{
			name: "incr: no parent -> ErrIncrBackupNoParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				bs := Backups{
					id: {ID: id, Type: BackupTypeIncr, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrIncrBackupNoParent,
		},
		{
			name: "incr: parent not diff -> ErrIncrBackupParentNotDiff",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrIncrBackupParentNotDiff,
		},
		{
			name: "incr: parent diff has no parent -> ErrDiffBackupNoParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeDiff, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrDiffBackupNoParent,
		},
		{
			name: "incr: parent diff's parent not full -> ErrDiffBackupParentNotFull",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				parent := newID()
				gparent := newID()
				bs := Backups{
					id:      {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &parent},
					parent:  {ID: parent, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &gparent},
					gparent: {ID: gparent, Type: BackupTypeIncr, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrDiffBackupParentNotFull,
		},
		{
			name: "incr: parent diff -> parent full valid -> ok",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				diffID := newID()
				fullID := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &diffID},
					diffID: {ID: diffID, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &fullID},
					fullID: {ID: fullID, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: nil,
		},
		{
			name: "incr: parent diff -> parent full in future -> ErrBackupCreatedInFuture",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				diffID := newID()
				fullID := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &diffID},
					diffID: {ID: diffID, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &fullID},
					fullID: {ID: fullID, Type: BackupTypeFull, CreatedAt: future},
				}
				return bs, id
			},
			wantErr: ErrBackupCreatedInFuture,
		},
		{
			name: "incr: parent diff -> parent full with own parent -> ErrFullBackupHasParent",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				diffID := newID()
				fullID := newID()
				x := newID()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: past, DependsOn: &diffID},
					diffID: {ID: diffID, Type: BackupTypeDiff, CreatedAt: past, DependsOn: &fullID},
					fullID: {ID: fullID, Type: BackupTypeFull, CreatedAt: past, DependsOn: &x},
					x:      {ID: x, Type: BackupTypeFull, CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrFullBackupHasParent,
		},
		{
			name: "unknown type -> ErrUnknownBackupType",
			setup: func() (Backups, ulid.ULID) {
				id := newID()
				bs := Backups{
					id: {ID: id, Type: BackupType("unknown"), CreatedAt: past},
				}
				return bs, id
			},
			wantErr: ErrUnknownBackupType,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			bs, id := tc.setup()
			err := bs.Validate(id)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestBackupExpired(t *testing.T) {
	now := time.Now()
	oneHour := time.Hour
	thirtyMin := 30 * time.Minute
	twoHours := 2 * time.Hour

	expiry := config.Expiry{
		Full: oneHour,
		Diff: oneHour,
		Incr: oneHour,
	}

	tests := []struct {
		name        string
		setup       func() (Backups, ulid.ULID)
		expiry      config.Expiry
		wantExpired bool
		wantErr     error
	}{
		// Full backup cases
		{
			name: "full: not expired (created 30m ago, expiry 1h)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     nil,
		},
		{
			name: "full: expired (created 2h ago, expiry 1h)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupTypeFull, CreatedAt: now.Add(-twoHours)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "full: validate error (future time)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupTypeFull, CreatedAt: now.Add(2 * time.Minute)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrBackupCreatedInFuture,
		},

		// Diff backup cases
		{
			name: "diff: validate error (no parent)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrDiffBackupNoParent,
		},
		{
			name: "diff: validate error (parent not full)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrDiffBackupParentNotFull,
		},
		{
			name: "diff: not expired and parent not expired",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     nil,
		},
		{
			name: "diff: expired by own age",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-twoHours), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "diff: expired because parent full is expired",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: now.Add(-twoHours)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "diff: parent validate error propagates (parent future)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: now.Add(2 * time.Minute)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrBackupCreatedInFuture,
		},

		// Incr backup cases
		{
			name: "incr: validate error (no parent)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrIncrBackupNoParent,
		},
		{
			name: "incr: validate error (parent not diff)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrIncrBackupParentNotDiff,
		},
		{
			name: "incr: parent diff no parent -> parent validate error",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				parent := ulid.Make()
				bs := Backups{
					id:     {ID: id, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin), DependsOn: &parent},
					parent: {ID: parent, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrDiffBackupNoParent,
		},
		{
			name: "incr: not expired and parent chain not expired",
			setup: func() (Backups, ulid.ULID) {
				incr := ulid.Make()
				diff := ulid.Make()
				full := ulid.Make()
				bs := Backups{
					incr: {ID: incr, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin), DependsOn: &diff},
					diff: {ID: diff, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &full},
					full: {ID: full, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, incr
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     nil,
		},
		{
			name: "incr: expired by own age",
			setup: func() (Backups, ulid.ULID) {
				incr := ulid.Make()
				diff := ulid.Make()
				full := ulid.Make()
				bs := Backups{
					incr: {ID: incr, Type: BackupTypeIncr, CreatedAt: now.Add(-twoHours), DependsOn: &diff},
					diff: {ID: diff, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &full},
					full: {ID: full, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, incr
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "incr: expired because parent diff is expired",
			setup: func() (Backups, ulid.ULID) {
				incr := ulid.Make()
				diff := ulid.Make()
				full := ulid.Make()
				bs := Backups{
					incr: {ID: incr, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin), DependsOn: &diff},
					diff: {ID: diff, Type: BackupTypeDiff, CreatedAt: now.Add(-twoHours), DependsOn: &full},
					full: {ID: full, Type: BackupTypeFull, CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, incr
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "incr: expired because parent full is expired",
			setup: func() (Backups, ulid.ULID) {
				incr := ulid.Make()
				diff := ulid.Make()
				full := ulid.Make()
				bs := Backups{
					incr: {ID: incr, Type: BackupTypeIncr, CreatedAt: now.Add(-thirtyMin), DependsOn: &diff},
					diff: {ID: diff, Type: BackupTypeDiff, CreatedAt: now.Add(-thirtyMin), DependsOn: &full},
					full: {ID: full, Type: BackupTypeFull, CreatedAt: now.Add(-twoHours)},
				}
				return bs, incr
			},
			expiry:      expiry,
			wantExpired: true,
			wantErr:     nil,
		},
		{
			name: "unknown type -> ErrUnknownBackupType (from validate)",
			setup: func() (Backups, ulid.ULID) {
				id := ulid.Make()
				bs := Backups{
					id: {ID: id, Type: BackupType("unknown"), CreatedAt: now.Add(-thirtyMin)},
				}
				return bs, id
			},
			expiry:      expiry,
			wantExpired: false,
			wantErr:     ErrUnknownBackupType,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			bs, id := tc.setup()
			expired, err := bs.Expired(id, &tc.expiry)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if expired != tc.wantExpired {
					t.Fatalf("expired mismatch: want %v, got %v", tc.wantExpired, expired)
				}
				return
			}

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("expected error %v, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLatestFull(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-2 * time.Hour)
	mid := now.Add(-1 * time.Hour)
	later := now.Add(-30 * time.Minute)

	mk := func(tp BackupType, ts time.Time) (*Backup, ulid.ULID) {
		id := ulid.Make()
		return &Backup{ID: id, Type: tp, CreatedAt: ts}, id
	}

	tests := []struct {
		name   string
		build  func() Backups
		wantID *ulid.ULID
	}{
		{
			name:   "empty -> nil",
			build:  func() Backups { return Backups{} },
			wantID: nil,
		},
		{
			name: "only non-full -> nil",
			build: func() Backups {
				d1, id1 := mk(BackupTypeDiff, earlier)
				i1, id2 := mk(BackupTypeIncr, later)
				return Backups{id1: d1, id2: i1}
			},
			wantID: nil,
		},
		{
			name: "single full -> that id",
			build: func() Backups {
				f1, id := mk(BackupTypeFull, mid)
				return Backups{id: f1}
			},
			wantID: func() *ulid.ULID { id := ulid.Make(); return &id }(), // placeholder, replaced below
		},
		{
			name: "multiple full -> latest by CreatedAt",
			build: func() Backups {
				fOld, idOld := mk(BackupTypeFull, earlier)
				fMid, idMid := mk(BackupTypeFull, mid)
				fNew, idNew := mk(BackupTypeFull, later)
				x, idx := mk(BackupTypeDiff, now) // ensure mixed types ignored
				return Backups{idOld: fOld, idMid: fMid, idNew: fNew, idx: x}
			},
			wantID: func() *ulid.ULID { id := ulid.Make(); return &id }(), // placeholder, replaced below
		},
		{
			name: "mixed types with one full -> that full",
			build: func() Backups {
				d, idd := mk(BackupTypeDiff, later)
				i, idi := mk(BackupTypeIncr, later)
				f, idf := mk(BackupTypeFull, mid)
				return Backups{idd: d, idi: i, idf: f}
			},
			wantID: func() *ulid.ULID { id := ulid.Make(); return &id }(), // placeholder, replaced below
		},
	}

	// Populate expected IDs dynamically since we generate inside builders
	for ti := range tests {
		bset := tests[ti].build()
		got := bset.LatestFull()
		var expected *ulid.ULID
		switch tests[ti].name {
		case "single full -> that id":
			for id, b := range bset {
				if b.Type == BackupTypeFull {
					idCopy := id
					expected = &idCopy
				}
			}
		case "multiple full -> latest by CreatedAt":
			var want *Backup
			var wantID ulid.ULID
			for id, b := range bset {
				if b.Type != BackupTypeFull {
					continue
				}
				if want == nil || want.CreatedAt.Before(b.CreatedAt) {
					want = b
					wantID = id
				}
			}
			expected = &wantID
		case "mixed types with one full -> that full":
			for id, b := range bset {
				if b.Type == BackupTypeFull {
					idCopy := id
					expected = &idCopy
				}
			}
		}

		if (got == nil) != (expected == nil) {
			t.Fatalf("%s: mismatch nilness: got %v wantID nil? %v", tests[ti].name, got == nil, expected == nil)
		}
		if got == nil {
			continue
		}
		if got.ID != *expected {
			t.Fatalf("%s: expected latest full %v, got %v", tests[ti].name, *expected, got.ID)
		}
	}
}

func TestLatestDiff(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-2 * time.Hour)
	later := now.Add(-10 * time.Minute)

	mk := func(tp BackupType, ts time.Time) (*Backup, ulid.ULID) {
		id := ulid.Make()
		return &Backup{ID: id, Type: tp, CreatedAt: ts}, id
	}

	tests := []struct {
		name   string
		build  func() Backups
		wantID *ulid.ULID
	}{
		{name: "empty -> nil", build: func() Backups { return Backups{} }},
		{
			name: "only non-diff -> nil",
			build: func() Backups {
				f, idf := mk(BackupTypeFull, earlier)
				i, idi := mk(BackupTypeIncr, later)
				return Backups{idf: f, idi: i}
			},
		},
		{
			name: "single diff -> that id",
			build: func() Backups {
				d, id := mk(BackupTypeDiff, earlier)
				return Backups{id: d}
			},
		},
		{
			name: "multiple diff -> latest by CreatedAt",
			build: func() Backups {
				d1, id1 := mk(BackupTypeDiff, earlier)
				d2, id2 := mk(BackupTypeDiff, later)
				f, idf := mk(BackupTypeFull, now)
				return Backups{id1: d1, id2: d2, idf: f}
			},
		},
	}

	for _, tc := range tests {
		bs := tc.build()
		got := bs.LatestDiff()
		// compute expected
		var want *Backup
		for _, b := range bs {
			if b.Type != BackupTypeDiff {
				continue
			}
			if want == nil || want.CreatedAt.Before(b.CreatedAt) {
				want = b
			}
		}
		if (got == nil) != (want == nil) {
			t.Fatalf("%s: mismatch nilness: got %v want %v", tc.name, got == nil, want == nil)
		}
		if got == nil {
			continue
		}
		if got.ID != want.ID {
			t.Fatalf("%s: expected %v, got %v", tc.name, want.ID, got.ID)
		}
	}
}

func TestLatestIncr(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-3 * time.Hour)
	later := now.Add(-5 * time.Minute)

	mk := func(tp BackupType, ts time.Time) (*Backup, ulid.ULID) {
		id := ulid.Make()
		return &Backup{ID: id, Type: tp, CreatedAt: ts}, id
	}

	tests := []struct {
		name  string
		build func() Backups
	}{
		{name: "empty -> nil", build: func() Backups { return Backups{} }},
		{
			name: "only non-incr -> nil",
			build: func() Backups {
				f, idf := mk(BackupTypeFull, earlier)
				d, idd := mk(BackupTypeDiff, later)
				return Backups{idf: f, idd: d}
			},
		},
		{
			name: "single incr -> that id",
			build: func() Backups {
				i, id := mk(BackupTypeIncr, later)
				return Backups{id: i}
			},
		},
		{
			name: "multiple incr -> latest by CreatedAt",
			build: func() Backups {
				i1, id1 := mk(BackupTypeIncr, earlier)
				i2, id2 := mk(BackupTypeIncr, later)
				f, idf := mk(BackupTypeFull, now)
				return Backups{id1: i1, id2: i2, idf: f}
			},
		},
	}

	for _, tc := range tests {
		bs := tc.build()
		got := bs.LatestIncr()
		// compute expected
		var want *Backup
		for _, b := range bs {
			if b.Type != BackupTypeIncr {
				continue
			}
			if want == nil || want.CreatedAt.Before(b.CreatedAt) {
				want = b
			}
		}
		if (got == nil) != (want == nil) {
			t.Fatalf("%s: mismatch nilness: got %v want %v", tc.name, got == nil, want == nil)
		}
		if got == nil {
			continue
		}
		if got.ID != want.ID {
			t.Fatalf("%s: expected %v, got %v", tc.name, want.ID, got.ID)
		}
	}
}
