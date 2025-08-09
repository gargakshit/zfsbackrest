package storage

import (
	"context"
	"io"
)

type Storage interface {
	// Store management.
	LoadStoreContent(ctx context.Context) ([]byte, error)
	SaveStoreContent(ctx context.Context, content []byte) error

	// Snapshots.
	OpenSnapshotWriteStream(ctx context.Context, dataset string, snapshot string) (io.WriteCloser, error)
	DeleteSnapshot(ctx context.Context, dataset string, snapshot string) error
}
