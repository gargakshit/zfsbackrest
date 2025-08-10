package storage

import (
	"context"
	"io"

	"github.com/gargakshit/zfsbackrest/encryption"
)

type StrongStore interface {
	// Store management.
	LoadStoreContent(ctx context.Context) ([]byte, error)
	SaveStoreContent(ctx context.Context, content []byte) error

	// Snapshots.
	OpenSnapshotWriteStream(
		ctx context.Context,
		dataset string,
		snapshot string,
		encryption encryption.Encryption,
	) (io.WriteCloser, error)
	DeleteSnapshot(
		ctx context.Context,
		dataset string,
		snapshot string,
		encryption encryption.Encryption,
	) error
}
