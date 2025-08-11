package storage

import (
	"context"
	"io"

	"github.com/gargakshit/zfsbackrest/encryption"
)

type StrongStore interface {
	// Store management.

	// LoadStoreContent loads the store content from the storage.
	LoadStoreContent(ctx context.Context) ([]byte, error)
	// SaveStoreContent saves the store content to the storage.
	SaveStoreContent(ctx context.Context, content []byte) error

	// Snapshots.

	// OpenSnapshotWriteStream opens a stream for writing a snapshot.
	// The size is the size of the snapshot. Can be set to -1 to stream unknown size.
	// The encryption is the encryption to use for the snapshot.
	// The stream is returned and must be closed to ensure the snapshot is uploaded.
	OpenSnapshotWriteStream(
		ctx context.Context,
		dataset string,
		snapshot string,
		size int64,
		encryption encryption.Encryption,
	) (io.WriteCloser, error)
	// DeleteSnapshot deletes a snapshot from the storage.
	DeleteSnapshot(ctx context.Context, dataset string, snapshot string) error
}
