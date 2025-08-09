package storage

import (
	"context"
	"io"
	"log/slog"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3StrongStorage is a storage implementation that uses S3 as the backend and
// provides strong consistency guarantees. It uses an AEAD cipher to encrypt the
// data and ensure integrity.
type S3StrongStorage struct {
	mc         *minio.Client
	encryption encryption.Encryption
}

func NewS3StrongStorage(ctx context.Context, s3Config *config.S3Store, encryptionConfig *config.Encryption) (*S3StrongStorage, error) {
	minioClient, err := minio.New(s3Config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3Config.Key, s3Config.Secret, ""),
		Secure: true,
	})
	if err != nil {
		slog.Error("Failed to create minio client", "error", err)
		return nil, err
	}

	encryption, err := encryption.NewEncryption(encryptionConfig)
	if err != nil {
		slog.Error("Failed to create encryption", "error", err)
		return nil, err
	}

	return &S3StrongStorage{
		mc:         minioClient,
		encryption: encryption,
	}, nil
}

func (s *S3StrongStorage) LoadStoreContent(ctx context.Context) ([]byte, error) {
	panic("not implemented")
}

func (s *S3StrongStorage) SaveStoreContent(ctx context.Context, content []byte) error {
	panic("not implemented")
}

func (s *S3StrongStorage) OpenSnapshotWriteStream(ctx context.Context, dataset string, snapshot string) (io.WriteCloser, error) {
	panic("not implemented")
}

func (s *S3StrongStorage) DeleteSnapshot(ctx context.Context, dataset string, snapshot string) error {
	panic("not implemented")
}
