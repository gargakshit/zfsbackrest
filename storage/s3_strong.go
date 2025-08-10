package storage

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path"

	"github.com/gargakshit/zfsbackrest/config"
	"github.com/gargakshit/zfsbackrest/encryption"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3StrongStorage is a storage implementation that uses S3 as the backend and
// provides strong consistency guarantees. It uses an AEAD cipher to encrypt the
// data and ensure integrity.
type S3StrongStorage struct {
	mc       *minio.Client
	s3Config *config.S3Store
}

func NewS3StrongStorage(ctx context.Context, s3Config *config.S3Store) (*S3StrongStorage, error) {
	minioClient, err := minio.New(s3Config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3Config.Key, s3Config.Secret, ""),
		Secure: true,
	})
	if err != nil {
		slog.Error("Failed to create minio client", "error", err)
		return nil, err
	}

	return &S3StrongStorage{
		mc:       minioClient,
		s3Config: s3Config,
	}, nil
}

// storePath is the path to the store file in the S3 bucket. It is not encrypted.
var storePath = "zfsbackrest_store_v1.json"

func (s *S3StrongStorage) LoadStoreContent(ctx context.Context) ([]byte, error) {
	reader, err := s.mc.GetObject(ctx, s.s3Config.Bucket, storePath, minio.GetObjectOptions{})
	if err != nil {
		slog.Error("Failed to get store content", "error", err)
		return nil, err
	}

	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		slog.Error("Failed to read store content", "error", err)
		return nil, err
	}

	return content, nil
}

func (s *S3StrongStorage) SaveStoreContent(ctx context.Context, content []byte) error {
	_, err := s.mc.PutObject(ctx, s.s3Config.Bucket, storePath, bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})
	if err != nil {
		slog.Error("Failed to save store content", "error", err)
		return err
	}

	return nil
}

func (s *S3StrongStorage) OpenSnapshotWriteStream(
	ctx context.Context,
	dataset string,
	snapshot string,
	encryption encryption.Encryption,
) (io.WriteCloser, error) {
	panic("not implemented")
}

func (s *S3StrongStorage) DeleteSnapshot(
	ctx context.Context,
	dataset string,
	snapshot string,
	encryption encryption.Encryption,
) error {
	filePath := s.filePath(dataset, snapshot)
	err := s.mc.RemoveObject(ctx, s.s3Config.Bucket, filePath, minio.RemoveObjectOptions{})
	if err != nil {
		slog.Error("Failed to delete snapshot", "error", err)
		return err
	}

	return nil
}

func (s *S3StrongStorage) filePath(dataset string, snapshot string) string {
	return path.Join("snaps", dataset, snapshot)
}
