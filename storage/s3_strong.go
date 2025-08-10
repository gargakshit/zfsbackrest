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
	slog.Debug("Creating S3 strong storage", "s3Config", s3Config)

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
	slog.Debug("Loading store content", "bucket", s.s3Config.Bucket, "path", storePath)

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
	slog.Debug("Saving store content", "bucket", s.s3Config.Bucket, "path", storePath)

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
	size int64,
	encryption encryption.Encryption,
) (io.WriteCloser, error) {
	filePath := s.filePath(dataset, snapshot)
	slog.Debug("Opening snapshot write stream", "bucket", s.s3Config.Bucket, "path", filePath)

	pr, pw := io.Pipe()

	// Kick off the upload that consumes from the pipe reader.
	done := make(chan error)
	go func() {
		defer close(done)
		_, err := s.mc.PutObject(ctx, s.s3Config.Bucket, filePath, pr, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			slog.Error("Failed to upload snapshot", "path", filePath, "error", err)
			// Ensure the writer side sees an error
			_ = pr.CloseWithError(err)
			done <- err
			return
		}
		done <- nil
	}()

	// Wrap the pipe writer with encryption so callers write plaintext
	encWriter, err := encryption.EncryptedWriter(pw)
	if err != nil {
		// If encryption setup fails, close the pipe and return the error
		_ = pw.Close()
		return nil, err
	}

	// Return a WriteCloser that forwards writes to the encrypted writer and
	// waits for the upload to complete on Close.
	return &s3EncryptedWriteCloser{
		enc:  encWriter,
		pw:   pw,
		done: done,
	}, nil
}

func (s *S3StrongStorage) DeleteSnapshot(
	ctx context.Context,
	dataset string,
	snapshot string,
	encryption encryption.Encryption,
) error {
	filePath := s.filePath(dataset, snapshot)
	slog.Debug("Deleting snapshot", "bucket", s.s3Config.Bucket, "path", filePath)

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

type s3EncryptedWriteCloser struct {
	enc  io.WriteCloser
	pw   *io.PipeWriter
	done chan error
}

func (w *s3EncryptedWriteCloser) Write(p []byte) (int, error) {
	return w.enc.Write(p)
}

func (w *s3EncryptedWriteCloser) Close() error {
	// Close the encryption stream first to flush and finalize
	encErr := w.enc.Close()
	// Ensure the pipe writer is closed to signal EOF to the reader
	_ = w.pw.Close()
	// Wait for the upload goroutine to finish and capture its error
	putErr := <-w.done
	if encErr != nil {
		return encErr
	}
	return putErr
}
