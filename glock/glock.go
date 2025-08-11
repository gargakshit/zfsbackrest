//go:build !windows

package glock

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

// GlobalLock provides a system-wide single-instance lock using a filesystem lock file.
// On Unix-like systems this uses flock with an exclusive, non-blocking lock.
type GlobalLock struct {
	path string
	file *os.File
}

// Acquire attempts to acquire a global lock for the given application name.
// The lock file will live in the system temp dir as <appName>.lock.
func Acquire(appName string) (*GlobalLock, error) {
	lockPath := filepath.Join(os.TempDir(), fmt.Sprintf("%s.lock", appName))
	return AcquireAtPath(lockPath)
}

// AcquireAtPath attempts to acquire a global lock at a specific lock file path.
func AcquireAtPath(lockPath string) (*GlobalLock, error) {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	// Try to acquire exclusive non-blocking lock
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another instance appears to be running (lock held at %s)", lockPath)
	}

	// Write some metadata (pid, start time) for observability. Best-effort.
	_ = f.Truncate(0)
	_, _ = f.WriteAt([]byte(fmt.Sprintf("pid=%d\nstart=%s\n", os.Getpid(), time.Now().Format(time.RFC3339))), 0)
	_ = f.Sync()

	slog.Debug("Acquired global process lock", "path", lockPath)

	return &GlobalLock{path: lockPath, file: f}, nil
}

// Release releases the global lock and removes the lock file.
func (l *GlobalLock) Release() error {
	if l == nil || l.file == nil {
		return nil
	}
	slog.Debug("Releasing global process lock", "path", l.path)
	_ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	err := l.file.Close()
	// Best-effort removal. It's okay if this fails; the lock is advisory via flock.
	_ = os.Remove(l.path)
	return err
}
