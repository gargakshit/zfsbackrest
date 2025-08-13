package util

import (
	"errors"
	"log/slog"
	"os"

	"github.com/gargakshit/zfsbackrest/glock"
)

type CommandGuardOpts struct {
	NeedsRoot       bool
	NeedsGlobalLock bool
}

type CommandGuard struct {
	lock *glock.GlobalLock
}

func NewCommandGuard(opts CommandGuardOpts) (*CommandGuard, error) {
	if opts.NeedsRoot && os.Getuid() != 0 {
		slog.Error("zfsbackrest must be run as root", "user", os.Getuid())
		return nil, errors.New("zfsbackrest must be run as root")
	}

	var lock *glock.GlobalLock
	if opts.NeedsGlobalLock {
		slog.Debug("Acquiring global process lock")

		var err error
		lock, err = glock.Acquire("zfsbackrest")
		if err != nil {
			slog.Error("Failed to acquire global lock", "error", err)
			return nil, err
		}
	}

	return &CommandGuard{lock: lock}, nil
}

func (g *CommandGuard) OnExit() error {
	if g.lock != nil {
		slog.Debug("Releasing global process lock")
		return g.lock.Release()
	}

	return nil
}
