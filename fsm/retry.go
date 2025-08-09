package fsm

import (
	"errors"
	"log/slog"
	"time"
)

var (
	UnrecoverableError     = errors.New("unrecoverable error")
	RetryAttemptsExhausted = errors.New("retry attempts exhausted")
)

func IsUnrecoverableError(err error) bool {
	return errors.Is(err, UnrecoverableError)
}

func NewUnrecoverableError(err error) error {
	return errors.Join(UnrecoverableError, err)
}

type RetryStrategy interface {
	RetryAfter(err error) (time.Duration, error)
}

type RetryExponentialBackoffConfig struct {
	MaxRetries     int
	WaitIncrements time.Duration
	MaxWait        time.Duration
}

type RetryExponentialBackoff struct {
	Config       RetryExponentialBackoffConfig
	currentRetry int
}

func NewRetryExponentialBackoff(config RetryExponentialBackoffConfig) *RetryExponentialBackoff {
	return &RetryExponentialBackoff{Config: config}
}

func (r *RetryExponentialBackoff) RetryAfter(err error) (time.Duration, error) {
	if IsUnrecoverableError(err) {
		slog.Warn("Unrecoverable error, not retrying", "error", err)
		return 0, err
	}

	if r.currentRetry >= r.Config.MaxRetries {
		slog.Error("Retry attempts exhausted", "error", err)
		return 0, RetryAttemptsExhausted
	}

	wait := r.Config.WaitIncrements * time.Duration(r.currentRetry)
	if wait > r.Config.MaxWait {
		wait = r.Config.MaxWait
	}

	slog.Info("Retrying after", "wait", wait, "currentRetry", r.currentRetry)
	r.currentRetry++

	return wait, nil
}
