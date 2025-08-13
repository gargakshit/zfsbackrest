package util

import (
	"io"
	"log/slog"
	"time"
)

type LoggedReader struct {
	tag          string
	underlying   io.ReadCloser
	logInterval  time.Duration
	lastLog      time.Time
	totalRead    int64
	expectedSize int64
}

func NewLoggedReader(tag string, underlying io.ReadCloser, logInterval time.Duration, expectedSize int64) *LoggedReader {
	return &LoggedReader{
		tag:          tag,
		underlying:   underlying,
		logInterval:  logInterval,
		lastLog:      time.Now().Add(-logInterval),
		totalRead:    0,
		expectedSize: expectedSize,
	}
}

func (r *LoggedReader) Read(p []byte) (int, error) {
	n, err := r.underlying.Read(p)
	r.totalRead += int64(n)

	if time.Since(r.lastLog) > r.logInterval {
		if r.expectedSize > 0 {
			slog.Info("Read",
				"tag", r.tag,
				"total", r.totalRead,
				"expected", r.expectedSize,
				"progress", float64(r.totalRead)/float64(r.expectedSize),
			)
		} else {
			slog.Info("Read",
				"tag", r.tag,
				"total", r.totalRead,
			)
		}
		r.lastLog = time.Now()
	}

	return n, err
}

func (r *LoggedReader) Close() error {
	return r.underlying.Close()
}
