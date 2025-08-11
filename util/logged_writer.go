package util

import (
	"io"
	"log/slog"
	"time"
)

type LoggedWriter struct {
	underlying   io.WriteCloser
	logInterval  time.Duration
	lastLog      time.Time
	totalWritten int64
	expectedSize int64
}

func NewLoggedWriter(underlying io.WriteCloser, logInterval time.Duration, expectedSize int64) *LoggedWriter {
	return &LoggedWriter{
		underlying:   underlying,
		logInterval:  logInterval,
		lastLog:      time.Now().Add(-logInterval),
		totalWritten: 0,
		expectedSize: expectedSize,
	}
}

func (w *LoggedWriter) Write(p []byte) (int, error) {
	n, err := w.underlying.Write(p)
	w.totalWritten += int64(n)
	if err != nil {
		return n, err
	}

	if time.Since(w.lastLog) > w.logInterval {
		if w.expectedSize > 0 {
			slog.Info("Written", "total", w.totalWritten, "expected", w.expectedSize, "progress", float64(w.totalWritten)/float64(w.expectedSize))
		} else {
			slog.Info("Written", "total", w.totalWritten)
		}
		w.lastLog = time.Now()
	}

	return n, nil
}

func (w *LoggedWriter) Close() error {
	return w.underlying.Close()
}
