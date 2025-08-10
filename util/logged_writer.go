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
}

func NewLoggedWriter(underlying io.WriteCloser, logInterval time.Duration) *LoggedWriter {
	return &LoggedWriter{
		underlying:   underlying,
		logInterval:  logInterval,
		lastLog:      time.Now().Add(-logInterval),
		totalWritten: 0,
	}
}

func (w *LoggedWriter) Write(p []byte) (int, error) {
	n, err := w.underlying.Write(p)
	w.totalWritten += int64(n)
	if err != nil {
		return n, err
	}

	if time.Since(w.lastLog) > w.logInterval {
		slog.Info("Written", "bytes", n, "total", w.totalWritten)
		w.lastLog = time.Now()
	}

	return n, nil
}

func (w *LoggedWriter) Close() error {
	return w.underlying.Close()
}
