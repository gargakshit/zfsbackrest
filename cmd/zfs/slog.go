package main

import (
	"log/slog"
	"os"
)

func setSlog(level slog.Level) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})))
}
