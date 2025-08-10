package main

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

func setSlog(level slog.Level) {
	var handler slog.Handler

	if isatty.IsTerminal(os.Stderr.Fd()) {
		handler = tint.NewHandler(os.Stderr, &tint.Options{
			Level:     level,
			AddSource: true,
			NoColor:   false,
		})
	} else {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level:     level,
			AddSource: true,
		})
	}

	slog.SetDefault(slog.New(handler))
}
