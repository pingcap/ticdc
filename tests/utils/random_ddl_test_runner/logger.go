package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

func newRunnerLogger(workdir string) (*log.Logger, func(), error) {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(filepath.Join(workdir, "runner.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	w := io.MultiWriter(os.Stdout, f)
	l := log.New(w, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	return l, func() { _ = f.Close() }, nil
}
