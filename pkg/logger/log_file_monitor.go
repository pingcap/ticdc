// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultLogFileMonitorInterval = 30 * time.Second

	// https://github.com/natefinch/lumberjack/blob/v2.2.1/lumberjack.go#L39
	lumberjackBackupTimeFormat = "2006-01-02T15-04-05.000"
	lumberjackCompressSuffix   = ".gz"
)

var logFileMonitorOnce sync.Once

// StartLogFileMonitor periodically updates log-file size, total rotated log size,
// and disk usage metrics for the filesystem containing the log file.
//
// It is a no-op if logFile is empty.
func StartLogFileMonitor(ctx context.Context, logFile string, interval time.Duration) {
	if logFile == "" {
		return
	}
	if interval <= 0 {
		interval = defaultLogFileMonitorInterval
	}

	logFileMonitorOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()

			updateLogFileAndDiskMetrics(logFile)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					updateLogFileAndDiskMetrics(logFile)
				}
			}
		}()
	})
}

func updateLogFileAndDiskMetrics(logFile string) {
	fileSize, totalSize := collectLogFileSizes(logFile)
	setLogFileSizeBytes(fileSize)
	setLogTotalSizeBytes(totalSize)

	diskTotal, diskUsed := collectDiskTotalUsedBytes(filepath.Dir(logFile))
	setLogDiskTotalBytes(diskTotal)
	setLogDiskUsedBytes(diskUsed)
}

func collectLogFileSizes(logFile string) (fileSize uint64, totalSize uint64) {
	base := filepath.Base(logFile)
	dir := filepath.Dir(logFile)

	if st, err := os.Stat(logFile); err == nil {
		fileSize = uint64(st.Size())
	}

	ext := filepath.Ext(base)
	prefix := strings.TrimSuffix(base, ext) + "-"

	totalSize = fileSize
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fileSize, totalSize
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == base {
			continue
		}
		if !isLumberjackBackupLog(name, prefix, ext) && !isLumberjackBackupLog(name, prefix, ext+lumberjackCompressSuffix) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		totalSize += uint64(info.Size())
	}
	return fileSize, totalSize
}

func isLumberjackBackupLog(name, prefix, suffix string) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	ts := name[len(prefix) : len(name)-len(suffix)]
	_, err := time.Parse(lumberjackBackupTimeFormat, ts)
	return err == nil
}

func collectDiskTotalUsedBytes(path string) (totalBytes, usedBytes uint64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0
	}
	totalBytes = uint64(st.Blocks) * uint64(st.Bsize)
	freeBytes := uint64(st.Bfree) * uint64(st.Bsize)
	usedBytes = totalBytes - freeBytes
	return totalBytes, usedBytes
}
