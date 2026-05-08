package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func scanLogsForPatterns(workdir string, patterns []string, failOnMatch bool, logger *log.Logger) error {
	// scanLogsForPatterns is a lightweight alternative to external tools (e.g., rg) so the
	// runner can be used in minimal environments.
	//
	// Implementation notes:
	//   - Convert to lowercase on the fly and use bytes.Contains for substring search.
	//   - Use bufio.Reader.ReadLine to cap memory and handle very long lines by stitching
	//     a small suffix ("carry") across fragments.
	files, err := collectLogFiles(workdir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		if logger != nil {
			logger.Printf("log scan: no log files found under %s", workdir)
		}
		return nil
	}

	lowerPatterns := make([]string, 0, len(patterns))
	for _, p := range patterns {
		lowerPatterns = append(lowerPatterns, strings.ToLower(p))
	}

	type hit struct {
		file string
		line int
		pat  string
	}
	var hits []hit

	maxPatternLen := 0
	patternBytes := make([][]byte, 0, len(lowerPatterns))
	for _, p := range lowerPatterns {
		b := []byte(p)
		patternBytes = append(patternBytes, b)
		if len(b) > maxPatternLen {
			maxPatternLen = len(b)
		}
	}

	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		lineNo := 0
		reader := bufio.NewReaderSize(f, 256*1024)

		carry := make([]byte, 0, maxPatternLen)
		scratch := make([]byte, 0, 256*1024)
		tmp := make([]byte, 0, 256*1024)
		lineMatched := false

		for {
			part, isPrefix, err := reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
				_ = f.Close()
				return err
			}

			if !lineMatched {
				tmp = append(tmp[:0], part...)
				for i := range tmp {
					c := tmp[i]
					if c >= 'A' && c <= 'Z' {
						tmp[i] = c + ('a' - 'A')
					}
				}

				scratch = append(scratch[:0], carry...)
				scratch = append(scratch, tmp...)

				for i, p := range patternBytes {
					if bytes.Contains(scratch, p) {
						hits = append(hits, hit{file: filepath.Base(path), line: lineNo + 1, pat: lowerPatterns[i]})
						lineMatched = true
						break
					}
				}
			}

			if !isPrefix {
				lineNo++
				carry = carry[:0]
				lineMatched = false
				continue
			}
			// Keep a small suffix from the previous fragment to detect patterns spanning boundaries.
			if len(scratch) == 0 {
				carry = carry[:0]
				continue
			}
			keep := maxPatternLen - 1
			if keep <= 0 {
				carry = carry[:0]
				continue
			}
			if keep > len(scratch) {
				keep = len(scratch)
			}
			carry = append(carry[:0], scratch[len(scratch)-keep:]...)
		}

		_ = f.Close()
	}

	if len(hits) == 0 {
		if logger != nil {
			logger.Printf("log scan: no matches")
		}
		return nil
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].file != hits[j].file {
			return hits[i].file < hits[j].file
		}
		return hits[i].line < hits[j].line
	})

	if logger != nil {
		logger.Printf("log scan: found %d matches", len(hits))
		for i := 0; i < len(hits) && i < 20; i++ {
			logger.Printf("log scan match: file=%s line=%d pattern=%q", hits[i].file, hits[i].line, hits[i].pat)
		}
	}

	if failOnMatch {
		return fmt.Errorf("log scan found %d panic/fatal/race matches", len(hits))
	}
	return nil
}

func collectLogFiles(workdir string) ([]string, error) {
	globs := []string{
		filepath.Join(workdir, "runner.log"),
		filepath.Join(workdir, "ddl_trace.log"),
		filepath.Join(workdir, "stdout*.log"),
		filepath.Join(workdir, "cdc*.log"),
		filepath.Join(workdir, "cdc_*_consumer*.log"),
		filepath.Join(workdir, "cdc_*_consumer_stdout*.log"),
	}
	seen := make(map[string]struct{})
	for _, g := range globs {
		m, err := filepath.Glob(g)
		if err != nil {
			return nil, err
		}
		for _, p := range m {
			seen[p] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}
