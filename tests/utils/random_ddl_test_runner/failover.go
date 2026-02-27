package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func (r *runner) failoverLoop(ctx context.Context, motifStep *int32, trace *ddlTrace) error {
	_ = motifStep
	_ = trace

	// failoverLoop randomly kills and restarts captures to simulate process-level failover.
	//
	// This is only meaningful when the integration harness has started multiple captures
	// (and the addresses are provided via config.failover.capture_addrs).
	//
	// The restart uses the integration helper scripts (run_cdc_server/kill_cdc_pid) which
	// are expected to be in PATH when run under tests/integration_tests/*.
	minD := r.cfg.Failover.MinInterval.Duration
	maxD := r.cfg.Failover.MaxInterval.Duration
	if maxD < minD {
		maxD, minD = minD, maxD
	}
	if minD <= 0 {
		minD = 20 * time.Second
	}
	if maxD <= 0 {
		maxD = 40 * time.Second
	}
	if len(r.cfg.Failover.CaptureAddrs) == 0 {
		return nil
	}

	rng := rand.New(rand.NewSource(r.cfg.Seed + 30_000))
	restartRound := 0

	for {
		sleep := minD
		if maxD > minD {
			sleep = minD + time.Duration(rng.Int63n(int64(maxD-minD)))
		}
		if err := sleepWithContext(ctx, sleep); err != nil {
			return nil
		}

		if rng.Float64() < r.cfg.Failover.GatedProbability {
			// Optional gating: avoid failover when checkpoint is not advancing to reduce
			// the chance of amplifying an existing stall.
			st1, err := r.getChangefeedStatus(ctx)
			if err != nil {
				return err
			}
			if err := sleepWithContext(ctx, 3*time.Second); err != nil {
				return nil
			}
			st2, err := r.getChangefeedStatus(ctx)
			if err != nil {
				return err
			}
			if st1.Checkpoint != 0 && st1.Checkpoint == st2.Checkpoint {
				r.logger.Printf("failover gated: checkpoint did not advance (%d)", st1.Checkpoint)
				continue
			}
		}

		target := r.cfg.Failover.CaptureAddrs[rng.Intn(len(r.cfg.Failover.CaptureAddrs))]
		pid, err := getCapturePID(ctx, target, r.cfg.CDC.User, r.cfg.CDC.Password)
		if err != nil {
			r.logger.Printf("failover: cannot get pid addr=%s err=%v", target, err)
			continue
		}
		if pid == 0 {
			r.logger.Printf("failover: empty pid addr=%s", target)
			continue
		}

		r.logger.Printf("failover: killing capture addr=%s pid=%d", target, pid)
		if err := execCommand(ctx, "kill_cdc_pid", strconv.Itoa(pid)); err != nil {
			r.logger.Printf("failover: kill failed pid=%d err=%v", pid, err)
			continue
		}

		restartRound++
		suffix := fmt.Sprintf("failover-%d", restartRound)
		r.logger.Printf("failover: restarting capture addr=%s suffix=%s", target, suffix)
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			lastErr = execCommand(ctx, "run_cdc_server",
				"--workdir", r.cfg.Workdir,
				"--binary", r.cfg.Failover.CdcBinary,
				"--logsuffix", suffix,
				"--addr", target,
			)
			if lastErr == nil {
				break
			}
			r.logger.Printf("failover: restart attempt=%d err=%v", attempt+1, lastErr)
			if err := sleepWithContext(ctx, 3*time.Second); err != nil {
				return nil
			}
		}
		if lastErr != nil {
			return lastErr
		}
	}
}

func getCapturePID(ctx context.Context, addr, user, password string) (int, error) {
	u := fmt.Sprintf("http://%s/status", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return 0, err
		}
		req2.SetBasicAuth(user, password)
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			return 0, err
		}
		defer resp2.Body.Close()
		resp = resp2
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status http %d", resp.StatusCode)
	}

	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return 0, err
	}
	p, ok := raw["pid"].(float64)
	if !ok {
		return 0, nil
	}
	return int(p), nil
}

func execCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
