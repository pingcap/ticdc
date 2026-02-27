package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type changefeedStatus struct {
	State      string
	Checkpoint uint64
}

func (r *runner) getChangefeedStatus(ctx context.Context) (changefeedStatus, error) {
	// Query TiCDC OpenAPI to obtain changefeed state and checkpoint tso.
	//
	// The integration tests use a fixed basic auth user/password. This runner keeps the
	// request logic small and dependency-free to remain easy to vendor into test envs.
	if r.cfg.CDC.ChangefeedID == "" {
		return changefeedStatus{}, fmt.Errorf("cdc.changefeed_id is required")
	}

	u := url.URL{
		Scheme: "http",
		Host:   r.cfg.CDC.Addr,
		Path:   "/api/v2/changefeeds/" + url.PathEscape(r.cfg.CDC.ChangefeedID) + "/status",
	}
	q := u.Query()
	q.Set("keyspace", r.cfg.CDC.Keyspace)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return changefeedStatus{}, err
	}
	req.SetBasicAuth(r.cfg.CDC.User, r.cfg.CDC.Password)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return changefeedStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return changefeedStatus{}, fmt.Errorf("cdc status http %d: %s", resp.StatusCode, string(b))
	}

	var raw map[string]any
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&raw); err != nil {
		return changefeedStatus{}, err
	}

	state, _ := raw["state"].(string)
	checkpoint := parseUint64(raw["checkpoint_tso"])
	if checkpoint == 0 {
		checkpoint = parseUint64(raw["checkpoint_ts"])
	}
	return changefeedStatus{
		State:      state,
		Checkpoint: checkpoint,
	}, nil
}

func parseUint64(v any) uint64 {
	// TiCDC API fields may be encoded as number or string depending on endpoint/version.
	switch x := v.(type) {
	case float64:
		if x < 0 {
			return 0
		}
		return uint64(x)
	case string:
		n, err := strconv.ParseUint(x, 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}
