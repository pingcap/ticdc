// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

type Client struct{ URL string }

func (c Client) Call(ctx context.Context, method string, req Request, out any) error {
	b, e := json.Marshal(req)
	if e != nil {
		return protocol.Wrap(e)
	}
	r, e := http.NewRequestWithContext(ctx, "POST", c.URL+"/"+method, bytes.NewReader(b))
	if e != nil {
		return protocol.Wrap(e)
	}
	r.Header.Set("Content-Type", "application/json")
	client := http.Client{Timeout: 30 * time.Second}
	resp, e := client.Do(r)
	if e != nil {
		return protocol.Wrap(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return protocol.Invalid("provider %s: %s", method, b)
	}
	return protocol.Wrap(json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out))
}
