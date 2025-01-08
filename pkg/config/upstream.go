package config

import (
	"encoding/json"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// UpstreamID is the type for upstream ID
type UpstreamID = uint64

// UpstreamInfo store in etcd.
type UpstreamInfo struct {
	ID            uint64   `json:"id"`
	PDEndpoints   string   `json:"pd-endpoints"`
	KeyPath       string   `json:"key-path"`
	CertPath      string   `json:"cert-path"`
	CAPath        string   `json:"ca-path"`
	CertAllowedCN []string `json:"cert-allowed-cn"`
}

// Marshal using json.Marshal.
func (c *UpstreamInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *UpstreamInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

// Clone returns a cloned upstreamInfo
func (c *UpstreamInfo) Clone() (*UpstreamInfo, error) {
	s, err := c.Marshal()
	if err != nil {
		return nil, err
	}
	cloned := new(UpstreamInfo)
	err = cloned.Unmarshal(s)
	return cloned, err
}
