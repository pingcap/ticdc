// Copyright 2026 PingCAP, Inc.
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

package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

const (
	spiffeClientIDPatternKey = "spiffe-client-id-pattern"
	spiffeServerIDPatternKey = "spiffe-server-id-pattern"
)

var (
	spiffeTLSInitTimeout      = 30 * time.Second
	spiffeTLSRegistrySequence atomic.Uint64
)

type spiffeIDPattern struct {
	raw         string
	exact       spiffeid.ID
	trustDomain spiffeid.TrustDomain
	pathPrefix  string
	wildcard    bool
}

func parseSPIFFEIDPattern(raw string) (spiffeIDPattern, error) {
	if raw == "" {
		return spiffeIDPattern{}, fmt.Errorf("SPIFFE ID pattern is empty")
	}

	if !strings.Contains(raw, "*") {
		id, err := spiffeid.FromString(raw)
		if err != nil {
			return spiffeIDPattern{}, fmt.Errorf("invalid SPIFFE ID pattern %q: %w", raw, err)
		}
		return spiffeIDPattern{
			raw:         raw,
			exact:       id,
			trustDomain: id.TrustDomain(),
			pathPrefix:  id.Path(),
		}, nil
	}

	if strings.Count(raw, "*") != 1 || !strings.HasSuffix(raw, "/*") {
		return spiffeIDPattern{}, fmt.Errorf(
			"invalid SPIFFE ID pattern %q: wildcard must be the single final path segment", raw)
	}

	prefixID, err := spiffeid.FromString(strings.TrimSuffix(raw, "/*"))
	if err != nil {
		return spiffeIDPattern{}, fmt.Errorf("invalid SPIFFE ID pattern %q: %w", raw, err)
	}
	return spiffeIDPattern{
		raw:         raw,
		trustDomain: prefixID.TrustDomain(),
		pathPrefix:  prefixID.Path(),
		wildcard:    true,
	}, nil
}

func (p spiffeIDPattern) matches(actual spiffeid.ID) bool {
	if !p.wildcard {
		return actual == p.exact
	}
	if actual.TrustDomain() != p.trustDomain {
		return false
	}

	suffix := strings.TrimPrefix(actual.Path(), p.pathPrefix+"/")
	return suffix != actual.Path() && suffix != "" && !strings.Contains(suffix, "/")
}

type spiffeX509Source interface {
	x509svid.Source
	x509bundle.Source
	Close() error
}

var newSPIFFEX509Source = func(
	ctx context.Context,
	endpoint string,
	picker func([]*x509svid.SVID) *x509svid.SVID,
) (spiffeX509Source, error) {
	return workloadapi.NewX509Source(
		ctx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(endpoint)),
		workloadapi.WithDefaultX509SVIDPicker(picker),
	)
}

type matchingX509SVIDSource struct {
	source  x509svid.Source
	pattern spiffeIDPattern
}

func (s matchingX509SVIDSource) GetX509SVID() (*x509svid.SVID, error) {
	svid, err := s.source.GetX509SVID()
	if err != nil {
		return nil, err
	}
	if err := validateMatchingX509SVID(svid, s.pattern); err != nil {
		return nil, err
	}
	return svid, nil
}

func validateMatchingX509SVID(svid *x509svid.SVID, pattern spiffeIDPattern) error {
	if svid == nil {
		return fmt.Errorf("Workload API returned a nil X509-SVID")
	}
	if len(svid.Certificates) == 0 {
		return fmt.Errorf("Workload API returned an X509-SVID without certificates")
	}
	certID, err := x509svid.IDFromCert(svid.Certificates[0])
	if err != nil {
		return fmt.Errorf("Workload API returned an invalid X509-SVID: %w", err)
	}
	if certID != svid.ID {
		return fmt.Errorf("Workload API X509-SVID ID does not match its certificate URI SAN")
	}
	now := time.Now()
	if now.Before(svid.Certificates[0].NotBefore) {
		return fmt.Errorf("Workload API X509-SVID is not valid yet")
	}
	if now.After(svid.Certificates[0].NotAfter) {
		return fmt.Errorf("Workload API X509-SVID is expired")
	}
	if !pattern.matches(certID) {
		return fmt.Errorf("Workload API X509-SVID %q does not match client pattern %q", certID, pattern.raw)
	}
	return nil
}

func matchingX509SVIDPicker(pattern spiffeIDPattern) func([]*x509svid.SVID) *x509svid.SVID {
	return func(svids []*x509svid.SVID) *x509svid.SVID {
		var match *x509svid.SVID
		for _, candidate := range svids {
			if validateMatchingX509SVID(candidate, pattern) != nil {
				continue
			}
			if match != nil {
				return nil
			}
			match = candidate
		}
		return match
	}
}

func authorizeSPIFFEIDPattern(pattern spiffeIDPattern) tlsconfig.Authorizer {
	return func(actual spiffeid.ID, _ [][]*x509.Certificate) error {
		if !pattern.matches(actual) {
			return fmt.Errorf("peer SPIFFE ID %q does not match server pattern %q", actual, pattern.raw)
		}
		return nil
	}
}

func newSPIFFEClientTLSConfig(
	source spiffeX509Source,
	clientPattern spiffeIDPattern,
	serverPattern spiffeIDPattern,
) (*tls.Config, error) {
	matchingSource := matchingX509SVIDSource{source: source, pattern: clientPattern}
	if _, err := matchingSource.GetX509SVID(); err != nil {
		return nil, err
	}
	bundle, err := source.GetX509BundleForTrustDomain(serverPattern.trustDomain)
	if err != nil {
		return nil, fmt.Errorf("get X.509 bundle for server trust domain %q: %w", serverPattern.trustDomain, err)
	}
	if bundle == nil || len(bundle.X509Authorities()) == 0 {
		return nil, fmt.Errorf("X.509 bundle for server trust domain %q has no authorities", serverPattern.trustDomain)
	}

	return tlsconfig.MTLSClientConfig(
		matchingSource,
		source,
		authorizeSPIFFEIDPattern(serverPattern),
	), nil
}

type spiffeTLSResource struct {
	name      string
	source    spiffeX509Source
	closeOnce sync.Once
	closeErr  error
}

func (r *spiffeTLSResource) close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		dmysql.DeregisterTLSConfig(r.name)
		r.closeErr = r.source.Close()
	})
	return r.closeErr
}

type spiffeTLSOptions struct {
	endpoint      string
	clientPattern spiffeIDPattern
	serverPattern spiffeIDPattern
	registryName  string
}

func (c *Config) configureTLS(
	ctx context.Context,
	values url.Values,
	changefeedID common.ChangeFeedID,
) error {
	_, hasClientPattern := values[spiffeClientIDPatternKey]
	_, hasServerPattern := values[spiffeServerIDPatternKey]
	if !hasClientPattern && !hasServerPattern {
		return c.getFileSSLCA(values, changefeedID, &c.TLS)
	}

	if err := c.rejectMixedSPIFFETLS(values); err != nil {
		return err
	}
	clientPatternRaw, err := singleSPIFFEPatternValue(values, spiffeClientIDPatternKey)
	if err != nil {
		return err
	}
	serverPatternRaw, err := singleSPIFFEPatternValue(values, spiffeServerIDPatternKey)
	if err != nil {
		return err
	}
	clientPattern, err := parseSPIFFEIDPattern(clientPatternRaw)
	if err != nil {
		return cerror.ErrMySQLInvalidConfig.GenWithStack("invalid %s: %v", spiffeClientIDPatternKey, err)
	}
	serverPattern, err := parseSPIFFEIDPattern(serverPatternRaw)
	if err != nil {
		return cerror.ErrMySQLInvalidConfig.GenWithStack("invalid %s: %v", spiffeServerIDPatternKey, err)
	}

	endpoint := os.Getenv("SPIFFE_ENDPOINT_SOCKET")
	if endpoint == "" {
		return cerror.ErrMySQLInvalidConfig.GenWithStack(
			"SPIFFE_ENDPOINT_SOCKET must be set when SPIFFE MySQL TLS is configured")
	}

	options := &spiffeTLSOptions{
		endpoint:      endpoint,
		clientPattern: clientPattern,
		serverPattern: serverPattern,
		registryName:  newSPIFFETLSRegistryName(changefeedID),
	}
	if err := c.startSPIFFETLS(ctx, options, options.registryName); err != nil {
		return err
	}
	c.spiffeTLS = options
	return nil
}

func (c *Config) rejectMixedSPIFFETLS(values url.Values) error {
	for _, key := range []string{"ssl-ca", "ssl-cert", "ssl-key", "tls"} {
		if _, ok := values[key]; ok {
			return cerror.ErrMySQLInvalidConfig.GenWithStack(
				"%s cannot be combined with SPIFFE MySQL TLS", key)
		}
	}
	if c.SSLCa != "" || c.SSLCert != "" || c.SSLKey != "" {
		return cerror.ErrMySQLInvalidConfig.GenWithStack(
			"configured ssl-ca, ssl-cert, or ssl-key cannot be combined with SPIFFE MySQL TLS")
	}
	return nil
}

func singleSPIFFEPatternValue(values url.Values, key string) (string, error) {
	entries, ok := values[key]
	if !ok || len(entries) != 1 || entries[0] == "" {
		return "", cerror.ErrMySQLInvalidConfig.GenWithStack(
			"%s must be specified exactly once and cannot be empty", key)
	}
	return entries[0], nil
}

func mysqlTLSRegistryName(changefeedID common.ChangeFeedID) string {
	return fmt.Sprintf("cdc_mysql_tls%s_%s", changefeedID.Keyspace(), changefeedID.ID())
}

func newSPIFFETLSRegistryName(changefeedID common.ChangeFeedID) string {
	return fmt.Sprintf("%s_spiffe_%d", mysqlTLSRegistryName(changefeedID), spiffeTLSRegistrySequence.Add(1))
}

func (c *Config) startSPIFFETLS(
	ctx context.Context,
	options *spiffeTLSOptions,
	registryName string,
) error {
	initCtx, cancelInit := context.WithTimeout(ctx, spiffeTLSInitTimeout)
	defer cancelInit()
	source, err := newSPIFFEX509Source(
		initCtx,
		options.endpoint,
		matchingX509SVIDPicker(options.clientPattern),
	)
	if err != nil {
		return cerror.ErrMySQLConnectionError.Wrap(err).
			GenWithStack("connect to the SPIFFE Workload API")
	}

	tlsCfg, err := newSPIFFEClientTLSConfig(source, options.clientPattern, options.serverPattern)
	if err != nil {
		_ = source.Close()
		return cerror.ErrMySQLConnectionError.Wrap(err).
			GenWithStack("initialize SPIFFE MySQL TLS")
	}
	if err := dmysql.RegisterTLSConfig(registryName, tlsCfg); err != nil {
		_ = source.Close()
		return cerror.ErrMySQLConnectionError.Wrap(err).
			GenWithStack("register SPIFFE MySQL TLS")
	}

	c.TLS = "?tls=" + registryName
	c.tlsResource = &spiffeTLSResource{name: registryName, source: source}
	return nil
}

// CloseTLS releases a SPIFFE Workload API source owned by the config and
// removes its MySQL driver TLS registration. It is safe to call more than once.
func (c *Config) CloseTLS() error {
	if c == nil {
		return nil
	}
	return c.tlsResource.close()
}

// NewCleanupConfig returns a config suitable for the short-lived remove
// cleanup connection. SPIFFE configs get a fresh Workload API source because
// the sink-owned source has already been closed by the normal close path.
// The caller must call CloseTLS on the returned config when cleanup completes.
func (c *Config) NewCleanupConfig(ctx context.Context) (*Config, error) {
	if c.spiffeTLS == nil {
		return c, nil
	}

	clone := *c
	clone.TLS = ""
	clone.tlsResource = nil
	if err := clone.startSPIFFETLS(
		ctx, c.spiffeTLS, c.spiffeTLS.registryName+fmt.Sprintf("_cleanup_%d", spiffeTLSRegistrySequence.Add(1)),
	); err != nil {
		return nil, err
	}
	return &clone, nil
}
