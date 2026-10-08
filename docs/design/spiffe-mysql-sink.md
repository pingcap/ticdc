# SPIFFE Workload API mTLS for the MySQL Sink

## Motivation and scope

The MySQL sink currently loads TLS credentials from certificate files. A
SPIFFE deployment instead supplies short-lived X.509-SVIDs and trust bundles
through the Workload API. This design lets `mysql` and `tidb` sinks consume
those credentials directly and use rotated material when opening new TLS
connections.

The feature applies to downstream transport authentication. SQL usernames,
passwords, and grants continue to use the existing MySQL protocol and account
configuration. Mapping SPIFFE URI SANs to TiDB SQL accounts is separate TiDB
work; this change does not add that capability.

The implementation is owned by
[`pkg/sink/mysql/spiffe.go`](../../pkg/sink/mysql/spiffe.go), using the official
go-spiffe `workloadapi.X509Source`.

## Configuration and prerequisites

SPIFFE TLS is enabled only when the sink URI supplies these parameters:

| Parameter | Meaning |
| --- | --- |
| `spiffe-client-id-pattern` | Selects and verifies the local client X.509-SVID. |
| `spiffe-server-id-pattern` | Authorizes the downstream server's SPIFFE ID after certificate-chain verification. |

Both parameters must occur exactly once and have nonempty values. If neither
is present, existing certificate-file TLS behavior is preserved. SPIFFE TLS
cannot be combined with the URI parameters `ssl-ca`, `ssl-cert`, `ssl-key`, or
`tls`, including empty values, or with configured certificate-file paths.

Each TiCDC process that can open a downstream connection must have access to
a Workload API endpoint and set `SPIFFE_ENDPOINT_SOCKET`. This includes
processes opening sink, control, validation, cluster-check, and changefeed
cleanup connections; setting the variable only in the CLI process is
insufficient. For example, set the following in the TiCDC process environment:

```sh
export SPIFFE_ENDPOINT_SOCKET=unix:///run/spire/agent.sock
```

The Workload API must supply exactly one currently valid SVID matching the
client pattern and a trust bundle for the server pattern's trust domain. The
downstream endpoint must present a valid X.509-SVID matching the server pattern
and be configured to accept the selected client certificate. SQL account
credentials and permissions must also be configured as usual.

An example sink URI, with URL-encoded SPIFFE patterns, is:

```text
mysql://replicator:example-password@db.example.org:4000/?spiffe-client-id-pattern=spiffe%3A%2F%2Fexample.org%2Fticdc%2F%2A&spiffe-server-id-pattern=spiffe%3A%2F%2Fexample.org%2Fdatabase%2Fserver
```

This selects a client identity such as `spiffe://example.org/ticdc/worker-1`
and authorizes only the server identity
`spiffe://example.org/database/server`. The password is illustrative; use the
credentials required by the downstream SQL account.

### Identity pattern policy

A pattern is either an exact SPIFFE ID or a SPIFFE ID with one final `/*`
segment. The wildcard matches exactly one nonempty path segment in the same
trust domain. For example:

| Pattern | Accepted | Rejected |
| --- | --- | --- |
| `spiffe://example.org/database/server` | That exact ID | `spiffe://example.org/database/other` |
| `spiffe://example.org/ticdc/*` | `spiffe://example.org/ticdc/worker-1` | `spiffe://example.org/ticdc`, `spiffe://example.org/ticdc/worker-1/child`, IDs in other trust domains |

Partial, intermediate, or multiple wildcards are invalid. Invalid SPIFFE IDs,
including query strings and percent-encoded identity components, are rejected
after decoding the sink URI parameter. URL encoding the entire parameter value
for the outer sink URI, as in the example, is supported.

## Handshakes and rotation

Initialization waits for the first Workload API update for at most 30 seconds,
or until an earlier caller deadline. Missing credentials, ambiguous client
selection, absent trust bundles, and invalid identities fail initialization.
The source continues watching after the initialization context is canceled.

New TLS handshakes fetch the current client SVID and server trust bundle. The
client SVID is checked against its certificate URI SAN, validity period, and
configured pattern. The server certificate chain is verified using the bundle
for its trust domain, then its SPIFFE ID is checked against the server pattern.
Certificate or bundle rotation therefore affects new connections without a
sink restart. Existing SQL connections are not reauthenticated or forcibly
closed when material rotates.

The Workload API client reconnects after transient watch errors. During those
errors, the source retains the last received material; each new handshake still
checks validity and identity. A later update with no unique matching client
SVID causes new connections to fail until a suitable update arrives. SPIFFE
configuration, credential acquisition, and TLS verification failures never
fall back to plaintext or certificate-file TLS.

## Resource ownership

Each initialized config owns its Workload API client, watcher, and unique MySQL
driver TLS registration. Initialization and database-pool failures release
these resources. Validation and cluster-check connections release them after
closing their database handles; normal sink shutdown closes the pools before
releasing TLS resources. Closing the TLS resource is idempotent and cancels and
joins the watcher.

An initialized SPIFFE config is single-use, including after it closes.
Changefeed removal can require a new SQL connection after normal sink shutdown,
so cleanup creates a fresh source and TLS registration with bounded
initialization, then releases them when cleanup finishes.

## Dependency choice and review considerations

The selected dependency is go-spiffe v2.8.2, the first tagged release containing
the bundle-rotation race fix from
[PR #420](https://github.com/spiffe/go-spiffe/pull/420), released in
[v2.8.2](https://github.com/spiffe/go-spiffe/releases/tag/v2.8.2). The official
`X509Source` synchronizes reads of SVIDs and bundle pointers with Workload API
updates. go-spiffe owns parsing, reconnects, source synchronization, watcher
cleanup, and TLS certificate-chain verification. TiCDC owns client selection,
identity-pattern authorization, validity checks, and the MySQL driver TLS
registration.

Real Workload API and MySQL-handshake regression coverage remains applicable to
this official source. Future dependency or implementation changes must preserve
the exact single-segment wildcard policy; a general prefix authorizer can accept
a broader set of identities.

The feature adds one watch stream per owned source and short read locks during
new-connection TLS setup. It adds no Workload API request per SQL operation.
Using the fixed official source keeps Workload API synchronization and lifecycle
maintenance in go-spiffe while retaining TiCDC's configuration and authorization
policy.
