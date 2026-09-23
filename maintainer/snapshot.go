// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package maintainer

import (
	"net/url"
	"sort"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/snapshot/bootstrap"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

// The first native deployment keeps a single capture. The local Capture
// interface is the seam for later MessageCenter assignment transport.
func (m *Maintainer) snapshotBeforeBootstrap(responses map[node.ID]*heartbeatpb.MaintainerBootstrapResponse) bool {
	if m.info.Config.Snapshot == nil || m.snapshotDone {
		return true
	}
	if m.snapshotRunning {
		return false
	}
	cfg := m.info.Config.Snapshot
	if err := cfg.ValidateRuntime(); err != nil {
		m.handleError(err)
		return false
	}
	uri, err := url.Parse(m.info.SinkURI)
	if err != nil {
		m.handleError(protocol.Wrap(err))
		return false
	}
	if len(responses) != 1 || responses[m.selfNode.ID] == nil || uri.Scheme != "kafka" || uri.Query().Get("protocol") != "open-protocol" || cfg.ArtifactDir == "" || cfg.ProviderURL == "" || cfg.BackupURI == "" || len(cfg.BackupDigest) != 64 || m.enableRedo || len(m.info.Config.Filter.EventFilters) > 0 {
		m.handleError(protocol.Invalid("snapshot demo requires single capture, kafka open-protocol, frozen backup digest, artifact dir, Provider and no redo/event filters"))
		return false
	}
	options := kafka.NewOptions()
	if err := options.Apply(m.changefeedID, uri, m.info.Config.Sink); err != nil {
		m.handleError(err)
		return false
	}
	if options.RequiredAcks == kafka.NoResponse {
		m.handleError(protocol.Invalid("snapshot receipts require Kafka broker acknowledgements"))
		return false
	}
	repo, ok := appcontext.TryGetService[bootstrap.Repository](bootstrap.RepositoryService)
	if !ok {
		m.handleError(protocol.Invalid("local coordinator snapshot service unavailable"))
		return false
	}
	capture, ok := appcontext.TryGetService[bootstrap.Capture](appcontext.DispatcherOrchestrator)
	if !ok {
		m.handleError(protocol.Invalid("capture snapshot capability unavailable"))
		return false
	}
	controller := bootstrap.Controller{ID: m.changefeedID, Epoch: m.info.Epoch, TS: m.info.StartTs, Keyspace: m.controller.keyspaceMeta.ID, Config: cfg, Repository: repo, Capture: capture, Freeze: func() (bootstrap.SchemaBundle, []protocol.Span, error) {
		bundle := bootstrap.SchemaBundle{SnapshotTS: m.info.StartTs}
		tables, err := m.controller.loadTables(m.info.StartTs)
		if err != nil {
			return bundle, nil, protocol.Wrap(err)
		}
		sort.Slice(tables, func(i, j int) bool { return tables[i].TableID < tables[j].TableID })
		schemas := appcontext.GetService[schemastore.SchemaStore](appcontext.SchemaStore)
		var spans []protocol.Span
		for _, t := range tables {
			// Snapshot mounting precedes incremental dispatcher registration. Pin
			// this version explicitly while copying its frozen schema.
			if err := schemas.RegisterTable(m.controller.keyspaceMeta, t.TableID, m.info.StartTs); err != nil {
				return bundle, nil, protocol.Wrap(err)
			}
			table, err := schemas.GetTableInfo(m.controller.keyspaceMeta, t.TableID, m.info.StartTs)
			unregisterErr := schemas.UnregisterTable(m.controller.keyspaceMeta, t.TableID)
			if err != nil {
				return bundle, nil, protocol.Wrap(err)
			}
			if unregisterErr != nil {
				return bundle, nil, protocol.Wrap(unregisterErr)
			}
			b, err := table.Marshal()
			if err != nil {
				return bundle, nil, protocol.Wrap(err)
			}
			bundle.Tables = append(bundle.Tables, bootstrap.Schema{TableID: t.TableID, Data: b})
			start := tablecodec.GenTableRecordPrefix(t.TableID)
			end := append([]byte(nil), start...)
			end[len(end)-1]++
			spans = append(spans, protocol.Span{TableID: t.TableID, Start: start, End: end})
		}
		return bundle, spans, nil
	}}
	m.snapshotRunning = true
	go func() {
		err := controller.Run(m.snapshotContext)
		select {
		case <-m.snapshotContext.Done():
			return
		case m.eventCh.In() <- &Event{changefeedID: m.changefeedID, eventType: EventSnapshotDone, snapshotError: err, snapshotResponses: responses}:
		}
	}()
	return false
}
