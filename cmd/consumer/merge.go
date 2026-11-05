package main

import (
	"cmp"
	"slices"

	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// rowMerger turns the rows of one delivery round into writer events. The CSV
// encoder splits an update into delete and insert lines, so the merger pairs
// them back into one event.
type rowMerger interface {
	merge([]*dmlWrite) []*dmlWrite
	sequential() bool
}

type noopMerger struct{}

func (noopMerger) merge(items []*dmlWrite) []*dmlWrite {
	return items
}

func (noopMerger) sequential() bool {
	return false
}

type csvMerger struct {
	memory *memoryBudget
	sort   bool
}

func (m *csvMerger) sequential() bool {
	return true
}

func (m *csvMerger) merge(items []*dmlWrite) []*dmlWrite {
	if m.sort {
		slices.SortStableFunc(items, func(x, y *dmlWrite) int {
			return cmp.Or(cmp.Compare(x.dml.CommitTs, y.dml.CommitTs), cmp.Compare(x.dml.RowTypes[0], y.dml.RowTypes[0]))
		})
	}
	for _, item := range items {
		item.sequential = true
	}
	prepared := items[:0]
	remaining := items
	for len(remaining) != 0 {
		item := remaining[0]
		// Merge only the bounded batch already available. Waiting for an entire
		// transaction could retain arbitrarily large input in memory.
		first := item.dml
		count, retained := int(first.Len()), item.bytes
		end := 1
		for end < len(remaining) {
			if count >= batchRows || retained >= batchBytes {
				break
			}
			next := remaining[end].dml
			if next.PhysicalTableID != first.PhysicalTableID || next.CommitTs != first.CommitTs || !sameTableSchema(first.TableInfo, next.TableInfo) {
				break
			}
			count += int(next.Len())
			retained += remaining[end].bytes
			end++
		}
		if end == 1 {
			prepared = append(prepared, item)
			remaining = remaining[1:]
			continue
		}
		var rows int
		for _, part := range remaining[:end] {
			rows += len(part.dml.RowTypes)
		}
		dml := event.NewDMLEvent(first.DispatcherID, first.PhysicalTableID, first.StartTs, first.CommitTs, first.TableInfo)
		dml.Version, dml.Seq, dml.Epoch = first.Version, first.Seq, first.Epoch
		dml.ReplicatingTs, dml.TableInfoVersion = first.ReplicatingTs, first.TableInfoVersion
		dml.Rows = chunk.NewChunkWithCapacity(first.TableInfo.GetFieldSlice(), rows)
		for _, source := range remaining[:end] {
			part := source.dml
			dml.Rows.Append(part.Rows, part.PreviousTotalOffset, part.PreviousTotalOffset+len(part.RowTypes))
			dml.RowTypes = append(dml.RowTypes, part.RowTypes...)
			if len(dml.RowKeys) != 0 || len(part.RowKeys) != 0 {
				if len(dml.RowKeys) == 0 {
					dml.RowKeys = make([][]byte, dml.Rows.NumRows()-len(part.RowTypes))
				}
				keys := part.RowKeys
				if len(keys) == 0 {
					keys = make([][]byte, len(part.RowTypes))
				}
				dml.RowKeys = append(dml.RowKeys, keys...)
			}
			if len(dml.Checksum) != 0 || len(part.Checksum) != 0 {
				if len(dml.Checksum) == 0 {
					dml.Checksum = make([]*integrity.Checksum, dml.Length)
				}
				checksums := part.Checksum
				if len(checksums) == 0 {
					checksums = make([]*integrity.Checksum, part.Length)
				}
				dml.Checksum = append(dml.Checksum, checksums...)
			}
			dml.Length += part.Length
			dml.ApproximateSize += part.ApproximateSize
			if len(part.PostTxnEnqueued) != 0 || len(part.PostTxnFlushed) != 0 {
				postEnqueue, postFlush := part.DetachPostCallbacks()
				dml.AddPostEnqueueFunc(postEnqueue)
				dml.AddPostFlushFunc(postFlush)
			}
			part.Rows = nil
		}
		bytes := dml.Rows.MemoryUsage()
		m.memory.ForceAcquire(bytes)
		m.memory.Release(retained)
		prepared = append(prepared, &dmlWrite{
			dml:        dml,
			bytes:      bytes,
			sequential: true,
		})
		remaining = remaining[end:]
	}
	clear(items[len(prepared):])
	return prepared
}

func newRowMerger(cfg *codecCommon.Config, memory *memoryBudget) rowMerger {
	if cfg.Protocol != config.ProtocolCsv {
		return noopMerger{}
	}
	return &csvMerger{
		memory: memory,
		sort:   cfg.OutputOldValue && cfg.IncludeCommitTs,
	}
}
