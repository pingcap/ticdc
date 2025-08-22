package txnsink

import (
	"context"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// EventProcessor handles DML events and checkpoint processing
type EventProcessor struct {
	txnStore *TxnStore
}

// NewEventProcessor creates a new event processor
func NewEventProcessor(txnStore *TxnStore) *EventProcessor {
	return &EventProcessor{
		txnStore: txnStore,
	}
}

// ProcessDMLEvents processes DML events from the input channel
func (p *EventProcessor) ProcessDMLEvents(ctx context.Context, dmlEventChan <-chan *commonEvent.DMLEvent) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-dmlEventChan:
			if !ok {
				return nil
			}
			p.processDMLEvent(event)
		}
	}
}

// ProcessCheckpoints processes checkpoint timestamps from the input channel
func (p *EventProcessor) ProcessCheckpoints(ctx context.Context, checkpointChan <-chan uint64, txnChan chan<- *TxnGroup) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case checkpointTs, ok := <-checkpointChan:
			if !ok {
				return nil
			}
			if err := p.processCheckpoint(checkpointTs, txnChan); err != nil {
				return err
			}
		}
	}
}

// processDMLEvent processes a single DML event
func (p *EventProcessor) processDMLEvent(event *commonEvent.DMLEvent) {
	// Add event to the transaction store
	p.txnStore.AddEvent(event)

	log.Debug("txnSink: processed DML event",
		zap.Uint64("commitTs", event.CommitTs),
		zap.Uint64("startTs", event.StartTs),
		zap.Int64("tableID", event.GetTableID()),
		zap.Int32("rowCount", event.Len()))
}

// processCheckpoint processes a checkpoint timestamp
func (p *EventProcessor) processCheckpoint(checkpointTs uint64, txnChan chan<- *TxnGroup) error {
	// Get all events with commitTs <= checkpointTs
	events := p.txnStore.GetEventsByCheckpointTs(checkpointTs)
	if len(events) == 0 {
		log.Debug("txnSink: no events to process for checkpoint",
			zap.Uint64("checkpointTs", checkpointTs))
		return nil
	}

	// Events are already grouped by TxnStore.GetEventsByCheckpointTs
	txnGroups := events

	// Send transaction groups to the output channel
	for _, txnGroup := range txnGroups {
		txnChan <- txnGroup
		log.Debug("txnSink: sent transaction group",
			zap.Uint64("commitTs", txnGroup.CommitTs),
			zap.Uint64("startTs", txnGroup.StartTs),
			zap.Int("eventCount", len(txnGroup.Events)))
	}

	// Remove processed events from the store
	p.txnStore.RemoveEventsByCheckpointTs(checkpointTs)

	log.Info("txnSink: processed checkpoint",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Int("eventCount", len(events)),
		zap.Int("txnGroupCount", len(txnGroups)))

	return nil
}
