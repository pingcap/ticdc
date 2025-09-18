package txnsink

import (
	"context"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// EventProcessor handles DML events and checkpoint processing
type EventProcessor struct {
	txnStore        *TxnStore
	progressTracker *ProgressTracker
}

// NewEventProcessor creates a new event processor
func NewEventProcessor(txnStore *TxnStore, progressTracker *ProgressTracker) *EventProcessor {
	return &EventProcessor{
		txnStore:        txnStore,
		progressTracker: progressTracker,
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
}

// processCheckpoint processes a checkpoint timestamp
func (p *EventProcessor) processCheckpoint(checkpointTs uint64, txnChan chan<- *TxnGroup) error {
	// Get all events with commitTs <= checkpointTs (already sorted by commitTs)
	txnGroups := p.txnStore.GetEventsByCheckpointTs(checkpointTs)
	if len(txnGroups) == 0 {
		log.Info("txnSink: no events to process for checkpoint",
			zap.Uint64("checkpointTs", checkpointTs))
		p.progressTracker.UpdateCheckpointTs(checkpointTs)
		return nil
	}

	// Send transaction groups to the output channel
	for _, txnGroup := range txnGroups {
		p.progressTracker.AddPendingTxn(txnGroup.CommitTs, txnGroup.StartTs)
		txnChan <- txnGroup
	}

	// Update checkpoint progress
	p.progressTracker.UpdateCheckpointTs(checkpointTs)

	// Remove processed events from the store
	p.txnStore.RemoveEventsByCheckpointTs(checkpointTs)

	log.Info("txnSink: processed checkpoint",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Int("txnGroupCount", len(txnGroups)))

	return nil
}
