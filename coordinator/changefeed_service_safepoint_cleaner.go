package coordinator

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type changefeedServiceSafepointCleaner struct {
	pdClient pd.Client

	// gcServiceIDPrefix is the prefix used to build the service ID passed to PD.
	gcServiceIDPrefix string

	pending *pendingChangefeedServiceSafepoint

	triggerCh chan struct{}
}

func newChangefeedServiceSafepointCleaner(
	pdClient pd.Client,
	gcServiceIDPrefix string,
	pending *pendingChangefeedServiceSafepoint,
) *changefeedServiceSafepointCleaner {
	return &changefeedServiceSafepointCleaner{
		pdClient:          pdClient,
		gcServiceIDPrefix: gcServiceIDPrefix,
		pending:           pending,
		triggerCh:         make(chan struct{}, 1),
	}
}

func (c *changefeedServiceSafepointCleaner) Trigger() {
	select {
	case c.triggerCh <- struct{}{}:
	default:
	}
}

func (c *changefeedServiceSafepointCleaner) Run(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.triggerCh:
			err := c.tryClearEnsureGCSafepoint(ctx)
			if err == nil {
				backoff = time.Second
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (c *changefeedServiceSafepointCleaner) getEnsureGCServiceID(tag string) string {
	return c.gcServiceIDPrefix + tag
}

func (c *changefeedServiceSafepointCleaner) tryClearEnsureGCSafepoint(ctx context.Context) error {
	tasks := c.pending.takeReadyForUndo()
	for i, task := range tasks {
		childCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		gcServiceID := c.getEnsureGCServiceID(task.tag)
		err := gc.UndoEnsureChangefeedStartTsSafety(childCtx, c.pdClient, task.keyspaceID, gcServiceID, task.changefeedID)
		cancel()
		if err == nil {
			continue
		}

		switch task.tag {
		case gc.EnsureGCServiceCreating:
			log.Warn("failed to delete create changefeed gc safepoint", zap.Error(err))
		case gc.EnsureGCServiceResuming:
			log.Warn("failed to delete resume changefeed gc safepoint", zap.Error(err))
		default:
			log.Warn("failed to delete changefeed gc safepoint",
				zap.String("gcServiceTag", task.tag),
				zap.Error(err))
		}

		// Put back the failed task and the remaining ones, and retry next time.
		for _, requeue := range tasks[i:] {
			switch requeue.tag {
			case gc.EnsureGCServiceCreating:
				c.pending.addCreating(requeue.changefeedID, requeue.keyspaceID)
			case gc.EnsureGCServiceResuming:
				c.pending.addResuming(requeue.changefeedID, requeue.keyspaceID)
			default:
				log.Warn("failed to requeue changefeed gc safepoint task",
					zap.String("gcServiceTag", requeue.tag),
					zap.String("changefeed", requeue.changefeedID.String()))
			}
		}
		return err
	}
	return nil
}
