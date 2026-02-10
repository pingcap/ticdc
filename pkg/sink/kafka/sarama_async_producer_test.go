package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/recoverable"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type fakeSaramaAsyncProducer struct {
	inputCh     chan *sarama.ProducerMessage
	successesCh chan *sarama.ProducerMessage
	errorsCh    chan *sarama.ProducerError
}

func newFakeSaramaAsyncProducer() *fakeSaramaAsyncProducer {
	return &fakeSaramaAsyncProducer{
		inputCh:     make(chan *sarama.ProducerMessage, 16),
		successesCh: make(chan *sarama.ProducerMessage, 16),
		errorsCh:    make(chan *sarama.ProducerError, 16),
	}
}

func (p *fakeSaramaAsyncProducer) AsyncClose() {}
func (p *fakeSaramaAsyncProducer) Close() error {
	return nil
}

func (p *fakeSaramaAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return p.inputCh
}

func (p *fakeSaramaAsyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.successesCh
}

func (p *fakeSaramaAsyncProducer) Errors() <-chan *sarama.ProducerError {
	return p.errorsCh
}

func (p *fakeSaramaAsyncProducer) IsTransactional() bool {
	return false
}

func (p *fakeSaramaAsyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnStatusFlag(0)
}

func (p *fakeSaramaAsyncProducer) BeginTxn() error {
	return nil
}

func (p *fakeSaramaAsyncProducer) CommitTxn() error {
	return nil
}

func (p *fakeSaramaAsyncProducer) AbortTxn() error {
	return nil
}

func (p *fakeSaramaAsyncProducer) AddOffsetsToTxn(_ map[string][]*sarama.PartitionOffsetMetadata, _ string) error {
	return nil
}

func (p *fakeSaramaAsyncProducer) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	return nil
}

func TestSaramaAsyncProducer_TransientKErrorDoesNotExitAndReports(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeProducer := newFakeSaramaAsyncProducer()
	recoverableCh := make(chan *recoverable.ErrorEvent, 1)

	saramaProducer := &saramaAsyncProducer{
		producer:     fakeProducer,
		changefeedID: common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		closed:       atomic.NewBool(false),
		failpointCh:  make(chan *sarama.ProducerError, 1),
	}
	saramaProducer.SetRecoverableErrorChan(recoverableCh)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- saramaProducer.AsyncRunCallback(ctx)
	}()

	dispatcherID := common.NewDispatcherID()
	fakeProducer.errorsCh <- &sarama.ProducerError{
		Msg: &sarama.ProducerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Metadata: &messageMetadata{
				logInfo: &codecCommon.MessageLogInfo{
					DispatcherIDs: []common.DispatcherID{dispatcherID},
				},
			},
		},
		Err: sarama.KError(20), // NOT_ENOUGH_REPLICAS_AFTER_APPEND
	}

	select {
	case <-time.After(3 * time.Second):
		t.Fatal("recoverable event not reported in time")
	case event := <-recoverableCh:
		require.NotNil(t, event)
		require.Equal(t, []common.DispatcherID{dispatcherID}, event.DispatcherIDs)
	}

	select {
	case err := <-doneCh:
		t.Fatalf("AsyncRunCallback exited unexpectedly: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case <-time.After(3 * time.Second):
		t.Fatal("AsyncRunCallback did not exit after context canceled")
	case err := <-doneCh:
		require.Equal(t, context.Canceled, perrors.Cause(err))
	}
}

func TestSaramaAsyncProducer_NonTransientKErrorExits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fakeProducer := newFakeSaramaAsyncProducer()
	saramaProducer := &saramaAsyncProducer{
		producer:     fakeProducer,
		changefeedID: common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		closed:       atomic.NewBool(false),
		failpointCh:  make(chan *sarama.ProducerError, 1),
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- saramaProducer.AsyncRunCallback(ctx)
	}()

	fakeProducer.errorsCh <- &sarama.ProducerError{
		Msg: &sarama.ProducerMessage{
			Topic:     "test-topic",
			Partition: 0,
		},
		Err: sarama.KError(1),
	}

	select {
	case <-time.After(3 * time.Second):
		t.Fatal("AsyncRunCallback did not exit in time")
	case err := <-doneCh:
		code, ok := cerrors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, cerrors.ErrKafkaAsyncSendMessage.RFCCode(), code)
	}
}
