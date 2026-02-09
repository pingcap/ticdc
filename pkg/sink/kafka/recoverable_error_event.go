package kafka

import (
	"time"

	"github.com/IBM/sarama"
	commonPkg "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

const RecoverableKafkaKErrorRunningErrorCode = "CDC:ErrKafkaRecoverableKError"

type ErrorEvent struct {
	Time time.Time

	KafkaErrCode int16
	KafkaErrName string
	Message      string

	Topic     string
	Partition int32

	LogInfo *common.MessageLogInfo
}

type ErrorReport struct {
	KafkaErrCode  int16
	KafkaErrName  string
	Message       string
	Topic         string
	Partition     int32
	DispatcherIDs []commonPkg.DispatcherID
}

func isProducerKErrorRecoverable(err *sarama.ProducerError) bool {
	if err == nil {
		return false
	}
	kerr, ok := err.Err.(sarama.KError)
	if !ok {
		return false
	}
	return isTransientKError(kerr)
}

func isTransientKError(kerr sarama.KError) bool {
	switch kerr {
	case sarama.ErrNotEnoughReplicasAfterAppend,
		sarama.ErrNotEnoughReplicas,
		sarama.ErrLeaderNotAvailable,
		sarama.ErrNotLeaderForPartition,
		sarama.ErrRequestTimedOut,
		sarama.ErrNetworkException:
		return true
	default:
		return false
	}
}
