package kafka

import (
	"github.com/IBM/sarama"
)

func isTransientKError(err *sarama.ProducerError) bool {
	if err == nil {
		return false
	}
	kerr, ok := err.Err.(sarama.KError)
	if !ok {
		return false
	}
	switch kerr {
	case sarama.ErrNotEnoughReplicasAfterAppend,
		sarama.ErrNotEnoughReplicas,
		sarama.ErrLeaderNotAvailable,
		sarama.ErrNotLeaderForPartition,
		sarama.ErrRequestTimedOut,
		sarama.ErrNetworkException:
		return true
	default:
	}
	return false
}
