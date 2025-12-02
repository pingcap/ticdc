package event

import "github.com/prometheus/client_golang/prometheus"

var (
	DMLDecodeDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event",
			Name:      "decode_duration",
			Help:      "The duration of decoding a row from eventStore",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})

	DMLIgnoreComputeDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "event",
			Name:      "ignore_compute_duration",
			Help:      "The duration of computing if a row should be ignored",
			Buckets:   prometheus.ExponentialBuckets(0.00004, 2.0, 28), // 40us to 1.5h
		})
)

func InitEventMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DMLDecodeDuration)
	registry.MustRegister(DMLIgnoreComputeDuration)
}
