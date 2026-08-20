package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	JobsClaimedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forge_jobs_claimed_total",
			Help: "Total number of jobs claimed by worker nodes",
		},
		[]string{"worker_id"},
	)

	JobsCompletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "forge_jobs_completed_total",
			Help: "Total number of jobs completed by status (succeeded/failed)",
		},
		[]string{"status", "worker_id"},
	)

	JobExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "forge_job_execution_duration_seconds",
			Help:    "Execution duration of jobs in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"status"},
	)

	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "forge_queue_depth",
			Help: "Current depth of queued jobs in database",
		},
	)

	LeaderStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "forge_leader_status",
			Help: "1 if current scheduler instance is active leader, 0 otherwise",
		},
		[]string{"scheduler_id"},
	)
)

func init() {
	prometheus.MustRegister(
		JobsClaimedTotal,
		JobsCompletedTotal,
		JobExecutionDuration,
		QueueDepth,
		LeaderStatus,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
