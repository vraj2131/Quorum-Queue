package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/forge/shared/models"
)

type TaskPayload struct {
	Type     string `json:"type"`     // e.g. "sleep", "compute", "echo"
	Duration string `json:"duration"` // for sleep e.g. "100ms"
	Message  string `json:"message"`
}

type Executor struct {
	logger *slog.Logger
}

func NewExecutor(logger *slog.Logger) *Executor {
	return &Executor{logger: logger}
}

func (e *Executor) Execute(ctx context.Context, job *models.Job) error {
	e.logger.Info("Starting task execution",
		"job_id", job.ID.String(),
		"idempotency_key", job.IdempotencyKey,
		"attempts", job.Attempts,
	)

	var payload TaskPayload
	if len(job.Payload) > 0 {
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			// If not typed json, log raw
			e.logger.Info("Executing generic raw payload job", "job_id", job.ID.String(), "raw", string(job.Payload))
		}
	}

	switch payload.Type {
	case "sleep":
		d, err := time.ParseDuration(payload.Duration)
		if err != nil {
			d = 100 * time.Millisecond
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	case "fail":
		return fmt.Errorf("simulated job execution failure for job_id=%s", job.ID)
	default:
		// Default lightweight operation
		time.Sleep(50 * time.Millisecond)
	}

	e.logger.Info("Finished task execution successfully", "job_id", job.ID.String())
	return nil
}
