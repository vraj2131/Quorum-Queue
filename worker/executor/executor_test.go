package executor

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/forge/shared/models"
	"github.com/google/uuid"
)

func TestExecutor_ExecuteSuccess(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	exec := NewExecutor(logger)

	payload, _ := json.Marshal(TaskPayload{Type: "sleep", Duration: "10ms"})
	job := &models.Job{
		ID:             uuid.New(),
		IdempotencyKey: "test-exec-key",
		Payload:        payload,
		Status:         models.StatusRunning,
	}

	err := exec.Execute(context.Background(), job)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestExecutor_ExecuteFailure(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	exec := NewExecutor(logger)

	payload, _ := json.Marshal(TaskPayload{Type: "fail"})
	job := &models.Job{
		ID:             uuid.New(),
		IdempotencyKey: "test-fail-key",
		Payload:        payload,
		Status:         models.StatusRunning,
	}

	err := exec.Execute(context.Background(), job)
	if err == nil {
		t.Fatalf("expected error for failure task, got nil")
	}
}
