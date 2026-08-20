package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

type Job struct {
	ID             uuid.UUID       `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Priority       int16           `json:"priority"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	WorkerID       *string         `json:"worker_id,omitempty"`
	LastHeartbeat  *time.Time      `json:"last_heartbeat,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type WorkerHeartbeat struct {
	WorkerID   string     `json:"worker_id"`
	LastSeen   time.Time  `json:"last_seen"`
	CurrentJob *uuid.UUID `json:"current_job,omitempty"`
}
