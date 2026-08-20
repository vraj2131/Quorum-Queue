package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/forge/shared/models"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Store struct {
	DB *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{DB: db}
}

func Open(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}

// ClaimNextJob uses SELECT ... FOR UPDATE SKIP LOCKED to atomically pick and claim a job.
func (s *Store) ClaimNextJob(ctx context.Context, workerID string) (*models.Job, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, idempotency_key, payload, status, priority, attempts, max_attempts, worker_id, last_heartbeat, created_at, updated_at
		FROM jobs
		WHERE status = $1
		ORDER BY priority DESC, created_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`

	var job models.Job
	var workerIDNull sql.NullString
	var heartbeatNull sql.NullTime

	err = tx.QueryRowContext(ctx, query, models.StatusQueued).Scan(
		&job.ID,
		&job.IdempotencyKey,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.Attempts,
		&job.MaxAttempts,
		&workerIDNull,
		&heartbeatNull,
		&job.CreatedAt,
		&job.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No jobs available
		}
		return nil, fmt.Errorf("failed to query queued job: %w", err)
	}

	// Update job state to running and assign worker_id
	now := time.Now().UTC()
	job.Status = models.StatusRunning
	job.WorkerID = &workerID
	job.Attempts++
	job.LastHeartbeat = &now
	job.UpdatedAt = now

	updateQuery := `
		UPDATE jobs
		SET status = $1, worker_id = $2, attempts = $3, last_heartbeat = $4, updated_at = $5
		WHERE id = $6
	`
	_, err = tx.ExecContext(ctx, updateQuery, job.Status, workerID, job.Attempts, now, now, job.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to update job status to running: %w", err)
	}

	// Update worker heartbeat record
	hbQuery := `
		INSERT INTO worker_heartbeats (worker_id, last_seen, current_job)
		VALUES ($1, $2, $3)
		ON CONFLICT (worker_id)
		DO UPDATE SET last_seen = EXCLUDED.last_seen, current_job = EXCLUDED.current_job
	`
	_, err = tx.ExecContext(ctx, hbQuery, workerID, now, job.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to update worker heartbeat: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit job claim transaction: %w", err)
	}

	return &job, nil
}

func (s *Store) SendHeartbeat(ctx context.Context, workerID string, jobID uuid.UUID) error {
	now := time.Now().UTC()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start heartbeat tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `UPDATE jobs SET last_heartbeat = $1, updated_at = $1 WHERE id = $2 AND worker_id = $3 AND status = $4`, now, jobID, workerID, models.StatusRunning)
	if err != nil {
		return fmt.Errorf("failed to update job heartbeat: %w", err)
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO worker_heartbeats (worker_id, last_seen, current_job) VALUES ($1, $2, $3) ON CONFLICT (worker_id) DO UPDATE SET last_seen = EXCLUDED.last_seen, current_job = EXCLUDED.current_job`, workerID, now, jobID)
	if err != nil {
		return fmt.Errorf("failed to update worker_heartbeats table: %w", err)
	}

	return tx.Commit()
}

func (s *Store) CompleteJob(ctx context.Context, jobID uuid.UUID, workerID string) error {
	now := time.Now().UTC()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start complete tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = $1, updated_at = $2 WHERE id = $3 AND worker_id = $4`, models.StatusSucceeded, now, jobID, workerID)
	if err != nil {
		return fmt.Errorf("failed to set job status succeeded: %w", err)
	}

	_, err = tx.ExecContext(ctx, `UPDATE worker_heartbeats SET last_seen = $1, current_job = NULL WHERE worker_id = $2`, now, workerID)
	if err != nil {
		return fmt.Errorf("failed to clear current_job on heartbeat: %w", err)
	}

	return tx.Commit()
}

func (s *Store) FailJob(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) error {
	now := time.Now().UTC()
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to start fail job tx: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = $1, updated_at = $2 WHERE id = $3 AND worker_id = $4`, models.StatusFailed, now, jobID, workerID)
	if err != nil {
		return fmt.Errorf("failed to set job status failed: %w", err)
	}

	_, err = tx.ExecContext(ctx, `UPDATE worker_heartbeats SET last_seen = $1, current_job = NULL WHERE worker_id = $2`, now, workerID)
	if err != nil {
		return fmt.Errorf("failed to clear worker heartbeat on fail: %w", err)
	}

	return tx.Commit()
}

// RequeueStuckJobs identifies jobs with expired heartbeats and either requeues them or marks them failed if max attempts reached.
func (s *Store) RequeueStuckJobs(ctx context.Context, heartbeatTimeout time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-heartbeatTimeout)
	now := time.Now().UTC()

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin requeue tx: %w", err)
	}
	defer tx.Rollback()

	// 1. Mark jobs that exceeded max_attempts as failed
	failQuery := `
		UPDATE jobs
		SET status = $1, updated_at = $2
		WHERE status = $3 AND last_heartbeat < $4 AND attempts >= max_attempts
	`
	_, err = tx.ExecContext(ctx, failQuery, models.StatusFailed, now, models.StatusRunning, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to mark exhausted stuck jobs as failed: %w", err)
	}

	// 2. Requeue remaining stuck jobs
	requeueQuery := `
		UPDATE jobs
		SET status = $1, worker_id = NULL, updated_at = $2
		WHERE status = $3 AND last_heartbeat < $4 AND attempts < max_attempts
	`
	res, err := tx.ExecContext(ctx, requeueQuery, models.StatusQueued, now, models.StatusRunning, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to requeue stuck jobs: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to count requeued jobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit requeue tx: %w", err)
	}

	return int(rowsAffected), nil
}

func (s *Store) GetQueueDepth(ctx context.Context) (int, error) {
	var count int
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status = $1`, models.StatusQueued).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count queued jobs: %w", err)
	}
	return count, nil
}

func (s *Store) GetJobByID(ctx context.Context, jobID uuid.UUID) (*models.Job, error) {
	query := `
		SELECT id, idempotency_key, payload, status, priority, attempts, max_attempts, worker_id, last_heartbeat, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`
	var job models.Job
	var workerIDNull sql.NullString
	var heartbeatNull sql.NullTime

	err := s.DB.QueryRowContext(ctx, query, jobID).Scan(
		&job.ID,
		&job.IdempotencyKey,
		&job.Payload,
		&job.Status,
		&job.Priority,
		&job.Attempts,
		&job.MaxAttempts,
		&workerIDNull,
		&heartbeatNull,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get job by id: %w", err)
	}
	if workerIDNull.Valid {
		job.WorkerID = &workerIDNull.String
	}
	if heartbeatNull.Valid {
		job.LastHeartbeat = &heartbeatNull.Time
	}
	return &job, nil
}
