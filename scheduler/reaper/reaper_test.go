package reaper

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestReaper_StartStop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := Config{
		ScanInterval:     50 * time.Millisecond,
		HeartbeatTimeout: 100 * time.Millisecond,
	}

	r := NewReaper(cfg, nil, nil, logger)
	r.Start()
	time.Sleep(100 * time.Millisecond)
	r.Stop()
}
