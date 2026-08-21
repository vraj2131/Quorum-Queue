package reaper

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMultiShardReaper_StartStop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := Config{
		ScanInterval:     50 * time.Millisecond,
		HeartbeatTimeout: 100 * time.Millisecond,
	}

	msr := NewMultiShardReaper(cfg, nil, nil, logger)
	msr.Start()
	time.Sleep(100 * time.Millisecond)
	msr.Stop()
}
