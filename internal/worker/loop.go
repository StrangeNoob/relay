package worker

import (
	"context"
	"log/slog"
	"time"
)

// drainPass runs one bounded pass of a maintenance operation (reap or promote)
// and reports how many items it moved.
type drainPass func(ctx context.Context) (int, error)

// runDrainLoop periodically runs pass until ctx is cancelled, then returns nil.
// Each tick drains fully — it repeats pass until a pass moves nothing — so a
// burst is cleared promptly rather than one batch per interval. Transient errors
// are logged (with name for context) and retried on the next tick rather than
// killing the loop.
func runDrainLoop(ctx context.Context, interval time.Duration, logger *slog.Logger, name string, pass drainPass) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		for {
			n, err := pass(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("relay "+name+": pass failed", "err", err)
				break
			}
			if n == 0 {
				break
			}
		}
		if !wait(ctx, interval) {
			return nil
		}
	}
}
