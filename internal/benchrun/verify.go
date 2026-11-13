package benchrun

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"

	riflrpc "github.com/tsembp/rifl-go"
)

// VerifyBatchAck checks the server after a completed batch-ack run without
// mutating its counter: every acknowledged sequence of each worker's client must
// be rejected as stale, and retrying each worker's final (acknowledging) request
// must replay its saved result instead of applying it again.
//
// cfg must be the configuration that produced the run, including ClientPrefix.
func VerifyBatchAck(ctx context.Context, cfg Config) error {
	if cfg.Scenario != ScenarioBatchAck || cfg.Implementation != "protected" {
		return errors.New("batch-ack verification requires the protected batch-ack scenario")
	}
	before, err := stats(ctx, cfg.Address, cfg.Timeout)
	if err != nil {
		return fmt.Errorf("read stats: %w", err)
	}
	if before.CompletionRecords != cfg.Concurrency {
		return fmt.Errorf("expected %d retained completion records, found %d", cfg.Concurrency, before.CompletionRecords)
	}
	for worker := 0; worker < cfg.Concurrency; worker++ {
		clientID := fmt.Sprintf("%s-%d", cfg.ClientPrefix, worker)
		acked := uint64(workerRequestCount(cfg.Requests, cfg.Concurrency, worker))
		final := acked + 1
		for _, sequence := range []uint64{1, acked} {
			_, err := add(ctx, cfg.Address, cfg.Timeout, riflrpc.AddRequest{
				ID:              riflrpc.RequestID{ClientID: clientID, Sequence: sequence},
				Delta:           cfg.Delta,
				FirstIncomplete: final,
			})
			var serverError rpc.ServerError
			if !errors.As(err, &serverError) || string(serverError) != "stale request" {
				return fmt.Errorf("client %s sequence %d: expected stale rejection, got %v", clientID, sequence, err)
			}
		}
		if _, err := add(ctx, cfg.Address, cfg.Timeout, riflrpc.AddRequest{
			ID:              riflrpc.RequestID{ClientID: clientID, Sequence: final},
			Delta:           cfg.Delta,
			FirstIncomplete: final,
		}); err != nil {
			return fmt.Errorf("client %s final sequence %d: expected saved result, got %v", clientID, final, err)
		}
	}
	after, err := stats(ctx, cfg.Address, cfg.Timeout)
	if err != nil {
		return fmt.Errorf("read stats: %w", err)
	}
	if after.Value != before.Value || after.CompletionRecords != before.CompletionRecords {
		return fmt.Errorf("verification changed server state: value %d->%d, completions %d->%d",
			before.Value, after.Value, before.CompletionRecords, after.CompletionRecords)
	}
	return nil
}
