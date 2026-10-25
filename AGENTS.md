# Contributor and AI-agent context

Read README.md first. This is a small CS452 experiment adapting RIFL ideas to Go
net/rpc. The former trading-system design is superseded. Financial operations
may become workloads; an exchange, order book, UI, or real trading integration is
outside the current implementation.

## Current design

- The root package is riflrpc; rpc.go holds wire types and the counter service.
- client.go handles logical IDs, per-attempt deadlines, retries and reconnection.
- cmd/server and cmd/client are minimal runnable examples.
- Counter uses one mutex for state mutation and completion recording. Deduplication
  state belongs to the service, not the TCP connection.
- Replies are saved per logical request. Do not substitute the current counter
  value when replaying an old request.
- Transport failures are ambiguous and retryable; rpc.ServerError is a definitive
  application reply and must not be retried indefinitely.
- A timed-out request may still execute. Never allocate a new sequence on retry.
- Each attempt has its own connection and reply variable. Wait for decoder
  completion before returning from a cancelled attempt.

## Scope and correctness

The implementation is in-memory only. Do not claim restart recovery, durable
exactly-once execution, generic transactional RPC, bounded metadata, or full RIFL
support. Keep improvements small and explain the failure model in documentation.
Do not silently evict completion records or introduce TTL cleanup: a late retry
could execute again. Future persistence must commit the application mutation and
completion result atomically, with crash-point tests before claiming recovery.

The lost-response test wraps a server connection and fails its response write
after net/rpc has executed the handler. Keep this test exercising a real RPC
connection, not just invoking the counter directly.

## Verification and workflow

Use gofmt on changed Go files, then run:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Preserve user changes. Do not commit, push, add dependencies, expand into shared
memory, or add financial-service complexity without a corresponding request.
Treat loopback TCP tests as protocol correctness checks, not CXL measurements
or performance claims. Check course rules with the team before preparing graded
submission material.
