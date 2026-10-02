# Development Guide — Low-Latency Trading System

This document is the technical source of context for developers and AI coding agents working on the project. Read `README.md` first for the project purpose and scope.

## 1. Engineering Goal

Build a small but credible distributed electronic trading system in **Go**, then use it as an experimental platform for studying latency, throughput, resource usage, and the effects of communication/deployment choices.

This is a **datacenter/distributed-systems project**, not a quantitative-finance project. Prioritize correctness, instrumentation, reproducibility, networking, concurrency, and performance analysis over trading features.

## 2. Scope Rules

### Core functionality

Implement:

- synthetic workload generation;
- multiple concurrent logical clients;
- order gateway;
- matching engine;
- in-memory limit order book;
- LIMIT BUY;
- LIMIT SELL;
- CANCEL;
- acknowledgements/rejections;
- execution/trade events where matching occurs;
- metrics and timestamps;
- repeatable benchmark scenarios.

### Explicitly out of scope for the core project

Do not spend core project time implementing:

- real-money trading;
- connectivity to real exchanges or brokers;
- profitable trading strategies;
- prediction/ML models;
- options/futures/derivatives;
- margin systems;
- portfolio management;
- P&L dashboards;
- authentication/user accounts;
- production-grade exchange compliance;
- complex frontend/UI.

These do not answer the primary CS452 research question.

## 3. Proposed Architecture

```text
+----------------------+       +----------------+       +------------------+
| Workload Generator / | ----> | Order Gateway  | ----> | Matching Engine  |
| Simulated Clients    | <---- |                | <---- | + Order Book     |
+----------------------+       +----------------+       +------------------+
          |                           |                         |
          +---------------------------+-------------------------+
                                      |
                                  Metrics
```

Each major component should be independently runnable so components can later be placed on separate machines.

Avoid unnecessary microservices. Three primary runtime roles are sufficient.

## 4. Suggested Repository Structure

```text
low-latency-trading-system/
├── README.md
├── DEVELOPMENT.md
├── go.mod
├── go.sum
├── cmd/
│   ├── gateway/
│   │   └── main.go
│   ├── matching-engine/
│   │   └── main.go
│   └── loadgen/
│       └── main.go
├── internal/
│   ├── book/
│   ├── matching/
│   ├── gateway/
│   ├── protocol/
│   ├── metrics/
│   └── workload/
├── pkg/
│   └── ...                 # only if reusable public packages are justified
├── configs/
├── scripts/
├── experiments/
│   ├── configs/
│   ├── raw/
│   └── results/
├── deployments/
│   └── ...
└── tests/
```

Prefer `internal/` packages unless code genuinely needs to be imported externally.

## 5. Domain Model

Keep the financial model deliberately small.

### Order

Conceptually:

```go
type Side uint8

const (
    Buy Side = iota + 1
    Sell
)

type Order struct {
    ID        uint64
    ClientID  uint64
    Symbol    string
    Side      Side
    Price     int64
    Quantity  uint64
    Timestamp int64
}
```

Use integer/fixed-point prices rather than `float64`. For example, `$200.03` can be represented as `20003` cents or using another documented fixed precision.

### Core messages

At minimum define semantics for:

- NewOrder
- CancelOrder
- Ack
- Reject
- Execution

Keep transport-independent domain structures separate from HTTP/JSON or binary wire representations where practical.

## 6. Order Book and Matching Engine

### Matching policy

Use **price-time priority**.

For BUY orders:

- highest bid has priority;
- among orders at the same price, oldest order has priority.

For SELL orders:

- lowest ask has priority;
- among orders at the same price, oldest order has priority.

A new BUY can match resting SELL orders when:

```text
buy.price >= bestAsk
```

A new SELL can match resting BUY orders when:

```text
sell.price <= bestBid
```

Support partial fills if implementation time permits; they are recommended because they make the matching engine realistic without dramatically increasing scope.

### Correctness before optimization

The first implementation should optimize for:

1. deterministic behavior;
2. correctness;
3. testability;
4. understandable code.

Only optimize after profiling identifies a relevant bottleneck.

Do not introduce lock-free structures or complicated concurrency solely because the project concerns low latency.

## 7. Concurrency Model

A strong initial design is to give the matching engine a single authoritative event-processing loop.

```text
Network readers
     |
     v
 inbound queue
     |
     v
MATCHING ENGINE
(single ordered processor)
     |
     v
 outbound results
```

This avoids concurrent mutation of one order book and makes price-time ordering easier to reason about.

Concurrency can still exist in:

- network handling;
- multiple client connections;
- workload generation;
- gateway processing;
- metrics collection;
- separate symbols later if desired.

Do not prematurely shard the matching engine.

## 8. Baseline and Optimized Configurations

The exact comparison should remain configurable until the baseline works and initial measurements are available.

### Architecture / Configuration A — Baseline

Candidate:

```text
Client
  |
HTTP + JSON
  v
Gateway
  |
HTTP + JSON
  v
Matching Engine
```

Benefits:

- easy to implement;
- easy to inspect/debug;
- clear baseline.

### Architecture / Configuration B — Optimized

Candidate:

```text
Client
  |
persistent TCP + compact/binary encoding
  v
Gateway
  |
persistent TCP + compact/binary encoding
  v
Matching Engine
```

Possible serialization options include a small custom fixed binary representation or an established encoding such as Protocol Buffers. Choose based on implementation effort and experimental clarity.

Do not change many unrelated variables simultaneously if the goal is to attribute performance differences to a specific design choice. If both transport and serialization change together, describe the comparison as an architectural/configuration comparison rather than claiming one individual factor caused the result.

## 9. Workload Generator

The load generator is part of the experimental infrastructure and should be treated as a first-class component.

It should support configuration such as:

```text
clients = 50
order_rate = 10000/s
duration = 60s
symbol_count = 1
cancel_ratio = 0.10
buy_ratio = 0.50
seed = 12345
```

Use seeded pseudo-random generation where randomness is required so experiments can be reproduced.

### Workload modes

Implement at least:

#### Constant load

```text
5k -> 5k -> 5k -> 5k orders/s
```

#### Increasing load

Run independent experiments at increasing offered rates until the system approaches/surpasses saturation.

#### Burst load

Example:

```text
5k/s -> 5k/s -> 40k/s -> 40k/s -> 5k/s
```

This is useful for observing queues and tail-latency spikes.

Do not hard-code the example rates. Determine meaningful values empirically for the actual deployment hardware.

## 10. Metrics

### Required system metrics

Collect at least:

- offered request rate;
- successfully processed throughput;
- request count;
- rejection/error count;
- p50 end-to-end latency;
- p95 end-to-end latency;
- p99 end-to-end latency;
- CPU utilization;
- memory usage.

Optionally collect p99.9 when experiments contain enough observations for it to be meaningful.

### Timing points

Useful conceptual timestamps:

```text
T0 client creates/sends request
T1 gateway receives request
T2 matching engine receives request
T3 matching engine completes processing
T4 client receives response
```

Derived metrics:

```text
end_to_end       = T4 - T0
matching_service = T3 - T2
```

Intermediate timings must be interpreted carefully across separate hosts because unsynchronized clocks can corrupt one-way latency measurements. Prefer end-to-end measurements from the same client clock unless host clocks are demonstrably synchronized.

### Percentiles

Do not report only average latency. Tail latency is a central part of the project.

At minimum report:

```text
p50
p95
p99
```

Keep raw or histogrammed measurements sufficient to reproduce reported percentiles.

## 11. Experiment Methodology

Experiments should be repeatable and controlled.

For every benchmark configuration record:

- Git commit/version;
- architecture/configuration;
- machine/node allocation;
- hardware details where available;
- number of clients;
- offered order rate;
- duration;
- workload seed;
- protocol/serialization;
- CPU/memory settings if relevant;
- raw results.

### Suggested experiment procedure

1. Start all system components.
2. Verify health/readiness.
3. Run a short warm-up where appropriate.
4. Run the configured workload for a fixed duration.
5. Persist metrics/results.
6. Stop/reset the system.
7. Repeat the experiment multiple times.
8. Analyze variability before drawing conclusions.

Do not compare two architectures using different workload definitions unless that difference is intentional and documented.

## 12. Deployment Strategy

### Development

Run all components locally as independent processes first.

### Integration

Optionally containerize components once local execution is stable.

### Final experimental deployment

If suitable course/CloudLab infrastructure is available:

```text
Node 1
  loadgen

Node 2
  gateway

Node 3
  matching-engine
```

Two nodes are also sufficient if only two are available; for example, run the workload generator on one and gateway + matching engine on the other. The important requirement is that the experiment and topology are documented.

Do not make multi-node deployment a blocker for implementing and testing the system locally.

## 13. Testing Strategy

### Unit tests

Order-book tests should cover at least:

- insert buy;
- insert sell;
- best bid/ask;
- price priority;
- FIFO/time priority at equal prices;
- full match;
- partial fill;
- no match;
- cancellation;
- cancellation of unknown order;
- multiple matches across price levels.

### Integration tests

Test complete paths such as:

```text
client -> gateway -> matching engine -> response
```

Validate both successful and malformed/invalid requests.

### Performance tests

Performance benchmarks must be separated conceptually from correctness tests. A benchmark that runs quickly is not evidence that matching behavior is correct.

Use Go's race detector during development where applicable.

## 14. Logging

Logging must not invalidate performance experiments.

During development, detailed logs are acceptable.

During benchmarks:

- disable verbose per-order logging;
- avoid synchronous disk writes in the hot path;
- record aggregate/histogram metrics efficiently;
- document the logging/metrics configuration used.

## 15. Profiling and Optimization

Go tooling such as benchmarks and `pprof` can be used to investigate:

- CPU hotspots;
- allocations;
- memory usage;
- blocking;
- goroutine behavior.

Optimization process:

```text
Measure -> Profile -> Identify bottleneck -> Change -> Measure again
```

Do not optimize based only on intuition.

Potential later experiments include:

- persistent vs non-persistent connections;
- JSON vs binary serialization;
- batching;
- queue size/backpressure behavior;
- allocation reduction;
- CPU affinity where infrastructure permits;
- single-node vs multi-node deployment.

These are optional until the core system works.

## 16. Development Phases

### Phase 1 — Core domain

- repository setup;
- message/domain definitions;
- order book;
- matching engine;
- comprehensive unit tests.

### Phase 2 — Baseline distributed system

- gateway;
- baseline protocol;
- client/load generator;
- end-to-end integration tests.

At this stage the complete path must work before optimization begins.

### Phase 3 — Instrumentation

- timestamps;
- latency recording;
- throughput measurement;
- resource monitoring;
- reproducible experiment configuration.

### Phase 4 — Baseline experiments

- determine sustainable load range;
- identify saturation point;
- profile bottlenecks;
- establish baseline results.

### Phase 5 — Optimized configuration

- implement the selected alternative communication/configuration design;
- maintain equivalent order semantics;
- validate correctness;
- benchmark using the same workloads.

### Phase 6 — Multi-node experiments and analysis

- deploy across available nodes;
- run repeated experiments;
- analyze p50/p95/p99 latency and throughput;
- produce plots/tables;
- explain observed trade-offs.

## 17. Minimum, Target, and Stretch Deliverables

### Minimum viable project

Must work even if later optimization work runs into problems:

- matching engine;
- order book;
- gateway;
- load generator;
- concurrent clients;
- one complete distributed architecture;
- metrics;
- multiple workload levels;
- latency/throughput analysis.

### Target project

Everything above plus:

- two communication/configuration approaches;
- controlled comparison;
- burst experiments;
- CPU and memory analysis;
- multi-node deployment where available.

### Stretch goals

Only after the target project is stable:

- additional serialization comparison;
- backpressure strategies;
- batching;
- CPU pinning/affinity;
- multiple symbols/sharding;
- more detailed tracing;
- additional deployment topologies.

## 18. Team Parallelization

A possible initial ownership split is:

### Developer A

- order book;
- matching engine;
- protocol/network implementation;
- optimized configuration.

### Developer B

- workload generator;
- simulated clients;
- metrics/experiment harness;
- result collection.

Both developers should jointly handle:

- architecture decisions;
- integration;
- deployment;
- experiments;
- analysis;
- course deliverables.

Ownership does not imply exclusive knowledge. Both developers should be able to explain the complete system for the final presentation.

## 19. Rules for AI Coding Agents

AI agents working in this repository should follow these rules:

1. Read `README.md` and this document before proposing architectural changes.
2. Preserve the project's CS452/datacenter focus.
3. Do not introduce real trading APIs, financial prediction, or unnecessary finance features.
4. Prefer simple, idiomatic Go over clever abstractions.
5. Do not add dependencies without a concrete reason.
6. Keep the hot path understandable and measurable.
7. Preserve deterministic matching semantics.
8. Add/update tests for behavior changes.
9. Do not silently change benchmark methodology.
10. Avoid optimizing before measurement/profiling demonstrates a need.
11. Keep experiments reproducible and configurable rather than hard-coded.
12. Treat generated benchmark numbers as invalid unless they come from an actual recorded run.
13. Clearly distinguish measured results from hypothetical/example numbers in documentation.
14. Keep components independently runnable to preserve multi-node deployment capability.
15. When uncertain about scope, prefer the smallest implementation that answers the research question.

## 20. Definition of Done

The project is technically successful when we can reproducibly answer questions such as:

> How does the system's throughput and p99 end-to-end latency change as offered order rate increases?

and:

> Under equivalent workloads, how do the selected baseline and optimized communication/deployment configurations differ in latency, throughput, and resource usage?

The final value of the project comes from the combination of a working distributed system **and defensible experimental evidence**, not from the number of trading features implemented.
