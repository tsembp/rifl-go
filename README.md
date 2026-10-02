# Low-Latency Trading System

A distributed electronic trading system built in Go to study how system architecture and communication choices affect latency, throughput, and resource usage under realistic and bursty workloads.

## Project Context

This project is developed for **ΕΠΛ452 / CS452 — Datacenter Computing** at the University of Cyprus by a team of two students.

The course project asks teams to apply datacenter-computing concepts to a real-world problem. This project follows the **System Development and Optimization** direction: we build a distributed system, benchmark it under controlled workloads, and evaluate architectural optimizations.

This is an educational simulation. It does not connect to a real exchange, trade real money, or attempt to build a profitable trading strategy.

## Problem

Electronic trading systems must process large numbers of orders while keeping latency—especially tail latency—low and predictable. Communication protocols, serialization, service boundaries, deployment, contention, and workload bursts can all affect performance.

The project investigates these effects using a simplified electronic exchange that we can fully control, instrument, deploy, and benchmark.

## Main Research Question

**How do communication and deployment choices affect throughput and tail latency in a distributed electronic trading system under different workload intensities?**

## Objectives

1. Build a functional electronic trading system in Go.
2. Implement a simplified limit order book and matching engine.
3. Generate deterministic and configurable synthetic trading workloads.
4. Run the system as multiple communicating components/services.
5. Implement and compare at least two communication/configuration approaches.
6. Measure end-to-end latency, internal component latency, throughput, CPU usage, and memory usage.
7. Study system behavior under increasing load and sudden traffic bursts.
8. Identify bottlenecks and explain the performance trade-offs observed experimentally.

## System Overview

The initial system consists of four logical components:

```text
Market / Workload Generator
          |
          v
   Simulated Clients
          |
          | orders
          v
     Order Gateway
          |
          v
    Matching Engine
     + Order Book
          |
          v
 Execution / ACK Response
```

### Workload Generator / Simulated Clients

Synthetic clients generate orders at controlled rates. The workload can vary by number of clients, orders per second, order distribution, and burst behavior.

No real market data is required. Synthetic market/order data gives us reproducible experiments and precise control over system load.

### Order Gateway

The gateway accepts client requests, validates/parses messages, forwards valid orders to the matching engine, and returns acknowledgements or execution results.

### Matching Engine

The matching engine maintains an in-memory limit order book and processes orders using price-time priority.

The initial supported operations are intentionally limited to:

- LIMIT BUY
- LIMIT SELL
- CANCEL

Features such as derivatives, portfolios, margin, P&L, real brokerage connectivity, and sophisticated trading strategies are outside the core scope.

## Architecture Comparison

The project will maintain a simple baseline and an optimized communication configuration. The exact optimized protocol may evolve after initial implementation and profiling.

A likely baseline is:

```text
Clients -> HTTP/JSON -> Gateway -> HTTP/JSON -> Matching Engine
```

A likely optimized configuration is:

```text
Clients -> persistent TCP/binary protocol -> Gateway -> persistent TCP/binary protocol -> Matching Engine
```

The purpose is not to assume that one approach is better, but to measure the behavior of both under controlled conditions.

## Experimental Evaluation

Experiments will vary workload intensity, for example:

```text
1,000 orders/s
5,000 orders/s
10,000 orders/s
25,000 orders/s
50,000 orders/s
```

Actual ranges will be selected based on the capacity of the available hardware so that experiments cover both normal operation and saturation.

We will also introduce burst workloads that emulate sudden increases in trading activity.

### Primary Metrics

- successful throughput (orders/second)
- p50 latency
- p95 latency
- p99 latency
- optionally p99.9 latency where sample size permits
- CPU utilization
- memory usage
- error/rejection rate

Where useful, requests will be timestamped at several stages so that end-to-end latency can be separated from gateway and matching-engine processing time.

## Deployment

During development, all components may run locally as separate processes or containers.

For final experiments, we aim to deploy components across multiple nodes when course infrastructure permits, for example:

```text
Node 1: Workload Generator / Clients
Node 2: Order Gateway
Node 3: Matching Engine
```

The project does not require one physical machine per simulated trader; many logical clients can be generated from a small number of machines.

## Technology

The primary implementation language is **Go**.

Go is suitable for the project because it provides straightforward networking, concurrency, profiling, testing, and deployment as standalone binaries while keeping the implementation manageable for a two-person course project.

Supporting tooling may include Docker/containers, Linux performance tools, Go benchmarks/profiling, and experiment-analysis scripts where appropriate.

## Scope and Success Criteria

The minimum successful project consists of:

- a correct basic matching engine and order book;
- synthetic concurrent clients;
- a working distributed request path;
- a reproducible workload generator;
- instrumentation and metrics collection;
- controlled performance experiments;
- analysis of latency and throughput under multiple loads.

The target version additionally compares two communication/configuration approaches and runs experiments across multiple machines.

Stretch work may investigate CPU affinity, alternative serialization, queueing/backpressure, additional deployment configurations, or other optimizations identified through profiling.

The project deliberately prioritizes **systems engineering and experimental analysis over financial complexity**.

## Course Deliverables

According to the CS452 project specification, the formal milestones are:

- Project proposal — end of Week 5
- Midterm status report — end of Week 9
- Final presentation — Week 13

The proposal must clearly define the problem, objectives, achievable scope, methodology, rough timeline, and team responsibilities.

## Repository Documentation

This `README.md` provides the high-level project definition and should remain understandable to instructors, contributors, recruiters, and AI development agents.

Detailed architecture, engineering conventions, protocols, benchmarking rules, and implementation guidance are maintained in [`DEVELOPMENT.md`](DEVELOPMENT.md).
