# Proposal: RIFL state in shared memory (CXL-inspired)

Status: draft for agreement between both team members. Nothing here is
implemented yet.

## 1. Goal and what we claim

CXL lets several machines share one pool of memory. That memory keeps working
when one of the attached machines crashes. CXL-SHM (SOSP 2023) studies how to
manage such memory under partial failure. Our current system has the opposite
weakness: all RIFL state lives in one server's local bbolt file, and the
documented limitation is that only the same server can recover it.

We propose to move the RIFL state into shared memory:

- the counter;
- the saved completion records;
- the acknowledgement boundaries;
- the request and reply channels.

When the server process crashes, a **replacement server process attaches to the
same memory, finishes or discards the interrupted operation, and continues
serving**. Retries of requests that already ran must replay their saved results
and must never run again.

**Model.** One Linux machine. A shared memory region (`/dev/shm`, `mmap
MAP_SHARED`) stands in for a CXL memory device. Separate OS processes stand in
for compute nodes.

**We will claim:**

- exactly-once effects for the counter RPC across server *process* crashes at
  every tested point;
- takeover by a different process without using a disk;
- measured failover time, throughput and latency on this model.

**We will not claim:**

- CXL hardware performance or CXL latency;
- multi-host coherence;
- survival of power loss or machine crash (CXL memory is not persistent);
- generic transactional RPC or full RIFL.

## 2. Three configurations to compare

We compare three configurations so that we can separate the effect of the
transport from the effect of storage:

| | Transport | RIFL state | Survives |
|---|---|---|---|
| A (today) | TCP `net/rpc` | bbolt file, synced | process crash, power loss |
| B | shared-memory slots | bbolt file, synced | process crash, power loss |
| C (main proposal) | shared-memory slots | shared memory | process crash only |

- A vs B shows the cost of TCP.
- B vs C shows the cost of synchronous disk commits.

C is expected to be much faster. The report must state that C buys this speed
with weaker durability, not with a better protocol.

## 3. Shared region layout

One file, `/dev/shm/rifl-<name>`. Its size is fixed when it is created.

```text
Header
  magic, layout version, capacities (slots S, clients C, records per client R)
  owner epoch (uint64), owner pid
Apply log (one entry; the server applies one operation at a time)
Request/reply slots [S]
Client table [C] (open addressing on client ID)
Completion rings [C][R]
Counter value
```

**Request/reply slot.** There is one per client worker, with one request in
flight per slot, matching our synchronous RPC.

- Fields:
  - `state`: EMPTY, REQUEST or REPLY;
  - client ID: at most 64 bytes, and longer IDs are rejected definitively;
  - sequence, delta and `FirstIncomplete`;
  - reply value and error code.
- The client writes the request fields, then stores `state=REQUEST` with an
  atomic release.
- The server polls the slots, processes the request, writes the reply fields,
  then stores `state=REPLY` atomically.
- The client reads the reply and resets the slot to EMPTY.

**Client table entry.** Each entry holds the client ID and its durable
`FirstIncomplete` boundary. Entries are never deleted, which matches today's
rule that boundaries remain until leases exist.

**Completion ring.** Each client has R fixed records: sequence, delta, reply
value and error code.

- Sequence `s` lives in index `s mod R`. The record is valid only if its stored
  sequence equals `s`.
- A request whose sequence is at least `boundary + R` would overwrite an
  unacknowledged record. The server refuses it **before executing it**, with a
  retryable "table full" error. The client treats that like an ambiguous
  failure and retries the same ID later.
- We never evict an unacknowledged record. A definitive refusal would be unsafe:
  a delayed duplicate could still execute after the client had already accepted
  the error.

The capacities S, C and R are set at creation:

- 1,024 clients × R=64 needs about 2 MB of completion records;
- the batch test with 4 clients × R=16,384 also needs about 2 MB.

## 4. Atomic updates and crash recovery

bbolt gave us one transaction for the mutation, the completion record and the
acknowledgement. In shared memory we build that ourselves with a one-entry redo
log. The server is the only writer, so one entry is enough.

1. **Prepare.** Write the *absolute* new values into the log entry, with
   `state=PREPARED`:
   - the new counter value;
   - the completion record (client, sequence, delta, reply, error);
   - the new boundary and the range of ring indices to clear;
   - the owner epoch.
2. **Commit point.** Atomically store `state=COMMITTED`. This single store is
   what makes the operation happen.
3. **Apply.** Write the values into the counter, the client table and the ring.
   Then store `state=APPLIED`.
4. **Reply.** Write the reply slot.

**Recovery by a new owner:**

- **PREPARED:** discard it. Nothing became visible.
- **COMMITTED:** apply it again. Every value in the entry is absolute, so
  re-applying it is idempotent.
- **APPLIED:** nothing to do.

Then the new owner scans the slots and processes every slot still in REQUEST.
A request whose reply was lost in the crash finds its completion record and
gets the saved result. This is the same lost-response guarantee we test today.

Test-only hooks will kill the server at each step: before the commit point,
after commit but before apply, during apply, and after apply but before the
reply. This matches the existing before-commit and after-commit subprocess
tests.

**Ordering assumption.** Within one machine, Go `sync/atomic` operations on
`MAP_SHARED` memory are coherent between processes. Field writes before the
release store are visible after an acquire load of `state`. On real
multi-host CXL, coherence and flush behaviour depend on the hardware, so this is
a modelling assumption that we state rather than prove.

## 5. Ownership and takeover

Only one server may write at a time. Two writers would corrupt the state.

- The server takes an exclusive `flock` on the region file. The kernel releases
  it when the process dies, which gives an exact failure detector on one
  machine. A standby process blocks on the same lock, takes over as soon as it
  gets it, increments the owner epoch, runs recovery and starts serving.
- The epoch is stored in each log entry. This makes it visible in recovery
  which owner prepared an interrupted operation.
- Across real CXL hosts, a lock like this would not exist. A lease with fencing
  would be needed instead, and we list that as future work.

Takeover does not need a disk, restart scripts or a client reconnect. Clients
keep their slots and only see a delay.

## 6. Experiments

1. **Correctness:**
   - kill the server at every hook point and take over, then check that there
     are zero duplicate effects and that every retry gets its original reply;
   - the lost-response test;
   - blocked gaps, stale requests below the boundary, and a full ring returning
     the retryable error.
2. **Failover time:** kill the owner under load and measure how long until the
   standby serves again. Compare this with restarting configuration A on the
   same bbolt file.
3. **Throughput and latency** for A, B and C:
   - 4 and 1,024 clients, and a batch acknowledgement, using the same workloads
     as the CloudLab runs;
   - one machine for all three configurations, because C cannot span hosts in
     our model. The A numbers are therefore loopback numbers and are labelled
     as such.
4. **Memory footprint** as a function of clients and R.

## 7. Implementation plan

The work is Linux-only and needs no new dependencies (`syscall.Mmap`,
`syscall.Flock`, `sync/atomic`).

1. `internal/shm`: create and attach the region, the layout and header
   validation, with unit tests.
2. Slots plus the bbolt `PersistentCounter` behind them. This is configuration
   B, and it checks the transport on its own.
3. The client table, completion rings, redo log and recovery. This is
   configuration C. Add crash-point subprocess tests.
4. Standby process, takeover, and the failover-time measurement.
5. Benchmark support for the `shm` transport, then the experiments and report.

We would merge after each step, with tests passing.

## 8. Decisions to agree on

1. Should we include configuration B? We recommend yes. It is cheap, and
   without it the A-vs-C comparison mixes up two effects.
2. Should the server poll the slots, or block on a futex? We recommend polling
   with backoff first because it is simpler, and adding futex only if CPU use
   distorts the results.
3. Capacities: 64-byte client IDs and R chosen per experiment, both recorded in
   the results.
4. Client process crashes: reclaiming a dead client's slots would be out of
   scope at first, and documented as a limitation.
