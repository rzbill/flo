# Flo

Build durable, event‑driven systems by importing a package—not by adopting a platform.

Flo is a library‑first, single‑binary runtime you embed in your service. Your code gets hard guarantees—replayable logs, leased queues, retries/DLQ—without spinning up a new cluster. When you need multi‑process or non‑Go clients, flip on the optional thin gRPC/REST server. Same semantics, no rewrite.

Flo provides two core building blocks with durable semantics:
- EventLog: append‑only, replayable logs with cursors, blocking reads, and retention trims
- WorkQueue: leased task queues with priority, delay/retry, DLQ, and backpressure

Designed to embed in Go processes by default with local durability, plus an optional thin gRPC/REST server for multi‑process access and SDK parity.

## Why Flo?
- Built‑in, not bolted‑on: add durable logs/queues to your app with a function call
- No ceremony: a single binary you can run locally and ship anywhere
- Strong guarantees, simple API: at‑least‑once, strict ordering, blocking reads, retries/DLQ
- Evolve gradually: start embedded; enable a thin server for SDKs when you need it

## Project status
MVP (Milestone M1) is in progress. Highlights implemented so far:
- EventLog: keys/encoding, Append, Read/Seek, blocking reads, cursors, trims, archiver hook
- WorkQueue: keys/encoding, Enqueue (priority/delay), Dequeue with leases, ExtendLease, Complete, Fail (retry/DLQ), basic backpressure, expired‑lease reclaim + background sweeper
- Tooling/CI and protobuf toolchain are set up; public protos limited to Channels/Admin/Health (facades); EventLog/WorkQueue are internal‑only


## Quickstart
Prereqs: Go 1.23+

```bash
git clone https://github.com/rzbill/flo
cd flo
make test     # run unit tests
make build    # build all packages
```

Minimal embed example (pseudo‑usage):
```go
// Open an embedded queue and enqueue then dequeue
q, _ := workqueue.OpenQueue(db, "ns", "payments", 1)
seq, _ := q.Enqueue(ctx, nil, []byte("hello"), 5 /*priority*/, 0 /*delayMs*/, 0)
msgs, _ := q.Dequeue(ctx, "workers", 1, 30_000 /*leaseMs*/, 0)
_ = q.Complete(ctx, "workers", []uint64{seq})
```

## Repository layout (selected)
- `internal/eventlog/` — EventLog core (keys/encoding, append/read/seek, cursors, trims)
- `internal/workqueue/` — WorkQueue core (keys/encoding, enqueue/dequeue, leases, retry/DLQ)
- `internal/storage/pebble/` — Pebble wrapper (options, batches, snapshots)
- `proto/flo/v1/` — Public service surfaces (Channels/Admin/Health)

## Contributing
Issues and PRs are welcome. Please run tests locally:
```bash
make test
```
Follow conventional commits and include context in PR descriptions.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 