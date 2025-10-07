# WorkQueue CLI Commands

Complete command-line interface for Flo WorkQueues operations.

---

## Overview

The WorkQueue CLI provides 17 commands for managing work queues, messages, consumers, and administrative operations. All commands use gRPC transport for direct communication with the Flo server.

**Message Lifecycle States**:
```
Ready → [Dequeue] → Pending → [Complete] → Completed
                       ↓ (fail after retries)
                      DLQ
```

**Usage**:
```bash
flo workqueue [command] [flags]
# Alias: flo wq [command] [flags]
```

**Environment Variables**:
- `FLO_GRPC` - gRPC server address (default: `127.0.0.1:50051`)

---

## Commands

### Queue Management

#### `create`
Create a new work queue.

```bash
flo workqueue create --name payments --partitions 32
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `--partitions` - Number of partitions (default: `16`)

---

### Message Operations

#### `enqueue`
Add a message to the work queue.

```bash
flo workqueue enqueue --name payments --data '{"amount":100}' --priority 5
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `--data` - Payload data (required)
- `--key` - Partitioning key (optional)
- `--priority` - Message priority - higher = processed first (default: `0`)
- `--delay-ms` - Delay before message is available (default: `0`)
- `--header` - Message header key=value (repeatable)
- `--header-json` - Headers as JSON object

**Output**:
```json
{
  "status": "OK",
  "id": "AAECAwQFBgcICQ=="
}
```

---

#### `dequeue`
Dequeue messages from the queue (worker mode).

```bash
flo workqueue dequeue --name payments --group workers --count 5 --auto-ack
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (default: `default`)
- `--consumer-id` - Consumer ID (auto-generated if empty)
- `--count` - Number of messages to dequeue at once (default: `1`)
- `--block-ms` - Block time in milliseconds (default: `5000`)
- `--lease-ms` - Lease duration in milliseconds (default: `30000`)
- `--auto-ack` - Automatically acknowledge messages (default: `false`)

**Output** (streaming):
```json
{
  "id_b64": "AAECAwQFBgcICQ==",
  "payload_json": {"amount": 100},
  "partition": 3,
  "delivery_count": 1,
  "lease_expires_at_ms": 1234567890123,
  "enqueued_at_ms": 1234567860123,
  "headers": {"retry": "1"}
}
```

**Notes**:
- Runs continuously until Ctrl+C
- Auto-generates consumer ID if not provided
- Automatically acknowledges if `--auto-ack` is set
- Supports JSON, text, and base64 payload decoding

---

#### `complete`
Mark a message as successfully processed.

```bash
flo workqueue complete --name payments --group workers --id AAECAwQFBgcICQ==
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--id` - Message ID in base64 (required)

---

#### `fail`
Mark a message as failed (will retry if policy allows).

```bash
flo workqueue fail --name payments --group workers --id AAECAwQFBgcICQ== \
  --error "Payment gateway timeout" --retry-after-ms 60000
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--id` - Message ID in base64 (required)
- `--error` - Error message
- `--retry-after-ms` - Retry delay override in milliseconds

---

#### `extend-lease`
Extend the lease on a message to prevent it from being reclaimed.

```bash
flo workqueue extend-lease --name payments --group workers \
  --id AAECAwQFBgcICQ== --extension-ms 30000
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--id` - Message ID in base64 (required)
- `--extension-ms` - Additional lease time in milliseconds (default: `30000`)

**Output**:
```json
{
  "status": "OK",
  "new_expires_at_ms": 1234567920123
}
```

---

### State Queries

#### `ready`
List messages waiting in the queue (ready to be dequeued).

```bash
flo workqueue ready --name payments --limit 50
```

**Aliases**: `queued`, `waiting`, `messages`, `msgs`

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `--partition` - Partition to query (default: `0`)
- `--limit` - Max entries to return (default: `100`)
- `--include-payload` - Include message payload in output (default: `false`)

**Output**:
```json
{"id_b64":"AAECAwQFBgcICQ==","seq":123,"partition":0,"priority":10,"payload_size":256}
```

**Description**: Shows messages in the priority queue, ordered by priority (lower values first), waiting to be dequeued by consumers.

---

#### `pending`
List messages currently being processed (in-flight).

```bash
flo workqueue pending --name payments --group workers --limit 50
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--consumer-id` - Filter by consumer ID (optional)
- `--limit` - Max items to return (default: `100`)

**Output**:
```json
{
  "entries": [
    {
      "id": "AAECAwQFBgcICQ==",
      "consumer_id": "worker-01",
      "delivery_count": 2,
      "last_delivery_ms": 1234567890123,
      "lease_expires_at_ms": 1234567920123,
      "idle_ms": 5000
    }
  ]
}
```

**Description**: Shows messages in the Pending Entries List (PEL), leased to specific consumers with active leases.

---

#### `completed`
List recently completed messages (last 1K per partition).

```bash
flo workqueue completed --name payments --group workers --limit 100
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `--group` - Consumer group filter (optional)
- `--partition` - Partition to query (default: `0`)
- `--limit` - Max entries to return (default: `100`)

**Output**:
```json
{
  "entries": [
    {
      "id": "AAECAwQFBgcICQ==",
      "seq": 123,
      "consumer_id": "worker-01",
      "completed_at_ms": 1234567890123,
      "duration_ms": 5420,
      "delivery_count": 1
    }
  ]
}
```

**Description**: Shows recently completed messages with execution metadata. Kept in a circular buffer (max 1K per partition, 24h retention).

---

#### `list`
Unified view across multiple states.

```bash
# Show counts for all states
flo workqueue list --name payments --group workers --all

# Show specific states
flo workqueue list --name payments --state ready,pending

# Power user mode
flo workqueue list --name payments --state all
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group filter (default: `default`)
- `--state` - State(s) to show: `ready`, `pending`, `completed`, `dlq` (comma-separated)
- `--all` - Show counts for all states (summary view)
- `--partition` - Partition to query (default: `0`)
- `--limit` - Max entries per state (default: `100`)

**Output (--all)**:
```
WorkQueue: payments (namespace: default)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  🟢 Ready:     42 messages (waiting to be dequeued)
  🟡 Pending:   12 messages (being processed)
  🔵 Completed: 856 messages (recently finished)
  🔴 DLQ:       3 messages (failed)

Total visible: 913 messages

Lifecycle: Ready → Pending → Completed (or DLQ if failed)
```

**Output (--state ready,pending)**:
```
State: ready
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{"id_b64":"...","seq":123,"priority":10}
{"id_b64":"...","seq":124,"priority":20}

State: pending
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{"id":"...","consumer_id":"worker-01","idle_ms":5000}
```

**Description**: Powerful unified command for viewing messages across multiple states. Use `--all` for quick overview, or `--state` for detailed inspection.

**State Aliases**:
- `ready` → `queued`, `waiting`
- `pending` → `inflight`, `in-flight`
- `completed` → `done`
- `dlq` → `dead-letter`

---

### Consumer Registry

#### `register-consumer`
Register a consumer with the consumer registry.

```bash
flo workqueue register-consumer --name payments --group workers \
  --consumer-id worker-01
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--consumer-id` - Consumer ID (auto-generated if empty)

---

#### `heartbeat`
Send a consumer heartbeat to maintain liveness.

```bash
flo workqueue heartbeat --name payments --group workers --consumer-id worker-01
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--consumer-id` - Consumer ID (required)

---

#### `unregister-consumer`
Unregister a consumer from the registry.

```bash
flo workqueue unregister-consumer --name payments --group workers \
  --consumer-id worker-01
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--consumer-id` - Consumer ID (required)

---

#### `consumers`
List all consumers in a group.

```bash
flo workqueue consumers --name payments --group workers
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)

**Output**:
```json
{
  "consumers": [
    {
      "consumer_id": "worker-01",
      "capacity": 10,
      "in_flight": 3,
      "labels": {"zone": "us-west"},
      "last_seen_ms": 1234567890123,
      "expires_at_ms": 1234567905123,
      "is_alive": true
    }
  ]
}
```

---

### PEL & Claims

#### `claim`
Manually claim (reassign) specific messages to a different consumer.

```bash
flo workqueue claim --name payments --group workers \
  --new-consumer-id worker-02 \
  --id AAECAwQFBgcICQ== --id AAECAwQFBgcICA== \
  --lease-ms 30000
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (required)
- `--new-consumer-id` - New consumer ID to assign to (required)
- `--id` - Message IDs to claim in base64 (repeatable, required)
- `--lease-ms` - New lease duration in milliseconds (default: `30000`)

**Output**:
```json
{
  "claimed_count": 2
}
```

---

### Admin & Stats

#### `stats`
Get work queue statistics.

```bash
flo workqueue stats --name payments --group workers
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (optional, for group-specific stats)

**Output**:
```json
{
  "total_enqueued": 10000,
  "total_completed": 9500,
  "total_failed": 450,
  "pending_count": 50,
  "in_flight_count": 25,
  "retry_count": 15,
  "dlq_count": 10,
  "groups": [
    {
      "group": "workers",
      "pending_count": 50,
      "in_flight_count": 25,
      "retry_count": 15,
      "dlq_count": 10,
      "consumer_count": 5
    }
  ]
}
```

---

#### `flush`
Flush (clear) a work queue.

```bash
flo workqueue flush --name payments --group workers
```

**Flags**:
- `-n, --namespace` - Namespace (default: `default`)
- `--name` - WorkQueue name (required)
- `-g, --group` - Consumer group (optional, flush specific group only)

---

## Examples

### Complete Worker Flow

```bash
# 1. Create a queue
flo workqueue create --name orders --partitions 16

# 2. Register as a consumer
flo workqueue register-consumer --name orders --group processors \
  --consumer-id processor-1

# 3. Enqueue some messages
flo workqueue enqueue --name orders --data '{"order_id":123}' --priority 10
flo workqueue enqueue --name orders --data '{"order_id":124}' --priority 5

# 4. Dequeue and process (runs continuously)
flo workqueue dequeue --name orders --group processors \
  --consumer-id processor-1 --count 5 --auto-ack

# 5. Send heartbeats (in another terminal/script)
while true; do
  flo workqueue heartbeat --name orders --group processors \
    --consumer-id processor-1
  sleep 5
done

# 6. View stats
flo workqueue stats --name orders --group processors

# 7. Check pending messages
flo workqueue pending --name orders --group processors --limit 20

# 8. Cleanup
flo workqueue unregister-consumer --name orders --group processors \
  --consumer-id processor-1
```

---

### Manual Processing with Lease Extension

```bash
# Dequeue without auto-ack
flo workqueue dequeue --name orders --group processors \
  --consumer-id processor-1 --count 1 --lease-ms 60000

# Output: {"id_b64": "AAECAwQFBgcICQ==", ...}

# Process the message (takes time)...
# Extend lease if needed
flo workqueue extend-lease --name orders --group processors \
  --id AAECAwQFBgcICQ== --extension-ms 60000

# Complete or fail
flo workqueue complete --name orders --group processors --id AAECAwQFBgcICQ==
# OR
flo workqueue fail --name orders --group processors \
  --id AAECAwQFBgcICQ== --error "Processing timeout"
```

---

### Claim Expired Messages

```bash
# List pending messages
flo workqueue pending --name orders --group processors --limit 50

# Claim idle messages from failed consumer
flo workqueue claim --name orders --group processors \
  --new-consumer-id processor-2 \
  --id AAECAwQFBgcICQ== --id AAECAwQFBgcICA== \
  --lease-ms 30000
```

---

## Implementation Details

**Transport**: All commands use gRPC (`flov1.WorkQueuesServiceClient`)

**Connection**: Established per command invocation via `withWorkQueuesClient` helper

**Output**: JSON-encoded responses with pretty printing (2-space indent)

**Payload Decoding**: Messages are decoded intelligently:
1. JSON if payload starts with `{` or `[`
2. UTF-8 text if valid and printable
3. Base64 as fallback

**Error Handling**: Go errors are returned directly; gRPC errors include status codes

---

## File Structure

```
flo/internal/cmd/client/
├── workqueue.go      (~1020 lines) - All 17 WorkQueue commands
├── utils.go          - withWorkQueuesClient helper
├── root.go           - Command registration
└── doc.go            - Package documentation
```

**Integration**: Registered in `flo/cmd/flo/main.go` line 169

---

## Comparison with Streams CLI

| Feature | Streams | WorkQueues |
|---------|---------|------------|
| Transport | gRPC + HTTP hybrid | gRPC only |
| Commands | 8 | 17 |
| Streaming | Subscribe, Tail | Dequeue |
| Auto-ack | No | Yes (optional) |
| Consumer registry | No | Yes |
| Lease management | No | Yes |
| State queries | No | Yes (ready, pending, completed) |
| Unified view | No | Yes (`list --all`) |
| PEL queries | No | Yes |
| Manual claims | No | Yes |

---

## Future Enhancements

- [ ] HTTP transport support for non-streaming operations
- [ ] Batch enqueue from file or stdin
- [ ] Interactive dequeue mode with prompt for ack/nack
- [ ] Consumer group management commands
- [ ] DLQ inspection and replay
- [ ] Message search/filter capabilities
- [ ] Shell completion scripts

---

**Last Updated**: October 7, 2025  
**Status**: ✅ Complete and functional
