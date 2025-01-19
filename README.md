<img src="assets/stream.png" alt="Redis Stream" />

[![Build & Test](https://github.com/smarter-day/redstream/actions/workflows/build-and-test.yml/badge.svg?branch=main)](https://github.com/smarter-day/redstream/actions/workflows/build-and-test.yml)

A **Golang** library for robust **Redis Streams** consumption with:

1. **Consumer Groups** for once-per-message processing.
2. **Redsync** locks to prevent duplicates across multiple workers.
3. **Auto-reclaim** (`XAUTOCLAIM`) to recover stuck pending messages.
4. **Optional ephemeral “publish lock”** to skip concurrent duplicates.
5. **Exponential backoff** for repeated failures.
6. **Dead Letter Queue (DLQ)** support for over-limit messages.
7. **Universal Client** for single-node, cluster, or sentinel setups.
8. **Concurrency Limit** to throttle simultaneous message handling.

---

## Installation

```bash
go get github.com/smarter-day/redstream
```

---

## Quick Usage Example

Below is a **simplified** demonstration:

```go
package main

import (
    "context"
    "log"

    "github.com/redis/go-redis/v9"
    "github.com/smarter-day/redstream"
)

func main() {
    ctx := context.Background()

    cfg := redstream.Config{
        StreamName:      "myStream",
        GroupName:       "myGroup",
        ConsumerName:    "myConsumer",
        EnableReclaim:   true,
        ReclaimStr:      "5s",
        ReclaimCount:    10,
        MaxReclaimAttempts: 3,
        // Control concurrency (optional). If > 0, RedStream limits simultaneous handling.
        MaxConcurrency:  5,
        // Optional DLQ handler for messages that exceed attempts
        DLQHandler: func(ctx context.Context, msg *redis.XMessage) error {
            log.Printf("DLQ got message ID=%s\n", msg.ID)
            // e.g. write to a separate "errors" stream
            return nil
        },
    }

    // Provide *redis.UniversalOptions for single node, cluster, or sentinel
    universalOpts := &redis.UniversalOptions{Addrs: []string{"localhost:6379"}}

    stream := redstream.New(universalOpts, cfg)

    // Register a handler that processes messages
    stream.RegisterHandler(func(ctx context.Context, msg map[string]string) error {
        // Return an error to test retries or DLQ.
        return nil
    })

    if err := stream.StartConsumer(ctx); err != nil {
        log.Fatal("Cannot start consumer:", err)
    }

    // Publish
    msgID, err := stream.Publish(ctx, map[string]string{"foo": "bar"})
    if err != nil {
        log.Println("Publish error:", err)
    } else {
        log.Println("Published with ID:", msgID)
    }

    // Block forever
    select {}
}
```

### High-Level Flow

1. **Publish**: By default calls `XADD`. If `DropConcurrentDuplicates=true`, it uses a brief Redsync lock to skip duplicates that arrive almost simultaneously.
2. **Consume**: Worker(s) call `XREADGROUP`. Each message is protected by a Redsync lock so only one worker processes it. Success => `XACK + XDEL`; failure => attempt tracking & exponential backoff.
3. **Reclaim**: If `EnableReclaim=true`, a background loop calls `XAUTOCLAIM` to rescue stuck messages. Each is retried until `MaxReclaimAttempts`.
4. **Concurrency Limit (optional)**: If `MaxConcurrency > 0`, the library throttles simultaneous message processing in each worker, reducing the risk of goroutine overload.

---

## Configuration

Configure via `redstream.Config` plus a **`*redis.UniversalOptions`**. Important fields include:

| Field                             | Default                 | Description                                                                                                                                                      |
|-----------------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`StreamName`**                  | **required**            | Redis stream name.                                                                                                                                               |
| **`GroupName`**                   | **required**            | Consumer group name (auto-created at `0-0` if missing).                                                                                                          |
| **`ConsumerName`**                | *auto-generated*        | Must be unique across consumers.                                                                                                                                 |
| **`LockExpiryStr`**               | `"10s"`                 | Redsync lock expiration for message-level locks.                                                                                                                 |
| **`LockExtendStr`**               | half of `LockExpiryStr` | Interval to keep the lock alive via `ExtendContext`.                                                                                                             |
| **`BlockDurationStr`**            | `"5s"`                  | How long `XREADGROUP` waits if no new messages arrive.                                                                                                           |
| **`ReclaimStr`**                  | `"5s"`                  | Frequency of auto-reclaim if `EnableReclaim=true`.                                                                                                               |
| **`EnableReclaim`**               | `false`                 | Whether to run reclaim logic.                                                                                                                                    |
| **`CleanOldReclaimDuration`**     | `"1h"`                  | Age threshold for removing stale reclaim records.                                                                                                                |
| **`MaxReclaimAttempts`**          | `3`                     | After exceeding this, messages are removed or go to DLQ.                                                                                                         |
| **`ReclaimCount`**                | `10`                    | Number of messages to handle per `XAUTOCLAIM` pass.                                                                                                              |
| **`ReclaimMaxExponentialFactor`** | `3`                     | Max exponent for backoff. E.g. `factor = min(2^attempts, 8)`.                                                                                                    |
| **`DLQHandler`**                  | `nil`                   | Optional function for messages beyond `MaxReclaimAttempts`.                                                                                                      |
| **`IgnoreDLQHandlerErrors`**      | `false`                 | If **true**, remove message even if DLQ fails. Otherwise, keep it pending.                                                                                       |
| **`DropConcurrentDuplicates`**    | `false`                 | If **true**, ephemeral lock on `Publish(...)` to skip near-simultaneous duplicates.                                                                              |
| **`MaxConcurrency`**              | `10`                    | If > 0, the library uses a channel-based limit for concurrency in `processMessage`.                                                                              |
| **`UseRedisIdAsUniqueID`**        | `false`                 | If **true**, - uses redis.XMessage -> ID as unique ID to not repeat processing of duplicates. <br/>Otherwise calculates full sha256 from entire message payload. |

*(Your `*redis.UniversalOptions` can specify single node, cluster, or sentinel.)*

---

## Deeper Explanation & Best Practices

- **Universal Client**:  
  `redis.NewUniversalClient(...)` lets you toggle between single-node, cluster, or sentinel seamlessly.
- **Consumer Groups & PEL**:  
  Redis Streams track unacknowledged messages in a Pending Entries List (PEL). If a consumer crashes, messages remain pending.
- **Redsync Locking**:  
  Ensures only one worker (even in a large pool) processes each message. This helps if `XAUTOCLAIM` reassigns the same message to multiple consumers.
- **Exponential Backoff**:  
  On repeated failures, we store the next earliest time to reprocess each message. If that time is in the future, we skip reprocessing for now.
- **Dead Letter Queue**:  
  After `MaxReclaimAttempts`, messages are removed or passed to `DLQHandler`. If `IgnoreDLQHandlerErrors=false`, we keep them in pending if the DLQ fails.
- **Concurrency Limiting**:  
  By using `MaxConcurrency`, each consumer instance only handles up to N messages simultaneously. This helps if you have a CPU- or I/O-intensive handler, preventing goroutine overload.
- **Cleanup**:  
  Reclaim attempts are stored in a Redis hash. Stale entries older than `CleanOldReclaimDuration` are removed automatically.
- **Performance Tuning**:  
  Adjust intervals (`LockExpiryStr`, `ReclaimStr`, etc.) to match your throughput. For large streams, ensure your concurrency and backoff are tuned to avoid overload or excessive retries.

---

## License

[MIT License](LICENSE) – open for adaptation & improvement.

Issues and contributions are always welcome!
