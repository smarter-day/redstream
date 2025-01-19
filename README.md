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
9. **Automatic stale-consumer removal** (clears out “dead” consumers to reclaim messages).
10. **Auto-rejoin** if the current consumer was forcibly removed from the group.

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
        StreamName:         "myStream",
        GroupName:          "myGroup",
        ConsumerName:       "myConsumer",
        EnableReclaim:      true,
        ReclaimStr:         "5s",
        ReclaimCount:       10,
        MaxReclaimAttempts: 3,
        MaxConcurrency:     5, // optional concurrency limit
        // Optional DLQ handler for messages that exceed attempts
        DLQHandler: func(ctx context.Context, xMsg *redis.XMessage) error {
            log.Printf("[DLQ] message ID=%s\n", xMsg.ID)
            return nil
        },
        // (Optional) Auto-remove other stale consumers, rejoin if we're removed
        EnableStaleConsumerCleanup: true,
        EnableAutoRejoinOnRemoved:  true,
    }

    // For single node, cluster, or sentinel
    uniOpts := &redis.UniversalOptions{Addrs: []string{"localhost:6379"}}
    stream := redstream.New(uniOpts, cfg)

    // Register your message handler
    stream.RegisterHandler(func(ctx context.Context, fields map[string]string) error {
        // Return an error to test the backoff/reclaim. Otherwise, handle your data here.
        return nil
    })

    if err := stream.StartConsumer(ctx); err != nil {
        log.Fatal("Cannot start consumer:", err)
    }

    // Publish
    msgID, err := stream.Publish(ctx, map[string]any{"foo": "bar"})
    if err != nil {
        log.Println("Publish error:", err)
    } else {
        log.Println("Published message ID:", msgID)
    }

    // Block forever
    select {}
}
```

### High-Level Flow

1. **Publish**: Uses `XADD`. If `DropConcurrentDuplicates=true`, a short Redsync lock can skip near-simultaneous duplicates.
2. **Consume**: Each message is read with `XREADGROUP`. On success => `XACK + XDEL`; on failure => attempts/backoff. If `UseDistributedLock=true`, only one node processes a given message at a time.
3. **Reclaim**: A background loop calls `XAUTOCLAIM` to reclaim messages stuck in PEL. Retries until `MaxReclaimAttempts`.
4. **Stale Consumers**: If `EnableStaleConsumerCleanup=true`, periodically removes consumer names that have been idle beyond `StaleConsumerIdleThresholdStr`, freeing their stuck messages for reclamation.
5. **Auto-Rejoin**: If `EnableAutoRejoinOnRemoved=true`, a consumer that detects it’s been removed by another node automatically re-joins the group.
6. **DLQ**: Messages exceeding `MaxReclaimAttempts` go to `DLQHandler` (if set). If `IgnoreDLQHandlerErrors=false`, it’ll remain pending on DLQ errors.
7. **Concurrency Limit**: With `MaxConcurrency>0`, each consumer instance only processes that many messages simultaneously.

---

## Configuration

Configure via `redstream.Config` plus `*redis.UniversalOptions`. Key fields include:

| Field                              | Default                 | Description                                                                                                                                                            |
|------------------------------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`StreamName`**                   | **required**            | Redis stream name.                                                                                                                                                     |
| **`GroupName`**                    | **required**            | Consumer group name (auto-created at `0-0` if missing).                                                                                                                |
| **`ConsumerName`**                 | *auto-generated*        | Must be unique across consumers.                                                                                                                                        |
| **`EnableReclaim`**                | `false`                 | Whether to run reclaim logic (XAUTOCLAIM).                                                                                                                             |
| **`ReclaimStr`**                   | `"5s"`                  | Frequency of auto-reclaim checks.                                                                                                                                      |
| **`MaxReclaimAttempts`**           | `3`                     | After this many fails, remove or DLQ the message.                                                                                                                      |
| **`DLQHandler`**                   | `nil`                   | Callback for messages that exceed `MaxReclaimAttempts`.                                                                                                                |
| **`IgnoreDLQHandlerErrors`**       | `false`                 | If true, remove message even if DLQ fails.                                                                                                                             |
| **`DropConcurrentDuplicates`**     | `false`                 | Adds a brief Redsync lock on Publish to skip duplicates.                                                                                                               |
| **`MaxConcurrency`**               | `10`                    | If >0, concurrency is throttled in `processMessage`.                                                                                                                   |
| **`StaleConsumerIdleThresholdStr`**| `"2m"`                  | Consumers idle longer than this are considered “stale” (if `EnableStaleConsumerCleanup=true`).                                                                          |
| **`EnableStaleConsumerCleanup`**   | `false`                 | If true, we remove “stale” consumers, letting a healthy consumer reclaim their messages.                                                                               |
| **`EnableAutoRejoinOnRemoved`**    | `false`                 | If a consumer sees it was removed, it automatically re-joins the group.                                                                                                |
| **`AutoRejoinCheckIntervalStr`**   | `"30s"`                 | Frequency for checking if our consumer name still exists in the group.                                                                                                 |
| **`UseRedisIdAsUniqueID`**         | `false`                 | If true, use the Redis `XMessage.ID` for dedup. Otherwise, compute sha256 from message payload.                                                                        |

---

## Deeper Explanation & Best Practices

- **Universal Client**:  
  `redis.NewUniversalClient(...)` works for single-node, cluster, or sentinel modes.
- **Redsync Locks**:  
  With `UseDistributedLock=true`, each message is locked so only one node processes it at a time.  
  With `DropConcurrentDuplicates=true`, `Publish` also uses a tiny ephemeral lock.
- **Auto-Reclaim**:  
  Recovers messages left in the Pending Entries List if a consumer crashed.
- **Exponential Backoff**:  
  Each re-failed message remains in PEL; we store a “nextBackoffSec” so we skip it until it’s ready again.
- **DLQ**:  
  Once a message passes `MaxReclaimAttempts`, it’s removed from the stream. Optionally, `DLQHandler` is called.
- **Stale Consumers**:  
  If a consumer remains idle beyond `StaleConsumerIdleThresholdStr`, we run `XGROUP DELCONSUMER`, letting other consumers reclaim messages.  
  A forcibly removed consumer can rejoin if `EnableAutoRejoinOnRemoved=true`.
- **Fallback**:  
  If a Lua script fails (e.g. “NOSCRIPT”), you can do a forced `XACK+XDEL` to avoid duplicates stuck in pending.

---

## License

[MIT License](LICENSE) – open for adaptation & improvement. Issues and contributions are always welcome!
