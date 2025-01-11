<img src="assets/stream.png" alt="Redis Stream" />

A **Golang** library for robust **Redis Streams** consumption with:

1. **Consumer Groups** for once-per-message processing.
2. **Redsync** locks to prevent duplicate handling across multiple workers.
3. **Auto-reclaim** (`XAUTOCLAIM`) to recover stuck pending messages.
4. **Optional ephemeral “publish lock”** to avoid concurrent duplicates.
5. **Exponential backoff** for repeated failures.
6. **Dead Letter Queue (DLQ)** support for over-limit messages.
7. **Universal Client** approach to support single-node, cluster, or sentinel setups.

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
    "fmt"
    "log"

    "github.com/redis/go-redis/v9"
    "github.com/smarter-day/redstream"
)

func main() {
    ctx := context.Background()

    cfg := redstream.Config{
        StreamName: "myStream",
        GroupName:  "myGroup",
        ConsumerName: "myConsumer",

        EnableReclaim:    true,
        ReclaimStr:       "5s",
        ReclaimCount:     10,
        MaxReclaimAttempts: 3,

        // Optional DLQ handler
        DLQHandler: func(ctx context.Context, msg *redis.XMessage) error {
            log.Println("DLQ got message ID=", msg.ID)
            // put it somewhere else, e.g. "errors" stream, DB...
            return nil
        },
    }

    // Provide a *redis.UniversalOptions, e.g. for a single node or cluster
    universalOpts := &redis.UniversalOptions{
        Addrs: []string{"localhost:6379"},
    }

    // Create a RedStream instance
    stream := redstream.New(universalOpts, cfg)

    // Register a message handler
    stream.RegisterHandler(func(ctx context.Context, msg map[string]string) error {
        // If an error is returned, message is retried or eventually hits DLQ
        return nil
    })

    // Start consumer loop
    if err := stream.StartConsumer(ctx); err != nil {
        log.Fatal("Cannot start consumer:", err)
    }

    // Publish a test message
    msgID, err := stream.Publish(ctx, map[string]any{"foo": "bar"})
    if err != nil {
        fmt.Println("Publish error:", err)
    } else {
        fmt.Println("Published with ID:", msgID)
    }

    // Block forever
    select {}
}
```

### High-Level Flow

1. **Publish**: By default calls `XADD`. Optionally ephemeral-locks if `DropConcurrentDuplicates=true`.
2. **Consume**: Worker(s) do `XREADGROUP`, then a Redsync lock ensures only one worker processes each message. If success, we `XACK + XDEL`; if failure, we track attempts & backoff.
3. **Reclaim**: If `EnableReclaim=true`, a background loop calls `XAUTOCLAIM` on idle pending messages. Each message is reprocessed or eventually removed / sent to DLQ if it exceeds `MaxReclaimAttempts`.

---

## Configuration

Pass a `Config` and **`*redis.UniversalOptions`** into `redstream.New(...)`. Key fields:

| Field                              | Default                  | Description                                                                                                                        |
|------------------------------------|--------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| **`StreamName`**                   | **required**             | Redis stream name.                                                                                                                 |
| **`GroupName`**                    | **required**             | Consumer group name (created at `0-0` if missing).                                                                                 |
| **`ConsumerName`**                | auto-generated           | Must be unique across consumers.                                                                                                    |
| **`LockExpiryStr`**               | `"10s"`                  | Redsync lock expiration for message-level locks.                                                                                    |
| **`LockExtendStr`**               | half of `LockExpiryStr`  | Interval to `ExtendContext` so the lock stays valid if your handler is still working.                                               |
| **`BlockDurationStr`**            | `"5s"`                   | How long `XREADGROUP` will block if no new messages arrive.                                                                         |
| **`ReclaimStr`**                  | `"5s"`                   | Frequency of reclaim passes if `EnableReclaim=true`.                                                                                |
| **`EnableReclaim`**               | `false`                  | Whether or not to run reclaim logic.                                                                                                |
| **`CleanOldReclaimDuration`**     | `"1h"`                   | Age threshold for removing old reclaim records from the Redis hash.                                                                 |
| **`MaxReclaimAttempts`**          | `3`                      | If a message fails beyond this, we remove it or pass it to a DLQ if configured.                                                     |
| **`ReclaimCount`**                | `10`                     | Number of messages to handle per `XAUTOCLAIM` pass.                                                                                 |
| **`ReclaimMaxExponentialFactor`** | `3`                      | Cap for exponential backoff (e.g. factor = `min(2^attempts, 2^3=8)`).                                                               |
| **`DLQHandler`**                  | `nil`                    | If set, a function to handle messages that exceed `MaxReclaimAttempts`.                                                             |
| **`IgnoreDLQHandlerErrors`**      | `false`                  | If **true**, we remove the message even if the DLQ handler fails. If **false**, we keep it pending if DLQ fails.                    |
| **`DropConcurrentDuplicates`**    | `false`                  | If **true**, ephemeral-lock on `Publish(...)` to skip duplicates.                                                                   |

*(Additionally, your `*redis.UniversalOptions` may specify single node, cluster, or sentinel configurations.)*

---

## Deeper Explanation & Best Practices

1. **Universal Client**
   - We call `redis.NewUniversalClient(...)` so you can use single-node, cluster, or sentinel with the **same** code. Just set your `redis.UniversalOptions`.

2. **Consumer Groups & PEL**
   - Redis Streams track pending messages. If your consumer or code fails, messages remain in the PEL until reclaimed.

3. **Redsync Locking**
   - Ensures only one worker processes each message, preventing duplicates even if `XAUTOCLAIM` reassigns the same message to multiple consumers.

4. **Exponential Backoff**
   - On repeated failures, we store `"<msgID>:nextBackoffSec"`. If that time has not arrived, we skip reprocessing the message in the next reclaim pass.

5. **Dead Letter Queue**
   - Exceed `MaxReclaimAttempts`: optionally route the message to a **DLQ** with `DLQHandler(...)`.
   - If `DLQHandler` fails and `IgnoreDLQHandlerErrors=false`, we keep the message pending. Otherwise, we remove it.

6. **Clean Up**
   - We store attempt counters in `"<streamName>:reclaim_attempts"` (hash). Old records older than `CleanOldReclaimDuration` get removed each pass.

7. **Performance Tuning**
   - Adjust `LockExpiryStr`, `ReclaimStr`, and so on to suit your system’s scale and throughput.

---

## License

[MIT License](LICENSE) – Feel free to adapt & extend.  

Contributions & issues welcome!