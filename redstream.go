package redstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"log"
	"strconv"
	"strings"
	"time"
)

// Config represents the configuration for the RedStream.
type Config struct {
	// StreamName represents the name of the Redis stream.
	StreamName string `validate:"required"`
	// GroupName represents the name of the consumer group.
	GroupName string `validate:"required"`
	// ConsumerName represents the name of the consumer.
	// It should be unique. Use UniqueConsumerName helper method to generate a unique name based on your own name.
	ConsumerName string `validate:"required"`

	// MaxConcurrency represents the maximum number of concurrent consumers.
	MaxConcurrency int

	// LockExpiryStr represents the duration for which the lock will be held.
	LockExpiryStr string `validate:"duration"`
	// LockExtendStr represents the duration for which the lock will be extended after it's held.
	LockExtendStr string `validate:"duration"`
	// BlockDurationStr represents the duration for which the consumer will block if no new messages are available.
	BlockDurationStr string `validate:"duration"`

	// EnableReclaim enables reclaiming messages.
	EnableReclaim bool
	// ReclaimStr represents the duration for which the consumer will reclaim messages if it's blocked.
	ReclaimStr string `validate:"duration"`
	// CleanOldReclaimDuration represents the duration for which old reclaim messages will be cleaned.
	CleanOldReclaimDuration string `validate:"duration"`
	// MaxReclaimAttempts represents the maximum number of attempts to reclaim messages.
	MaxReclaimAttempts int
	// ReclaimCount represents the number of times messages to reclaim.
	ReclaimCount int64
	// ReclaimMaxExponentialFactor represents the maximum exponential factor for reclaiming messages delay.
	ReclaimMaxExponentialFactor int
	// DLQ handler enables handling messages that are sent to a Dead Letter Queue (DLQ).
	DLQHandler DLQHandlerFn
	// IgnoreDLQHandlerErrors enables ignoring errors that occur when handling messages in a DLQ handler.
	IgnoreDLQHandlerErrors bool

	// DropConcurrentDuplicates enables dropping duplicate messages if they arrive in nearly the same time.
	DropConcurrentDuplicates bool
}

type RedisStream struct {
	Client redis.Cmdable
	Rs     *redsync.Redsync
	Cfg    Config

	LockExpiry              time.Duration
	LockExtend              time.Duration
	BlockDuration           time.Duration
	ReclaimInterval         time.Duration
	CleanOldReclaimDuration time.Duration

	handler func(ctx context.Context, msg map[string]string) error

	debugLogger LogFn
	infoLogger  LogFn
	errorLogger LogFn

	concurrencyCh chan struct{}
}

// New creates and initializes a new IRedStream instance.
//
// It sets up a Redis client, initializes a distributed lock using redsync,
// and configures various timing parameters based on the provided configuration.
// It also creates a consumer group if it doesn't already exist.
//
// Parameters:
//   - redisOptions: A pointer to redis.Options, containing Redis connection details.
//   - cfg: A Config struct containing configuration parameters for the stream.
//
// Returns:
//
//	An IRedStream interface implementation if successful, or nil if there's an error
//	in validating the configuration or setting up the consumer group.
func New(redisOptions *redis.UniversalOptions, cfg Config) IRedStream {
	client := redis.NewUniversalClient(redisOptions)
	rs := redsync.New(redsyncredis.NewPool(client))

	lockExpiry := ParseDurationOrDefault(&cfg.LockExpiryStr, 10*time.Second)
	lockExtend := ParseDurationOrDefault(&cfg.LockExtendStr, lockExpiry/2)
	blockDuration := ParseDurationOrDefault(&cfg.BlockDurationStr, 5*time.Second)
	reclaimInterval := ParseDurationOrDefault(&cfg.ReclaimStr, 5*time.Second)
	cleanOldReclaimDuration := ParseDurationOrDefault(&cfg.CleanOldReclaimDuration, 1*time.Hour)

	err := Validate.Struct(cfg)
	if err != nil {
		return nil
	}

	if cfg.MaxReclaimAttempts <= 0 {
		cfg.MaxReclaimAttempts = 3
	}
	if cfg.ReclaimCount <= 0 {
		cfg.ReclaimCount = 10
	}
	if cfg.ReclaimMaxExponentialFactor <= 0 {
		cfg.ReclaimMaxExponentialFactor = 3
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 10
	}
	if lockExtend >= lockExpiry {
		log.Fatalf("Lock extend interval (%v) must be < lock expiry (%v)", lockExtend, lockExpiry)
	}
	if lockExtend < time.Duration(0.2*float64(lockExpiry)) {
		log.Fatalf("Lock extend interval (%v) must be >= 20%% of expiry (%v)", lockExtend, lockExpiry)
	}
	if err = CreateGroupIfNotExists(client, cfg.StreamName, cfg.GroupName); err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	return &RedisStream{
		Client:                  client,
		Rs:                      rs,
		Cfg:                     cfg,
		LockExpiry:              lockExpiry,
		LockExtend:              lockExtend,
		BlockDuration:           blockDuration,
		ReclaimInterval:         reclaimInterval,
		CleanOldReclaimDuration: cleanOldReclaimDuration,
	}
}

// UseDebug sets a custom debug logger for the IRedStream instance.
func (r *RedisStream) UseDebug(fn LogFn) { r.debugLogger = fn }

// UseInfo sets a custom info logger for the IRedStream instance.
func (r *RedisStream) UseInfo(fn LogFn) { r.infoLogger = fn }

// UseError sets a custom error logger for the IRedStream instance.
func (r *RedisStream) UseError(fn LogFn) { r.errorLogger = fn }

// debug logs debug-level messages using the configured debug logger or falls back to standard logging.
//
// It first checks if a custom debug logger is set. If so, it uses that logger to log the message.
// Otherwise, it falls back to using the standard log package.
//
// Parameters:
//   - ctx: The context.Context for the logging operation.
//   - args: Variadic parameter for the log message and any additional values to be logged.
//
// The function does not return any value.
func (r *RedisStream) debug(ctx context.Context, args ...interface{}) {
	if r.debugLogger != nil {
		_ = r.debugLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[DEBUG]"}, args...)
		log.Println(args...)
	}
}

// info logs info-level messages using the configured info logger or falls back to standard logging.
//
// It first checks if a custom info logger is set. If so, it uses that logger to log the message.
// Otherwise, it falls back to using the standard log package.
//
// Parameters:
//   - ctx: The context.Context for the logging operation.
//   - args: Variadic parameter for the log message and any additional values to be logged.
//
// The function does not return any value.
func (r *RedisStream) info(ctx context.Context, args ...interface{}) {
	if r.infoLogger != nil {
		_ = r.infoLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[INFO]"}, args...)
		log.Println(args...)
	}
}

// err logs error-level messages using the configured error logger or falls back to standard logging.
//
// It first checks if a custom error logger is set. If so, it uses that logger to log the message.
// Otherwise, it falls back to using the standard log package.
//
// Parameters:
//   - ctx: The context.Context for the logging operation.
//   - args: Variadic parameter for the log message and any additional values to be logged.
//
// The function does not return any value.
func (r *RedisStream) err(ctx context.Context, args ...interface{}) {
	if r.errorLogger != nil {
		_ = r.errorLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[ERROR]"}, args...)
		log.Println(args...)
	}
}

// RegisterHandler registers a handler function that will be called when a new message is received.
func (r *RedisStream) RegisterHandler(handler func(ctx context.Context, msg map[string]string) error) {
	r.handler = handler
}

// Publish adds a new message to the Redis stream. It optionally checks for concurrent duplicates
// before publishing, based on the configuration.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - data: Any data type that can be marshaled into JSON.
//
// Returns:
//   - string: The ID of the published message in the Redis stream. Empty if a duplicate is detected.
//   - error: An error if the publish operation fails, or nil if successful.
func (r *RedisStream) Publish(ctx context.Context, data any) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data: %w", err)
	}

	if r.Cfg.DropConcurrentDuplicates {
		// Increase lock expiry to 500ms (vs. 100ms) to reduce chance of missing duplicates
		publishLockStr := publishLockKey(r.Cfg.StreamName, raw)
		pubLock := r.Rs.NewMutex(publishLockStr, redsync.WithExpiry(500*time.Millisecond), redsync.WithTries(1))
		if e := pubLock.LockContext(ctx); e != nil {
			r.debug(ctx, "concurrency duplicate lock taken:", publishLockStr)
			return "", nil
		}
		defer func() {
			if _, uErr := pubLock.Unlock(); uErr != nil {
				r.err(ctx, "ephemeral publish lock unlock error:", uErr)
			}
		}()
	}

	args := &redis.XAddArgs{
		Stream: r.Cfg.StreamName,
		Values: map[string]any{"json": string(raw)},
	}
	id, xErr := r.Client.XAdd(ctx, args).Result()
	if xErr == nil {
		r.debug(ctx, "Published", id, string(raw))
	}
	return id, xErr
}

// StartConsumer initiates the consumer process for the Redis stream.
// It starts two goroutines: one for listening to new messages and another for
// reclaiming pending messages (if enabled in the configuration).
//
// Parameters:
//   - ctx: A context.Context for managing the lifecycle of the consumer goroutines.
//     It can be used to cancel the consumer operations.
//
// Returns:
//   - error: Returns error if handler is not set.
func (r *RedisStream) StartConsumer(ctx context.Context) error {
	if r.handler == nil {
		return fmt.Errorf("handler function not set")
	}

	// Optional concurrency limit
	if r.Cfg.MaxConcurrency > 0 {
		r.concurrencyCh = make(chan struct{}, r.Cfg.MaxConcurrency)
	}

	go r.listenNewMessages(ctx)

	if r.Cfg.EnableReclaim {
		go r.autoClaimLoop(ctx)
	}

	return nil
}

// listenNewMessages continuously listens for new messages in the Redis stream.
// It uses XReadGroup to read messages from the stream and processes each message
// in a separate goroutine. This function runs indefinitely until the context is cancelled.
//
// Parameters:
//   - ctx: A context.Context for cancellation and timeout control. When this context
//     is cancelled, the function will return.
//
// The function does not return any value. It runs as a long-lived goroutine,
// processing messages until the context is cancelled.
func (r *RedisStream) listenNewMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		streams, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.Cfg.GroupName,
			Consumer: r.Cfg.ConsumerName,
			Streams:  []string{r.Cfg.StreamName, ">"},
			Count:    1,
			Block:    r.BlockDuration,
		}).Result()

		if errors.Is(err, redis.Nil) {
			continue
		}

		if err != nil {
			r.err(ctx, "XReadGroup error:", err)
			continue
		}

		for _, s := range streams {
			for _, message := range s.Messages {
				go r.processMessage(ctx, message)
			}
		}
	}
}

// autoClaimLoop runs a continuous loop that periodically attempts to reclaim pending messages
// from the Redis stream. It uses a ticker to trigger reclaim attempts at regular intervals
// defined by the ReclaimInterval configuration.
//
// This function is intended to be run as a goroutine and will continue until the provided
// context is cancelled.
//
// Parameters:
//   - ctx: A context.Context that controls the lifecycle of the loop. When this context
//     is cancelled, the function will return, stopping the auto-claim process.
//
// The function does not return any value.
func (r *RedisStream) autoClaimLoop(ctx context.Context) {
	// Fire a small internal ticker (e.g. every second) but only do "heavy reclaim"
	// if ReclaimInterval has elapsed. This prevents the spam you see in logs.
	t := time.NewTicker(r.ReclaimInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.reclaimPending(ctx)
		}
	}
}

// processMessage handles the processing of a single message from the Redis stream.
// It acquires a lock, executes the user-defined handler, and manages the message
// lifecycle based on the handler's success or failure.
//
// The function performs the following steps:
//  1. Extracts the JSON content from the message.
//  2. Acquires a distributed lock for the message.
//  3. Executes the user-defined handler.
//  4. Based on the handler's result, either acknowledges and deletes the message,
//     or increments the attempt count for failed messages.
//  5. Releases the lock.
//
// Parameters:
//   - ctx: A context.Context for cancellation and timeout control.
//   - msg: A redis.XMessage containing the message data from the Redis stream.
//
// The function does not return any value. It logs various stages of message
// processing using the RedisStream's logging methods.
func (r *RedisStream) processMessage(ctx context.Context, msg redis.XMessage) {
	// Optional concurrency limit
	if r.concurrencyCh != nil {
		r.concurrencyCh <- struct{}{}
		defer func() { <-r.concurrencyCh }()
	}

	fields := convertFields(msg.Values)
	jsonString, ok := fields["json"]
	if !ok {
		r.err(ctx, "missing 'json' field in message:", msg)
		return
	}

	lockKey := streamLockKey(r.Cfg.StreamName, jsonString)
	mutex := r.Rs.NewMutex(lockKey, redsync.WithExpiry(r.LockExpiry), redsync.WithTries(1))
	if err := mutex.LockContext(ctx); err != nil {
		return
	}
	r.debug(ctx, "acquired for msg:", msg)

	// We create a context we can cancel if the lock extension fails
	extendCtx, extendCancel := context.WithCancel(ctx)
	go r.extendLockLoop(extendCtx, msg.ID, mutex, extendCancel)

	var handlerErr error
	if r.handler != nil {
		handlerErr = r.handler(extendCtx, fields)
	}

	attemptsKeyStr := reclaimAttemptsKey(r.Cfg)
	pipe := r.Client.Pipeline()

	if handlerErr == nil {
		// Success => ACK & delete
		r.ackAndDelete(ctx, pipe, msg.ID)
		_, _ = pipe.HDel(ctx, attemptsKeyStr,
			msg.ID,
			msg.ID+":count",
			msg.ID+":ts",
			msg.ID+":nextBackoffSec",
		).Result()

		if _, err := pipe.Exec(ctx); err != nil {
			r.err(ctx, "ackAndDelete fail:", msg, err)
		} else {
			r.debug(ctx, "success & deleted msg:", msg)
		}

	} else {
		// Failure => increment attempt count
		nowSec := time.Now().Unix()
		countKey := msg.ID + ":count"
		tsKey := msg.ID + ":ts"
		backoffKey := msg.ID + ":nextBackoffSec"

		newCount, _ := pipe.HIncrBy(ctx, attemptsKeyStr, countKey, 1).Result()
		if newCount > int64(r.Cfg.MaxReclaimAttempts) {
			var dlqHandlerErr error
			isDLQHandlerSet := r.Cfg.DLQHandler != nil

			if isDLQHandlerSet {
				dlqHandlerErr = r.Cfg.DLQHandler(extendCtx, &msg)
				if dlqHandlerErr != nil {
					r.err(ctx, "DLQ handler fail:", msg, dlqHandlerErr)
				}
			} else {
				r.err(ctx, "DLQ handler not set")
			}

			isDLQHandlerSuccessful := dlqHandlerErr == nil
			shouldIgnoreDLQErrors := r.Cfg.IgnoreDLQHandlerErrors
			shouldRemove := !isDLQHandlerSet || (isDLQHandlerSuccessful || shouldIgnoreDLQErrors)

			if shouldRemove {
				r.err(ctx, "msg exceeded attempts:", msg, handlerErr)
				r.ackAndDelete(ctx, pipe, msg.ID)
				_, _ = pipe.HDel(ctx, attemptsKeyStr,
					msg.ID,
					countKey,
					tsKey,
					backoffKey,
				).Result()
			}
		} else {
			r.err(ctx, "handler fail for msg:", msg, handlerErr)
			// We do NOT ack => remains in PEL
			_, _ = pipe.HSet(ctx, attemptsKeyStr, tsKey, nowSec).Result()

			// Exponential backoff
			factor := 1 << newCount
			if factor > r.Cfg.ReclaimMaxExponentialFactor {
				factor = r.Cfg.ReclaimMaxExponentialFactor
			}
			nextBackoffSec := nowSec + int64(r.ReclaimInterval.Seconds())*int64(factor)
			_, _ = pipe.HSet(ctx, attemptsKeyStr, backoffKey, nextBackoffSec).Result()
		}

		if _, pipeErr := pipe.Exec(ctx); pipeErr != nil {
			r.err(ctx, "process fail:", msg, pipeErr)
		}
	}

	// Stop the lock-extension goroutine
	extendCancel()

	if _, uErr := mutex.Unlock(); uErr != nil {
		r.err(ctx, "unlock fail for msg:", msg, uErr)
	}
}

// extendLockLoop continuously extends the lock for a message being processed.
// It runs in a separate goroutine and periodically attempts to extend the lock
// until the context is cancelled or an error occurs during extension.
//
// Parameters:
//   - ctx: A context.Context for cancellation control. When this context
//     is cancelled, the function will return, stopping the lock extension process.
//   - msgID: A string representing the ID of the message being processed.
//     This is used for logging purposes.
//   - m: A pointer to a redsync.Mutex representing the distributed lock
//     that needs to be extended.
//
// The function does not return any value. It runs as a long-lived goroutine,
// extending the lock at regular intervals until the context is cancelled or
// an error occurs.
func (r *RedisStream) extendLockLoop(ctx context.Context, msgID string, m *redsync.Mutex, cancel context.CancelFunc) {
	ticker := time.NewTicker(r.LockExtend)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := m.ExtendContext(ctx)
			if err != nil || !ok {
				r.err(ctx, "lock extend fail for msg:", msgID, err)
				// Cancel so the handler code can end
				cancel()
				return
			}
			r.debug(ctx, "extended for msg:", msgID)
		}
	}
}

// reclaimPending attempts to reclaim pending messages from the Redis stream.
// It uses XAUTOCLAIM to fetch pending messages, processes them, and manages
// the reclaim attempts count. This function also implements a distributed lock
// to ensure only one worker performs the reclaim operation at a time.
//
// The function performs the following steps:
// 1. Acquires a distributed lock to ensure exclusive reclaim operation.
// 2. Checks if sufficient time has passed since the last reclaim operation.
// 3. Uses XAUTOCLAIM to fetch pending messages.
// 4. Processes each claimed message, either by reprocessing or acknowledging and deleting.
// 5. Updates the next start ID for future reclaim operations.
// 6. Updates the last reclaim time.
// 7. Cleans up old reclaim records.
//
// Parameters:
//   - ctx: A context.Context for cancellation and timeout control.
//     It is used for all Redis operations and message processing.
//
// The function does not return any value. It logs various stages of the reclaim
// process using the RedisStream's logging methods.
func (r *RedisStream) reclaimPending(ctx context.Context) {
	reclaimLock := r.Rs.NewMutex(
		reclaimLockKey(r.Cfg),
		redsync.WithExpiry(r.LockExpiry),
		redsync.WithTries(1),
	)
	if err := reclaimLock.LockContext(ctx); err != nil {
		// Another process holds the reclaim lock
		return
	}
	defer func() {
		if _, uErr := reclaimLock.Unlock(); uErr != nil {
			r.err(ctx, "unlock fail reclaim-lock:", uErr)
		}
	}()

	reclaimNextStartKeyStr := reclaimNextStartKey(r.Cfg)
	storedStart, _ := r.Client.Get(ctx, reclaimNextStartKeyStr).Result()
	if storedStart == "" {
		storedStart = "0-0"
	}

	attemptsKeyStr := reclaimAttemptsKey(r.Cfg)
	startID := storedStart

	// Keep calling XAutoClaim until no more big chunks left
	for {
		claimed, newNextStart, err := r.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   r.Cfg.StreamName,
			Group:    r.Cfg.GroupName,
			Consumer: r.Cfg.ConsumerName,
			MinIdle:  r.LockExpiry,
			Start:    startID,
			Count:    r.Cfg.ReclaimCount,
		}).Result()

		if err != nil {
			r.err(ctx, "XAutoClaim error", err)
			return
		}
		r.debug(ctx, "reclaimed", len(claimed), "messages, next start:", newNextStart)

		pipe := r.Client.Pipeline()

		for _, m := range claimed {
			countKey := m.ID + ":count"
			tsKey := m.ID + ":ts"
			backoffKey := m.ID + ":nextBackoffSec"

			val, _ := pipe.HGet(ctx, attemptsKeyStr, backoffKey).Result()
			if val != "" {
				nextSec, _ := strconv.ParseInt(val, 10, 64)
				nowSec := time.Now().Unix()
				if nowSec < nextSec {
					r.debug(ctx, "skip, still in backoff for msg:", m.ID)
					continue
				}
			}

			newCount, _ := pipe.HIncrBy(ctx, attemptsKeyStr, countKey, 1).Result()
			_, _ = pipe.HSet(ctx, attemptsKeyStr, tsKey, time.Now().Unix()).Result()

			if newCount > int64(r.Cfg.MaxReclaimAttempts) {
				r.err(ctx, "msg exceeded attempts:", m.ID)
				r.ackAndDelete(ctx, pipe, m.ID)
			} else {
				// Reprocess in a goroutine (still might want concurrency-limits)
				go r.processMessage(ctx, m)
			}
		}

		// Update next reclaim start
		if isGreaterID(newNextStart, startID) {
			_, _ = pipe.Set(ctx, reclaimNextStartKeyStr, newNextStart, 0).Result()
		}

		if _, execErr := pipe.Exec(ctx); execErr != nil {
			r.err(ctx, "pipeline exec failed:", execErr)
		}

		// Stop if fewer than Count or ID not advancing
		if len(claimed) < int(r.Cfg.ReclaimCount) || !isGreaterID(newNextStart, startID) {
			break
		}
		startID = newNextStart
	}

	// Finally, cleanup old reclaim records
	r.cleanupOldReclaimRecords(ctx, attemptsKeyStr)
}

// cleanupOldReclaimRecords removes reclaim records that are older than 1 hour from the Redis hash.
// This function helps to maintain the reclaim records by removing outdated entries.
//
// Parameters:
//   - ctx: A context.Context for cancellation and timeout control.
//   - pipe: A redis.Pipeliner for batching Redis commands.
//   - key: A string representing the Redis key for the hash containing reclaim records.
//
// The function does not return any value. It logs errors using the RedisStream's error logger
// if there's a failure in retrieving the hash entries.
func (r *RedisStream) cleanupOldReclaimRecords(ctx context.Context, key string) {
	all, err := r.Client.HGetAll(ctx, key).Result()
	if err != nil {
		r.err(ctx, "cleanup - HGetAll failed:", err)
		return
	}

	nowSec := time.Now().Unix()
	pipe := r.Client.Pipeline()

	for field, val := range all {
		// Only handle fields that store a timestamp
		if strings.HasSuffix(field, ":ts") {
			ts, convErr := strconv.ParseInt(val, 10, 64)
			if convErr != nil {
				continue
			}
			if float64(nowSec-ts) > r.CleanOldReclaimDuration.Seconds() {
				// baseID = msg.ID, e.g. removing "xyz-123:ts" => baseID="xyz-123"
				baseID := strings.TrimSuffix(field, ":ts")
				pipe.HDel(ctx, key,
					field,           // The ts field
					baseID,          // In case we stored something at baseID
					baseID+":count", // Attempts count
					baseID+":nextBackoffSec",
				)
			}
		}
	}

	if _, execErr := pipe.Exec(ctx); execErr != nil {
		r.err(ctx, "pipeline exec failed:", execErr)
	}
}

// HealthCheck performs a health check on the Redis connection.
// It sends a PING command to the Redis server to verify connectivity.
//
// Parameters:
//   - ctx: A context.Context for cancellation and timeout control.
//
// Returns:
//   - error: nil if the PING was successful, otherwise an error describing the failure.
func (r *RedisStream) HealthCheck(ctx context.Context) error {
	_, err := r.Client.Ping(ctx).Result()
	return err
}
