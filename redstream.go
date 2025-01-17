package redstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/smarter-day/redstream/lua"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// Config represents the configuration for the RedStream.
type Config struct {
	StreamName   string `validate:"required"`
	GroupName    string `validate:"required"`
	ConsumerName string `validate:"required"`

	MaxConcurrency      int
	UseDistributedLock  bool // if false => skip the redsnyc-based lock
	NoProgressThreshold int  // after how many consecutive zero-reclaims do we call readPendingMessagesOnce

	LockExpiryStr    string `validate:"duration"`
	LockExtendStr    string `validate:"duration"`
	BlockDurationStr string `validate:"duration"`

	EnableReclaim               bool
	ReclaimStr                  string `validate:"duration"`
	CleanOldReclaimDuration     string `validate:"duration"`
	MaxReclaimAttempts          int
	ReclaimCount                int64
	ReclaimMaxExponentialFactor int

	DLQHandler               DLQHandlerFn
	IgnoreDLQHandlerErrors   bool
	DropConcurrentDuplicates bool

	ProcessedIdsMaxAgeStr string `validate:"duration"`
}

// RedisStream is the main struct implementing IRedStream.
type RedisStream struct {
	Client redis.Cmdable
	Rs     *redsync.Redsync
	Cfg    Config

	LockExpiry              time.Duration
	LockExtend              time.Duration
	BlockDuration           time.Duration
	ReclaimInterval         time.Duration
	CleanOldReclaimDuration time.Duration
	ProcessedIdsMaxAge      time.Duration

	handler func(ctx context.Context, msg map[string]string) error

	debugLogger LogFn
	infoLogger  LogFn
	errorLogger LogFn

	concurrencyCh   chan struct{}
	noProgressCount int // # times we saw zero reclaims in a row

	luaScripts *lua.Scripts
}

// New creates and initializes a new IRedStream instance.
//
// It sets up the Redis client, configures distributed locking if enabled,
// parses and validates configuration values, creates the consumer group if it doesn't exist,
// and registers Lua scripts.
//
// Parameters:
//   - redisOptions: A pointer to redis.UniversalOptions containing Redis connection settings.
//   - cfg: A Config struct containing the configuration for the RedStream.
//
// Returns:
//   - An IRedStream interface implementation. If there's an error during initialization,
//     it returns nil and logs a fatal error.
func New(redisOptions *redis.UniversalOptions, cfg Config) IRedStream {
	client := redis.NewUniversalClient(redisOptions)
	var rs *redsync.Redsync
	if cfg.UseDistributedLock {
		rs = redsync.New(redsyncredis.NewPool(client))
	}

	// parse durations with fallback defaults
	lockExpiry := ParseDurationOrDefault(&cfg.LockExpiryStr, 10*time.Second)
	lockExtend := ParseDurationOrDefault(&cfg.LockExtendStr, lockExpiry/2)
	blockDuration := ParseDurationOrDefault(&cfg.BlockDurationStr, 5*time.Second)
	reclaimInterval := ParseDurationOrDefault(&cfg.ReclaimStr, 5*time.Second)
	cleanOldReclaimDuration := ParseDurationOrDefault(&cfg.CleanOldReclaimDuration, 1*time.Hour)
	processedIdsMaxAge := ParseDurationOrDefault(&cfg.ProcessedIdsMaxAgeStr, 24*time.Hour)

	// validate or fix defaults
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
	if cfg.NoProgressThreshold <= 0 {
		cfg.NoProgressThreshold = 3 // default: 3 consecutive zero-reclaims
	}

	// only relevant if using distributed lock
	if cfg.UseDistributedLock {
		if lockExtend >= lockExpiry {
			log.Fatalf("Lock extend interval (%v) must be < lock expiry (%v)", lockExtend, lockExpiry)
		}
		if lockExtend < time.Duration(0.2*float64(lockExpiry)) {
			log.Fatalf("Lock extend interval (%v) must be >= 20%% of expiry (%v)", lockExtend, lockExpiry)
		}
	}

	if err := Validate.Struct(cfg); err != nil {
		return nil
	}

	// create group if not exist
	if err := CreateGroupIfNotExists(client, cfg.StreamName, cfg.GroupName); err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx := context.Background()

	scripts, errLoad := lua.RegisterLuaScripts(ctx, client)
	if errLoad != nil {
		log.Fatalf("Error registering Lua scripts: %v", errLoad)
	}

	return &RedisStream{
		Client:                  client,
		Rs:                      rs,
		Cfg:                     cfg,
		luaScripts:              scripts,
		LockExpiry:              lockExpiry,
		LockExtend:              lockExtend,
		BlockDuration:           blockDuration,
		ReclaimInterval:         reclaimInterval,
		CleanOldReclaimDuration: cleanOldReclaimDuration,
		ProcessedIdsMaxAge:      processedIdsMaxAge,
	}
}

// RegisterHandler sets the handler for new messages.
func (r *RedisStream) RegisterHandler(handler func(ctx context.Context, msg map[string]string) error) {
	r.handler = handler
}

func (r *RedisStream) UseDebug(fn LogFn) { r.debugLogger = fn }
func (r *RedisStream) UseInfo(fn LogFn)  { r.infoLogger = fn }
func (r *RedisStream) UseError(fn LogFn) { r.errorLogger = fn }

// debug logs [DEBUG] + your message
func (r *RedisStream) debug(ctx context.Context, args ...interface{}) {
	if r.debugLogger != nil {
		_ = r.debugLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[DEBUG]"}, args...)
		log.Println(args...)
	}
}

// info logs [INFO] + your message
func (r *RedisStream) info(ctx context.Context, args ...interface{}) {
	if r.infoLogger != nil {
		_ = r.infoLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[INFO]"}, args...)
		log.Println(args...)
	}
}

// err logs [ERROR] + your message
func (r *RedisStream) err(ctx context.Context, args ...interface{}) {
	if r.errorLogger != nil {
		_ = r.errorLogger(ctx, args...)
	} else {
		args = append([]interface{}{"[ERROR]"}, args...)
		log.Println(args...)
	}
}

// Publish adds a new message to the Redis stream. It optionally checks for and drops concurrent duplicates.
//
// The function marshals the provided data into JSON, optionally acquires a lock to prevent concurrent duplicates,
// and then adds the message to the stream using Redis XADD command.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - data: Any data type that can be marshaled into JSON to be published as a message.
//
// Returns:
//   - string: The ID of the published message in the Redis stream. Empty if skipped due to duplication.
//   - error: An error if any step in the publishing process fails, nil otherwise.
func (r *RedisStream) Publish(ctx context.Context, data any) (string, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data: %w", err)
	}

	if r.Cfg.DropConcurrentDuplicates {
		publishLockStr := publishLockKey(r.Cfg.StreamName, raw)
		if r.Rs != nil { // only if lock is enabled
			pubLock := r.Rs.NewMutex(publishLockStr,
				redsync.WithExpiry(500*time.Millisecond),
				redsync.WithTries(1),
			)
			if e := pubLock.LockContext(ctx); e != nil {
				r.debug(ctx, "duplicate lock taken, skipping publish:", publishLockStr)
				return "", nil
			}
			defer func() {
				if _, uErr := pubLock.Unlock(); uErr != nil {
					r.err(ctx, "ephemeral publish lock unlock error:", uErr)
				}
			}()
		}
	}

	args := &redis.XAddArgs{
		Stream: r.Cfg.StreamName,
		Values: map[string]any{"json": string(raw)},
	}
	id, xErr := r.Client.XAdd(ctx, args).Result()
	if xErr == nil {
		r.debug(ctx, "Published =>", id, string(raw))
	}
	return id, xErr
}

// StartConsumer initiates the consumer process for the Redis stream. It starts
// goroutines for listening to new messages, reclaiming pending messages (if enabled),
// and performing periodic cleanup of processed message IDs.
//
// This function sets up the necessary channels and goroutines to handle message
// processing concurrency, new message consumption, and maintenance tasks.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// Returns:
//   - error: An error if the handler function is not set, nil otherwise.
func (r *RedisStream) StartConsumer(ctx context.Context) error {
	if r.handler == nil {
		return fmt.Errorf("handler function not set")
	}
	if r.Cfg.MaxConcurrency > 0 {
		r.concurrencyCh = make(chan struct{}, r.Cfg.MaxConcurrency)
	}

	go r.listenNewMessages(ctx)

	if r.Cfg.EnableReclaim {
		go r.autoClaimLoop(ctx)
	}

	// rolling cleanup for processed IDs every 10m
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.cleanupProcessedIDs(ctx)
			}
		}
	}()

	return nil
}

// readPendingMessagesOnce reads and processes pending messages for the current consumer
// from the beginning of the stream (ID "0"). It continues reading messages in batches
// until there are no more pending messages or an error occurs.
//
// This function is typically used to reprocess messages that might have been left
// unacknowledged due to consumer failures or restarts.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// The function doesn't return any value, but it processes messages and handles errors internally.
func (r *RedisStream) readPendingMessagesOnce(ctx context.Context) {
	for {
		streams, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.Cfg.GroupName,
			Consumer: r.Cfg.ConsumerName,
			Streams:  []string{r.Cfg.StreamName, "0"},
			Count:    r.Cfg.ReclaimCount,
			Block:    r.BlockDuration,
		}).Result()

		if errors.Is(err, redis.Nil) {
			r.debug(ctx, "No more old pending for consumer:", r.Cfg.ConsumerName)
			return
		}
		if err != nil {
			r.err(ctx, "XReadGroup re-check error:", err)
			return
		}

		count := 0
		for _, s := range streams {
			for _, msg := range s.Messages {
				count++
				go r.processMessage(ctx, msg)
			}
		}

		// if we got fewer than ReclaimCount => we're done
		if int64(count) < r.Cfg.ReclaimCount {
			return
		}
	}
}

// listenNewMessages continuously listens for new messages in the Redis stream
// and processes them asynchronously. It uses XREADGROUP with the ">" identifier
// to read only new messages that haven't been delivered to any other consumer
// in the same consumer group.
//
// This function runs in an infinite loop until the context is cancelled. It
// blocks for a specified duration while waiting for new messages, and when
// messages are received, it spawns a new goroutine to process each message.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// The function doesn't return any value, but it continues to run until the
// context is cancelled.
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
			for _, msg := range s.Messages {
				go r.processMessage(ctx, msg)
			}
		}
	}
}

// autoClaimLoop runs a periodic reclaim process for pending messages in the Redis stream.
// It continuously attempts to reclaim and process messages that have not been acknowledged
// within the specified reclaim interval.
//
// The function runs indefinitely until the provided context is cancelled.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// This function does not return any value.
func (r *RedisStream) autoClaimLoop(ctx context.Context) {
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
// It manages concurrency, calls the user-defined handler, and performs skip or process
// operations based on the message status and handler result.
//
// The function follows these steps:
// 1. Manages concurrency using a channel if configured.
// 2. Converts message fields and checks for required 'json' field.
// 3. Calls the user-defined handler if set.
// 4. Handles message failure or processes the message using a Lua script.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - msg: A redis.XMessage containing the message data from the Redis stream.
//
// The function doesn't return any value but handles message processing internally,
// including error logging and debug information.
func (r *RedisStream) processMessage(ctx context.Context, msg redis.XMessage) {
	if r.concurrencyCh != nil {
		r.concurrencyCh <- struct{}{}
		defer func() { <-r.concurrencyCh }()
	}

	// (A) Possibly call user handler first to see if success/fail
	// Alternatively, you can do the "skip check" first.
	// Below, we'll do the user handler first for a typical flow.

	// 1) Convert fields
	fields := convertFields(msg.Values)
	_, ok := fields["json"]
	if !ok {
		r.err(ctx, "missing 'json' field:", msg)
		return
	}

	// 2) User-defined handler
	var handlerErr error
	if r.handler != nil {
		handlerErr = r.handler(ctx, fields)
	}

	// 3) Build Lua call arguments
	// skipOrProcess.lua => we either "skip" if in ZSET, or if not in ZSET, do XACK+XDEL+ZADD
	// We also consider success or fail.
	// If fail => we skip the "ZADD" portion.
	// => We'll do a minor tweak: if handlerErr != nil, we won't call skipOrProcess, we'll call handleFail instead.

	if handlerErr != nil {
		// we fail => handleFail in 1 atomic call
		r.handleFail(ctx, msg, handlerErr)
		return
	}

	// if success => call skipOrProcess, but specifically we want to mark "processed"
	// We'll pass a separate arg so the script knows we want to "mark processed" not just "skip check".
	// For simplicity, let's do the skip-check anyway (in case we didn't do it earlier).
	skipOrProcessRes, err := r.Client.EvalSha(
		ctx,
		r.luaScripts.SkipOrProcessSha,
		[]string{
			processedSetKey(r.Cfg), // KEYS[1] => processedZsetKey
			r.Cfg.StreamName,       // KEYS[2] => stream name
			r.Cfg.GroupName,        // KEYS[3] => group name
		},
		// ARGV
		uniqueIDForMessage(r.Cfg, msg.ID),             // ARGV[1] => unique ID in ZSET
		fmt.Sprintf("%f", float64(time.Now().Unix())), // ARGV[2] => Score (current time)
		msg.ID, // ARGV[3] => Redis message ID
	).Result()

	if err != nil {
		// fallback or log error
		r.err(ctx, "skipOrProcess script error => fallback ack?", err)
		// You might do a fallback XACK+XDEL, but that might cause duplicates. Up to you.
		return
	}

	switch skipOrProcessRes {
	case "skip":
		// means we already processed it => nothing more to do
		r.debug(ctx, "skip => already processed =>", msg.ID)
	case "processed":
		// The script has done XACK+XDEL + ZADD => we're good
		r.debug(ctx, "processed => script ack+del =>", msg.ID)
	default:
		r.err(ctx, "unknown result from skipOrProcess =>", skipOrProcessRes)
	}
}

// handleFail processes a failed message by incrementing its attempt count and
// determining whether to move it to a Dead Letter Queue (DLQ) or keep it in the
// Pending Entries List (PEL) with a backoff.
//
// The function uses a Lua script to atomically handle the failure, which includes
// incrementing attempts, potentially acknowledging and deleting the message if
// attempts are exceeded, and setting a backoff if the message remains in the PEL.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - msg: The redis.XMessage that failed processing.
//   - handlerErr: The error that occurred during message processing.
//
// The function doesn't return any value but handles the failed message internally,
// including potential DLQ operations and error logging.
func (r *RedisStream) handleFail(ctx context.Context, msg redis.XMessage, handlerErr error) {
	attemptsKey := reclaimAttemptsKey(r.Cfg)
	nowSec := time.Now().Unix()

	// We'll pass some arguments the script needs
	// handleFail.lua =>
	// KEYS[1] => attemptsKey
	// KEYS[2] => streamName
	// KEYS[3] => groupName
	// ARGV[1] => msg.ID
	// ARGV[2] => r.Cfg.MaxReclaimAttempts
	// ARGV[3] => nowSec
	// ARGV[4] => (optional) we can pass ReclaimMaxExponentialFactor or base backoff, if the script is using it.
	// We'll keep it simple and not do full code there.

	// We'll do a quick "DLQHandler" approach in the script if needed.
	// Or we can do a partial approach: if script returns "exceeded", we do a separate DLQ call.

	scriptRes, err := r.Client.EvalSha(ctx,
		r.luaScripts.HandleFailSha,
		[]string{
			attemptsKey,
			r.Cfg.StreamName,
			r.Cfg.GroupName,
		},
		msg.ID,
		fmt.Sprintf("%d", r.Cfg.MaxReclaimAttempts),
		fmt.Sprintf("%d", nowSec),
		// we could pass more if needed
	).Result()

	if err != nil {
		r.err(ctx, "handleFail script error =>", err)
		return
	}

	switch scriptRes {
	case "exceeded":
		// The script did XACK+XDEL and removed attempts fields
		r.err(ctx, "msg exceeded attempts => removed from stream:", msg, handlerErr)
		// if we want to do a go-level DLQHandler call here:
		if r.Cfg.DLQHandler != nil {
			dlqErr := r.Cfg.DLQHandler(ctx, &msg)
			if dlqErr != nil && !r.Cfg.IgnoreDLQHandlerErrors {
				r.err(ctx, "DLQ handler fail => message stays removed though, or revert? up to design", dlqErr)
			}
		}
	default:
		// e.g. "failed" => script set nextBackoff => remain in PEL
		r.err(ctx, "handler fail => remain in PEL =>", scriptRes, msg, handlerErr)
	}
}

// extendLockLoop periodically extends the distributed lock for a message until the context is cancelled or the lock extension fails.
// It runs in a separate goroutine to maintain the lock while the message is being processed.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - msgID: A string representing the ID of the message being processed.
//   - m: A pointer to a redsync.Mutex representing the distributed lock.
//   - cancel: A context.CancelFunc to cancel the parent context if lock extension fails.
//
// The function doesn't return any value but continues to extend the lock until the context is done or an error occurs.
func (r *RedisStream) extendLockLoop(ctx context.Context, msgID string, m *redsync.Mutex, cancel context.CancelFunc) {
	tick := time.NewTicker(r.LockExtend)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			ok, err := m.ExtendContext(ctx)
			if err != nil || !ok {
				r.err(ctx, "lock extend fail => stopping for msg:", msgID, err)
				cancel()
				return
			}
			r.debug(ctx, "lock extended for msg:", msgID)
		}
	}
}

// reclaimPending attempts to reclaim and process pending messages in the Redis stream.
// It uses XAUTOCLAIM to fetch pending messages, processes them if they're not in backoff,
// and manages the reclaim process state. If no messages are reclaimed for a certain number
// of attempts, it triggers a full pending message read.
//
// The function optionally uses a distributed lock to ensure only one node performs the reclaim
// operation at a time. It also manages the next start ID for subsequent reclaim operations
// and cleans up old reclaim records.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// The function doesn't return any value but processes reclaimed messages internally
// and updates the reclaim state.
func (r *RedisStream) reclaimPending(ctx context.Context) {
	if r.Rs != nil && r.Cfg.UseDistributedLock {
		// we do a small lock to ensure only 1 node reclaims
		reclaimLock := r.Rs.NewMutex(
			reclaimLockKey(r.Cfg),
			redsync.WithExpiry(r.LockExpiry),
			redsync.WithTries(1),
		)
		if err := reclaimLock.LockContext(ctx); err != nil {
			return
		}
		defer func() {
			if _, uErr := reclaimLock.Unlock(); uErr != nil {
				r.err(ctx, "unlock fail reclaim-lock:", uErr)
			}
		}()
	}

	startKey := reclaimNextStartKey(r.Cfg)
	stored, _ := r.Client.Get(ctx, startKey).Result()
	if stored == "" {
		stored = "0-0"
	}

	attemptsKey := reclaimAttemptsKey(r.Cfg)
	startID := stored

	totalReclaimed := 0

	for {
		claimed, newNextStart, err := r.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   r.Cfg.StreamName,
			Group:    r.Cfg.GroupName,
			Consumer: r.Cfg.ConsumerName,
			MinIdle:  0,
			Start:    startID,
			Count:    r.Cfg.ReclaimCount,
		}).Result()
		if err != nil {
			r.err(ctx, "XAutoClaim error:", err)
			return
		}
		r.debug(ctx, "reclaimed =>", len(claimed), " next =>", newNextStart)
		totalReclaimed += len(claimed)

		pipe := r.Client.Pipeline()

		for _, m := range claimed {
			backoffKey := m.ID + ":nextBackoffSec"
			val, _ := pipe.HGet(ctx, attemptsKey, backoffKey).Result()
			if val != "" {
				nextSec, _ := strconv.ParseInt(val, 10, 64)
				if time.Now().Unix() < nextSec {
					r.debug(ctx, "skip, still in backoff =>", m.ID)
					continue
				}
			}
			// handle once more
			go r.processMessage(ctx, m)
		}

		if isGreaterID(newNextStart, startID) {
			pipe.Set(ctx, startKey, newNextStart, 0)
		}
		if _, e := pipe.Exec(ctx); e != nil {
			r.err(ctx, "reclaim pipeline fail:", e)
		}

		if len(claimed) < int(r.Cfg.ReclaimCount) || !isGreaterID(newNextStart, startID) {
			break
		}
		startID = newNextStart
	}

	// no progress => increment
	if totalReclaimed == 0 {
		r.noProgressCount++
		r.debug(ctx, "no progress => noProgressCount=", r.noProgressCount)
		if r.noProgressCount >= r.Cfg.NoProgressThreshold {
			r.debug(ctx, "trigger readPendingMessagesOnce => noProgress")
			r.readPendingMessagesOnce(ctx)
			r.noProgressCount = 0
		}
	} else {
		r.noProgressCount = 0
	}

	r.cleanupOldReclaimRecords(ctx, attemptsKey)
}

// cleanupOldReclaimRecords removes older attempt records from the Redis hash.
// It iterates through all fields in the hash, identifies timestamp fields,
// and removes records that are older than the specified cleanup duration.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//   - key: A string representing the Redis key for the hash containing reclaim records.
//
// The function doesn't return any value but performs cleanup operations internally,
// removing old records and logging any errors encountered during the process.
func (r *RedisStream) cleanupOldReclaimRecords(ctx context.Context, key string) {
	all, err := r.Client.HGetAll(ctx, key).Result()
	if err != nil {
		r.err(ctx, "cleanup - HGetAll failed:", err)
		return
	}
	nowSec := time.Now().Unix()

	pipe := r.Client.Pipeline()
	for field, val := range all {
		if strings.HasSuffix(field, ":ts") {
			ts, convErr := strconv.ParseInt(val, 10, 64)
			if convErr != nil {
				continue
			}
			if float64(nowSec-ts) > r.CleanOldReclaimDuration.Seconds() {
				baseID := strings.TrimSuffix(field, ":ts")
				pipe.HDel(ctx, key,
					field,
					baseID,
					baseID+":count",
					baseID+":nextBackoffSec",
				)
			}
		}
	}
	if _, e := pipe.Exec(ctx); e != nil {
		r.err(ctx, "cleanup pipeline fail:", e)
	}
}

// HealthCheck performs a health check on the Redis connection by sending a PING command.
// It verifies if the Redis server is responsive and the connection is active.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// Returns:
//   - error: An error if the PING command fails or the connection is not healthy,
//     nil if the health check is successful.
func (r *RedisStream) HealthCheck(ctx context.Context) error {
	_, err := r.Client.Ping(ctx).Result()
	return err
}

// cleanupProcessedIDs removes processed message IDs from the Redis sorted set
// that are older than the specified maximum age. This helps to maintain the size
// of the processed IDs set and remove unnecessary old entries.
//
// The function uses ZREMRANGEBYSCORE to remove entries with scores (timestamps)
// older than the cutoff time determined by ProcessedIdsMaxAge.
//
// Parameters:
//   - ctx: A context.Context for handling cancellation and timeouts.
//
// The function doesn't return any value but logs debug information about the
// number of removed entries and any errors encountered during the cleanup process.
func (r *RedisStream) cleanupProcessedIDs(ctx context.Context) {
	k := processedSetKey(r.Cfg)
	cutoff := float64(time.Now().Add(-r.ProcessedIdsMaxAge).Unix())

	removed, err := r.Client.ZRemRangeByScore(ctx, k, "0", fmt.Sprintf("%f", cutoff)).Result()
	if err != nil {
		r.err(ctx, "ZRemRangeByScore error =>", err)
		return
	}
	if removed > 0 {
		r.debug(ctx, "cleanup => removed", removed, "older than", r.ProcessedIdsMaxAge)
	}
}
