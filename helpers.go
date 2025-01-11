package redstream

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	RedisKeysPrefix = "redstream"
)

// ParseDurationOrDefault attempts to parse a string into a time.Duration.
// If parsing fails or results in a non-positive duration, it returns the default value.
//
// Parameters:
//   - s: A string representation of a duration (e.g., "5s", "1m30s").
//   - def: The default duration to return if parsing fails or results in a non-positive value.
//
// Returns:
//
//	A time.Duration parsed from the input string, or the default value if parsing fails.
func ParseDurationOrDefault(s *string, def time.Duration) time.Duration {
	d, err := time.ParseDuration(*s)
	if err != nil || d <= 0 {
		*s = def.String()
		return def
	}
	return d
}

// CreateGroupIfNotExists attempts to create a consumer group for a Redis stream if it doesn't already exist.
// It uses the XGroupCreateMkStream command, which creates both the stream and the group if they don't exist.
//
// Parameters:
//   - client: A pointer to a redis.Client instance used to execute Redis commands.
//   - stream: The name of the Redis stream to create or use.
//   - group: The name of the consumer group to create.
//
// Returns:
//   - error: nil if the group was created successfully or already exists, otherwise returns an error.
func CreateGroupIfNotExists(client redis.Cmdable, stream, group string) error {
	err := client.XGroupCreateMkStream(context.Background(), stream, group, "0").Err()
	if err != nil && !isGroupExistsErr(err) {
		return err
	}
	return nil
}

// isGroupExistsErr checks if an error is returned by XGroupCreateMkStream indicating that the group already exists.
func isGroupExistsErr(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "busy")
}

// convertFields converts a map with string keys and any values to a map with string keys and string values.
// It attempts to convert each value to a string, using fmt.Sprintf for non-string types.
//
// Parameters:
//   - in: A map[string]any representing the input map to be converted.
//
// Returns:
//   - A map[string]string where all values have been converted to strings.
func convertFields(in map[string]any) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if vs, ok := v.(string); ok {
			out[k] = vs
		} else {
			out[k] = fmt.Sprintf("%v", v)
		}
	}
	return out
}

// streamLockKey generates a unique lock key for a Redis stream based on the stream name and JSON string.
// It creates a SHA256 hash of the JSON string and uses the first 8 bytes of the hash in the key.
//
// Parameters:
//   - stream: The name of the Redis stream.
//   - jsonStr: A JSON string representing the data to be locked.
//
// Returns:
//
//	A string representing the unique lock key in the format "<RedisKeysPrefix>::streamLock::<stream>::<hash>".
func streamLockKey(stream, jsonStr string) string {
	sum := sha256.Sum256([]byte(jsonStr))
	return fmt.Sprintf("%s::streamLock::%s::%s", RedisKeysPrefix, stream, hex.EncodeToString(sum[:8]))
}

// reclaimAttemptsKey generates a Redis key for tracking reclaim attempts for a specific stream.
// This key is used to store the number of times a reclaim operation has been attempted.
//
// Parameters:
//   - cfg: A Config struct containing the configuration for the stream, including the StreamName.
//
// Returns:
//   - A string representing the Redis key in the format "<RedisKeysPrefix>::<StreamName>::reclaimAttempts".
func reclaimAttemptsKey(cfg Config) string {
	return fmt.Sprintf("%s::%s::reclaimAttempts", RedisKeysPrefix, cfg.StreamName)
}

// reclaimLockKey generates a Redis key for the reclaim lock of a specific stream.
// This key is used to ensure that only one process can perform the reclaim operation at a time.
//
// Parameters:
//   - cfg: A Config struct containing the configuration for the stream, including the StreamName.
//
// Returns:
//   - A string representing the Redis key in the format "<RedisKeysPrefix>::<StreamName>::reclaimLock".
func reclaimLockKey(cfg Config) string {
	return fmt.Sprintf("%s::%s::reclaimLock", RedisKeysPrefix, cfg.StreamName)
}

// lastReclaimKey generates a Redis key for storing the timestamp of the last reclaim operation for a specific stream.
// This key is used to track when the last reclaim operation was performed on the stream.
//
// Parameters:
//   - cfg: A Config struct containing the configuration for the stream, including the StreamName.
//
// Returns:
//   - A string representing the Redis key in the format "<RedisKeysPrefix>::<StreamName>::lastReclaimTime".
func lastReclaimKey(cfg Config) string {
	return fmt.Sprintf("%s::%s::lastReclaimTime", RedisKeysPrefix, cfg.StreamName)
}

// reclaimNextStartKey generates a Redis key for storing the next starting point for reclaim operations on a specific stream.
// This key is used to keep track of where the next reclaim operation should begin in the stream.
//
// Parameters:
//   - cfg: A Config struct containing the configuration for the stream, including the StreamName.
//
// Returns:
//   - A string representing the Redis key in the format "<RedisKeysPrefix>::<StreamName>::reclaimNextStart".
func reclaimNextStartKey(cfg Config) string {
	return fmt.Sprintf("%s::%s::reclaimNextStart", RedisKeysPrefix, cfg.StreamName)
}

// publishLockKey generates a unique lock key for publishing to a Redis stream.
// It creates a SHA256 hash of the raw data and uses it to form a unique identifier.
//
// Parameters:
//   - name: A string representing the name or identifier for the lock.
//   - raw: A byte slice containing the raw data to be hashed.
//
// Returns:
//   - interface{}: A string representing the unique lock key in the format
//     "<RedisKeysPrefix>::<name>::publishLockKey::<hash>", where <hash> is the
//     first 16 characters of the hexadecimal representation of the SHA256 hash.
func publishLockKey(name string, raw []byte) string {
	h := sha256.Sum256(raw)
	return fmt.Sprintf("%s::%s::publishLockKey::%s", RedisKeysPrefix, name, hex.EncodeToString(h[:]))
}

// ackAndDelete acknowledges and deletes a message from a Redis stream.
// It pipelines the XACK and XDEL commands to remove the message from both
// the Pending Entries List (PEL) and the stream itself.
//
// Parameters:
//   - ctx: The context for the operation, which can be used for cancellation.
//   - pipe: A Redis pipeliner used to execute multiple Redis commands in a single round-trip.
//   - msgID: The ID of the message to be acknowledged and deleted.
//
// This method does not return any value. The pipeline commands are queued
// and will be executed when the pipeline is flushed.
func (r *RedisStream) ackAndDelete(ctx context.Context, pipe redis.Pipeliner, msgID string) {
	pipe.XAck(ctx, r.Cfg.StreamName, r.Cfg.GroupName, msgID)
	pipe.XDel(ctx, r.Cfg.StreamName, msgID)
}

// isGreaterID compares two Redis stream IDs and determines if the first ID is greater than the second.
// It performs a lexicographical comparison of the IDs, which are typically in the format "timestamp-sequence".
//
// Parameters:
//   - a: A string representing the first Redis stream ID to compare.
//   - b: A string representing the second Redis stream ID to compare.
//
// Returns:
//
//	A boolean value:
//	- true if 'a' is greater than 'b'.
//	- false if 'a' is less than or equal to 'b'.
//
// The comparison is done by splitting each ID into its timestamp and sequence components.
// If the splitting fails, it falls back to a direct string comparison.
func isGreaterID(a, b string) bool {
	if a == b {
		return false
	}
	// Split "A-B" => [A,B]
	aParts := strings.SplitN(a, "-", 2)
	bParts := strings.SplitN(b, "-", 2)
	if len(aParts) < 2 || len(bParts) < 2 {
		// fallback: direct string compare
		return a > b
	}
	// parse int64
	aMS, _ := strconv.ParseInt(aParts[0], 10, 64)
	aSeq, _ := strconv.ParseInt(aParts[1], 10, 64)
	bMS, _ := strconv.ParseInt(bParts[0], 10, 64)
	bSeq, _ := strconv.ParseInt(bParts[1], 10, 64)

	if aMS == bMS {
		return aSeq > bSeq
	}
	return aMS > bMS
}

// UniqueConsumerName generates a unique consumer name for use in Redis streams.
// It combines a base name with the hostname, process ID, and a random suffix
// to ensure uniqueness across different instances and executions.
//
// Parameters:
//   - base: A string that serves as the prefix for the generated consumer name.
//
// Returns:
//
//	A string representing a unique consumer name in the format:
//	"<base>-<hostname>-<pid>-<random_suffix>"
func UniqueConsumerName(base string) string {
	hostname, _ := os.Hostname()
	suffix := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(int(time.Now().Unix()))
	return fmt.Sprintf("%s-%s-%d-%d", base, hostname, os.Getpid(), suffix)
}
