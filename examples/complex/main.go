package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smarter-day/redstream"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// MessageLog tracks each message by user-level ID.
//
// - repeatedWrites increments only if the same message is successfully processed again.
// - failedRetries increments each time the message fails.
type MessageLog struct {
	ID             uint   `gorm:"primaryKey"`
	MsgID          string `gorm:"uniqueIndex"` // user-level ID
	Payload        string
	RepeatedWrites int // increments if we re-process a message successfully
	FailedRetries  int // increments each time we fail
	CreatedAt      time.Time
}

// DLQMessageLog tracks messages that end up in Dead Letter Queue.
type DLQMessageLog struct {
	ID        uint   `gorm:"primaryKey"`
	RedisID   string // The XMessage.ID from Redis
	Payload   string // raw JSON from the "json" field
	Reason    string // e.g. "exceeded max attempts"
	CreatedAt time.Time
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) Connect to Postgres via GORM
	dsn := "host=db user=postgres password=secret dbname=testdb port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("[FATAL] cannot open Postgres:", err)
	}

	// 2) Auto-migrate our tables
	if err := db.AutoMigrate(&MessageLog{}, &DLQMessageLog{}); err != nil {
		log.Fatal("[FATAL] cannot migrate schema:", err)
	}

	consumerName := os.Getenv("CONSUMER_NAME")
	if consumerName == "" {
		consumerName = "redstream-consumer"
	}

	// 3) RedStream config
	cfg := redstream.Config{
		StreamName:         "myStream",
		GroupName:          "myGroup",
		ConsumerName:       consumerName,
		EnableReclaim:      true,
		LockExpiryStr:      "5s",
		ReclaimStr:         "3s",
		MaxReclaimAttempts: 1, // after 1 failure, next pass => DLQ
		MaxConcurrency:     10,

		// A DLQ handler logs messages to the DLQMessageLog table
		DLQHandler: func(ctx context.Context, xMsg *redis.XMessage) error {
			rawJSON, _ := xMsg.Values["json"].(string)
			log.Printf("[DLQ] Exceeded attempts. RedisID=%s raw=%s\n", xMsg.ID, rawJSON)

			dlqEntry := DLQMessageLog{
				RedisID: xMsg.ID,
				Payload: rawJSON,
				Reason:  "exceeded max attempts",
			}
			if err := db.Create(&dlqEntry).Error; err != nil {
				log.Println("[ERROR] inserting DLQ record:", err)
				// If IgnoreDLQHandlerErrors=false, returning an error
				// would keep the message pending.
				return err
			}
			return nil
		},
	}

	// 4) Redis connection
	uniOpts := &redis.UniversalOptions{Addrs: []string{"redis:6379"}}
	stream := redstream.New(uniOpts, cfg)

	// Track last time a message was fully processed (success).
	var lastMessageTime int64
	atomic.StoreInt64(&lastMessageTime, time.Now().Unix())

	// For computing a rate in printStats
	startTime := time.Now()

	role := os.Getenv("ROLE")
	if role == "subscriber" {
		// 5a) Handle messages
		stream.RegisterHandler(func(ctx context.Context, msg map[string]string) error {
			// The raw JSON payload is in msg["json"]
			rawJSON, ok := msg["json"]
			if !ok {
				return fmt.Errorf("missing 'json' key")
			}

			// Parse user-level ID from JSON
			var parsed map[string]any
			if err := json.Unmarshal([]byte(rawJSON), &parsed); err != nil {
				return fmt.Errorf("json unmarshal fail: %w", err)
			}

			userIDRaw, ok := parsed["id"]
			if !ok {
				return fmt.Errorf("missing 'id' in payload JSON")
			}
			userID := fmt.Sprintf("%v", userIDRaw)

			log.Println("[HANDLER] Processing ID=", userID)

			// Random fail ~1/15 chance to test reclaim & DLQ
			if rand.Intn(15) == 0 {
				// If we fail, we want to record 'failedRetries++'
				recordFail(db, userID, rawJSON)
				return fmt.Errorf("random handler failure for ID=%s", userID)
			}

			// Otherwise, success => record an upsert
			recordSuccess(db, userID, rawJSON)

			// Track last success time for stats
			atomic.StoreInt64(&lastMessageTime, time.Now().Unix())
			return nil
		})

		if err := stream.StartConsumer(ctx); err != nil {
			log.Fatal("[FATAL] cannot start consumer:", err)
		}

		// 5b) Stats goroutine
		go func() {
			tick := time.NewTicker(10 * time.Second)
			defer tick.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					last := atomic.LoadInt64(&lastMessageTime)
					if time.Since(time.Unix(last, 0)) > 10*time.Second {
						printStats(db, startTime)
					}
				}
			}
		}()

	} else if role == "publisher" {
		time.Sleep(10 * time.Second) // wait for consumer to start

		// 5c) Publish messages
		go func() {
			iterations, _ := strconv.Atoi(os.Getenv("ITERATIONS"))
			if iterations <= 0 {
				iterations = 100
			}
			delay, _ := time.ParseDuration(os.Getenv("DELAY"))
			if delay <= 0 {
				delay = 1 * time.Second
			}

			for i := 0; i < iterations; i++ {
				payload := map[string]any{"id": fmt.Sprintf("%d", i), "foo": "bar"}
				id, err := stream.Publish(ctx, payload)
				if err != nil {
					log.Println("[ERROR] Publish:", err)
				} else if id == "" {
					log.Println("[INFO] Publish skipped (duplicate lock).")
				} else {
					log.Println("[INFO] Published msg with RedisID:", id)
				}
				time.Sleep(delay)
			}
		}()
	}

	// block forever
	select {}
}

// recordFail increments 'failedRetries' for the given userID (if already known),
// or creates a new record with failedRetries=1 if not found.
func recordFail(db *gorm.DB, userID string, rawJSON string) {
	var rec MessageLog
	tx := db.Where("msg_id = ?", userID).First(&rec)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		// create fresh with failedRetries=1
		rec = MessageLog{
			MsgID:          userID,
			Payload:        rawJSON,
			RepeatedWrites: 0,
			FailedRetries:  1,
		}
		if err := db.Create(&rec).Error; err != nil {
			log.Println("[ERROR] DB create fail:", err)
		}
	} else if tx.Error == nil {
		rec.FailedRetries++
		rec.Payload = rawJSON // update payload if you like
		if err := db.Save(&rec).Error; err != nil {
			log.Println("[ERROR] DB update fail:", err)
		}
	} else {
		log.Println("[ERROR] recordFail DB query:", tx.Error)
	}
}

// recordSuccess increments 'repeatedWrites' if the record already existed,
// or creates a new record with repeatedWrites=0 if it's the first time.
func recordSuccess(db *gorm.DB, userID string, rawJSON string) {
	var rec MessageLog
	tx := db.Where("msg_id = ?", userID).First(&rec)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		// create brand new => repeatedWrites=0 (first success)
		rec = MessageLog{
			MsgID:          userID,
			Payload:        rawJSON,
			RepeatedWrites: 0,
			FailedRetries:  0,
		}
		if err := db.Create(&rec).Error; err != nil {
			log.Println("[ERROR] DB create fail:", err)
		}
	} else if tx.Error == nil {
		// found => repeatedWrites++
		rec.RepeatedWrites++
		rec.Payload = rawJSON
		if err := db.Save(&rec).Error; err != nil {
			log.Println("[ERROR] DB save fail:", err)
		}
	} else {
		log.Println("[ERROR] recordSuccess DB query:", tx.Error)
	}
}

// printStats queries DB for total rows, how many had repeatedWrites>0,
// how many ended in the DLQ table, and a processing rate.
func printStats(db *gorm.DB, startTime time.Time) {
	var totalCount int64
	if err := db.Model(&MessageLog{}).Count(&totalCount).Error; err != nil {
		log.Println("[ERROR] counting total rows:", err)
		return
	}

	var repeatedCount int64
	if err := db.Model(&MessageLog{}).
		Where("repeated_writes > ?", 0).
		Count(&repeatedCount).Error; err != nil {
		log.Println("[ERROR] counting repeatedWrites rows:", err)
		return
	}

	var dlqCount int64
	if err := db.Model(&DLQMessageLog{}).Count(&dlqCount).Error; err != nil {
		log.Println("[ERROR] counting DLQ rows:", err)
		return
	}

	elapsedSec := time.Since(startTime).Seconds()
	rate := float64(totalCount) / elapsedSec

	log.Printf("[STATS] total: %d, repeatedWrites: %d, dlqCount: %d, rate: %.2f msg/sec\n",
		totalCount, repeatedCount, dlqCount, rate)
}
