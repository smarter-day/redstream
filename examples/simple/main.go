package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/smarter-day/redstream"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a RedStream config
	cfg := redstream.Config{
		StreamName:    "myStream",
		GroupName:     "myGroup",
		ConsumerName:  redstream.UniqueConsumerName("consumer"),
		EnableReclaim: true, // Let it do XAUTOCLAIM on idle messages
		// (Optionally override other fields if needed)
	}

	// Use UniversalOptions for single node, cluster, or sentinel
	uniOpts := &redis.UniversalOptions{
		Addrs: []string{"localhost:6379"},
	}

	// Construct the stream
	stream := redstream.New(uniOpts, cfg)

	// If ROLE=subscriber, we start a consumer
	if os.Getenv("ROLE") == "subscriber" {
		stream.RegisterHandler(func(ctx context.Context, msg map[string]string) error {
			log.Println("[HANDLER] Processing:", msg)
			// random fail ~1/11 chance
			if rand.Intn(11) == 0 {
				return fmt.Errorf("random failure in handler")
			}
			time.Sleep(1 * time.Second)
			return nil
		})

		if err := stream.StartConsumer(ctx); err != nil {
			log.Fatal("[FATAL] cannot start consumer:", err)
		}
	}

	// If ROLE=publisher, we publish messages
	if os.Getenv("ROLE") == "publisher" {
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
				payload := map[string]any{"id": i, "foo": "bar"}
				id, err := stream.Publish(ctx, payload)
				if err != nil {
					log.Println("[ERROR] Publish:", err)
				} else if id == "" {
					log.Println("[INFO] Publish skipped (duplicate lock).")
				} else {
					log.Println("[INFO] Published msg:", id)
				}
				time.Sleep(delay)
			}
		}()
	}

	// Block forever
	select {}
}
