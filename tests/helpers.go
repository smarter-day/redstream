package tests

import (
	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/smarter-day/redstream"
	"github.com/smarter-day/redstream/mocks"
	"log"
	"time"
)

func NewRedStream(mockRedisClient *mocks.MockUniversalClient, cfg redstream.Config) redstream.IRedStream {
	rs := redsync.New(redsyncredis.NewPool(mockRedisClient))

	lockExpiry := redstream.ParseDurationOrDefault(&cfg.LockExpiryStr, 10*time.Second)
	lockExtend := redstream.ParseDurationOrDefault(&cfg.LockExtendStr, lockExpiry/2)
	blockDuration := redstream.ParseDurationOrDefault(&cfg.BlockDurationStr, 5*time.Second)
	reclaimInterval := redstream.ParseDurationOrDefault(&cfg.ReclaimStr, 1*time.Second)
	cleanOldReclaimDuration := redstream.ParseDurationOrDefault(&cfg.CleanOldReclaimDuration, 1*time.Hour)

	err := redstream.Validate.Struct(cfg)
	if err != nil {
		return nil
	}

	if cfg.MaxReclaimAttempts <= 0 {
		cfg.MaxReclaimAttempts = 3
	}
	if cfg.ReclaimCount <= 0 {
		cfg.ReclaimCount = 10
	}
	if lockExtend >= lockExpiry {
		log.Fatalf("Lock extend interval (%v) must be < lock expiry (%v)", lockExtend, lockExpiry)
	}
	if lockExtend < time.Duration(0.2*float64(lockExpiry)) {
		log.Fatalf("Lock extend interval (%v) must be >= 20%% of expiry (%v)", lockExtend, lockExpiry)
	}
	if err = redstream.CreateGroupIfNotExists(mockRedisClient, cfg.StreamName, cfg.GroupName); err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	return &redstream.RedisStream{
		Client:                  mockRedisClient,
		Rs:                      rs,
		Cfg:                     cfg,
		LockExpiry:              lockExpiry,
		LockExtend:              lockExtend,
		BlockDuration:           blockDuration,
		ReclaimInterval:         reclaimInterval,
		CleanOldReclaimDuration: cleanOldReclaimDuration,
	}
}
