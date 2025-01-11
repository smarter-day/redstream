package redstream

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type LogFn func(ctx context.Context, args ...interface{}) error
type DLQHandlerFn func(ctx context.Context, msg *redis.XMessage) error

type IRedStream interface {
	StartConsumer(ctx context.Context) error
	RegisterHandler(handler func(ctx context.Context, msg map[string]string) error)
	Publish(ctx context.Context, data any) (string, error)
	HealthCheck(ctx context.Context) error

	UseDebug(fn LogFn)
	UseInfo(fn LogFn)
	UseError(fn LogFn)
}
