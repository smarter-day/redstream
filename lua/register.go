package lua

import (
	"context"
	"github.com/redis/go-redis/v9"
)

type Scripts struct {
	SkipOrProcessSha string
	HandleFailSha    string
}

func RegisterLuaScripts(ctx context.Context, client redis.Cmdable) (*Scripts, error) {
	// Load the scripts
	sha1, err := client.ScriptLoad(ctx, SkipOrProcessLua).Result()
	if err != nil {
		return nil, err
	}
	sha2, err := client.ScriptLoad(ctx, HandleFailLua).Result()
	if err != nil {
		return nil, err
	}
	return &Scripts{
		SkipOrProcessSha: sha1,
		HandleFailSha:    sha2,
	}, nil
}
