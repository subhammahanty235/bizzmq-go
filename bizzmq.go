// Package bizzmq provides a Redis-based message queue system.
package bizzmq

import (
	"errors"
	"fmt"

	"github.com/subhammahanty235/bizzmq-go/internal"
)

type BizzMQ struct {
	redis *internal.RedisClient
}

func NewBizzMQ(redisUri string) (*BizzMQ, error) {
	//* redis uri will be passed from the client end as it's the primary requirement
	if redisUri == "" {
		return nil, errors.New("redis URL is required")
	}
	redisInstance, err := internal.NewRedisClient(redisUri)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &BizzMQ{
		redis: redisInstance,
	}, nil
}

func (b *BizzMQ) Close() error {
	return b.redis.CloseRedisClient()
}
