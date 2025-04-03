package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient(redisUrl string) (*RedisClient, error) {
	if redisUrl == "" {
		return nil, errors.New("redis URL is required")
	}

	opts, err := redis.ParseURL(redisUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	fmt.Println("🔌 Connected to Redis Database")

	return &RedisClient{
		client: client,
	}, nil
}

func (r *RedisClient) GetRedisClient() *redis.Client {
	return r.client
}

func (r *RedisClient) CloseRedisClient() error {
	return r.client.Close()
}
