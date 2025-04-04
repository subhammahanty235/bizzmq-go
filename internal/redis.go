package internal

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
	PrintWelcomeMessage()
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

func PrintWelcomeMessage() {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                                                            ║")
	fmt.Println("║   🚀 BizzMQ Queue System v1.0.0 (Early Release)            ║")
	fmt.Println("║         A lightweight message queue for go                 ║")
	fmt.Println("║                                                            ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Println("║                                                            ║")
	fmt.Println("║   ⚠️  EARLY VERSION NOTICE                                  ║")
	fmt.Println("║       This is an early version and may contain bugs.       ║")
	fmt.Println("║       Please report any issues you encounter to:           ║")
	fmt.Println("║       github.com/subhammahanty235/bizzmq-go/issues         ║")
	fmt.Println("║                                                            ║")
	fmt.Println("║   💡 USAGE                                                 ║")
	fmt.Println("║       1. Create queues                                     ║")
	fmt.Println("║       2. Publish messages                                  ║")
	fmt.Println("║       3. Set up consumers                                  ║")
	fmt.Println("║                                                            ║")
	fmt.Println("║   📚 For documentation visit:                              ║")
	fmt.Println("║       github.com/subhammahanty235/bizzmq-go                ║")
	fmt.Println("║                                                            ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
}
