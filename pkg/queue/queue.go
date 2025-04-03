package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/subhammahanty235/pkg/storage"
)

type BizzMQ struct {
	redis *storage.RedisClient
}

type QueueOptions struct {
	ConfigDeadLetterQueue bool `json:"config_dead_letter_queue"`
	Retry                 int  `json:"retry,omitempty"`
	MaxRetries            int  `json:"maxRetries,omitempty"`
	// Add other options as needed
}

func NewBizzMq(redisUri string) (*BizzMQ, error) {
	if redisUri == "" {
		return nil, errors.New("redis URL is required")
	}

	redisInstance, error := storage.NewRedisClient(redisUri)
	if error != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", error)
	}

	return &BizzMQ{
		redis: redisInstance,
	}, nil
}

func (b *BizzMQ) CreateQueue(ctx context.Context, queuename string, options QueueOptions) error {
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()
	exists, err := redisClientInstance.Exists(ctx, queueMetaKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check if queue exists: %w", err)
	}
	if exists == 1 {
		fmt.Printf("✅ Queue \"%s\" already exists.\n", queuename)
		return nil
	}

	queueDataBytes, err := json.Marshal(options)
	if err != nil {
		return fmt.Errorf("failed to marshal options: %w", err)
	}

	queueData := map[string]interface{}{
		"createdAt": time.Now().UnixMilli(),
	}

	var optionsMap map[string]interface{}
	if err := json.Unmarshal(queueDataBytes, &optionsMap); err != nil {
		return fmt.Errorf("failed to unmarshal options: %w", err)
	}

	for k, v := range optionsMap {
		queueData[k] = v
	}

	if err := redisClientInstance.HSet(ctx, queueMetaKey, queueData).Err(); err != nil {
		return fmt.Errorf("failed to create queue: %w", err)
	}

	fmt.Printf("📌 Queue \"%s\" created successfully.\n", queuename)
	return nil

}
