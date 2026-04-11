package bizzmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func (b *BizzMQ) PublishMessageToQueue(ctx context.Context, queuename string, message interface{}, options MessageOptions) (string, error) {
	if queuename == "" {
		return "", fmt.Errorf("❌ Queue name not provided")
	}
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	queueKey := fmt.Sprintf("queue:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()
	exists, err := redisClientInstance.Exists(ctx, queueMetaKey).Result()
	if err != nil {
		return "", fmt.Errorf("failed to check if queue exists: %w", err)
	}

	if exists == 0 {
		return "", fmt.Errorf("❌ Queue \"%s\" does not exist. Create it first", queuename)
	}

	messageId := fmt.Sprintf("message:%d", time.Now().UnixMilli())
	messageObj := NewMessage(queuename, messageId, message, options)
	messageJSON, err := json.Marshal(messageObj.ToJSON())
	if err != nil {
		return "", fmt.Errorf("failed to marshal message: %w", err)
	}

	timestamp := float64(time.Now().UnixMilli())
	score := float64(options.Priority)*1e10 + timestamp
	if err := redisClientInstance.ZAdd(ctx, queueKey, &redis.Z{
		Score:  score,
		Member: string(messageJSON),
	}).Err(); err != nil {
		return "", fmt.Errorf("failed to push message to queue: %w", err)
	}

	if err := redisClientInstance.Publish(ctx, queueKey, messageId).Err(); err != nil {
		return "", fmt.Errorf("failed to publish notification: %w", err)
	}

	fmt.Printf("📩 Job added to queue \"%s\" - ID: %s\n", queuename, messageId)
	return messageId, nil

}
