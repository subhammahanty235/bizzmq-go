package bizzmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type QueueOptions struct {
	ConfigDeadLetterQueue bool `json:"config_dead_letter_queue"`
	Retry                 int  `json:"retry,omitempty"`
	MaxRetries            int  `json:"maxRetries,omitempty"`
	// Add other options as needed
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

func (b *BizzMQ) GetDeadLetterMessages(ctx context.Context, queuename string, limit int) (string, error) {
	// 0 will be counted as a default value, as in this case we will mark is as 100
	if limit == 0 {
		limit = 100
	}

	dlqKey := fmt.Sprintf("queue:%s", queuename)
	dlqMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()
	exists, err := redisClientInstance.Exists(ctx, dlqMetaKey).Result()
	if exists == 0 {
		return " Dead Letter Queue for %s does not exists", fmt.Errorf("❌ Dead Letter Queue for %s does not exists", queuename)
	}
	if err != nil {
		return "❌ Error while ferching dead letter queue.", fmt.Errorf("❌ Error while ferching dead letter queue")
	}

	messages, err := redisClientInstance.LRange(ctx, dlqKey, 0, int64(limit-1)).Result()

	if err != nil {
		return "❌ Error while fetching dead letter messages", fmt.Errorf("❌ Error while fetching dead letter messages: %v", err)
	}

	jsonData, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return "", fmt.Errorf("❌ Failed to marshal messages to JSON: %v", err)
	}

	return string(jsonData), nil

}

func (b *BizzMQ) RetryDeadLetterMessage(ctx context.Context, queuename string, messageId string) (bool, error) {
	dlqKey := fmt.Sprintf("queue:%s", queuename)
	dlqMetaKey := fmt.Sprintf("queue_meta:%s", queuename)

	redisClientInstance := b.redis.GetRedisClient()

	exists, err := redisClientInstance.Exists(ctx, dlqMetaKey).Result()
	if exists == 0 {
		return false, fmt.Errorf("❌ Dead Letter Queue for %s does not exists", queuename)
	}
	if err != nil {
		return false, fmt.Errorf("❌ Error while ferching dead letter queue")
	}
	messages, err := redisClientInstance.LRange(ctx, dlqKey, 0, -1).Result()

	if err != nil {
		return false, fmt.Errorf("failed to get messages from DLQ")
	}

	for _, messageStr := range messages {
		var message map[string]interface{}
		if err := json.Unmarshal([]byte(messageStr), &message); err != nil {
			//we will skip all malformed messages
			continue
		}

		msgId, _ := message["message_Id"].(string)
		if msgId == messageId {
			removed, err := redisClientInstance.LRem(ctx, dlqKey, 1, messageStr).Result()
			if err != nil {
				return false, fmt.Errorf("failed tp remove message from DLQ")
			}

			if removed == 0 {
				return false, fmt.Errorf("message found but could not be removed (possibly already processed)")
			}

			message["timestamp_updated"] = time.Now().UnixMilli()

			var messageData interface{}
			if data, ok := message["data"]; ok {
				messageData = data
			} else {
				messageData = map[string]interface{}{"error": "No data found in original message"}
			}

			options := make(map[string]interface{})
			if opts, ok := message["options"].(map[string]interface{}); ok {
				for k, v := range opts {
					options[k] = v
				}
			}

			options["retries"] = 0
			msgOptions := MessageOptions{}
			optionsJson, err := json.Marshal(options)
			if err != nil {
				return false, fmt.Errorf("failed to marshal options")
			}

			if err := json.Unmarshal(optionsJson, &msgOptions); err != nil {
				return false, fmt.Errorf("failed to unmarshal options to Message Options")
			}

			_, err = b.PublishMessageToQueue(ctx, queuename, messageData, msgOptions)
			if err != nil {
				return false, fmt.Errorf("failed to publish message to DLQ")
			}

			fmt.Printf("🔄 Message %s moved from DLQ back to \"%s\"\n", messageId, queuename)
			return true, nil

		}
	}

	return false, fmt.Errorf("message with ID %s not found in DLQ", messageId)

}
