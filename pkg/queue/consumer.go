package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

type JobHandler func(message map[string]interface{}) error
type ConsumerCleanup func()

func (b *BizzMQ) ConsumeMessageFromQueue(ctx context.Context, queuename string, callback JobHandler) (ConsumerCleanup, error) {
	if queuename == "" {
		return nil, fmt.Errorf("❌ Queue name not provided")
	}
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	queueKey := fmt.Sprintf("queue:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()

	//Create an instance for subscriber
	subscriber := redis.NewClient(b.redis.GetRedisClient().Options())

	queueOptionsMap, err := redisClientInstance.HGetAll(ctx, queueMetaKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get queue options: %w", err)
	}

	deadLetterQueueValue := queueOptionsMap["dead_letter_queue"]
	useDeadLetterQueue := deadLetterQueueValue == "true" || deadLetterQueueValue == "1"

	maxRetries := 3 //Default value is 3
	if maxRetriesStr, ok := queueOptionsMap["maxRetries"]; ok {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	processMessage := func(messageStr string) {
		var parsedMessage map[string]interface{}
		if err := json.Unmarshal([]byte(messageStr), &parsedMessage); err != nil {
			log.Printf("❌ Failed to parse message: %v", err)
			return
		}

		if err := callback(parsedMessage); err != nil {
			if useDeadLetterQueue {
				if maxRetries > 0 {
					if err := b.requeueMessage(consumerCtx, queuename, messageStr, err); err != nil {
						log.Printf("❌ Error retrying message: %v", err)
					}
				} else {
					if err := b.moveMessageToDLQ(consumerCtx, queuename, messageStr, err); err != nil {
						log.Printf("❌ Error moving message to DLQ: %v", err)
					}
				}
			} else {
				log.Printf("⚠️ Message failed but no DLQ configured")
			}
		}
	}

	processExistingJobs := func() {
		for {
			// Pop message from queue
			messageStr, err := redisClientInstance.RPop(consumerCtx, queueKey).Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("❌ Error popping message from queue: %v", err)
				}
				break
			}

			processMessage(messageStr)
		}
	}

	processExistingJobs()

	wg.Add(1)
	go func() {
		defer wg.Done()

		pubsub := subscriber.Subscribe(consumerCtx, queueKey)
		defer pubsub.Close()

		// Wait for confirmation of subscription
		_, err := pubsub.Receive(consumerCtx)
		if err != nil {
			log.Printf("❌ Failed to subscribe: %v", err)
			return
		}

		// Start receiving messages
		ch := pubsub.Channel()
		for {
			select {
			case msg := <-ch:
				fmt.Printf("🔔 New job notification received: %s\n", msg.Payload)
				messageStr, err := redisClientInstance.RPop(consumerCtx, queueKey).Result()
				if err == nil {
					processMessage(messageStr)
				} else if err != redis.Nil {
					log.Printf("❌ Error popping message after notification: %v", err)
				}
			case <-consumerCtx.Done():
				return
			}
		}
	}()

	fallbackTicker := time.NewTicker(5 * time.Second)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-fallbackTicker.C:
				messageStr, err := redisClientInstance.RPop(consumerCtx, queueKey).Result()
				if err == nil {
					fmt.Println("⚠️ Fallback found unprocessed job")
					processMessage(messageStr)
					processExistingJobs()
				} else if err != redis.Nil {
					log.Printf("❌ Error in fallback check: %v", err)
				}
			case <-consumerCtx.Done():
				return
			}
		}
	}()

	fmt.Printf("📡 Listening for jobs on %s...\n", queuename)

	// Return cleanup function
	return func() {
		cancel()              // Cancel the context to stop all operations
		fallbackTicker.Stop() // Stop the ticker
		wg.Wait()             // Wait for all goroutines to finish
		subscriber.Close()
	}, nil

}

func (b *BizzMQ) requeueMessage(ctx context.Context, queuename, message interface{}, processingErr error) error {
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()

	queueOptionsMap, err := redisClientInstance.HGetAll(ctx, queueMetaKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get queue options")
	}

	maxRetries := 3
	if maxRetriesStr, ok := queueOptionsMap["maxRetries"]; ok {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	//get the retry count
	var parsedMessage map[string]interface{}

	switch msg := message.(type) {
	case string:
		if err := json.Unmarshal([]byte(msg), &parsedMessage); err != nil {
			return fmt.Errorf("❌ Failed to parse message: %v", err)
		}
	case map[string]interface{}:
		parsedMessage = msg

	default:
		return fmt.Errorf("❌ Message must be of string or map")
	}

	var retryCount int
	if options, ok := parsedMessage["options"].(map[string]interface{}); ok {
		if count, ok := options["retryCount"].(float64); ok {
			retryCount = int(count)
		}
	}

	retryCount++
	if retryCount <= maxRetries {
		options, _ := parsedMessage["options"].(map[string]interface{})
		if options == nil {
			options = make(map[string]interface{})
		}

		options["retryCount"] = retryCount
		options["timestamp_updated"] = time.Now().UnixMilli()
		parsedMessage["options"] = options

		messageJSON, err := json.Marshal(parsedMessage)
		if err != nil {
			return fmt.Errorf("failed to marshal updated message")
		}

		queueKey := fmt.Sprintf("queue:%s", queuename)
		if err := redisClientInstance.LPush(ctx, queueKey, string(messageJSON)).Err(); err != nil {
			return fmt.Errorf("failed to push messages back to queue")
		}

		fmt.Printf("🔄 Message Requeued for retry ")
		return nil
	} else {
		err = b.moveMessageToDLQ(ctx, queuename, parsedMessage, processingErr)
		if err != nil {
			return fmt.Errorf("failed to publish message to queue")
		}

		return nil
	}
}

func (b *BizzMQ) moveMessageToDLQ(ctx context.Context, queuename, message interface{}, processingErr error) error {
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()
	queueOptionsMap, err := redisClientInstance.HGetAll(ctx, queueMetaKey).Result()

	if err != nil {
		return fmt.Errorf("failed to get queue options")
	}

	deadLetterQueueValue := false
	if deadLetterQueueValueStr, ok := queueOptionsMap["dead_letter_queue"]; ok {
		if val, err := strconv.ParseBool(deadLetterQueueValueStr); err == nil {
			deadLetterQueueValue = val
		}
	}
	if !deadLetterQueueValue {
		fmt.Println("No Dead Letter Queue configured for this Queue, Failed message discarded")
		return nil
	}

	dlqName := fmt.Sprintf("%s_dlq", queuename)

	var parsedMessage map[string]interface{}
	switch msg := message.(type) {
	case string:
		if err := json.Unmarshal([]byte(msg), &parsedMessage); err != nil {
			return fmt.Errorf("❌ Failed to parse message: %v", err)
		}
	case map[string]interface{}:
		parsedMessage = msg

	default:
		return fmt.Errorf("❌ Message must be of string or map")
	}

	parsedMessage["options"] = map[string]interface{}{
		"message":   processingErr.Error,
		"stack":     string(debug.Stack()),
		"timestamp": time.Now().UnixMilli(),
	}

	var messageData interface{}
	if data, ok := parsedMessage["data"]; ok {
		messageData = data
	} else {
		messageData = map[string]interface{}{"error": "No data found in original message"}
	}

	options := make(map[string]interface{})
	if opts, ok := parsedMessage["options"].(map[string]interface{}); ok {
		for k, v := range opts {
			options[k] = v
		}
	}

	options["originalQueue"] = queuename
	options["failedAt"] = time.Now().UnixMilli()

	msgOptions := MessageOptions{}
	optionsJson, err := json.Marshal(options)
	if err != nil {
		return fmt.Errorf("failed to marshal options")
	}

	if err := json.Unmarshal(optionsJson, &msgOptions); err != nil {
		return fmt.Errorf("failed to unmarshal options to Message Options")
	}

	_, err = b.PublishMessageToQueue(ctx, dlqName, messageData, msgOptions)
	if err != nil {
		return fmt.Errorf("failed to publish message to DLQ")
	}
	return nil
}
