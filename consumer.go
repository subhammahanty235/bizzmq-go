package bizzmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type JobHandler func(message map[string]interface{}) error
type ConsumerCleanup func()

func (b *BizzMQ) ConsumeMessageFromQueue(ctx context.Context, queuename string, callback JobHandler) (ConsumerCleanup, error) {
	if queuename == "" {
		return nil, fmt.Errorf("❌ Queue name not provided")
	}

	queueKey := fmt.Sprintf("queue:%s", queuename)
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)

	redisClient := b.redis.GetRedisClient()
	subscriber := redis.NewClient(redisClient.Options())
	queueOptionsMap, err := redisClient.HGetAll(ctx, queueMetaKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get queue options: %w", err)
	}

	deadLetterQueueValue := queueOptionsMap["config_dead_letter_queue"]
	useDeadLetterQueue := string(deadLetterQueueValue) == "1"
	maxRetries := 3 // Default value for the retries, TODO: Better approach *
	if maxRetriesStr, ok := queueOptionsMap["maxRetries"]; ok {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	// This function will process the jobs/messages
	processJob := func(message string) error {
		if message == "" {
			return nil
		}

		var messageObj Message
		if err := json.Unmarshal([]byte(message), &messageObj); err != nil {
			log.Printf("❌ Failed to parse message: %v", err)

			return err
		}
		//Lifecycle Update -- wating ---> processing
		messageObj.UpdateLifeCycleStatus("processing")

		// Extract the  actual message data to pass to callback
		messageData := make(map[string]interface{})

		switch m := messageObj.Message.(type) {
		case map[string]interface{}:
			messageData = m

		default:
			// If it's not already a map, put it in a data field
			messageData["data"] = messageObj.Message
		}

		if err := callback(messageData); err != nil {
			//LifeCycle Update --- processing --> failed
			messageObj.UpdateLifeCycleStatus("failed")
			if useDeadLetterQueue {
				if maxRetries > 0 {
					if err := b.requeueMessage(ctx, queuename, message, err); err != nil {
						log.Printf("❌ Error retrying message: %v", err)

					}
				} else {
					if err := b.moveMessageToDLQ(ctx, queuename, message, err); err != nil {
						log.Printf("❌ Error moving message to DLQ: %v", err)

					}
				}
			} else {
				log.Printf("⚠️ Message failed but no DLQ configured")
			}
			return err
		} else {
			//Temp Code for testing
			if err := b.requeueMessage(ctx, queuename, message, err); err != nil {
				log.Printf("❌ Error retrying message: %v", err)

			}
			//LifeCycle Updated --- processing --> processed
			messageObj.UpdateLifeCycleStatus("processed")
		}

		return nil
	}

	// we will check if any existing messages are there in the queue for processing, we will process them first
	processExistingJobs := func() {
		for {
			message, err := redisClient.RPop(ctx, queueKey).Result()
			if err != nil {
				if err != redis.Nil {
					log.Printf("❌ Error popping message from queue: %v", err)
				}
				break
			}

			if err := processJob(message); err != nil {
				log.Printf("❌ Error processing message: %v", err)
			}
		}
	}

	processExistingJobs()

	pubsub := subscriber.Subscribe(ctx, queueKey)

	// go routine for start checking for messages
	go func() {
		defer pubsub.Close()

		channel := pubsub.Channel()
		for msg := range channel {
			log.Printf("🔔 New job notification received: %s", msg.Payload)

			//queue mechanism to pop and process one message
			message, err := redisClient.RPop(ctx, queueKey).Result()
			if err == nil {
				if err := processJob(message); err != nil {
					log.Printf("❌ Error processing message: %v", err)
				}
			} else if err != redis.Nil {
				log.Printf("❌ Error popping message after notification: %v", err)
			}
		}
	}()

	//interval timer
	fallbackTicker := time.NewTicker(5 * time.Second)

	go func() {
		for range fallbackTicker.C {
			message, err := redisClient.RPop(ctx, queueKey).Result()
			if err == nil {
				log.Printf("⚠️ Fallback found unprocessed job")

				if err := processJob(message); err != nil {
					log.Printf("❌ Error processing message from fallback: %v", err)
				}

				processExistingJobs()
			} else if err != redis.Nil {
				log.Printf("❌ Error in fallback check: %v", err)
			}
		}
	}()

	log.Printf("📡 Listening for jobs on %s...", queuename)

	//cleanup funcrions
	return func() {
		fallbackTicker.Stop()
		pubsub.Close()
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

		//Lifecycle Implmentation Code -->
		var messageObj Message
		if err := json.Unmarshal([]byte(string(messageJSON)), &messageObj); err != nil {
			log.Printf("❌ Failed to parse message: %v", err)

			return err
		}
		queueKey := fmt.Sprintf("queue:%s", queuename)
		if err := redisClientInstance.LPush(ctx, queueKey, string(messageJSON)).Err(); err != nil {
			return fmt.Errorf("failed to push messages back to queue")
		}
		messageObj.UpdateLifeCycleStatus("requeued")
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
	if deadLetterQueueValueStr, ok := queueOptionsMap["config_dead_letter_queue"]; ok {
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

	var errorMessage string
	if processingErr != nil {
		errorMessage = processingErr.Error()
	} else {
		errorMessage = "Unknown error"
	}

	parsedMessage["options"] = map[string]interface{}{
		"message":   errorMessage,
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
