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
	pb "github.com/subhammahanty235/bizzmq-go/proto"
)

type JobHandler func(message map[string]interface{}) error
type ConsumerCleanup func()

type Log struct {
	Queuename      string
	MessageId      string
	ProcessingTime string
	Status         string
}

func NewLog(queuename string, messageId string, processingtime string, status string) *Log {
	return &Log{
		Queuename:      queuename,
		MessageId:      messageId,
		ProcessingTime: processingtime,
		Status:         status,
	}
}

func (l *Log) ToString() string {
	return fmt.Sprintf(
		"Queue: %s | Message ID: %s | Processing Time: %s | Status: %s",
		l.Queuename,
		l.MessageId,
		l.ProcessingTime,
		l.Status,
	)
}

// isDLQEnabled normalises the Redis-stored boolean value for dead_letter_queue.
// Node.js ioredis stores booleans as "true"/"false"; Go redis client stores them as "1"/"0".
func isDLQEnabled(val string) bool {
	return val == "1" || val == "true"
}

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

	useDeadLetterQueue := isDLQEnabled(queueOptionsMap["dead_letter_queue"]) ||
		isDLQEnabled(queueOptionsMap["config_dead_letter_queue"])

	maxRetries := 3
	if maxRetriesStr, ok := queueOptionsMap["maxRetries"]; ok {
		if val, err := strconv.Atoi(maxRetriesStr); err == nil {
			maxRetries = val
		}
	}

	stream, err := pb.NewLiveLogsClient(b.grpcConn).LiveLogsTransfer(ctx)
	if err != nil {
		log.Printf("❌ Failed to open gRPC log stream: %v", err)
		return nil, err
	}

	processJob := func(messageStr string) error {
		if messageStr == "" {
			return nil
		}

		var messageObj Message
		if err := json.Unmarshal([]byte(messageStr), &messageObj); err != nil {
			log.Printf("❌ Failed to parse message: %v", err)
			return err
		}

		if err := messageObj.UpdateLifeCycleStatus("processing"); err != nil {
			log.Printf("❌ Invalid lifecycle transition: %v", err)
		}

		messageData := make(map[string]interface{})
		switch m := messageObj.Message.(type) {
		case map[string]interface{}:
			messageData = m
		default:
			messageData["data"] = messageObj.Message
		}

		if err := callback(messageData); err != nil {
			if lerr := messageObj.UpdateLifeCycleStatus("failed"); lerr != nil {
				log.Printf("❌ Invalid lifecycle transition: %v", lerr)
			}
			if useDeadLetterQueue {
				if maxRetries > 0 {
					if reqErr := b.requeueMessage(ctx, queuename, messageStr, err); reqErr != nil {
						log.Printf("❌ Error retrying message: %v", reqErr)
					}
				} else {
					if dlqErr := b.moveMessageToDLQ(ctx, queuename, messageStr, err); dlqErr != nil {
						log.Printf("❌ Error moving message to DLQ: %v", dlqErr)
					}
				}
			} else {
				log.Printf("⚠️ Message failed but no DLQ configured")
			}
			return err
		}

		if lerr := messageObj.UpdateLifeCycleStatus("processed"); lerr != nil {
			log.Printf("❌ Invalid lifecycle transition: %v", lerr)
		}

		strlog := NewLog(queuename, messageObj.MessageID, time.Now().GoString(), messageObj.Status).ToString()
		if err := stream.Send(&pb.SendLogData{Logs: strlog}); err != nil {
			log.Printf("❌ Failed to send log over gRPC stream: %v", err)
		}

		return nil
	}

	// Pop and process all existing messages in priority order (lowest score = highest priority).
	processExistingJobs := func() {
		for {
			entries, err := redisClient.ZRange(ctx, queueKey, 0, 0).Result()
			if err != nil || len(entries) == 0 {
				break
			}
			message := entries[0]
			removed, err := redisClient.ZRem(ctx, queueKey, message).Result()
			if err != nil || removed == 0 {
				// Another consumer grabbed it first — that's fine.
				break
			}
			if err := processJob(message); err != nil {
				log.Printf("❌ Error processing message: %v", err)
			}
		}
	}

	processExistingJobs()

	pubsub := subscriber.Subscribe(ctx, queueKey)

	go func() {
		defer pubsub.Close()
		for range pubsub.Channel() {
			// Drain all messages that may have accumulated since the notification.
			processExistingJobs()
		}
	}()

	fallbackTicker := time.NewTicker(5 * time.Second)
	go func() {
		for range fallbackTicker.C {
			processExistingJobs()
		}
	}()

	log.Printf("📡 Listening for jobs on %s...", queuename)

	return func() {
		fallbackTicker.Stop()
		pubsub.Close()
		subscriber.Close()
	}, nil
}

func (b *BizzMQ) requeueMessage(ctx context.Context, queuename string, message interface{}, processingErr error) error {
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	queueKey := fmt.Sprintf("queue:%s", queuename)
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

	var parsedMessage map[string]interface{}
	switch msg := message.(type) {
	case string:
		if err := json.Unmarshal([]byte(msg), &parsedMessage); err != nil {
			return fmt.Errorf("failed to parse message: %v", err)
		}
	case map[string]interface{}:
		parsedMessage = msg
	default:
		return fmt.Errorf("message must be a string or map")
	}

	// Read retries_made directly from the top-level field (matching Node.js).
	retriesMade := 0
	if v, ok := parsedMessage["retries_made"].(float64); ok {
		retriesMade = int(v)
	}
	retriesMade++

	if retriesMade <= maxRetries {
		parsedMessage["retries_made"] = retriesMade
		parsedMessage["timestamp_updated"] = time.Now().UnixMilli()

		options, _ := parsedMessage["options"].(map[string]interface{})
		if options == nil {
			options = make(map[string]interface{})
		}
		options["lastError"] = processingErr.Error()
		options["retryTimestamp"] = time.Now().UnixMilli()
		parsedMessage["options"] = options

		messageJSON, err := json.Marshal(parsedMessage)
		if err != nil {
			return fmt.Errorf("failed to marshal updated message")
		}

		priority := float64(0)
		if p, ok := options["priority"].(float64); ok {
			priority = p
		}
		score := priority*1e10 + float64(time.Now().UnixMilli())

		if err := redisClientInstance.ZAdd(ctx, queueKey, &redis.Z{
			Score:  score,
			Member: string(messageJSON),
		}).Err(); err != nil {
			return fmt.Errorf("failed to requeue message")
		}

		log.Printf("🔄 Message requeued (attempt %d/%d)", retriesMade, maxRetries)
		return nil
	}

	return b.moveMessageToDLQ(ctx, queuename, parsedMessage, processingErr)
}

func (b *BizzMQ) moveMessageToDLQ(ctx context.Context, queuename string, message interface{}, processingErr error) error {
	queueMetaKey := fmt.Sprintf("queue_meta:%s", queuename)
	redisClientInstance := b.redis.GetRedisClient()

	queueOptionsMap, err := redisClientInstance.HGetAll(ctx, queueMetaKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get queue options")
	}

	dlqEnabled := isDLQEnabled(queueOptionsMap["dead_letter_queue"]) ||
		isDLQEnabled(queueOptionsMap["config_dead_letter_queue"])
	if !dlqEnabled {
		log.Printf("⚠️ No Dead Letter Queue configured for \"%s\". Failed message discarded.", queuename)
		return nil
	}

	dlqName := fmt.Sprintf("%s_dlq", queuename)

	var parsedMessage map[string]interface{}
	switch msg := message.(type) {
	case string:
		if err := json.Unmarshal([]byte(msg), &parsedMessage); err != nil {
			return fmt.Errorf("failed to parse message: %v", err)
		}
	case map[string]interface{}:
		// Copy so we don't mutate the caller's map.
		parsedMessage = make(map[string]interface{}, len(msg))
		for k, v := range msg {
			parsedMessage[k] = v
		}
	default:
		return fmt.Errorf("message must be a string or map")
	}

	// Preserve original options before attaching error metadata.
	originalOptions, _ := parsedMessage["options"].(map[string]interface{})
	dlqOptions := make(map[string]interface{})
	for k, v := range originalOptions {
		dlqOptions[k] = v
	}
	dlqOptions["originalQueue"] = queuename
	dlqOptions["failedAt"] = time.Now().UnixMilli()
	dlqOptions["error"] = map[string]interface{}{
		"message":   processingErr.Error(),
		"stack":     string(debug.Stack()),
		"timestamp": time.Now().UnixMilli(),
	}

	var messageData interface{}
	if data, ok := parsedMessage["message"]; ok {
		messageData = data
	} else {
		messageData = map[string]interface{}{"error": "no data found in original message"}
	}

	msgOptions := MessageOptions{}
	optionsJSON, err := json.Marshal(dlqOptions)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ options")
	}
	if err := json.Unmarshal(optionsJSON, &msgOptions); err != nil {
		return fmt.Errorf("failed to unmarshal DLQ options")
	}

	if _, err := b.PublishMessageToQueue(ctx, dlqName, messageData, msgOptions); err != nil {
		return fmt.Errorf("failed to publish message to DLQ: %w", err)
	}

	return nil
}
