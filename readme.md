# BizzMQ-Go

A lightweight Redis-based message queue system with Dead Letter Queue support, implemented in Go.

## Features

- **Simple Redis-Based Queue**: Leverages Redis for reliable message storage and processing
- **Real-Time Processing**: Uses Redis Pub/Sub for immediate job notification with fallback polling
- **Dead Letter Queue**: Automatically handles failed messages with configurable DLQ
- **Retry Mechanism**: Configurable retry attempts for failed jobs before they're moved to DLQ
- **Message Persistence**: Jobs are safely stored in Redis until successfully processed
- **Error Handling**: Robust error management with detailed tracking in failed messages
- **Go Idiomatic Design**: Follows Go best practices with context support and proper error handling
- **Concurrency Support**: Leverages Go's goroutines for efficient parallel processing

## Installation

```bash
go get github.com/subhammahanty235/bizzmq-go
```

## Prerequisites

- Go 1.18+ (tested with Go 1.22.3)
- Redis server (v5 or higher)

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/subhammahanty235/bizzmq-go"
)

func main() {
    // Initialize with Redis connection string
    mq, err := bizzmq.NewBizzMQ("redis://localhost:6379")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Create a queue
    err = mq.CreateQueue(ctx, "email-queue", bizzmq.QueueOptions{
        ConfigDeadLetterQueue: true,
    })
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    
    // Add a job to the queue
    jobData := map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Welcome to Our Service",
        "body":    "Thank you for signing up!",
    }
    
    messageID, err := mq.PublishMessageToQueue(ctx, "email-queue", jobData, bizzmq.MessageOptions{})
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
    fmt.Printf("Published message with ID: %s\n", messageID)
    
    // Start consuming messages
    cleanup, err := mq.ConsumeMessageFromQueue(ctx, "email-queue", func(data map[string]interface{}) error {
        fmt.Printf("Processing message: %v\n", data)
        // Your job processing logic here
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to start consumer: %v", err)
    }

    // Handle graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    
    <-sigCh
    fmt.Println("Shutting down consumer...")
    
    // Call cleanup function
    cleanup()
    
    fmt.Println("Consumer shutdown complete")
}
```

### Using Dead Letter Queue with Retries

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/subhammahanty235/bizzmq-go"
)

func main() {
    // Initialize with Redis connection string
    mq, err := bizzmq.NewBizzMQ("redis://localhost:6379")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Create a queue with Dead Letter Queue enabled and custom retry settings
    err = mq.CreateQueue(ctx, "email-queue", bizzmq.QueueOptions{
        ConfigDeadLetterQueue: true,  // Enable DLQ
        MaxRetries:            3,     // Try 3 times before moving to DLQ
    })
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    
    // Add a job
    jobData := map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Welcome to Our Service",
        "body":    "Thank you for signing up!",
    }
    
    messageID, err := mq.PublishMessageToQueue(ctx, "email-queue", jobData, bizzmq.MessageOptions{})
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
    fmt.Printf("Published message with ID: %s\n", messageID)
    
    // Process jobs with error handling
    cleanup, err := mq.ConsumeMessageFromQueue(ctx, "email-queue", func(data map[string]interface{}) error {
        fmt.Printf("Sending email to %s\n", data["to"])
        
        // Simulate failure for demonstration
        return errors.New("simulated failure sending email")
    })
    if err != nil {
        log.Fatalf("Failed to consume messages: %v", err)
    }
    
    // Let the consumer run for a while so we can observe retries
    fmt.Println("Consumer running. Press Ctrl+C to stop...")
    
    // Set up graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    
    fmt.Println("Shutting down...")
    cleanup()
}
```

### Working with Dead Letter Queue

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/subhammahanty235/bizzmq-go"
)

func main() {
    mq, err := bizzmq.NewBizzMQ("redis://localhost:6379")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Get messages from the DLQ
    dlqName := "email-queue_dlq" // DLQs are named as originalQueueName_dlq
    messagesJSON, err := mq.GetDeadLetterMessages(ctx, dlqName, 10)
    if err != nil {
        log.Fatalf("Failed to get DLQ messages: %v", err)
    }
    
    fmt.Println("Failed messages in DLQ:")
    fmt.Println(messagesJSON)
    
    // Retry a specific message from the DLQ
    messageID := "message:1712345678901" // Example message ID
    success, err := mq.RetryDeadLetterMessage(ctx, "email-queue", messageID)
    if err != nil {
        log.Fatalf("Failed to retry message: %v", err)
    }
    
    if success {
        fmt.Printf("Message %s successfully moved back to the original queue\n", messageID)
    }
}
```

## API Reference

### Constructor

#### `NewBizzMQ(redisURI string) (*BizzMQ, error)`

Creates a new BizzMQ instance connected to the specified Redis server.

- `redisURI` (string): Redis connection string (e.g., "redis://localhost:6379")
- Returns: A BizzMQ instance and any error that occurred during initialization

### Queue Management

#### `CreateQueue(ctx context.Context, queueName string, options QueueOptions) error`

Creates a new queue. If the queue already exists, this operation is skipped.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue to create
- `options` (QueueOptions): Queue configuration options
  - `ConfigDeadLetterQueue` (bool): Whether to create a DLQ for this queue
  - `MaxRetries` (int): Maximum number of retry attempts before sending to DLQ
  - `Retry` (int): Initial retry count for messages in this queue

#### `PublishMessageToQueue(ctx context.Context, queueName string, message interface{}, options MessageOptions) (string, error)`

Publishes a message to the specified queue.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue
- `message` (interface{}): The message/job data to be processed
- `options` (MessageOptions): Optional message-specific settings
  - `Priority` (int64): Message priority level
  - `Retries` (int64): Custom retry setting for this message

Returns the generated message ID and any error that occurred.

#### `ConsumeMessageFromQueue(ctx context.Context, queueName string, handler JobHandler) (ConsumerCleanup, error)`

Starts consuming messages from the specified queue.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue to consume from
- `handler` (JobHandler): Function to process each message
  - Function signature: `func(message map[string]interface{}) error`
  - Should return an error if processing fails

Returns a cleanup function that should be called to stop consuming, and any error that occurred.

### Dead Letter Queue Management

#### `GetDeadLetterMessages(ctx context.Context, queueName string, limit int) (string, error)`

Retrieves messages from the dead letter queue as a JSON string.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the DLQ to get messages from
- `limit` (int): Maximum number of messages to retrieve (default: 100)

Returns a JSON string containing the messages and any error that occurred.

#### `RetryDeadLetterMessage(ctx context.Context, queueName string, messageID string) (bool, error)`

Moves a message from the dead letter queue back to the original queue for retry.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the original queue
- `messageID` (string): ID of the message to retry

Returns a boolean indicating success and any error that occurred.

## Error Handling and Retry Flow

When a job processing fails (handler returns an error):

1. The error is caught and logged
2. If retry is enabled and maxRetries > 0:
   - The job retry count is incremented
   - The job is added back to the queue
3. If retry count exceeds maxRetries or retries are disabled:
   - The job is moved to the Dead Letter Queue (if enabled)
   - Error details are preserved with the job for debugging

## Best Practices

1. **Always enable Dead Letter Queues** for production workloads to capture failed jobs
2. **Use appropriate contexts** for proper cancellation and timeouts
3. **Implement graceful shutdown** by calling the cleanup function returned by `ConsumeMessageFromQueue`
4. **Set appropriate MaxRetries** based on the nature of expected failures
5. **Include relevant metadata** in your job data for easier debugging
6. **Check your DLQ regularly** for repeated failures that might indicate systemic issues
7. **Always use `defer mq.Close()`** to ensure Redis connections are properly closed

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
