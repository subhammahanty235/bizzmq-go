# BizzMQ-G0

A lightweight Redis-based message queue with Dead Letter Queue support, implemented in Go

## Features

- **Simple Redis-Based Queue**: Leverage the power and reliability of Redis for message storage and processing
- **Real-Time Processing**: Utilizes Redis Pub/Sub for immediate job notification with fallback polling
- **Dead Letter Queue**: Automatic handling of failed messages with configurable DLQ
- **Retry Mechanism**: Configurable retry attempts for failed jobs before they're moved to DLQ
- **Message Persistence**: Jobs are safely stored in Redis until successfully processed
- **Low Overhead**: Minimal dependencies and lightweight design for optimal performance
- **Error Handling**: Robust error handling with detailed error tracking in failed messages
- **Go Idiomatic Design**: Follows Go best practices including context support, interfaces, and proper error handling
- **Concurrency Support**: Leverages Go's goroutines for efficient parallel processing

## Installation

```bash
go get github.com/subhammahanty235/bizzmq-go
```

## Prerequisites

- Go 1.18 or higher
- Redis server (v5 or higher)

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/yourusername/go-bizzmq/queue"
)

func main() {
    // Initialize with Redis connection string
    mq, err := queue.NewBizzMQ("redis://localhost:6379")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Create a queue
    err = mq.CreateQueue(ctx, "email-queue", queue.QueueOptions{})
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    
    // Add a job to the queue
    jobData := map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Welcome to Our Service",
        "body":    "Thank you for signing up!",
    }
    
    messageID, err := mq.PublishMessageToQueue(ctx, "email-queue", jobData, queue.MessageOptions{})
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
    fmt.Printf("Published message with ID: %s\n", messageID)
    
    // Process jobs
    cleanup, err := mq.ConsumeMessageFromQueue(ctx, "email-queue", func(data map[string]interface{}) error {
        fmt.Printf("Sending email to %s\n", data["to"])
        // Your job processing logic here
        // sendEmail(data)
        fmt.Println("Email sent successfully")
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to consume messages: %v", err)
    }
    
    // Keep the application running
    time.Sleep(60 * time.Second)
    
    // When you're done, call the cleanup function
    cleanup()
}
```

### Using Dead Letter Queue

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

    "github.com/yourusername/go-bizzmq/queue"
)

func main() {
    // Initialize with Redis connection string
    mq, err := queue.NewBizzMQ("redis://localhost:6379/0")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Create a queue with Dead Letter Queue enabled
    err = mq.CreateQueue(ctx, "email-queue", queue.QueueOptions{
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
    
    messageID, err := mq.PublishMessageToQueue(ctx, "email-queue", jobData, queue.MessageOptions{
        // Custom message options can be added here
    })
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
    fmt.Printf("Published message with ID: %s\n", messageID)
    
    // Process jobs with error handling
    cleanup, err := mq.ConsumeMessageFromQueue(ctx, "email-queue", func(data map[string]interface{}) error {
        fmt.Printf("Sending email to %s\n", data["to"])
        
        // Simulate failure for demonstration
        if data["to"] == "user@example.com" {
            return errors.New("simulated failure sending email")
        }
        
        fmt.Println("Email sent successfully")
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to consume messages: %v", err)
    }
    
    // Set up graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    
    fmt.Println("Shutting down...")
    cleanup()
}
```

## Complete Example

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
    "time"
    "github.com/subhammahanty235/bizzmq-go/queue"
)

func main() {
    // Initialize the queue system
    mq, err := queue.NewBizzMQ("redis://localhost:6379/0")
    if err != nil {
        log.Fatalf("Failed to connect to Redis: %v", err)
    }
    defer mq.Close()
    
    ctx := context.Background()
    
    // Create a queue with DLQ enabled
    err = mq.CreateQueue(ctx, "firstqueue", queue.QueueOptions{
        ConfigDeadLetterQueue: true,
        MaxRetries:            3,
    })
    if err != nil {
        log.Fatalf("Failed to create queue: %v", err)
    }
    
    // Prepare job data
    jobData := map[string]interface{}{
        "type":      "email-data",
        "to":        "user@example.com",
        "subject":   "Welcome to Our Platform!",
        "body":      "Thank you for joining. We're excited to have you onboard!",
        "createdAt": time.Now().UnixMilli(),
        "priority":  "high",
    }
    
    // Publish message to queue
    messageID, err := mq.PublishMessageToQueue(ctx, "firstqueue", jobData, queue.MessageOptions{})
    if err != nil {
        log.Fatalf("Failed to publish message: %v", err)
    }
    fmt.Printf("Job published successfully with ID: %s\n", messageID)
    
    // Set up consumer
    cleanup, err := mq.ConsumeMessageFromQueue(ctx, "firstqueue", func(data map[string]interface{}) error {
        to, _ := data["to"].(string)
        typ, _ := data["type"].(string)
        
        fmt.Printf("Processing %s job for %s\n", typ, to)
        
        // Simulate sending email
        if !strings.Contains(to, "@") || strings.Contains(to, "#") {
            return errors.New("invalid email address")
        }
        
        // Email sent successfully
        fmt.Printf("Email sent to %s\n", to)
        return nil
    })
    if err != nil {
        log.Fatalf("Failed to consume messages: %v", err)
    }
    
    // Set up graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    
    fmt.Println("Shutting down...")
    cleanup()
}
```

## API Reference

### Constructor

#### `NewBizzMQ(redisURI string) (*BizzMQ, error)`

Creates a new BizzMQ instance connected to the specified Redis server.

- `redisURI` (string): Redis connection string (e.g., "redis://localhost:6379/0")
- Returns: A BizzMQ instance and any error that occurred during initialization

### Queue Management

#### `CreateQueue(ctx context.Context, queueName string, options QueueOptions) error`

Creates a new queue. If the queue already exists, this operation is skipped.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue to create
- `options` (QueueOptions): Queue configuration options
  - `ConfigDeadLetterQueue` (bool): Whether to create a DLQ for this queue (default: false)
  - `MaxRetries` (int): Maximum number of retry attempts before sending to DLQ (default: 3)
  - `Retry` (int): Initial retry count for messages in this queue

#### `PublishMessageToQueue(ctx context.Context, queueName string, message interface{}, options MessageOptions) (string, error)`

Publishes a message to the specified queue.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue
- `message` (interface{}): The message/job data to be processed
- `options` (MessageOptions): Optional message-specific settings

Returns the generated message ID and any error that occurred.

#### `ConsumeMessageFromQueue(ctx context.Context, queueName string, handler JobHandler) (ConsumerCleanup, error)`

Starts consuming messages from the specified queue.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the queue to consume from
- `handler` (JobHandler): Function to process each message
  - Called with the message data as a map[string]interface{}
  - Should return an error if processing fails

Returns a cleanup function that should be called to stop consuming and any error that occurred.

### Dead Letter Queue Management

#### `GetDeadLetterMessages(ctx context.Context, queueName string, limit int) ([]map[string]interface{}, error)`

Retrieves messages from the dead letter queue without removing them.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the original queue
- `limit` (int): Maximum number of messages to retrieve (default: 100)

Returns an array of failed message objects and any error that occurred.

#### `RetryDeadLetterMessage(ctx context.Context, queueName string, messageID string) (bool, error)`

Moves a message from the dead letter queue back to the original queue for retry.

- `ctx` (context.Context): Context for the operation
- `queueName` (string): Name of the original queue
- `messageID` (string): ID of the message to retry

Returns a boolean indicating success and any error that occurred.

## Error Handling

When job processing fails (handler returns an error):

1. The error is caught and logged
2. If retry is enabled, the job retry count is incremented
3. If retry count < maxRetries, the job is added back to the queue
4. If retry count >= maxRetries, the job is moved to the DLQ (if enabled)
5. Error details are preserved with the job for debugging

## Best Practices

1. **Always use context for cancellation**: Pass appropriate context to operations that might need to be cancelled
2. **Enable Dead Letter Queues** for production workloads to capture failed jobs
3. **Set appropriate MaxRetries** based on the transient nature of expected failures
4. **Include relevant metadata** in your job data for easier debugging
5. **Check your DLQ regularly** for repeated failures that might indicate systemic issues
6. **Implement graceful shutdown** by calling the cleanup function returned by `ConsumeMessageFromQueue`
7. **Use defer for cleanup**: Always use `defer mq.Close()` to ensure Redis connections are properly closed
