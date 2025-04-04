//basic implementation of the mechanism, ***THIS CODE GENERATED USING AI***

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

	cleanup, err := mq.ConsumeMessageFromQueue(ctx, "email-queue", func(data map[string]interface{}) error {
		fmt.Printf("Processing message: %v\n", data)
		return nil
	})
	if err != nil {
		log.Fatalf("❌ Failed to start consumer: %v", err)
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("🛑 Shutting down consumer...")

	// Call cleanup function
	cleanup()

	log.Println("✅ Consumer shutdown complete")
}
