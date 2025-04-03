package main

import (
	"context"
	"log"

	"github.com/subhammahanty235/pkg/queue"
)

func main() {
	mq, err := queue.NewBizzMq("redis://localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	ctx := context.Background()
	// defer mq.Close();
	err = mq.CreateQueue(ctx, "notifications", queue.QueueOptions{
		ConfigDeadLetterQueue: false, // Explicitly disable DLQ
	})
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

}
