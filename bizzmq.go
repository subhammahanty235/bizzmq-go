// Package bizzmq provides a Redis-based message queue system.
package bizzmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/subhammahanty235/bizzmq-go/internal"
	pb "github.com/subhammahanty235/bizzmq-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedHandShakeServer
	pb.UnimplementedHeartBeatServer
}

type BizzMQ struct {
	redis *internal.RedisClient
	stop  chan struct{}
}

func (b *BizzMQ) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			CreateTCPServer()
		case <-b.stop:
			fmt.Println("Stopping ticker")
			return
		}
	}
}

func CreateTCPServer() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("did not connect: %v", err)
	}
	fmt.Println("TCP Connection is Running and Ready to process")

	defer conn.Close()
	client := pb.NewHeartBeatClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.HeartBeatExchange(ctx, &pb.RequestHeartBeat{InstanceId: "82828", SignalId: "00001"})
	if err != nil {
		log.Printf("could not exchange heartbeat: %v", err)
	}
	log.Printf(resp.InstanceId)
}

func NewBizzMQ(redisUri string) (*BizzMQ, error) {
	//* redis uri will be passed from the client end as it's the primary requirement
	if redisUri == "" {
		return nil, errors.New("redis URL is required")
	}
	redisInstance, err := internal.NewRedisClient(redisUri)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	b := &BizzMQ{
		redis: redisInstance,
		stop:  make(chan struct{}),
	}
	flag := true
	if flag {
		go b.StartTicker()
	}

	return b, err

	// return &BizzMQ{
	// 	redis: redisInstance,
	// }, nil
}

func (b *BizzMQ) Close() error {
	return b.redis.CloseRedisClient()
}
