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
	redis    *internal.RedisClient
	stop     chan struct{}
	grpcConn *grpc.ClientConn
}

func (b *BizzMQ) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.CreateTCPServerHeartBeat()
		case <-b.stop:
			fmt.Println("Stopping ticker")
			return
		}
	}
}

func (b *BizzMQ) CreateTCPServerHeartBeat() {
	// conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	// 	log.Printf("did not connect: %v", err)
	// }
	// fmt.Println("TCP Connection is Running and Ready to process")
	conn := b.grpcConn
	client := pb.NewHeartBeatClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	_, err := client.HeartBeatExchange(ctx, &pb.RequestHeartBeat{InstanceId: "82828", SignalId: "00001"})
	if err != nil {
		log.Printf("could not exchange heartbeat: %v", err)
	}

}

func (b *BizzMQ) SendTCPServerHandShakeRequest() bool {
	conn := b.grpcConn
	//loop based retry mechinism
	for i := 0; i < 3; i++ {
		client := pb.NewHandShakeClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		resp, err := client.HandShakeExchange(ctx, &pb.SendHandShake{Instanceid: "82828", Instancemodule: "Go-Package"})
		if err != nil {
			log.Printf("could not connect handshake :%v", err)
			if i < 4 {
				log.Printf("Retrying in %v...", (2 * time.Second))
				time.Sleep(2 * time.Second)
			}
			continue
		}

		log.Printf(resp.Message)
		if resp.Instanceid == "82828" {
			return true
		}

		time.Sleep(2 * time.Second)
	}
	return false
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
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("did not connect: %v", err)
	}

	b := &BizzMQ{
		redis:    redisInstance,
		stop:     make(chan struct{}),
		grpcConn: conn,
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
	close(b.stop)

	if b.grpcConn != nil {
		if err := b.grpcConn.Close(); err != nil {
			log.Printf("error closing gRPC connection: %v", err)
		}
	}
	return b.redis.CloseRedisClient()
}
