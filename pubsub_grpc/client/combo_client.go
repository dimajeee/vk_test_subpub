package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "pubsub/server/proto/gen/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	// Запускаем подписчика в отдельной goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		subscribe(ctx, client, "news")
	}()

	// Запускаем публикатора в отдельной goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		publish(ctx, client, "news", 5)
	}()

	// Ожидаем сигналов завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	cancel()

	wg.Wait()
	fmt.Println("Client shutdown complete")
}

func subscribe(ctx context.Context, client pb.PubSubClient, key string) {
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Key: key})
	if err != nil {
		log.Printf("subscribe failed: %v", err)
		return
	}

	fmt.Printf("Subscribed to '%s' channel\n", key)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			event, err := stream.Recv()
			if err != nil {
				log.Printf("subscription error: %v", err)
				return
			}
			fmt.Printf("[%s] Received: %s\n", key, event.Data)
		}
	}
}

func publish(ctx context.Context, client pb.PubSubClient, key string, count int) {
	for i := 1; i <= count; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			msg := fmt.Sprintf("Message %d at %v", i, time.Now().Format(time.RFC3339))
			_, err := client.Publish(ctx, &pb.PublishRequest{
				Key:  key,
				Data: msg,
			})
			if err != nil {
				log.Printf("publish failed: %v", err)
			} else {
				fmt.Printf("Published to '%s': %s\n", key, msg)
			}
			time.Sleep(3 * time.Second)
		}
	}
}
