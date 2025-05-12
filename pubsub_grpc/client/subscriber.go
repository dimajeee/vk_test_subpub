package main

import (
	"context"
	"fmt"
	"log"

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

	// Подписываемся на ключ "news"
	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{
		Key: "news",
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	fmt.Println("Subscribed to 'news' channel. Waiting for messages...")

	for {
		event, err := stream.Recv()
		if err != nil {
			log.Printf("subscription ended: %v", err)
			return
		}
		fmt.Printf("New event received: %s\n", event.Data)
	}
}
