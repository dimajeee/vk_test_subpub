package main

import (
	"context"
	"fmt"
	"log"
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

	for i := 1; i <= 5; i++ {
		msg := fmt.Sprintf("News update #%d - %v", i, time.Now().Format(time.RFC3339))

		_, err := client.Publish(context.Background(), &pb.PublishRequest{
			Key:  "news",
			Data: msg,
		})
		if err != nil {
			log.Printf("could not publish event: %v", err)
			continue
		}

		fmt.Printf("Published: %s\n", msg)
		time.Sleep(2 * time.Second)
	}
}
