package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	server "pubsub/server/internal"
	pb "pubsub/server/proto/gen/go"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port int `yaml:"port"`
	} `yaml:"server"`
	Logging struct {
		Level string `yaml:"level"`
	} `yaml:"logging"`
}

func loadConfig() (*Config, error) {
	file, err := os.ReadFile("config/config.yaml")
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.Unmarshal(file, &cfg)
	return &cfg, err
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	logger, err := server.NewLogger(cfg.Logging.Level)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(cfg.Server.Port))
	if err != nil {
		logger.Fatal("Failed to listen", zap.Error(err))
	}

	// Dependency Injection
	store := server.NewMemoryEventStore()
	pubSubServer := server.NewPubSubServer(store, logger)

	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServer(grpcServer, pubSubServer)
	reflection.Register(grpcServer)

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("Starting gRPC server", zap.Int("port", cfg.Server.Port))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve", zap.Error(err))
		}
	}()

	<-stopChan
	logger.Info("Shutting down server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		<-ctx.Done()
		logger.Info("Forcefully closing all subscriber channels")
		store.CloseAll()
	}()

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		logger.Warn("Forced shutdown due to timeout")
		grpcServer.Stop()
	case <-stopped:
		logger.Info("Server stopped gracefully")
	}
}
