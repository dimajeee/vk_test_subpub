package internal

import (
	"context"
	"sync"

	pb "pubsub/server/proto/gen/go"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Интерфейс EventStore
type EventStore interface {
	AddSubscriber(key string, ch chan *pb.Event)
	RemoveSubscriber(key string, ch chan *pb.Event)
	GetSubscribers(key string) ([]chan *pb.Event, bool)
	GetAllSubscribers() map[string][]chan *pb.Event
	CloseAll()
}

// Реализация EventStore в памяти
type MemoryEventStore struct {
	subscribers map[string][]chan *pb.Event
	mu          sync.RWMutex
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		subscribers: make(map[string][]chan *pb.Event),
	}
}

func (m *MemoryEventStore) AddSubscriber(key string, ch chan *pb.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers[key] = append(m.subscribers[key], ch)
}

func (m *MemoryEventStore) RemoveSubscriber(key string, ch chan *pb.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subscribers := m.subscribers[key]
	for i, subscriber := range subscribers {
		if subscriber == ch {
			m.subscribers[key] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}
}

func (m *MemoryEventStore) GetSubscribers(key string) ([]chan *pb.Event, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	subscribers, ok := m.subscribers[key]
	return subscribers, ok
}

func (m *MemoryEventStore) GetAllSubscribers() map[string][]chan *pb.Event {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.subscribers
}

func (m *MemoryEventStore) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, subscribers := range m.subscribers {
		for _, ch := range subscribers {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	}
}

// Реализация PubSubServer
type PubSubServer struct {
	pb.UnimplementedPubSubServer
	store  EventStore
	logger *zap.Logger
}

func NewPubSubServer(store EventStore, logger *zap.Logger) *PubSubServer {
	return &PubSubServer{
		store:  store,
		logger: logger,
	}
}

func (s *PubSubServer) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	ch := make(chan *pb.Event)
	s.store.AddSubscriber(req.Key, ch)

	s.logger.Info("New subscriber added", zap.String("key", req.Key))

	defer func() {
		s.store.RemoveSubscriber(req.Key, ch)

		if !isClosed(ch) {
			close(ch)
		}
		s.logger.Info("Subscriber removed", zap.String("key", req.Key))
	}()

	for event := range ch {
		if err := stream.Send(event); err != nil {
			s.logger.Error("Failed to send event", zap.String("key", req.Key), zap.Error(err))
			return status.Errorf(codes.Internal, "send failed: %v", err)
		}
	}

	return nil
}

func (s *PubSubServer) Publish(ctx context.Context, req *pb.PublishRequest) (*emptypb.Empty, error) {
	subscribers, ok := s.store.GetSubscribers(req.Key)
	if !ok {
		s.logger.Warn("No subscribers found", zap.String("key", req.Key))
		return nil, status.Errorf(codes.NotFound, "no subscribers for key: %s", req.Key)
	}

	event := &pb.Event{Data: req.Data}
	s.logger.Info("Publishing event", zap.String("key", req.Key), zap.Int("subscriber_count", len(subscribers)))

	for _, ch := range subscribers {
		select {
		case ch <- event:
		case <-ctx.Done():
			s.logger.Warn("Publishing canceled due to server shutdown")
			return nil, status.Error(codes.Canceled, "server shutting down")
		}
	}

	return &emptypb.Empty{}, nil
}

func isClosed(ch chan *pb.Event) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
