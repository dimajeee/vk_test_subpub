package subpub

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrClosed = errors.New("subpub is closed")
)

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)
	Publish(subject string, msg interface{}) error
	Close(ctx context.Context) error
}

type subscription struct {
	subject string
	ch      chan interface{}
	bus     *subPubImpl
}

func (s *subscription) Unsubscribe() {
	s.bus.unsubscribe(s)
}

type subPubImpl struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscription
	closed      bool
}

func NewSubPub() SubPub {
	return &subPubImpl{
		subscribers: make(map[string][]*subscription),
	}
}

func (s *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrClosed
	}

	sub := &subscription{
		subject: subject,
		ch:      make(chan interface{}, 100),
		bus:     s,
	}

	s.subscribers[subject] = append(s.subscribers[subject], sub)

	go func() {
		for msg := range sub.ch {
			cb(msg)
		}
	}()

	return sub, nil
}

func (s *subPubImpl) unsubscribe(sub *subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	subs, ok := s.subscribers[sub.subject]
	if !ok {
		return
	}

	for i, v := range subs {
		if v == sub {
			close(v.ch)
			s.subscribers[sub.subject] = append(subs[:i], subs[i+1:]...)
			break
		}
	}

	if len(s.subscribers[sub.subject]) == 0 {
		delete(s.subscribers, sub.subject)
	}
}

func (s *subPubImpl) Publish(subject string, msg interface{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return ErrClosed
	}

	for _, sub := range s.subscribers[subject] {
		select {
		case sub.ch <- msg:
		default:
			// Skip slow subscriber
		}
	}

	return nil
}

func (s *subPubImpl) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.closed = true

	subsToClose := make(map[string][]*subscription)
	for subject, subs := range s.subscribers {
		subsToClose[subject] = subs
	}
	s.subscribers = make(map[string][]*subscription)
	s.mu.Unlock()

	done := make(chan struct{}, 1)

	go func() {
		for _, subs := range subsToClose {
			for _, sub := range subs {
				close(sub.ch)
				time.Sleep(10 * time.Millisecond)
			}
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
