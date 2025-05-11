package subpub

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSubPub(t *testing.T) {
	bus := NewSubPub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []string
	var mu sync.Mutex

	sub, err := bus.Subscribe("test", func(msg interface{}) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, msg.(string))
	})
	assert.NoError(t, err)
	defer sub.Unsubscribe()

	err = bus.Publish("test", "hello")
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, []string{"hello"}, received)
	mu.Unlock()

	err = bus.Close(ctx)
	assert.NoError(t, err)
}

func TestMultipleSubscribers(t *testing.T) {
	bus := NewSubPub()
	defer bus.Close(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)

	sub1, _ := bus.Subscribe("news", func(msg interface{}) {
		fmt.Println("Subscriber 1:", msg)
		wg.Done()
	})
	defer sub1.Unsubscribe()

	sub2, _ := bus.Subscribe("news", func(msg interface{}) {
		fmt.Println("Subscriber 2:", msg)
		wg.Done()
	})
	defer sub2.Unsubscribe()

	bus.Publish("news", "test message")
	wg.Wait()
}

func TestSlowSubscriber(t *testing.T) {
	bus := NewSubPub()
	defer bus.Close(context.Background())

	fastSub, _ := bus.Subscribe("news", func(msg interface{}) {
		fmt.Println("Fast subscriber:", msg)
	})
	defer fastSub.Unsubscribe()

	slowSub, _ := bus.Subscribe("news", func(msg interface{}) {
		time.Sleep(1 * time.Second)
		fmt.Println("Slow subscriber:", msg)
	})
	defer slowSub.Unsubscribe()

	for i := 0; i < 10; i++ {
		bus.Publish("news", fmt.Sprintf("msg %d", i))
	}

	time.Sleep(2 * time.Second)
}

func TestMessageOrder(t *testing.T) {
	bus := NewSubPub()
	defer bus.Close(context.Background())

	var received []string
	var mu sync.Mutex

	sub, _ := bus.Subscribe("news", func(msg interface{}) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, msg.(string))
	})
	defer sub.Unsubscribe()

	for i := 0; i < 5; i++ {
		bus.Publish("news", fmt.Sprintf("msg %d", i))
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, []string{"msg 0", "msg 1", "msg 2", "msg 3", "msg 4"}, received)
	mu.Unlock()
}

func TestCloseWithContext(t *testing.T) {
	bus := NewSubPub()

	for i := 0; i < 10; i++ {
		sub, _ := bus.Subscribe("news", func(msg interface{}) {
			time.Sleep(500 * time.Millisecond)
			t.Log("Processed:", msg)
		})
		defer sub.Unsubscribe()
	}

	bus.Publish("news", "test message")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err := bus.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected context.DeadlineExceeded, got %v", err)
	}

	time.Sleep(1 * time.Second)
}

func TestNoGoroutineLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	bus := NewSubPub()
	sub, _ := bus.Subscribe("news", func(msg interface{}) {
		fmt.Println(msg)
	})

	bus.Publish("news", "test")
	sub.Unsubscribe()
	bus.Close(context.Background())

	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	assert.Equal(t, initialGoroutines, finalGoroutines)
}
