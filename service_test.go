package natscracker

import (
	"context"
	"errors"
	"github.com/NikitaPravoi/natscracker/pkg/pb"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// TestService tests the core service functionality
func TestService(t *testing.T) {
	t.Run("Service lifecycle", func(t *testing.T) {
		ctx := context.Background()
		service := createTestService(t)

		// Test starting service
		err := service.Start(ctx)
		require.NoError(t, err)

		// Test double start
		err = service.Start(ctx)
		require.Error(t, err)

		// Test shutdown
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		err = service.Shutdown(shutdownCtx)
		require.NoError(t, err)
	})

	t.Run("Message handler registration", func(t *testing.T) {
		service := createTestService(t)

		// Test valid handler registration
		err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			return nil
		})
		require.NoError(t, err)

		// Test duplicate handler
		err = service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			return nil
		})
		require.Error(t, err)

		// Test nil handler
		err = service.RegisterHandler("test.event2", nil)
		require.Error(t, err)
	})
}

// TestMessageProcessing tests message processing functionality
func TestMessageProcessing(t *testing.T) {
	t.Run("Successful message processing", func(t *testing.T) {
		ctx := context.Background()
		service := createTestService(t)

		// Register test handler
		handlerCalled := false
		err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			handlerCalled = true
			return nil
		})
		require.NoError(t, err)

		// Create test message
		msg := createTestMessage(t, "test.event")
		err = service.processMessage(ctx, msg, "test-worker")

		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})

	t.Run("Message processing with retryable error", func(t *testing.T) {
		ctx := context.Background()
		service := createTestService(t)

		// Register handler that returns retryable error
		err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			return &RetryableError{
				Err:       errors.New("temporary error"),
				Retryable: true,
			}
		})
		require.NoError(t, err)

		msg := createTestMessage(t, "test.event")
		err = service.processMessage(ctx, msg, "test-worker")

		require.Error(t, err)
		var retryErr *RetryableError
		require.True(t, errors.As(err, &retryErr))
		assert.True(t, retryErr.Retryable)
	})
}

// TestCircuitBreaker tests the circuit breaker functionality
func TestCircuitBreaker(t *testing.T) {
	t.Run("Circuit breaker triggers", func(t *testing.T) {
		cb := NewCircuitBreaker(2, time.Second)

		// Initial state should be closed
		assert.True(t, cb.AllowRequest())

		// Record failures
		cb.RecordFailure()
		assert.True(t, cb.AllowRequest())

		cb.RecordFailure()
		assert.False(t, cb.AllowRequest())
	})

	t.Run("Circuit breaker reset", func(t *testing.T) {
		cb := NewCircuitBreaker(1, time.Millisecond)

		cb.RecordFailure()
		assert.False(t, cb.AllowRequest())

		// Wait for reset
		time.Sleep(2 * time.Millisecond)
		assert.True(t, cb.AllowRequest())
	})
}

// Helper functions

func createTestService(t *testing.T) *Service {
	t.Helper()

	return &Service{
		handlers:         make(map[string]MessageHandler),
		eventIDGenerator: &UUIDGenerator{},
		errorChan:        make(chan error, 100),
		circuitBreaker:   NewCircuitBreaker(3, time.Second),
		logger:           slog.Default(),
	}
}

// MockJetstreamMsg implements jetstream.Msg interface for testing
type MockJetstreamMsg struct {
	mock.Mock
	subject string
	data    []byte
}

func (m *MockJetstreamMsg) Data() []byte      { return m.data }
func (m *MockJetstreamMsg) Subject() string   { return m.subject }
func (m *MockJetstreamMsg) Ack() error        { return nil }
func (m *MockJetstreamMsg) Nak() error        { return nil }
func (m *MockJetstreamMsg) Term() error       { return nil }
func (m *MockJetstreamMsg) InProgress() error { return nil }
func (m *MockJetstreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{}, nil
}
func (m *MockJetstreamMsg) DoubleAck(ctx context.Context) error    { return nil }
func (m *MockJetstreamMsg) TermWithReason(reason string) error     { return nil }
func (m *MockJetstreamMsg) Headers() nats.Header                   { return make(nats.Header) }
func (m *MockJetstreamMsg) Reply() string                          { return "" }
func (m *MockJetstreamMsg) NakWithDelay(delay time.Duration) error { return nil }

func createTestMessage(t *testing.T, eventType string) jetstream.Msg {
	t.Helper()

	event := &pb.BaseEvent{
		Id:        "test-id",
		Type:      eventType,
		Timestamp: timestamppb.Now(),
		Payload:   &anypb.Any{},
	}

	data, err := proto.Marshal(event)
	require.NoError(t, err)

	return &MockJetstreamMsg{
		subject: eventType,
		data:    data,
	}
}

// TestMetricsCollection tests the service metrics collection
func TestMetricsCollection(t *testing.T) {
	t.Run("Message processing metrics", func(t *testing.T) {
		ctx := context.Background()
		service := createTestService(t)

		// Register test handler
		err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			time.Sleep(10 * time.Millisecond) // Simulate processing time
			return nil
		})
		require.NoError(t, err)

		// Process multiple messages
		for i := 0; i < 5; i++ {
			msg := createTestMessage(t, "test.event")
			err = service.processMessage(ctx, msg, "test-worker")
			require.NoError(t, err)
		}

		// Check metrics
		metrics := service.GetMetrics()
		assert.Equal(t, int64(5), metrics["messages_processed"])
		assert.Equal(t, int64(5), metrics["messages_successful"])
		assert.Equal(t, int64(0), metrics["messages_failed"])
		assert.Greater(t, metrics["processing_duration"].(int64), int64(50*time.Millisecond))
	})

	t.Run("Failed message metrics", func(t *testing.T) {
		ctx := context.Background()
		service := createTestService(t)

		// Register failing handler
		err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
			return errors.New("handler error")
		})
		require.NoError(t, err)

		// Process messages
		for i := 0; i < 3; i++ {
			msg := createTestMessage(t, "test.event")
			_ = service.processMessage(ctx, msg, "test-worker")
		}

		metrics := service.GetMetrics()
		assert.Equal(t, int64(3), metrics["messages_processed"])
		assert.Equal(t, int64(0), metrics["messages_successful"])
		assert.Equal(t, int64(3), metrics["messages_failed"])
	})

	t.Run("Active workers tracking", func(t *testing.T) {
		service := createTestService(t)

		// Simulate workers starting
		service.metrics.ActiveWorkers.Add(3)

		metrics := service.GetMetrics()
		assert.Equal(t, int32(3), metrics["active_workers"])

		// Simulate workers stopping
		service.metrics.ActiveWorkers.Add(-2)

		metrics = service.GetMetrics()
		assert.Equal(t, int32(1), metrics["active_workers"])
	})
}

// TestErrorChannelMonitoring tests the error monitoring functionality
func TestErrorChannelMonitoring(t *testing.T) {
	t.Run("Error channel processing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		service := createTestService(t)

		// Start error monitoring
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.monitorErrors(ctx)
		}()

		// Send test errors
		expectedErr := errors.New("test error")
		service.errorChan <- expectedErr

		// Allow some time for error processing
		time.Sleep(100 * time.Millisecond)

		// Cancel context and wait for monitoring to stop
		cancel()
		wg.Wait()
	})

	t.Run("Error channel buffer size", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		service := createTestService(t)

		// Start error monitoring
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.monitorErrors(ctx)
		}()

		// Test that error channel doesn't block at buffer size
		for i := 0; i < 101; i++ { // Buffer size is 100
			select {
			case service.errorChan <- errors.New("test error"):
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Error channel blocked")
			}
		}
	})

	t.Run("Error handling during shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		service := createTestService(t)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			service.monitorErrors(ctx)
		}()

		// Cancel context during error processing
		cancel()

		// Try to send error after shutdown
		select {
		case service.errorChan <- errors.New("test error"):
		case <-time.After(100 * time.Millisecond):
			// Channel should be closed or unread after shutdown
		}

		wg.Wait()
	})
}

// BenchmarkMessageProcessing contains performance tests
func BenchmarkMessageProcessing(b *testing.B) {
	ctx := context.Background()
	service := createTestService(nil) // Using nil for testing.T as this is a benchmark

	// Register test handler
	err := service.RegisterHandler("test.event", func(ctx context.Context, event *pb.BaseEvent) error {
		return nil
	})
	require.NoError(b, err)

	// Create a reusable test message
	msg := createTestMessage(nil, "test.event")

	b.Run("Sequential message processing", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := service.processMessage(ctx, msg, "test-worker")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Parallel message processing", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				err := service.processMessage(ctx, msg, "test-worker")
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
