package natscracker

import (
	"context"
	"errors"
	"fmt"
	"github.com/NikitaPravoi/natscracker/pkg/pb"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// ServiceState represents the current state of the service
type ServiceState int32

const (
	StateInitial ServiceState = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
)

// ServiceMetrics holds service-level metrics
type ServiceMetrics struct {
	MessagesProcessed  atomic.Int64
	MessagesSuccessful atomic.Int64
	MessagesFailed     atomic.Int64
	MessagesPublished  atomic.Int64
	ProcessingDuration atomic.Int64 // nanoseconds
	ActiveWorkers      atomic.Int32
}

// Service represents the microservice
type Service struct {
	state     atomic.Int32 // ServiceState
	metrics   ServiceMetrics
	startTime time.Time

	consumers        []Consumer
	handlers         map[string]MessageHandler
	handlersMu       sync.RWMutex // protects handlers map
	tracer           *sdktrace.TracerProvider
	nc               *nats.Conn
	js               jetstream.JetStream
	eventIDGenerator EventIDGenerator
	shutdownWg       sync.WaitGroup
	errorChan        chan error
	logger           *slog.Logger

	// Circuit breaker configuration
	circuitBreaker *CircuitBreaker
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	failures    atomic.Int32
	lastFailure atomic.Int64
	threshold   int32
	resetAfter  time.Duration
	state       atomic.Int32 // 0: closed, 1: open
}

func NewCircuitBreaker(threshold int32, resetAfter time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:  threshold,
		resetAfter: resetAfter,
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	if cb.state.Load() == 0 {
		return true
	}

	lastFailure := time.Unix(0, cb.lastFailure.Load())
	if time.Since(lastFailure) > cb.resetAfter {
		cb.state.Store(0)
		cb.failures.Store(0)
		return true
	}
	return false
}

func (cb *CircuitBreaker) RecordFailure() {
	failures := cb.failures.Add(1)
	if failures >= cb.threshold {
		cb.state.Store(1)
		cb.lastFailure.Store(time.Now().UnixNano())
	}
}

// MessageHandler is a function type for message handlers
type MessageHandler func(ctx context.Context, event *pb.BaseEvent) error

// RegisterHandler registers a message handler for a specific event type
func (s *Service) RegisterHandler(eventType string, handler MessageHandler) error {
	if eventType == "" {
		return errors.New("event type cannot be empty")
	}
	if handler == nil {
		return errors.New("handler cannot be nil")
	}

	s.handlersMu.Lock()
	defer s.handlersMu.Unlock()

	if _, exists := s.handlers[eventType]; exists {
		return fmt.Errorf("handler already registered for event type: %s", eventType)
	}

	s.handlers[eventType] = handler
	return nil
}

// Start begins processing messages
func (s *Service) Start(ctx context.Context) error {
	if !s.state.CompareAndSwap(int32(StateInitial), int32(StateStarting)) {
		return errors.New("service already started or stopping")
	}

	s.startTime = time.Now()
	s.logger.Info("starting service", "consumers", len(s.consumers))

	// Start error monitoring
	go s.monitorErrors(ctx)

	// Start consumers
	if err := s.startConsumers(ctx); err != nil {
		return fmt.Errorf("failed to start consumers: %w", err)
	}

	s.circuitBreaker = NewCircuitBreaker(1, 5*time.Second)

	s.state.Store(int32(StateRunning))
	return nil
}

// startConsumers initializes all consumers
func (s *Service) startConsumers(ctx context.Context) error {
	for _, c := range s.consumers {
		for w := 0; w < int(c.Workers); w++ {
			s.shutdownWg.Add(1)
			workerID := fmt.Sprintf("%s-worker-%d", c.Name, w)

			go func(consumer Consumer, wID string) {
				defer s.shutdownWg.Done()
				defer s.metrics.ActiveWorkers.Add(-1)

				s.metrics.ActiveWorkers.Add(1)
				s.runWorker(ctx, consumer.Cons, wID)
			}(c, workerID)
		}
	}
	return nil
}

// runWorker processes messages for a single worker
func (s *Service) runWorker(ctx context.Context, cons jetstream.Consumer, workerID string) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			s.logger.Error("worker panic",
				"worker_id", workerID,
				"error", r,
				"stack", string(stack))
			s.errorChan <- fmt.Errorf("worker %s panic: %v", workerID, r)
		}
	}()

	messages := make(chan jetstream.Msg, 32) // Buffer for incoming messages

	consumer, err := cons.Consume(func(msg jetstream.Msg) {
		select {
		case messages <- msg:
		case <-ctx.Done():
			return
		}
	})

	if err != nil {
		s.logger.Error("failed to create consumer",
			"worker_id", workerID,
			"error", err)
		return
	}
	defer consumer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			if err := s.processMessage(ctx, msg, workerID); err != nil {
				s.logger.Error("message processing failed",
					"worker_id", workerID,
					"error", err)
			}
		}
	}
}

// processMessage handles a single message with retry logic and circuit breaker
func (s *Service) processMessage(ctx context.Context, m jetstream.Msg, workerID string) error {
	if !s.circuitBreaker.AllowRequest() {
		m.Nak()
		return errors.New("circuit breaker open")
	}

	startTime := time.Now()
	metadata, _ := m.Metadata()

	// Create span for message processing
	tracer := otel.Tracer(m.Subject())
	ctx, span := tracer.Start(ctx, "process_message")
	defer span.End()

	span.SetAttributes(
		attribute.String("worker.id", workerID),
		attribute.Int("attempt", int(metadata.NumDelivered)),
		attribute.String("subject", m.Subject()),
	)

	err := s.handleMessage(ctx, m)
	s.metrics.MessagesProcessed.Add(1)

	if err != nil {
		s.metrics.MessagesFailed.Add(1)
		s.circuitBreaker.RecordFailure()

		var retryErr *RetryableError
		if errors.As(err, &retryErr) && retryErr.Retryable {
			m.Nak()
			span.SetStatus(codes.Error, err.Error())
			s.logger.Warn("message processing failed, retrying",
				"worker_id", workerID,
				"attempt", metadata.NumDelivered,
				"error", err)
		} else {
			m.TermWithReason(err.Error())
			span.SetStatus(codes.Error, "terminal error")
			s.logger.Error("message processing failed permanently",
				"worker_id", workerID,
				"error", err)
		}
		return err
	}

	// Success handling
	s.metrics.MessagesSuccessful.Add(1)
	s.metrics.ProcessingDuration.Add(time.Since(startTime).Nanoseconds())
	m.Ack()
	span.SetStatus(codes.Ok, "")

	return nil
}

// handleMessage processes a single message
func (s *Service) handleMessage(ctx context.Context, m jetstream.Msg) error {
	event := &pb.BaseEvent{}

	if err := proto.Unmarshal(m.Data(), event); err != nil {
		return &RetryableError{
			Err:       ErrMessageUnmarshal,
			Retryable: false, // Don't retry on unmarshal errors
		}
	}

	s.handlersMu.RLock()
	handler, ok := s.handlers[event.Type]
	s.handlersMu.RUnlock()

	if !ok {
		return &RetryableError{
			Err:       fmt.Errorf("no handler registered for event type: %s", event.Type),
			Retryable: false,
		}
	}

	return handler(ctx, event)
}

// PublishEvent sends a protobuf event to NATS
func (s *Service) PublishEvent(ctx context.Context, subject string, eventType string, payload proto.Message) error {
	if s.state.Load() != int32(StateRunning) {
		return errors.New("service not running")
	}

	anyPayload, err := anypb.New(payload)
	if err != nil {
		return fmt.Errorf("failed to pack payload: %w", err)
	}

	event := &pb.BaseEvent{
		Id:        s.eventIDGenerator.GenerateID(),
		Type:      eventType,
		Timestamp: timestamppb.Now(),
		Payload:   anyPayload,
	}

	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = s.js.Publish(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
	}

	s.metrics.MessagesPublished.Add(1)
	return nil
}

// GetMetrics returns current service metrics
func (s *Service) GetMetrics() map[string]any {
	return map[string]any{
		"messages_processed":  s.metrics.MessagesProcessed.Load(),
		"messages_successful": s.metrics.MessagesSuccessful.Load(),
		"messages_published":  s.metrics.MessagesPublished.Load(),
		"messages_failed":     s.metrics.MessagesFailed.Load(),
		"processing_duration": s.metrics.ProcessingDuration.Load(),
		"active_workers":      s.metrics.ActiveWorkers.Load(),
	}
}

// GetStatus returns the current service status
func (s *Service) GetStatus() map[string]interface{} {
	uptime := time.Since(s.startTime)
	return map[string]any{
		"state":          ServiceState(s.state.Load()),
		"uptime":         uptime.String(),
		"active_workers": s.metrics.ActiveWorkers.Load(),
		"nats_connected": s.nc.IsConnected(),
	}
}

// Shutdown gracefully stops the service
func (s *Service) Shutdown(ctx context.Context) error {
	if !s.state.CompareAndSwap(int32(StateRunning), int32(StateStopping)) {
		return errors.New("service not running")
	}

	s.logger.Info("initiating service shutdown")
	shutdownComplete := make(chan struct{})

	go func() {
		s.shutdownWg.Wait()
		close(shutdownComplete)
	}()

	select {
	case <-shutdownComplete:
		s.logger.Info("service shutdown completed")
	case <-ctx.Done():
		return fmt.Errorf("shutdown timeout: %w", ctx.Err())
	}

	if s.tracer != nil {
		if err := s.tracer.Shutdown(ctx); err != nil {
			s.logger.Error("error shutting down tracer", "error", err)
		}
	}

	s.nc.Close()
	s.state.Store(int32(StateStopped))
	return nil
}

// Helper function to unpack event payload
func UnpackEventPayload[T proto.Message](event *pb.BaseEvent, empty T) (T, error) {
	if event.Payload == nil {
		return empty, errors.New("event payload is nil")
	}

	err := event.Payload.UnmarshalTo(empty)
	if err != nil {
		return empty, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return empty, nil
}
