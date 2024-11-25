package natscracker

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"log/slog"
	"sync"
	"time"
)

var _ Builder = (*ServiceBuilder)(nil)

// Builder interface defines the contract for service construction
type Builder interface {
	// Build creates and returns a new Service instance
	Build(ctx context.Context) (*Service, error)

	// Configuration methods
	WithNATS(opts ...nats.Option) Builder
	WithEventIDGenerator(generator EventIDGenerator) Builder
	WithErrorBufferSize(size int) Builder
	WithTracer(ctx context.Context) Builder

	// Stream and consumer management
	AddStream(ctx context.Context, config jetstream.StreamConfig) (jetstream.Stream, error)
	AddConsumer(ctx context.Context, stream jetstream.Stream, config jetstream.ConsumerConfig, workers int8) error
}

// ServiceBuilder implements the Builder interface
type ServiceBuilder struct {
	mu sync.Mutex // Protects concurrent builder usage

	// Required components
	config Config

	// Optional components with defaults
	natsOpts        []nats.Option
	errorBufferSize int

	// Internal state
	nc               *nats.Conn
	js               jetstream.JetStream
	consumers        []Consumer
	eventIDGenerator EventIDGenerator
	tracer           *sdktrace.TracerProvider
	errorChan        chan error
	logger           *slog.Logger

	// Build state
	built bool
}

// NewServiceBuilder creates a new ServiceBuilder instance
func NewServiceBuilder(cfg Config) (*ServiceBuilder, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &ServiceBuilder{
		config:           cfg,
		errorBufferSize:  100,              // Default buffer size
		eventIDGenerator: &UUIDGenerator{}, // Default ID generator
		logger:           slog.Default(),
	}, nil
}

// WithNATS configures NATS connection options
func (b *ServiceBuilder) WithNATS(opts ...nats.Option) Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Merge with default options
	defaultOpts := []nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second * 2),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if b.errorChan != nil {
				b.errorChan <- fmt.Errorf("%w: connection lost: %v", ErrNATSConnection, err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			b.logger.Info("reconnected to NATS server", "url", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			if b.errorChan != nil {
				b.errorChan <- fmt.Errorf("%w: subscription error: %v", ErrNATSConnection, err)
			}
		}),
	}

	b.natsOpts = append(defaultOpts, opts...)
	return b
}

// WithEventIDGenerator sets a custom event ID generator
func (b *ServiceBuilder) WithEventIDGenerator(generator EventIDGenerator) Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.eventIDGenerator = generator
	return b
}

// WithErrorBufferSize sets the error channel buffer size
func (b *ServiceBuilder) WithErrorBufferSize(size int) Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.errorBufferSize = size
	return b
}

// WithTracer configures OpenTelemetry tracing
func (b *ServiceBuilder) WithTracer(ctx context.Context) Builder {
	b.mu.Lock()
	defer b.mu.Unlock()

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		b.logger.Error("failed to create OTLP exporter", "error", err)
		return b
	}

	resource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(b.config.ServiceName),
	)

	b.tracer = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource),
	)
	otel.SetTracerProvider(b.tracer)

	return b
}

// initNATS initializes the NATS connection
func (b *ServiceBuilder) initNATS() error {
	nc, err := nats.Connect(b.config.NatsURL, b.natsOpts...)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNATSConnection, err)
	}
	b.nc = nc

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("%w: failed to create JetStream context: %v", ErrNATSConnection, err)
	}
	b.js = js

	return nil
}

// AddStream creates or updates a NATS stream
func (b *ServiceBuilder) AddStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.js == nil {
		if err := b.initNATS(); err != nil {
			return nil, err
		}
	}

	stream, err := b.js.CreateStream(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return stream, nil
}

// AddConsumer creates or updates a consumer
func (b *ServiceBuilder) AddConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig, workers int8) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cons, err := stream.CreateOrUpdateConsumer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrConsumerSetup, err)
	}

	b.consumers = append(b.consumers, Consumer{
		Name:    cfg.Name,
		Cons:    cons,
		Workers: workers,
	})
	return nil
}

// Build creates the final Service instance
func (b *ServiceBuilder) Build(ctx context.Context) (*Service, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.built {
		return nil, fmt.Errorf("builder already used")
	}

	// Initialize components if not already done
	if b.errorChan == nil {
		b.errorChan = make(chan error, b.errorBufferSize)
	}

	if b.js == nil {
		if err := b.initNATS(); err != nil {
			return nil, err
		}
	}

	// Create service instance
	svc := &Service{
		consumers:        b.consumers,
		eventIDGenerator: b.eventIDGenerator,
		nc:               b.nc,
		js:               b.js,
		errorChan:        b.errorChan,
		tracer:           b.tracer,
		handlers:         make(map[string]MessageHandler),
		logger:           b.logger,
	}

	b.built = true
	return svc, nil
}

// EventIDGenerator implementations remain the same
type EventIDGenerator interface {
	GenerateID() string
}

type UUIDGenerator struct{}

func (g *UUIDGenerator) GenerateID() string {
	return uuid.New().String()
}

type UnixIDGenerator struct{}

func (g *UnixIDGenerator) GenerateID() string {
	return fmt.Sprintf("evt_%d", time.Now().UnixNano())
}
