package main

import (
	"context"
	"fmt"
	nc "github.com/NikitaPravoi/natscracker"
	"github.com/NikitaPravoi/natscracker/pkg/pb"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	streamConfig = jetstream.StreamConfig{
		Name:      "mystream",
		Subjects:  []string{"events.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
	}
	consumerConfig = jetstream.ConsumerConfig{
		Name:          "myconsumer",
		Durable:       "myconsumer",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    4,
		BackOff:       []time.Duration{3 * time.Second, 5 * time.Second, 10 * time.Second},
		FilterSubject: "events.>",
	}
)

func main() {
	ctx := context.Background()

	cfg := nc.Config{
		ServiceName: "example-service",
		NatsURL:     "nats://localhost:4222",
		RetryCount:  3,           // Retry failed messages 3 times
		RetryDelay:  time.Second, // Wait 1 second between retries
	}

	// Create builder instance
	builder, err := nc.NewServiceBuilder(cfg)
	if err != nil {
		log.Fatal(err)
		return
	}

	// Configure builder
	builder.
		WithErrorBufferSize(200).
		WithEventIDGenerator(&nc.UUIDGenerator{}).
		WithTracer(ctx).
		WithNATS(nats.MaxReconnects(-1))

	// Add stream and consumer
	stream, err := builder.AddStream(ctx, streamConfig)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to add stream: %v", err))
		return
	}

	if err = builder.AddConsumer(ctx, stream, consumerConfig, 2); err != nil {
		log.Fatal(fmt.Sprintf("failed to add consumer: %v", err))
		return
	}

	// Build service
	service, err := builder.Build(ctx)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to build service: %v", err))
		return
	}

	service.RegisterHandler("user.created", func(ctx context.Context, event *pb.BaseEvent) error {
		fmt.Printf("Received msg: %v", event.Payload)
		return nil
	})

	// Create context that listens for interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	// Monitor metrics
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			log.Printf("Service metrics: %+v", service.GetMetrics())
		}
	}()

	// Start the service
	if err := service.Start(ctx); err != nil {
		log.Fatalf("Failed to start service: %v", err)
	}

	// Wait for shutdown signal
	<-ctx.Done()

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := service.Shutdown(shutdownCtx); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}
