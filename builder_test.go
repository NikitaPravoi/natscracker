package natscracker

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Mock implementations
type MockEventIDGenerator struct {
	mock.Mock
}

func (m *MockEventIDGenerator) GenerateID() string {
	args := m.Called()
	return args.String(0)
}

// TestBuilder tests the builder pattern implementation
func TestBuilder(t *testing.T) {
	t.Run("NewServiceBuilder with valid config", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-service",
			NatsURL:     "nats://localhost:4222",
			RetryCount:  3,
			RetryDelay:  time.Second,
		}

		builder, err := NewServiceBuilder(cfg)
		require.NoError(t, err)
		require.NotNil(t, builder)
	})

	t.Run("NewServiceBuilder with invalid config", func(t *testing.T) {
		cfg := Config{} // Empty config
		builder, err := NewServiceBuilder(cfg)
		require.Error(t, err)
		require.Nil(t, builder)
	})

	t.Run("Builder configuration methods", func(t *testing.T) {
		cfg := Config{
			ServiceName: "test-service",
			NatsURL:     "nats://localhost:4222",
			RetryCount:  3,
			RetryDelay:  time.Second,
		}

		builder, err := NewServiceBuilder(cfg)
		require.NoError(t, err)

		// Test WithErrorBufferSize
		builder.WithErrorBufferSize(200)

		// Test WithEventIDGenerator
		mockGenerator := &MockEventIDGenerator{}
		builder.WithEventIDGenerator(mockGenerator)

		// Test WithNATS
		builder.WithNATS(nats.Name("test"))

		// Final build should succeed
		ctx := context.Background()
		service, err := builder.Build(ctx)
		require.NoError(t, err)
		require.NotNil(t, service)
	})
}
