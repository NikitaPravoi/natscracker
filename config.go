package natscracker

import (
	"fmt"
	"time"
)

// Config holds the service configuration with validation
type Config struct {
	ServiceName string // ServiceName is used to identify service inside jaeger
	NatsURL     string
	RetryCount  int // Number of retries for failed messages
	RetryDelay  time.Duration
}

// Validate configuration
func (c *Config) Validate() error {
	// Required
	if c.ServiceName == "" {
		return fmt.Errorf("%w: service name is required", ErrValidation)
	}
	if c.NatsURL == "" {
		return fmt.Errorf("%w: NATS URL is required", ErrValidation)
	}

	if c.RetryCount < 0 {
		c.RetryCount = 3
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = time.Second
	}

	// Warnings
	return nil
}
