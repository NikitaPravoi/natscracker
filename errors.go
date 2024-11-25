package natscracker

import (
	"context"
	"errors"
	"log"
)

// Custom errors
var (
	ErrValidation       = errors.New("validation error")
	ErrEventProcessing  = errors.New("event processing error")
	ErrEventPublishing  = errors.New("event publishing error")
	ErrServiceShutdown  = errors.New("service shutdown error")
	ErrNATSConnection   = errors.New("NATS connection error")
	ErrConsumerSetup    = errors.New("consumer setup error")
	ErrMessageUnmarshal = errors.New("message unmarshal error")
)

// RetryableError indicates if an error should trigger a retry
type RetryableError struct {
	Err       error
	Retryable bool
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

// monitorErrors handles error reporting and monitoring
func (s *Service) monitorErrors(ctx context.Context) {
	s.shutdownWg.Add(1)
	defer s.shutdownWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-s.errorChan:
			// Log error and potentially report to monitoring system
			log.Printf("Service error: %v", err)
			// You could add metrics or additional error reporting here
		}
	}
}
