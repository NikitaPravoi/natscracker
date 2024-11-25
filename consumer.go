package natscracker

import "github.com/nats-io/nats.go/jetstream"

type Consumer struct {
	Name    string
	Cons    jetstream.Consumer
	Workers int8
}
