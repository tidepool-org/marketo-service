package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	KafkaContextTimeout = time.Duration(60) * time.Second

	Partition            = 0
	Broker               = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	Topic                = "Marketo"
	GroupId              = "Marketo-Group-Consumer"
	MaxMessages          = 40000000
	WriteCount           = 50000
	DeviceDataNumWorkers = 5
)

func reader(topic string, broker []string, partition int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   broker,
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(10)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
