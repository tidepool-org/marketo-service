package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	KafkaContextTimeout = time.Duration(60) * time.Second

	Partition            = 0
	Broker               = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	Topic                = "marketo"
	GroupId              = "Marketo-Group-Consumer"
	MaxMessages          = 40000000
	WriteCount           = 50000
	DeviceDataNumWorkers = 5
)

func reader(topic string, broker string, partition int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(10)

	for i := 0; i < MaxMessages; i++ {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}

func main() {
	topics, _ := os.LookupEnv("KAFKA_TOPIC")
	broker, _ := os.LookupEnv("KAFKA_BROKERS")

	fmt.Println("In main")
	time.Sleep(10 * time.Second)
	fmt.Println("Finished sleep")

	startTime := time.Now()

	for _, topic := range strings.Split(topics, ",") {
		go reader(topic, broker, 0)
	}

	fmt.Printf("Duration in seconds: %f\n", time.Now().Sub(startTime).Seconds())
	// Hack - do not quit for now
	fmt.Println("Sleeping until the end of time")
	for {
		time.Sleep(100 * time.Second)
	}
}
