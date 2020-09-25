package main

import (
	"context"
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/handler"
	"github.com/tidepool-org/marketo-service/marketo"
	"log"
	"os"
	"strconv"
)

type Config struct {
	Marketo marketo.Config `json:"marketo"`
}

func main() {
	var config Config
	logger := log.New(os.Stdout, "api/user", log.LstdFlags|log.Lshortfile)
	config.Marketo.ID, _ = os.LookupEnv("MARKETO_ID")
	config.Marketo.URL, _ = os.LookupEnv("MARKETO_URL")
	config.Marketo.Secret, _ = os.LookupEnv("MARKETO_SECRET")
	config.Marketo.ClinicRole, _ = os.LookupEnv("MARKETO_CLINIC_ROLE")
	config.Marketo.PatientRole, _ = os.LookupEnv("MARKETO_PATIENT_ROLE")
	unParsedTimeout, found := os.LookupEnv("MARKETO_TIMEOUT")
	if found {
		parsedTimeout64, err := strconv.ParseInt(unParsedTimeout, 10, 32)
		parsedTimeout := uint(parsedTimeout64)
		if err != nil {
			logger.Println(err)
		}
		config.Marketo.Timeout = parsedTimeout
	}

	var marketoManager marketo.Manager
	if err := config.Marketo.Validate(); err != nil {
		log.Fatalf("WARNING: Marketo config is invalid: %v", err)
	} else {
		log.Print("initializing marketo manager")
		marketoManager, _ = marketo.NewManager(logger, config.Marketo)
	}

	kafkaConfig := &events.KafkaConfig{}
	if err := kafkaConfig.LoadFromEnv(); err != nil {
		log.Fatalln(err)
	}
	consumer, err := events.NewKafkaCloudEventsConsumer(kafkaConfig)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("created consumer")
	userEventsHandler := events.NewUserEventsHandler(&handler.UserEventsHandler{
		MarketoManager: marketoManager,
	})
	consumer.RegisterHandler(userEventsHandler)
	consumer.RegisterHandler(&events.DebugEventHandler{})

	// Loop indefinitely
	for {
		// blocks until context is terminated or kafka returns EOF.
		// When Kafka returns EOF, the error return by the function is nil.
		if err := consumer.Start(context.Background()); err != nil {
			log.Fatalln(err)
		}
	}
}
