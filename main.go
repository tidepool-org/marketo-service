package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/tidepool-org/go-common/errors"
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/handler"
	"github.com/tidepool-org/marketo-service/marketo"
)

type Config struct {
	Marketo marketo.Config `json:"marketo"`
}

func main() {
	var config Config
	logger := log.New(os.Stdout, "marketo-service", log.LstdFlags|log.Lshortfile)
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

	cloudEventsConfig := events.NewConfig()
	if err := cloudEventsConfig.LoadFromEnv(); err != nil {
		logger.Println("error loading kafka config")
		log.Fatalln(err)
	}

	consumer, err := events.NewKafkaCloudEventsConsumer(cloudEventsConfig)
	if err != nil {
		log.Fatalln(err)
	}
	userEventsHandler := events.NewUserEventsHandler(&handler.UserEventsHandler{
		MarketoManager: marketoManager,
	})
	consumer.RegisterHandler(userEventsHandler)
	consumer.RegisterHandler(&events.DebugEventHandler{})

	// listen to signals to stop consumer
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancelFunc := context.WithCancel(context.Background())
	// convert to cancel on context that server listens to
	go func(stop chan os.Signal, cancelFunc context.CancelFunc) {
		<-stop
		log.Println("SIGINT or SIGTERM received. Shutting down consumer")
		cancelFunc()
	}(stop, cancelFunc)

	err = consumer.Start(ctx)
	if err != nil {
		log.Fatalln(errors.Wrap(err, "Unable to start consumer"))
	} else {
		log.Println("Consumer stopped")
	}
}
