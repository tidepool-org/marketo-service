package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/tidepool-org/go-common/errors"
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/handler"
	"github.com/tidepool-org/marketo-service/marketo"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Marketo       marketo.Config `json:"marketo"`
	ListenAddress string
}

func main() {
	var config Config

	config.ListenAddress = os.Getenv("LISTEN_ADDRESS")
	if config.ListenAddress == "" {
		config.ListenAddress = ":8080"
	}

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
		//log.Fatalf("WARNING: Marketo config is invalid: %v", err)
	} else {
		log.Print("initializing marketo manager")
		marketoManager, _ = marketo.NewManager(logger, config.Marketo)
	}

	cloudEventsConfig := events.NewConfig()
	if err := cloudEventsConfig.LoadFromEnv(); err != nil {
		logger.Println("error loading kafka config")
		log.Fatalln(err)
	}

	consumer, err := events.NewSaramaCloudEventsConsumer(cloudEventsConfig)
	if err != nil {
		log.Fatalln(err)
	}
	userEventsHandler := &handler.UserEventsHandler{
		MarketoManager: marketoManager,
	}
	consumer.RegisterHandler(events.NewUserEventsHandler(userEventsHandler))
	consumer.RegisterHandler(&events.DebugEventHandler{})

	router := mux.NewRouter()
	refreshUser := handler.RefreshUser(userEventsHandler)
	router.HandleFunc("/v1/users/{userId}/marketo", refreshUser).Methods("POST")

	srv := &http.Server{
		Addr:    config.ListenAddress,
		Handler: router,
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	shutdown := make(chan struct{}, 2)

	// listen to signals to stop consumer
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func(stop chan os.Signal) {
		<-stop
		log.Println("SIGINT or SIGTERM received")
		shutdown <- struct{}{}
	}(stop)

	ctx, cancel := context.WithCancel(context.Background())
	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()
		defer wg.Done()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Println(errors.Wrap(err, "Unable to start server"))
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()
		defer wg.Done()

		err = consumer.Start(ctx)
		if err != nil {
			log.Println(errors.Wrap(err, "Unable to start consumer"))
		} else {
			log.Println("Consumer stopped")
		}

	}(&wg)

	go func(shutdown chan struct{}, cancel context.CancelFunc, wg *sync.WaitGroup) {
		defer wg.Done()
		defer cancel()
		<-shutdown
		log.Println("Shutting down")

		shutdownCtx, c := context.WithTimeout(context.Background(), time.Second * 60)
		defer c()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Println(errors.Wrap(err, "Unable to shutdown server"))
		}
	}(shutdown, cancel, &wg)

	wg.Wait()
}
