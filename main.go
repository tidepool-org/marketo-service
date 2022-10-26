package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	clinic "github.com/tidepool-org/clinic/client"
	"github.com/tidepool-org/go-common/clients/disc"
	"github.com/tidepool-org/go-common/clients/shoreline"
	"github.com/tidepool-org/go-common/errors"
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/handler"
	"github.com/tidepool-org/marketo-service/marketo"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	keycloakUsersTopic = "keycloak.public.user_entity"
	keycloakRolesTopic = "keycloak.public.user_role_mapping"
)

type Config struct {
	Marketo marketo.Config `json:"marketo"`
}

type ServiceConfig struct {
	ClinicsHost   string `envconfig:"TIDEPOOL_CLINIC_CLIENT_ADDRESS" default:"http://clinic:8080"`
	ListenAddress string `envconfig:"LISTEN_ADDRESS" default:":8080"`
	ServerSecret  string `envconfig:"TIDEPOOL_SERVER_SECRET" required:"true"`
	ShorelineHost string `envconfig:"TIDEPOOL_SHORELINE_CLIENT_ADDRESS" default:"http://shoreline:9107"`
}

func (s *ServiceConfig) LoadFromEnv() error {
	return envconfig.Process("", s)
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
		//log.Fatalf("WARNING: Marketo config is invalid: %v", err)
	} else {
		log.Print("initializing marketo manager")
		marketoManager, _ = marketo.NewManager(logger, config.Marketo)
	}

	serviceConfig := &ServiceConfig{}
	if err := serviceConfig.LoadFromEnv(); err != nil {
		log.Fatalln(err)
	}

	cloudEventsConfig := events.NewConfig()
	if err := cloudEventsConfig.LoadFromEnv(); err != nil {
		logger.Println("error loading kafka config")
		log.Fatalln(err)
	}

	shorelineClient, err := buildShoreline(serviceConfig)
	if err != nil {
		log.Fatalln(err)
	}

	clinicService, err := buildClinicService(serviceConfig, shorelineClient)
	if err != nil {
		log.Fatalln(err)
	}

	userEventsHandler := &handler.UserEventsHandler{
		Clinics:        clinicService,
		Shoreline:      shorelineClient,
		MarketoManager: marketoManager,
	}

	handlers := []events.EventHandler{
		events.NewUserEventsHandler(userEventsHandler),
		&events.DebugEventHandler{},
	}

	createConsumer := func() (events.MessageConsumer, error) {
		return events.NewCloudEventsMessageHandler(handlers)
	}

	cg, err := events.NewFaultTolerantConsumerGroup(cloudEventsConfig, createConsumer)
	if err != nil {
		log.Fatalln(err)
	}

	keycloakEventsHandler := handler.KeycloakEventsHandler{
		Clinics:        clinicService,
		MarketoManager: marketoManager,
		Shoreline:      shorelineClient,
	}

	keycloakUsersConfig := *cloudEventsConfig
	keycloakUsersConfig.KafkaTopic = keycloakUsersTopic
	keycloakUsersConfig.KafkaDeadLettersTopic = ""
	// CDC topic use '.' separator instead of '-'
	if strings.HasSuffix(keycloakUsersConfig.KafkaTopicPrefix, "-") {
		keycloakUsersConfig.KafkaTopicPrefix = strings.TrimSuffix(keycloakUsersConfig.KafkaTopicPrefix, "-") + "."
	}

	keycloakUsersCg, err := events.NewFaultTolerantConsumerGroup(&keycloakUsersConfig, func() (events.MessageConsumer, error) {
		return handler.NewKeycloakUserEventsConsumer(&keycloakEventsHandler)
	})
	if err != nil {
		log.Fatalln(err)
	}

	keycloakRolesConfig := *cloudEventsConfig
	keycloakRolesConfig.KafkaTopic = keycloakRolesTopic
	keycloakRolesConfig.KafkaDeadLettersTopic = ""
	// CDC topic use '.' separator instead of '-'
	if strings.HasSuffix(keycloakRolesConfig.KafkaTopicPrefix, "-") {
		keycloakRolesConfig.KafkaTopicPrefix = strings.TrimSuffix(keycloakRolesConfig.KafkaTopicPrefix, "-") + "."
	}

	keycloakRolesCg, err := events.NewFaultTolerantConsumerGroup(cloudEventsConfig, func() (events.MessageConsumer, error) {
		return handler.NewKeycloakRoleEventsConsumer(&keycloakEventsHandler)
	})
	if err != nil {
		log.Fatalln(err)
	}

	router := mux.NewRouter()
	refreshUser := handler.RefreshUser(userEventsHandler, shorelineClient)
	router.HandleFunc("/v1/users/{userId}/marketo", refreshUser).Methods("POST")

	srv := &http.Server{
		Addr:    serviceConfig.ListenAddress,
		Handler: router,
	}

	wg := sync.WaitGroup{}
	wg.Add(4)
	shutdown := make(chan struct{}, 2)

	// listen to signals to stop consumer
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func(stop chan os.Signal) {
		<-stop
		log.Println("SIGINT or SIGTERM received")
		shutdown <- struct{}{}
	}(stop)

	_, cancel := context.WithCancel(context.Background())
	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Println(errors.Wrap(err, "Unable to start server"))
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()

		err = cg.Start()
		if err != nil {
			log.Println(errors.Wrap(err, "Unable to start consumer"))
		} else {
			log.Println("Consumer stopped")
		}

	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()

		err = keycloakUsersCg.Start()
		if err != nil {
			log.Println(errors.Wrap(err, "Unable to start consumer"))
		} else {
			log.Println("Consumer stopped")
		}
	}(&wg)

	go func(wg *sync.WaitGroup) {
		defer func() { shutdown <- struct{}{} }()

		err = keycloakRolesCg.Start()
		if err != nil {
			log.Println(errors.Wrap(err, "Unable to start consumer"))
		} else {
			log.Println("Consumer stopped")
		}
	}(&wg)

	go func(shutdown chan struct{}, cancel context.CancelFunc, wg *sync.WaitGroup) {
		defer cancel()
		<-shutdown
		log.Println("Shutting down")

		go func() {
			defer wg.Done()
			shutdownCtx, c := context.WithTimeout(context.Background(), time.Second*60)
			defer c()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Println(errors.Wrap(err, "Unable to shutdown server"))
			}
		}()

		go func() {
			defer wg.Done()
			if err := cg.Stop(); err != nil {
				log.Println(errors.Wrap(err, "Unable to stop user events consumer group"))
			}
		}()

		go func() {
			defer wg.Done()
			if err := keycloakUsersCg.Stop(); err != nil {
				log.Println(errors.Wrap(err, "Unable to stop keycloak users consumer group"))
			}
		}()

		go func() {
			defer wg.Done()
			if err := keycloakRolesCg.Stop(); err != nil {
				log.Println(errors.Wrap(err, "Unable to stop keycloak roles consumer group"))
			}
		}()
	}(shutdown, cancel, &wg)

	wg.Wait()
}

func buildShoreline(config *ServiceConfig) (shoreline.Client, error) {
	httpClient := &http.Client{}
	client := shoreline.NewShorelineClientBuilder().
		WithHostGetter(disc.NewStaticHostGetterFromString(config.ShorelineHost)).
		WithHttpClient(httpClient).
		WithName("marketo-service").
		WithSecret(config.ServerSecret).
		WithTokenRefreshInterval(time.Hour).
		Build()

	return client, client.Start()
}

func buildClinicService(config *ServiceConfig, shorelineClient shoreline.Client) (clinic.ClientWithResponsesInterface, error) {
	opts := clinic.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
		req.Header.Add("x-tidepool-session-token", shorelineClient.TokenProvide())
		return nil
	})
	return clinic.NewClientWithResponses(config.ClinicsHost, opts)
}
