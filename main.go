package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tidepool-org/go-common/clients/mongo"
	"github.com/tidepool-org/marketo-service/marketo"
	"github.com/tidepool-org/marketo-service/store"
)

var (
	Broker  = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	Topic   = "marketo"
	GroupId = "Marketo-Group-Consumer"
)

type Api struct {
	marketoManager marketo.Manager
	store          *store.MongoStoreClient
}
type Config struct {
	// clients.Config
	// Service disc.ServiceListing `json:"service"`
	Mongo   mongo.Config   `json:"mongo"`
	Marketo marketo.Config `json:"marketo"`
}
type User struct {
	Id            string   `json:"userid,omitempty" bson:"userid,omitempty"` // map userid to id
	Username      string   `json:"username,omitempty" bson:"username,omitempty"`
	Emails        []string `json:"emails,omitempty" bson:"emails,omitempty"`
	Roles         []string `json:"roles,omitempty" bson:"roles,omitempty"`
	TermsAccepted string   `json:"termsAccepted,omitempty" bson:"termsAccepted,omitempty"`
	EmailVerified bool     `json:"emailVerified" bson:"authenticated"` //tag is name `authenticated` for historical reasons
}
type NewUser struct {
	Username string   `json:"username,omitempty" bson:"username,omitempty"`
	Roles    []string `json:"roles,omitempty" bson:"roles,omitempty"`
}

func (u *User) IsClinic() bool {
	return u.HasRole("clinic")
}
func (u *User) Email() string {
	return u.Username
}
func (u *User) HasRole(role string) bool {
	for _, userRole := range u.Roles {
		if userRole == role {
			return true
		}
	}
	return false
}

func (a *Api) reader(ctx context.Context, topic string, broker string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		var oldUser User
		var newUser User
		var deletedUser User
		var message map[string]interface{}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading kafka message. Breaking out")
			break
		}
		if err := json.Unmarshal(m.Value, &message); err != nil {
			fmt.Println(topic, "Error Unmarshalling Message", err)
		} else {
			log.Println(message)
			if message["event"] == "create-user" {
				// newUserMessage := fmt.Sprintf("%v", message["user"])
				// log.Println(newUserMessage)
				// userFromDataBase, err := a.store.FindUser(ctx, newUserMessage)
				// if err != nil {
				// 	log.Println(err)
				// 	return
				// }
				// userFromDataBaseBytes, _ := json.Marshal(userFromDataBase)
				// if err := json.Unmarshal(userFromDataBaseBytes, &newUser); err != nil {
				// 	log.Println(topic, "Error Unmarshalling New User", err)
				// } else {
				// 	a.marketoManager.CreateListMembershipForUser(&newUser)
				// }
				log.Println(newUser)
				a.marketoUpdate(ctx, message, topic, newUser)
			}
			if message["event"] == "update-user" {
				log.Println(message)
				// oldUserMessage := fmt.Sprintf("%v", message["user"])
				// userFromDataBase, err := a.store.FindUser(ctx, oldUserMessage)
				// if err != nil {
				// 	log.Println(err)
				// 	return
				// }
				// userFromDataBaseBytes, _ := json.Marshal(userFromDataBase)
				// if err := json.Unmarshal(userFromDataBaseBytes, &oldUser); err != nil {
				// 	log.Println(topic, "Error Unmarshalling Old User", err)
				// } else {
				// 	a.marketoManager.UpdateListMembershipForUser(&oldUser, &oldUser, false)
				// }
				a.marketoUpdate(ctx, message, topic, oldUser)
			}
			if message["event"] == "delete-user" {
				log.Println(message)
				// deletedUserMessage := fmt.Sprintf("%v", message["user"])
				// userFromDataBase, err := a.store.FindUser(ctx, deletedUserMessage)
				// if err != nil {
				// 	log.Println(err)
				// 	return
				// }
				// userFromDataBaseBytes, _ := json.Marshal(userFromDataBase)
				// if err := json.Unmarshal(userFromDataBaseBytes, &deletedUser); err != nil {
				// 	log.Println(topic, "Error Unmarshalling New User", err)
				// } else {
				// 	a.marketoManager.UpdateListMembershipForUser(&oldUser, &oldUser, true)
				// }
				a.marketoUpdate(ctx, message, topic, deletedUser)
			}
			log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}
	}

	r.Close()
}

func (a *Api) marketoUpdate(ctx context.Context, message map[string]interface{}, topic string, user User) {
	UserMessage := fmt.Sprintf("%v", message["user"])
	log.Printf("User action message has been recieved for user: %v. preparing to get user info from database", UserMessage)
	userFromDataBase, err := a.store.FindUser(ctx, UserMessage)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("User " + userFromDataBase.Username + " retrieved from database")
	userFromDataBaseBytes, _ := json.Marshal(userFromDataBase)
	if err := json.Unmarshal(userFromDataBaseBytes, &user); err != nil {
		log.Println(topic, "Error Unmarshalling New User", err)
	} else if message["event"] == "create-user" {
		log.Printf("Creating user %v in marketo database", userFromDataBase.Username)
		a.marketoManager.CreateListMembershipForUser(UserMessage, &user)
	} else if message["event"] == "update-user" {
		log.Printf("Updating user %v in marketo database", userFromDataBase.Username)
		a.marketoManager.UpdateListMembershipForUser(UserMessage, &user, false)
	} else if message["event"] == "delete-user" {
		log.Printf("Removing user %v from email lists", userFromDataBase.Username)
		a.marketoManager.UpdateListMembershipForUser(UserMessage, &user, true)
	}
}

func main() {
	var config Config
	logger := log.New(os.Stdout, "api/user", log.LstdFlags|log.Lshortfile)
	log.SetPrefix("api/user")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	config.Mongo.FromEnv()
	clientStore := store.NewMongoStoreClient(&config.Mongo)
	defer clientStore.Disconnect(context.Background())
	clientStore.EnsureIndexes()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	prefix, _ := os.LookupEnv("KAFKA_PREFIX")
	topic, _ := os.LookupEnv("KAFKA_TOPIC")
	topicWithPrefix := prefix + topic
	broker, _ := os.LookupEnv("KAFKA_BROKERS")
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
		log.Println("WARNING: Marketo config is invalid", err)
	} else {
		log.Print("initializing marketo manager")
		marketoManager, _ = marketo.NewManager(logger, config.Marketo)
	}

	a := Api{
		marketoManager: marketoManager,
		store:          clientStore,
	}

	log.Println("In main testing version")
	time.Sleep(10 * time.Second)
	log.Println("Finished sleep")

	startTime := time.Now()
	log.Printf("Connecting to %v topic on %v broker", topicWithPrefix, broker)
	for _, topic := range strings.Split(topicWithPrefix, ",") {
		go a.reader(context.Background(), topic, broker)
	}

	log.Printf("Duration in seconds: %f\n", time.Now().Sub(startTime).Seconds())
	// Hack - do not quit for now
	log.Println("Sleeping until the end of time")
	for {
		time.Sleep(100 * time.Second)
	}
}
