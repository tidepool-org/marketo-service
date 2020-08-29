package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
	"log"


	"github.com/segmentio/kafka-go"
	"github.com/tidepool-org/marketo-service/marketo"
	"github.com/tidepool-org/marketo-service/store"
	"github.com/tidepool-org/go-common/clients/mongo"

)

var (
	Partition = 0
	Broker    = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	Topic     = "marketo"
	GroupId   = "Marketo-Group-Consumer"
)

type Api struct {
	marketoManager marketo.Manager
	store *store.MongoStoreClient
}
type Config struct {
	// clients.Config
	// Service disc.ServiceListing `json:"service"`
	Mongo   mongo.Config        `json:"mongo"`
	// User    user.ApiConfig      `json:"user"`
}
type User struct {
	Id             string   `json:"userid,omitempty" bson:"userid,omitempty"` // map userid to id
	Username       string   `json:"username,omitempty" bson:"username,omitempty"`
	Emails         []string `json:"emails,omitempty" bson:"emails,omitempty"`
	Roles          []string `json:"roles,omitempty" bson:"roles,omitempty"`
	TermsAccepted  string   `json:"termsAccepted,omitempty" bson:"termsAccepted,omitempty"`
	EmailVerified  bool     `json:"emailVerified" bson:"authenticated"` //tag is name `authenticated` for historical reasons
	PwHash         string   `json:"-" bson:"pwhash,omitempty"`
	Hash           string   `json:"-" bson:"userhash,omitempty"`
	CreatedTime    string   `json:"createdTime,omitempty" bson:"createdTime,omitempty"`
	CreatedUserID  string   `json:"createdUserId,omitempty" bson:"createdUserId,omitempty"`
	ModifiedTime   string   `json:"modifiedTime,omitempty" bson:"modifiedTime,omitempty"`
	ModifiedUserID string   `json:"modifiedUserId,omitempty" bson:"modifiedUserId,omitempty"`
	DeletedTime    string   `json:"deletedTime,omitempty" bson:"deletedTime,omitempty"`
	DeletedUserID  string   `json:"deletedUserId,omitempty" bson:"deletedUserId,omitempty"`
}
type NewUser struct {
	Username       string   `json:"username,omitempty" bson:"username,omitempty"`
	Roles          []string `json:"roles,omitempty" bson:"roles,omitempty"`
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

func (a *Api) reader(topic string, broker string, partition int, clientStore *store.MongoStoreClient) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: partition,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	
	for {
		var oldUser User
		var newUser User
		var deletedUser User
		var message map[string]interface{}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		log.Println(string(m.Value))
		if err := json.Unmarshal(m.Value, &message); err != nil {
			fmt.Println(topic, "Error Unmarshalling Message", err)
		} else {
			log.Println(message)
			log.Println(message["user"])
			
			if message["event"] == "create-user" {
				newUserMessage := fmt.Sprintf("%v", message["user"])
				log.Println(newUserMessage)
				userFromDataBase := a.findUser(newUserMessage)
				if err := json.Unmarshal([]byte(userFromDataBase.Id), &newUser); err != nil {
					log.Println(topic, "Error Unmarshalling New User", err)
				} else {
					a.marketoManager.CreateListMembershipForUser(&newUser)
				}
			}
			if message["event"] == "update-user" {
				oldUserMessage := fmt.Sprintf("%v", message["user"])
				userFromDataBase := a.findUser(oldUserMessage)
				if err := json.Unmarshal([]byte(userFromDataBase.Id), &oldUser); err != nil {
					log.Println(topic, "Error Unmarshalling Old User", err)
				} else {
					a.marketoManager.UpdateListMembershipForUser(&oldUser, &oldUser, false)
				}
			}
			if message["op"] == "d" {
				deletedUserMessage := fmt.Sprintf("%v", message["after"])
				if err := json.Unmarshal([]byte(deletedUserMessage), &deletedUser); err != nil {
					log.Println(topic, "Error Unmarshalling New User", err)
				} else {
					a.marketoManager.UpdateListMembershipForUser(&oldUser, &oldUser, true)
				}
			}
			log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}
	}

	r.Close()
}

func (a *Api) findUser(id string) *store.User{
	if results, err := a.store.WithContext(context.Background()).FindUser(id); err != nil {
		log.Printf("%v", err)
	} else {
		return results
	}
	return &store.User{}
}

func main() {
	var config Config
	config.Mongo.FromEnv()
	clientStore := store.NewMongoStoreClient(&config.Mongo)
	defer clientStore.Disconnect()
	clientStore.EnsureIndexes()

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	topics, _ := os.LookupEnv("KAFKA_TOPIC")
	broker, _ := os.LookupEnv("KAFKA_BROKERS")
	a := Api{}

	log.Println("In main testing version")
	time.Sleep(10 * time.Second)
	log.Println("Finished sleep")

	startTime := time.Now()

	for _, topic := range strings.Split(topics, ",") {
		go a.reader(topic, broker, 0, clientStore)
	}

	log.Printf("Duration in seconds: %f\n", time.Now().Sub(startTime).Seconds())
	// Hack - do not quit for now
	log.Println("Sleeping until the end of time")
	for {
		time.Sleep(100 * time.Second)
	}
}
