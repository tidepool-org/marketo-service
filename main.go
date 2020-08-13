package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/tidepool-org/marketo-service/marketo"
)

var (
	Partition = 0
	Broker    = "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	Topic     = "marketo"
	GroupId   = "Marketo-Group-Consumer"
)

type Api struct {
	marketoManager marketo.Manager
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

func (a *Api) reader(topic string, broker string, partition int) {
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
		fmt.Println(m.Value)
		if err := json.Unmarshal(m.Value, &message); err != nil {
			fmt.Println(topic, "Error Unmarshalling Message", err)
		} else {
			fmt.Println(message)
			fmt.Println(message["op"])
			if message["op"] == "c" || message["op"] == "r" {
				newUserMessage := fmt.Sprintf("%v", message["after"])
				fmt.Println(newUserMessage)
				if err := json.Unmarshal([]byte(newUserMessage), &newUser); err != nil {
					fmt.Println(topic, "Error Unmarshalling New User", err)
				} else {
					a.marketoManager.CreateListMembershipForUser(&newUser)
				}
			}
			if message["op"] == "u" {
				oldUserMessage := fmt.Sprintf("%v", message["before"])
				newUserMessage := fmt.Sprintf("%v", message["after"])
				if err := json.Unmarshal([]byte(oldUserMessage), &oldUser); err != nil {
					fmt.Println(topic, "Error Unmarshalling Old User", err)
				} else if err := json.Unmarshal([]byte(newUserMessage), &newUser); err != nil {
					fmt.Println(topic, "Error Unmarshalling New User", err)
				} else {
					a.marketoManager.UpdateListMembershipForUser(&oldUser, &newUser, false)
				}
			}
			if message["op"] == "d" {
				deletedUserMessage := fmt.Sprintf("%v", message["after"])
				if err := json.Unmarshal([]byte(deletedUserMessage), &deletedUser); err != nil {
					fmt.Println(topic, "Error Unmarshalling New User", err)
				} else {
					a.marketoManager.UpdateListMembershipForUser(&oldUser, &oldUser, true)
				}
			}
			fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}
	}

	r.Close()
}

func main() {
	topics, _ := os.LookupEnv("KAFKA_TOPIC")
	broker, _ := os.LookupEnv("KAFKA_BROKERS")
	a := Api{}

	fmt.Println("In main testing version")
	time.Sleep(10 * time.Second)
	fmt.Println("Finished sleep")

	startTime := time.Now()

	for _, topic := range strings.Split(topics, ",") {
		go a.reader(topic, broker, 0)
	}

	fmt.Printf("Duration in seconds: %f\n", time.Now().Sub(startTime).Seconds())
	// Hack - do not quit for now
	fmt.Println("Sleeping until the end of time")
	for {
		time.Sleep(100 * time.Second)
	}
}
