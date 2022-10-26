package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/cloudevents/sdk-go/protocol/kafka_sarama/v2"
	clinic "github.com/tidepool-org/clinic/client"
	"github.com/tidepool-org/go-common/clients/shoreline"
	"github.com/tidepool-org/go-common/clients/status"
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/marketo"
	"log"
	"net/http"
)

const (
	Snapshot = "r"
	Create   = "c"
	Update   = "u"
	Delete   = "d"
)

type UsersEventKey struct {
	Id string `json:"id"`
}

type KeycloakUsersEvent struct {
	Key    UsersEventKey
	Op     string   `json:"op"`
	Before *UserDAO `json:"before"`
	After  *UserDAO `json:"after"`
}

type RolesEventKey struct {
	RoleId string `json:"role_id"`
	UserId string `json:"user_id"`
}

type UserDAO struct {
	Id            string `json:"id"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Username      string `json:"username"`
}

var _ events.MessageConsumer = &KeycloakUserEventsConsumer{}

type KeycloakUserEventsConsumer struct {
	userEventsHandler *KeycloakEventsHandler
}

func NewKeycloakUserEventsConsumer(userEventsHandler *KeycloakEventsHandler) (*KeycloakUserEventsConsumer, error) {
	return &KeycloakUserEventsConsumer{userEventsHandler: userEventsHandler}, nil
}

func (k *KeycloakUserEventsConsumer) Initialize(config *events.CloudEventsConfig) error {
	return nil
}

func (k *KeycloakUserEventsConsumer) HandleKafkaMessage(cm *sarama.ConsumerMessage) error {
	m := kafka_sarama.NewMessageFromConsumerMessage(cm)
	if m.Value == nil {
		// Ignore tombstone messages
		return nil
	}

	event := KeycloakUsersEvent{}
	if err := json.Unmarshal(m.Value, &event); err != nil {
		return err
	}

	switch event.Op {
	case Snapshot, Create, Update:
		log.Printf("Upserting user %v\n", string(m.Value))
		return k.userEventsHandler.UpsertUser(event)
	case Delete:
		log.Printf("Deleting user %v\n", string(m.Value))
		return k.userEventsHandler.DeleteUser(event)
	default:
		return fmt.Errorf("unknown op %s", event.Op)
	}
}

var _ events.MessageConsumer = &KeycloakRoleEventsConsumer{}

type KeycloakRoleEventsConsumer struct {
	userEventsHandler *KeycloakEventsHandler
}

func NewKeycloakRoleEventsConsumer(userEventsHandler *KeycloakEventsHandler) (*KeycloakRoleEventsConsumer, error) {
	return &KeycloakRoleEventsConsumer{userEventsHandler: userEventsHandler}, nil
}

func (k *KeycloakRoleEventsConsumer) Initialize(config *events.CloudEventsConfig) error {
	return nil
}

func (k *KeycloakRoleEventsConsumer) HandleKafkaMessage(cm *sarama.ConsumerMessage) error {
	key := RolesEventKey{}
	if err := json.Unmarshal(cm.Key, &key); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("Refreshing user %v\n", key.UserId)
	return k.userEventsHandler.RefreshUser(ctx, key.UserId)
}

type KeycloakEventsHandler struct {
	Clinics        clinic.ClientWithResponsesInterface
	Shoreline      shoreline.Client
	MarketoManager marketo.Manager
}

func (k *KeycloakEventsHandler) UpsertUser(event KeycloakUsersEvent) error {
	if event.After == nil || !event.After.EmailVerified {
		return nil
	}

	userId := event.After.Id

	old := shoreline.UserData{}
	if event.Before != nil {
		old = shoreline.UserData{
			UserID:        userId,
			Username:      event.Before.Username,
			Emails:        []string{event.Before.Email},
			EmailVerified: event.Before.EmailVerified,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	clinics, err := k.getClinicsForClinician(ctx, userId)
	if err != nil {
		return err
	}

	user, err := k.getUserById(userId)
	if err != nil {
		return err
	}

	// Don't upsert the user in marketo because the user is already deleted
	if user == nil {
		return nil
	}

	// In the keycloak CDC we don't have previous values for roles,
	// whether the user has a password or if they have accepted the terms.
	// It's ok to use the updated values, because those are not used lookups.
	old.Roles = user.Roles
	old.PasswordExists = user.PasswordExists
	old.TermsAccepted = user.TermsAccepted

	k.MarketoManager.UpdateListMembershipForUser(userId, old, *user, false, clinics)
	return nil
}

func (k *KeycloakEventsHandler) DeleteUser(event KeycloakUsersEvent) error {
	if event.Before == nil || !event.Before.EmailVerified {
		return nil
	}

	userId := event.After.Id
	old := shoreline.UserData{}
	if event.Before != nil {
		old = shoreline.UserData{
			UserID:        userId,
			Username:      event.Before.Username,
			Emails:        []string{event.Before.Email},
			EmailVerified: event.Before.EmailVerified,
		}
	}

	clinics := make(clinic.ClinicianClinicRelationships, 0)
	k.MarketoManager.UpdateListMembershipForUser(userId, old, old, true, &clinics)
	return nil
}

func (k *KeycloakEventsHandler) RefreshUser(ctx context.Context, userId string) error {
	user, err := k.getUserById(userId)
	if err != nil {
		return err
	}
	// User is already deleted
	if user == nil {
		return nil
	}
	// The user hasn't verified their account.
	// They'll be added to marketo later when the user object is updated.
	if !user.EmailVerified {
		return nil
	}
	clinics, err := k.getClinicsForClinician(ctx, userId)
	if err != nil {
		return err
	}

	k.MarketoManager.UpdateListMembershipForUser(user.UserID, *user, *user, false, clinics)
	return nil
}

func (k *KeycloakEventsHandler) getClinicsForClinician(ctx context.Context, userId string) (*clinic.ClinicianClinicRelationships, error) {
	maxClinics := clinic.Limit(1000)
	params := &clinic.ListClinicsForClinicianParams{
		Limit: &maxClinics,
	}
	response, err := k.Clinics.ListClinicsForClinicianWithResponse(ctx, clinic.UserId(userId), params)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %v when fetching clinics for user %v", response.StatusCode(), userId)
	}
	return response.JSON200, nil
}

func (k *KeycloakEventsHandler) getUserById(userId string) (*shoreline.UserData, error) {
	user, err := k.Shoreline.GetUser(userId, k.Shoreline.TokenProvide())
	if err != nil {
		if e, ok := err.(*status.StatusError); ok && e.Code == http.StatusNotFound {
			return nil, nil
		}
	}

	return user, err
}
