package handler

import (
	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/marketo"
	"log"
)

var _ events.UserEventsHandler = &UserEventsHandler{}

type UserEventsHandler struct {
	events.NoopUserEventsHandler
	MarketoManager marketo.Manager
}

func (u *UserEventsHandler) HandleCreateUserEvent(event events.CreateUserEvent) {
	log.Printf("Received create user event: %v", event)
	u.MarketoManager.CreateListMembershipForUser(event.UserID, event.UserData)
}

func (u *UserEventsHandler) HandleUpdateUserEvent(event events.UpdateUserEvent) {
	log.Printf("Received update user event: %v", event)
	u.MarketoManager.UpdateListMembershipForUser(event.Updated.UserID, event.Updated, false)
}

func (u *UserEventsHandler) HandleDeleteUserEvent(event events.DeleteUserEvent) {
	log.Printf("Received delete user event: %v", event)
	u.MarketoManager.UpdateListMembershipForUser(event.UserID, event.UserData, true)
}
