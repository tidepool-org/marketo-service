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

func (u *UserEventsHandler) HandleCreateUserEvent(payload events.CreateUserEvent) {
	log.Printf("Received create user event: %v", payload)
	u.MarketoManager.CreateListMembershipForUser(payload.UserID, payload.UserData)
}

func (u *UserEventsHandler) HandleUpdateUserEvent(payload events.UpdateUserEvent) {
	u.MarketoManager.UpdateListMembershipForUser(payload.Updated.UserID, payload.Updated, false)
}

func (u *UserEventsHandler) HandleDeleteUserEvent(payload events.DeleteUserEvent) {
	log.Printf("Received delete user event: %v", payload)
	u.MarketoManager.UpdateListMembershipForUser(payload.UserID, payload.UserData, true)
}
