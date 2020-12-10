package handler

import (
	"log"

	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/marketo"
)

var _ events.UserEventsHandler = &UserEventsHandler{}

type UserEventsHandler struct {
	events.NoopUserEventsHandler
	MarketoManager marketo.Manager
}

func (u *UserEventsHandler) HandleCreateUserEvent(event events.CreateUserEvent) error {
	// if event.EmailVerified && event.TermsAccepted != "" {
	// 	log.Printf("Received create user event: %v", event)
	// 	u.MarketoManager.CreateListMembershipForUser(event.UserID, event.UserData)
	// }
	return nil
}

func (u *UserEventsHandler) HandleUpdateUserEvent(event events.UpdateUserEvent) error {
	if event.Updated.EmailVerified && event.Updated.TermsAccepted != "" {
		if event.Original.TermsAccepted == "" {
			log.Printf("Received create user event: %v", event)
			u.MarketoManager.CreateListMembershipForUser(event.Updated.UserID, event.Updated)
		} else {
			log.Printf("Received update user event: %v", event)
			u.MarketoManager.UpdateListMembershipForUser(event.Updated.UserID, event.Original, event.Updated, false)
		}
	}
	return nil
}

func (u *UserEventsHandler) HandleDeleteUserEvent(event events.DeleteUserEvent) error {
	log.Printf("Received delete user event: %v", event)
	u.MarketoManager.UpdateListMembershipForUser(event.UserID, event.UserData, event.UserData, true)
	return nil
}
