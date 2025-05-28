package handler

import (
	"context"
	"fmt"
	clinic "github.com/tidepool-org/clinic/client"
	"github.com/tidepool-org/go-common/clients/shoreline"
	"log"
	"net/http"
	"time"

	"github.com/tidepool-org/go-common/events"
	"github.com/tidepool-org/marketo-service/marketo"
)

const timeout = time.Second * 30

var _ events.UserEventsHandler = &UserEventsHandler{}

type UserEventsHandler struct {
	events.NoopUserEventsHandler
	MarketoManager marketo.Manager
	Clinics        clinic.ClientWithResponsesInterface
	Shoreline      shoreline.Client
}

func (u *UserEventsHandler) HandleUpdateUserEvent(event events.UpdateUserEvent) error {
	if event.Updated.EmailVerified && event.Updated.TermsAccepted != "" {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		userId := event.Original.UserID
		clinics, err := u.getClinicsForClinician(ctx, userId)
		if err != nil {
			return err
		}
		if event.Original.TermsAccepted == "" {
			log.Printf("Received create user event: %v", event)
			u.MarketoManager.CreateListMembershipForUser(event.Updated.UserID, event.Updated, clinics)
		} else {
			log.Printf("Received update user event: %v", event)
			u.MarketoManager.UpdateListMembershipForUser(event.Updated.UserID, event.Original, event.Updated, false, clinics)
		}
	}
	return nil
}

func (u *UserEventsHandler) HandleDeleteUserEvent(event events.DeleteUserEvent) error {
	log.Printf("Received delete user event: %v", event)
	u.MarketoManager.UpdateListMembershipForUser(event.UserID, event.UserData, event.UserData, true, nil)
	return nil
}

func (u *UserEventsHandler) RefreshUser(ctx context.Context, userId string) error {
	user, err := u.Shoreline.GetUser(userId, u.Shoreline.TokenProvide())
	if err != nil {
		return err
	}
	// User is already deleted
	if user == nil {
		return nil
	}
	// The user hasn't verified their account.
	// They'll be added to marketo later when the user object is updated.
	if !user.EmailVerified || user.TermsAccepted == "" {
		return nil
	}
	clinics, err := u.getClinicsForClinician(ctx, userId)
	if err != nil {
		return err
	}

	u.MarketoManager.UpdateListMembershipForUser(user.UserID, *user, *user, false, clinics)
	return nil
}

func (u *UserEventsHandler) getClinicsForClinician(ctx context.Context, userId string) (*clinic.ClinicianClinicRelationships, error) {
	maxClinics := clinic.Limit(1000)
	params := &clinic.ListClinicsForClinicianParams{
		Limit: &maxClinics,
	}
	response, err := u.Clinics.ListClinicsForClinicianWithResponse(ctx, clinic.UserId(userId), params)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %v when fetching clinics for user %v", response.StatusCode(), userId)
	}
	return response.JSON200, nil
}
