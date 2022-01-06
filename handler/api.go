package handler

import (
	"github.com/gorilla/mux"
	"github.com/tidepool-org/go-common/clients/shoreline"
	"log"
	"net/http"
)

const tidepoolSessionTokenKey = "x-tidepool-session-token"

func RefreshUser(handler *UserEventsHandler, shorelineClient shoreline.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		userId := vars["userId"]
		if userId == "" {
			http.Error(w, "user id is empty", http.StatusBadRequest)
			return
		}

		token := r.Header.Get(tidepoolSessionTokenKey)
		if td := shorelineClient.CheckToken(token); td == nil || !td.IsServer {
			http.Error(w, "session token is invalid", http.StatusForbidden)
			return
		}

		err := handler.RefreshUser(r.Context(), userId)
		if err != nil {
			log.Printf("unable to refresh user %v: %v\n", userId, err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}
