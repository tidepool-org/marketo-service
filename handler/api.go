package handler

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func RefreshUser(handler *UserEventsHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		userId := vars["userId"]
		if userId == "" {
			http.Error(w, "user id is empty", http.StatusBadRequest)
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
