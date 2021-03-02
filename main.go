package main

import (
	"log"
	"net/http"
	h "queryBuilder/handlers"

	"github.com/gorilla/mux"
)

//HandleRequests -handle requests
func HandleRequests() {
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/metrics/{dataSource}", h.HandlerFuncDruid)
	router.HandleFunc("/api/v1/inventory/{dataSource}", h.HandlerFuncPg)
	log.Fatalln(http.ListenAndServe(":40400", router))
}

func main() {
	HandleRequests()
}
