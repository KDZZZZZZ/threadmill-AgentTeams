package main

import (
	"log"
	"net/http"
	"os"

	"github.com/KDZZZZZZ/threadmill-AgentTeams/internal/demo"
)

func main() {
	addr := os.Getenv("THREADMILL_DEMO_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	server := demo.NewServer()
	log.Printf("threadmill demo listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, server.Handler()))
}
