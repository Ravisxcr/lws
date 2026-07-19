package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"lws/internal/app"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Initialize server instance
	server := app.NewServer(port)

	// Start server in background thread
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("Server failed to execute: %v", err)
		}
	}()

	// Graceful shutdown wiring
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down LWS gracefully...")
	server.Stop()
}