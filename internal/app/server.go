// Package app handles the HTTP server engine lifecycle and graceful
// shutdown for lws.
package app

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"lws/internal/platform/router"
	"lws/internal/services"
)

// Server owns the HTTP listener and the service engine backing it.
type Server struct {
	httpServer *http.Server
	engine     *services.Engine
}

// NewServer wires up the service engine and router, and returns a Server
// ready to Start on the given port.
func NewServer(port string) *Server {
	engine := services.NewEngine()
	handler := router.NewRouter(engine)

	return &Server{
		httpServer: &http.Server{
			Addr:        ":" + port,
			Handler:     handler,
			ReadTimeout: 30 * time.Second,
			IdleTimeout: 120 * time.Second,
			// WriteTimeout is intentionally left unset: real Textract OCR
			// (gocv preprocessing + gosseract) on larger images can take
			// several seconds, and a tight WriteTimeout would truncate
			// legitimate slow responses.
		},
		engine: engine,
	}
}

// Start begins serving requests and blocks until the server stops. A clean
// shutdown via Stop() surfaces as http.ErrServerClosed, which is not a
// failure and must be swallowed here — callers (main.go) treat any non-nil
// error from Start as fatal.
func (s *Server) Start() error {
	log.Printf("lws: listening on %s", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop gracefully shuts down the server, allowing in-flight requests
// (including slow Textract OCR calls) time to complete.
func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		log.Printf("lws: shutdown error: %v", err)
	}
}
