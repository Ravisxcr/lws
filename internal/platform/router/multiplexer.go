// Package router sniffs incoming AWS-style requests and dispatches them to
// the correct service handler.
package router

import (
	"net/http"

	"lws/pkg/awsutil"
)


type Multiplexer struct {
	jsonRoutes   map[string]http.HandlerFunc
	queryRoutes  map[string]http.HandlerFunc
	restFallback http.Handler
}

// NewMultiplexer returns an empty Multiplexer ready for route registration.
func NewMultiplexer() *Multiplexer {
	return &Multiplexer{
		jsonRoutes:  make(map[string]http.HandlerFunc),
		queryRoutes: make(map[string]http.HandlerFunc),
	}
}

// RegisterJSONAction registers a handler for a JSON-protocol operation,
func (m *Multiplexer) RegisterJSONAction(target string, h http.HandlerFunc) {
	m.jsonRoutes[target] = h
}

// RegisterQueryAction registers a handler for a Query-protocol action
func (m *Multiplexer) RegisterQueryAction(action string, h http.HandlerFunc) {
	m.queryRoutes[action] = h
}

// RegisterRESTFallback registers the handler for REST-protocol services
func (m *Multiplexer) RegisterRESTFallback(h http.Handler) {
	m.restFallback = h
}

// ServeHTTP implements http.Handler, sniffing the protocol and dispatching
// accordingly.
func (m *Multiplexer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		h, ok := m.jsonRoutes[target]
		if !ok {
			awsutil.WriteJSONError(w, http.StatusBadRequest, "UnknownOperationException", "operation "+target+" is not supported")
			return
		}
		h(w, r)
		return
	}

	if err := r.ParseForm(); err != nil {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MalformedInput", err.Error())
		return
	}

	if action := r.FormValue("Action"); action != "" {
		h, ok := m.queryRoutes[action]
		if !ok {
			awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidAction", "unknown Action: "+action)
			return
		}
		h(w, r)
		return
	}

	if m.restFallback != nil {
		m.restFallback.ServeHTTP(w, r)
		return
	}

	awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidAction", "missing Action")
}
