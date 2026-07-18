// Package router sniffs incoming AWS-style requests and dispatches them to
// the correct service handler. Two wire protocols are supported on the same
// port: the JSON protocol (Textract-style, identified by the X-Amz-Target
// header) and the Query protocol (SQS/SNS-style, form-encoded with an
// Action parameter).
package router

import (
	"net/http"

	"lws/pkg/awsutil"
)

// Multiplexer dispatches requests by protocol: X-Amz-Target header for the
// JSON protocol, form-encoded Action for the Query protocol.
type Multiplexer struct {
	jsonRoutes  map[string]http.HandlerFunc
	queryRoutes map[string]http.HandlerFunc
}

// NewMultiplexer returns an empty Multiplexer ready for route registration.
func NewMultiplexer() *Multiplexer {
	return &Multiplexer{
		jsonRoutes:  make(map[string]http.HandlerFunc),
		queryRoutes: make(map[string]http.HandlerFunc),
	}
}

// RegisterJSONAction registers a handler for a JSON-protocol operation,
// keyed by the exact X-Amz-Target value (e.g. "Textract.DetectDocumentText").
func (m *Multiplexer) RegisterJSONAction(target string, h http.HandlerFunc) {
	m.jsonRoutes[target] = h
}

// RegisterQueryAction registers a handler for a Query-protocol action
// (e.g. "SendMessage", "Publish").
func (m *Multiplexer) RegisterQueryAction(action string, h http.HandlerFunc) {
	m.queryRoutes[action] = h
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

	action := r.FormValue("Action")
	h, ok := m.queryRoutes[action]
	if action == "" || !ok {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidAction", "unknown or missing Action: "+action)
		return
	}
	h(w, r)
}
