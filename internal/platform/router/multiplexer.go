// Package router sniffs incoming AWS-style requests and dispatches them to
// the correct service handler. Three wire protocols are supported on the
// same port: the JSON protocol (Textract-style, identified by the
// X-Amz-Target header), the Query protocol (SQS/SNS-style, form-encoded
// with an Action parameter), and the REST protocol (S3-style, with no
// Action/Target and the operation instead selected by HTTP method, URL
// path, and query-string subresources), which is tried as a fallback once
// neither of the other two match.
package router

import (
	"net/http"

	"lws/pkg/awsutil"
)

// Multiplexer dispatches requests by protocol: X-Amz-Target header for the
// JSON protocol, form-encoded Action for the Query protocol, and a REST
// fallback handler for everything else.
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
// keyed by the exact X-Amz-Target value (e.g. "Textract.DetectDocumentText").
func (m *Multiplexer) RegisterJSONAction(target string, h http.HandlerFunc) {
	m.jsonRoutes[target] = h
}

// RegisterQueryAction registers a handler for a Query-protocol action
// (e.g. "SendMessage", "Publish").
func (m *Multiplexer) RegisterQueryAction(action string, h http.HandlerFunc) {
	m.queryRoutes[action] = h
}

// RegisterRESTFallback registers the handler for REST-protocol services
// (currently S3), tried once a request carries neither an X-Amz-Target
// header nor a Query-protocol Action parameter.
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
