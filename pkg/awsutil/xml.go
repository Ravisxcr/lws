// Package awsutil provides shared response/error envelope helpers used by
// every emulated AWS service: XML (Query protocol, SQS/SNS) and JSON
// (AWS JSON 1.1 protocol, Textract).
package awsutil

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Namespace documents the literal XML namespace strings AWS uses for each
// service. Go struct tags cannot interpolate constants, so every response
// struct's XMLName tag must still hardcode the namespace string itself
// (e.g. `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageResponse"`).
// These constants exist for reference/logging/tests only.
const (
	SQSNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	SNSNamespace = "http://sns.amazonaws.com/doc/2010-03-31/"
)

// ResponseMetadata is embedded into every per-action XML response root struct.
type ResponseMetadata struct {
	RequestID string `xml:"RequestId"`
}

// AWSError mirrors AWS Query-protocol's <Error> element.
type AWSError struct {
	XMLName xml.Name `xml:"Error"`
	Type    string   `xml:"Type"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// ErrorResponse mirrors AWS Query-protocol's <ErrorResponse> shape.
type ErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Error     AWSError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

// NewRequestID returns a fresh request identifier for response metadata.
func NewRequestID() string {
	return uuid.NewString()
}

// WriteXML marshals v (a fully-formed response root struct, XMLName and
// namespace already set on the struct itself) with the XML prologue and
// writes it with the given HTTP status.
func WriteXML(w http.ResponseWriter, status int, v any) error {
	body, err := xml.Marshal(v)
	if err != nil {
		log.Printf("awsutil: failed to marshal XML response: %v", err)
		WriteXMLError(w, http.StatusInternalServerError, "Receiver", "InternalError", "failed to encode response")
		return err
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, err = w.Write(body)
	return err
}

// WriteXMLError builds and writes an AWS-shaped <ErrorResponse>, logging the
// failure to stderr per the project's error paradigm (capture internally,
// bubble a structured exception to the client).
func WriteXMLError(w http.ResponseWriter, status int, errType, code, message string) {
	log.Printf("awsutil: %s error %s: %s", errType, code, message)
	resp := ErrorResponse{
		Error: AWSError{
			Type:    errType,
			Code:    code,
			Message: message,
		},
		RequestID: NewRequestID(),
	}
	body, err := xml.Marshal(resp)
	if err != nil {
		log.Printf("awsutil: failed to marshal XML error response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// JSONError is the AWS JSON 1.1 protocol error shape used by Textract.
type JSONError struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// WriteJSON sets the AWS JSON 1.1 content type and encodes v.
func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}

// WriteJSONError writes an AWS JSON 1.1 error body and logs the failure to
// stderr per the project's error paradigm.
func WriteJSONError(w http.ResponseWriter, status int, errType, message string) {
	log.Printf("awsutil: %s (%d): %s", errType, status, message)
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(JSONError{Type: errType, Message: message})
}

// ParseAttributePairs scans form for "<prefix>.N.Name"/"<prefix>.N.Value"
// pairs, the indexed parameter convention AWS Query-protocol requests use
// for things like SQS CreateQueue's Attribute.N.Name/Attribute.N.Value.
func ParseAttributePairs(form url.Values, prefix string) map[string]string {
	names := map[int]string{}
	values := map[int]string{}

	namePrefix := prefix + "."
	for key, vals := range form {
		if len(vals) == 0 {
			continue
		}
		rest, ok := strings.CutPrefix(key, namePrefix)
		if !ok {
			continue
		}
		idxStr, field, ok := strings.Cut(rest, ".")
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		switch field {
		case "Name":
			names[idx] = vals[0]
		case "Value":
			values[idx] = vals[0]
		}
	}

	result := make(map[string]string, len(names))
	for idx, name := range names {
		if value, ok := values[idx]; ok {
			result[name] = value
		}
	}
	return result
}

// ParseIndexedEntries scans form for "<prefix>.N.<field>" parameters,
// grouping them by N, for AWS's Query-protocol batch-request member list
// convention (e.g. SendMessageBatchRequestEntry.1.Id,
// SendMessageBatchRequestEntry.1.MessageBody).
func ParseIndexedEntries(form url.Values, prefix string) map[string]map[string]string {
	out := map[string]map[string]string{}
	p := prefix + "."
	for key, vals := range form {
		if len(vals) == 0 {
			continue
		}
		rest, ok := strings.CutPrefix(key, p)
		if !ok {
			continue
		}
		idx, field, ok := strings.Cut(rest, ".")
		if !ok {
			continue
		}
		if out[idx] == nil {
			out[idx] = map[string]string{}
		}
		out[idx][field] = vals[0]
	}
	return out
}

// SortedIndexKeys returns m's keys in ascending numeric order, so batch
// entries are processed in the order the caller listed them.
func SortedIndexKeys(m map[string]map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := strconv.Atoi(keys[i])
		b, _ := strconv.Atoi(keys[j])
		return a < b
	})
	return keys
}

// IndexedValues scans form for "<prefix>.N" parameters (e.g.
// AttributeName.1, AttributeName.2) and returns their values ordered by N.
func IndexedValues(form url.Values, prefix string) []string {
	type kv struct {
		idx int
		val string
	}
	var items []kv
	p := prefix + "."
	for key, vals := range form {
		if len(vals) == 0 {
			continue
		}
		idxStr, ok := strings.CutPrefix(key, p)
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		items = append(items, kv{idx: idx, val: vals[0]})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].idx < items[j].idx })
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.val
	}
	return out
}

// ParseTagPairs scans form for "<prefix>.N.Key"/"<prefix>.N.Value" pairs
// (AWS's Tag.N.Key/Tag.N.Value convention, distinct from
// ParseAttributePairs's Name/Value convention used elsewhere).
func ParseTagPairs(form url.Values, prefix string) map[string]string {
	grouped := ParseIndexedEntries(form, prefix)
	out := make(map[string]string, len(grouped))
	for _, e := range grouped {
		if e["Key"] != "" {
			out[e["Key"]] = e["Value"]
		}
	}
	return out
}
