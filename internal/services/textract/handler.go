package textract

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"lws/pkg/awsutil"
)

// s3ObjectInput mirrors Textract's Document.S3Object request shape. This
// local emulator has no real S3 backing, so requests using it are rejected
// with a documented, explicit error rather than silently failing.
type s3ObjectInput struct {
	Bucket  string `json:"Bucket"`
	Name    string `json:"Name"`
	Version string `json:"Version,omitempty"`
}

type documentInput struct {
	Bytes    string         `json:"Bytes,omitempty"`
	S3Object *s3ObjectInput `json:"S3Object,omitempty"`
}

type detectDocumentTextRequest struct {
	Document documentInput `json:"Document"`
}

type analyzeDocumentRequest struct {
	Document     documentInput `json:"Document"`
	FeatureTypes []string      `json:"FeatureTypes"`
}

// Handler binds Textract's JSON-protocol HTTP requests to Processor calls.
type Handler struct {
	proc *Processor
}

// NewHandler returns a Textract HTTP handler backed by proc.
func NewHandler(proc *Processor) *Handler {
	return &Handler{proc: proc}
}

func (h *Handler) HandleDetectDocumentText(w http.ResponseWriter, r *http.Request) {
	var req detectDocumentTextRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	raw, ok := h.decodeDocument(w, req.Document)
	if !ok {
		return
	}

	out, err := h.proc.DetectDocumentText(raw)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) HandleAnalyzeDocument(w http.ResponseWriter, r *http.Request) {
	var req analyzeDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	raw, ok := h.decodeDocument(w, req.Document)
	if !ok {
		return
	}

	out, err := h.proc.AnalyzeDocument(raw, req.FeatureTypes)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, out)
}

// decodeDocument validates and base64-decodes the Document.Bytes payload,
// writing an appropriate error response and returning ok=false if the
// document can't be used.
func (h *Handler) decodeDocument(w http.ResponseWriter, doc documentInput) ([]byte, bool) {
	if doc.Bytes == "" {
		if doc.S3Object != nil {
			awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidS3ObjectException", "S3Object input is not supported by this local emulator; supply Document.Bytes instead")
			return nil, false
		}
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Document.Bytes is required")
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(doc.Bytes)
	if err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Document.Bytes is not valid base64: "+err.Error())
		return nil, false
	}
	return raw, true
}

// writeTextractError maps a Processor error to the matching AWS JSON
// error shape/status.
func writeTextractError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrUnsupportedDocument):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "UnsupportedDocumentException", err.Error())
	case errors.Is(err, ErrInvalidParameter):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
	default:
		log.Printf("textract: internal error: %v", err)
		awsutil.WriteJSONError(w, http.StatusInternalServerError, "InternalServerError", "an internal error occurred")
	}
}
