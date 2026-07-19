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

type analyzeExpenseRequest struct {
	Document documentInput `json:"Document"`
}

// documentLocationInput mirrors Textract's async DocumentLocation request
// shape, used by StartDocumentTextDetection/StartDocumentAnalysis to point
// at an object in this emulator's S3.
type documentLocationInput struct {
	S3Object *s3ObjectInput `json:"S3Object"`
}

type startDocumentTextDetectionRequest struct {
	DocumentLocation documentLocationInput `json:"DocumentLocation"`
}

type startDocumentAnalysisRequest struct {
	DocumentLocation documentLocationInput `json:"DocumentLocation"`
	FeatureTypes     []string              `json:"FeatureTypes"`
}

type startJobResponse struct {
	JobId string `json:"JobId"`
}

type getAsyncResultRequest struct {
	JobId string `json:"JobId"`
}

type getAsyncResultResponse struct {
	JobStatus        string           `json:"JobStatus"`
	StatusMessage    string           `json:"StatusMessage,omitempty"`
	DocumentMetadata DocumentMetadata `json:"DocumentMetadata"`
	Blocks           []Block          `json:"Blocks,omitempty"`
}

// Handler binds Textract's JSON-protocol HTTP requests to Processor calls.
type Handler struct {
	proc  *Processor
	store DocumentStore
}

// NewHandler returns a Textract HTTP handler backed by proc, resolving
// async DocumentLocation.S3Object references via store.
func NewHandler(proc *Processor, store DocumentStore) *Handler {
	return &Handler{proc: proc, store: store}
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

func (h *Handler) HandleAnalyzeExpense(w http.ResponseWriter, r *http.Request) {
	var req analyzeExpenseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	raw, ok := h.decodeDocument(w, req.Document)
	if !ok {
		return
	}

	out, err := h.proc.AnalyzeExpense(raw)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) HandleStartDocumentTextDetection(w http.ResponseWriter, r *http.Request) {
	var req startDocumentTextDetectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	bucket, name, ok := h.requireS3Object(w, req.DocumentLocation)
	if !ok {
		return
	}

	jobID, err := h.proc.StartDocumentTextDetection(h.store, bucket, name)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, startJobResponse{JobId: jobID})
}

func (h *Handler) HandleGetDocumentTextDetection(w http.ResponseWriter, r *http.Request) {
	var req getAsyncResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	job, err := h.proc.GetDocumentTextDetection(req.JobId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, toGetAsyncResultResponse(job))
}

func (h *Handler) HandleStartDocumentAnalysis(w http.ResponseWriter, r *http.Request) {
	var req startDocumentAnalysisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	bucket, name, ok := h.requireS3Object(w, req.DocumentLocation)
	if !ok {
		return
	}

	jobID, err := h.proc.StartDocumentAnalysis(h.store, bucket, name, req.FeatureTypes)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, startJobResponse{JobId: jobID})
}

func (h *Handler) HandleGetDocumentAnalysis(w http.ResponseWriter, r *http.Request) {
	var req getAsyncResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	job, err := h.proc.GetDocumentAnalysis(req.JobId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, toGetAsyncResultResponse(job))
}

// requireS3Object validates that loc carries a usable S3Object, writing an
// InvalidParameterException and returning ok=false otherwise.
func (h *Handler) requireS3Object(w http.ResponseWriter, loc documentLocationInput) (bucket, name string, ok bool) {
	if loc.S3Object == nil || loc.S3Object.Bucket == "" || loc.S3Object.Name == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "DocumentLocation.S3Object.Bucket and Name are required")
		return "", "", false
	}
	return loc.S3Object.Bucket, loc.S3Object.Name, true
}

func toGetAsyncResultResponse(job *AsyncJob) getAsyncResultResponse {
	return getAsyncResultResponse{
		JobStatus:        job.Status,
		StatusMessage:    job.StatusMessage,
		DocumentMetadata: job.DocumentMetadata,
		Blocks:           job.Blocks,
	}
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
	case errors.Is(err, ErrInvalidS3Object):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidS3ObjectException", err.Error())
	case errors.Is(err, ErrJobNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidJobIdException", err.Error())
	default:
		log.Printf("textract: internal error: %v", err)
		awsutil.WriteJSONError(w, http.StatusInternalServerError, "InternalServerError", "an internal error occurred")
	}
}
