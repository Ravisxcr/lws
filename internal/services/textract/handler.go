package textract

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"lws/pkg/awsutil"
)

// s3ObjectInput mirrors Textract's Document.S3Object request shape; this
// local emulator has no real S3 backing, so requests using it are rejected.
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

type analyzeIDRequest struct {
	DocumentPages []documentInput `json:"DocumentPages"`
}

// documentLocationInput mirrors Textract's async DocumentLocation request
// shape, pointing at an object in this emulator's S3.
type documentLocationInput struct {
	S3Object *s3ObjectInput `json:"S3Object"`
}

type startDocumentTextDetectionRequest struct {
	DocumentLocation documentLocationInput `json:"DocumentLocation"`
}

// notificationChannelInput mirrors Textract's NotificationChannel request
// shape, used by Start* calls to request an SNS completion notification.
type notificationChannelInput struct {
	RoleArn     string `json:"RoleArn"`
	SNSTopicArn string `json:"SNSTopicArn"`
}

type startDocumentAnalysisRequest struct {
	DocumentLocation    documentLocationInput     `json:"DocumentLocation"`
	FeatureTypes        []string                  `json:"FeatureTypes"`
	JobTag              string                    `json:"JobTag,omitempty"`
	NotificationChannel *notificationChannelInput `json:"NotificationChannel,omitempty"`
}

type startJobResponse struct {
	JobId string `json:"JobId"`
}

type getAsyncResultRequest struct {
	JobId string `json:"JobId"`
}

type getDocumentTextDetectionResponse struct {
	JobStatus                      string           `json:"JobStatus"`
	StatusMessage                  string           `json:"StatusMessage,omitempty"`
	DocumentMetadata               DocumentMetadata `json:"DocumentMetadata"`
	DetectDocumentTextModelVersion string           `json:"DetectDocumentTextModelVersion,omitempty"`
	Blocks                         []Block          `json:"Blocks,omitempty"`
	Warnings                       []Warning        `json:"Warnings,omitempty"`
	NextToken                      string           `json:"NextToken,omitempty"`
}

type getDocumentAnalysisResponse struct {
	JobStatus                   string           `json:"JobStatus"`
	StatusMessage               string           `json:"StatusMessage,omitempty"`
	DocumentMetadata            DocumentMetadata `json:"DocumentMetadata"`
	AnalyzeDocumentModelVersion string           `json:"AnalyzeDocumentModelVersion,omitempty"`
	Blocks                      []Block          `json:"Blocks,omitempty"`
	Warnings                    []Warning        `json:"Warnings,omitempty"`
	NextToken                   string           `json:"NextToken,omitempty"`
}

type startExpenseAnalysisRequest struct {
	DocumentLocation documentLocationInput `json:"DocumentLocation"`
}

type getExpenseAnalysisResponse struct {
	JobStatus        string            `json:"JobStatus"`
	StatusMessage    string            `json:"StatusMessage,omitempty"`
	DocumentMetadata DocumentMetadata  `json:"DocumentMetadata"`
	ExpenseDocuments []ExpenseDocument `json:"ExpenseDocuments,omitempty"`
	Warnings         []Warning         `json:"Warnings,omitempty"`
	NextToken        string            `json:"NextToken,omitempty"`
}

type startLendingAnalysisRequest struct {
	DocumentLocation documentLocationInput `json:"DocumentLocation"`
}

type getLendingAnalysisResponse struct {
	JobStatus                  string           `json:"JobStatus"`
	StatusMessage              string           `json:"StatusMessage,omitempty"`
	DocumentMetadata           DocumentMetadata `json:"DocumentMetadata"`
	AnalyzeLendingModelVersion string           `json:"AnalyzeLendingModelVersion,omitempty"`
	Results                    []LendingResult  `json:"Results,omitempty"`
	Warnings                   []Warning        `json:"Warnings,omitempty"`
	NextToken                  string           `json:"NextToken,omitempty"`
}

type getLendingAnalysisSummaryResponse struct {
	JobStatus        string           `json:"JobStatus"`
	StatusMessage    string           `json:"StatusMessage,omitempty"`
	DocumentMetadata DocumentMetadata `json:"DocumentMetadata"`
	Summary          *LendingSummary  `json:"Summary,omitempty"`
	Warnings         []Warning        `json:"Warnings,omitempty"`
}

// Notifier publishes a Textract job-completion message to an SNS topic;
// satisfied structurally by *sns.Service.
type Notifier interface {
	Publish(topicArn, message, subject string) (string, error)
}

// Handler binds Textract's JSON-protocol HTTP requests to Processor calls.
type Handler struct {
	proc     *Processor
	store    DocumentStore
	notifier Notifier
}

// NewHandler returns a Textract HTTP handler backed by proc, resolving S3
// references via store and publishing Start* completion notifications via notifier.
func NewHandler(proc *Processor, store DocumentStore, notifier Notifier) *Handler {
	return &Handler{proc: proc, store: store, notifier: notifier}
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

func (h *Handler) HandleAnalyzeID(w http.ResponseWriter, r *http.Request) {
	var req analyzeIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	if len(req.DocumentPages) == 0 {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "DocumentPages is required")
		return
	}

	pages := make([][]byte, 0, len(req.DocumentPages))
	for _, doc := range req.DocumentPages {
		raw, ok := h.decodeDocument(w, doc)
		if !ok {
			return
		}
		pages = append(pages, raw)
	}

	out, err := h.proc.AnalyzeID(pages)
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
	awsutil.WriteJSON(w, http.StatusOK, toGetDocumentTextDetectionResponse(job))
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

	if req.NotificationChannel != nil && req.NotificationChannel.SNSTopicArn != "" {
		h.publishJobCompletion(jobID, "StartDocumentAnalysis", req.JobTag, bucket, name, req.NotificationChannel.SNSTopicArn)
	}
}

// textractJobCompletionMessage mirrors the JSON body real Textract publishes
// to SNS when an async job finishes.
type textractJobCompletionMessage struct {
	JobId            string            `json:"JobId"`
	Status           string            `json:"Status"`
	API              string            `json:"API"`
	JobTag           string            `json:"JobTag,omitempty"`
	Timestamp        int64             `json:"Timestamp"`
	DocumentLocation map[string]string `json:"DocumentLocation"`
}

// publishJobCompletion looks up the (already-finished) job and publishes its
// outcome to topicArn.
func (h *Handler) publishJobCompletion(jobID, api, jobTag, bucket, key, topicArn string) {
	if h.notifier == nil {
		return
	}
	job, err := h.proc.GetDocumentAnalysis(jobID)
	if err != nil {
		log.Printf("textract: could not look up job %s for notification: %v", jobID, err)
		return
	}
	msg := textractJobCompletionMessage{
		JobId:            jobID,
		Status:           job.Status,
		API:              api,
		JobTag:           jobTag,
		Timestamp:        time.Now().UnixMilli(),
		DocumentLocation: map[string]string{"S3ObjectName": key, "S3Bucket": bucket},
	}
	body, err := json.Marshal(msg)
	if err != nil {
		log.Printf("textract: failed to marshal job completion for %s: %v", jobID, err)
		return
	}
	if _, err := h.notifier.Publish(topicArn, string(body), ""); err != nil {
		log.Printf("textract: failed to publish job completion for %s: %v", jobID, err)
	}
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
	awsutil.WriteJSON(w, http.StatusOK, toGetDocumentAnalysisResponse(job))
}

func (h *Handler) HandleStartExpenseAnalysis(w http.ResponseWriter, r *http.Request) {
	var req startExpenseAnalysisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	bucket, name, ok := h.requireS3Object(w, req.DocumentLocation)
	if !ok {
		return
	}

	jobID, err := h.proc.StartExpenseAnalysis(h.store, bucket, name)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, startJobResponse{JobId: jobID})
}

func (h *Handler) HandleGetExpenseAnalysis(w http.ResponseWriter, r *http.Request) {
	var req getAsyncResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	job, err := h.proc.GetExpenseAnalysis(req.JobId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, getExpenseAnalysisResponse{
		JobStatus:        job.Status,
		StatusMessage:    job.StatusMessage,
		DocumentMetadata: job.DocumentMetadata,
		ExpenseDocuments: job.ExpenseDocuments,
	})
}

func (h *Handler) HandleStartLendingAnalysis(w http.ResponseWriter, r *http.Request) {
	var req startLendingAnalysisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	bucket, name, ok := h.requireS3Object(w, req.DocumentLocation)
	if !ok {
		return
	}

	jobID, err := h.proc.StartLendingAnalysis(h.store, bucket, name)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, startJobResponse{JobId: jobID})
}

func (h *Handler) HandleGetLendingAnalysis(w http.ResponseWriter, r *http.Request) {
	var req getAsyncResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	job, err := h.proc.GetLendingAnalysis(req.JobId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	var results []LendingResult
	if job.Lending != nil {
		results = []LendingResult{*job.Lending}
	}
	awsutil.WriteJSON(w, http.StatusOK, getLendingAnalysisResponse{
		JobStatus:                  job.Status,
		StatusMessage:              job.StatusMessage,
		DocumentMetadata:           job.DocumentMetadata,
		AnalyzeLendingModelVersion: modelVersion,
		Results:                    results,
	})
}

func (h *Handler) HandleGetLendingAnalysisSummary(w http.ResponseWriter, r *http.Request) {
	var req getAsyncResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}

	job, summary, err := h.proc.GetLendingAnalysisSummary(req.JobId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, getLendingAnalysisSummaryResponse{
		JobStatus:        job.Status,
		StatusMessage:    job.StatusMessage,
		DocumentMetadata: job.DocumentMetadata,
		Summary:          summary,
	})
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

func toGetDocumentTextDetectionResponse(job *AsyncJob) getDocumentTextDetectionResponse {
	return getDocumentTextDetectionResponse{
		JobStatus:                      job.Status,
		StatusMessage:                  job.StatusMessage,
		DocumentMetadata:               job.DocumentMetadata,
		DetectDocumentTextModelVersion: job.ModelVersion,
		Blocks:                         job.Blocks,
	}
}

func toGetDocumentAnalysisResponse(job *AsyncJob) getDocumentAnalysisResponse {
	return getDocumentAnalysisResponse{
		JobStatus:                   job.Status,
		StatusMessage:               job.StatusMessage,
		DocumentMetadata:            job.DocumentMetadata,
		AnalyzeDocumentModelVersion: job.ModelVersion,
		Blocks:                      job.Blocks,
	}
}

// decodeDocument validates and base64-decodes Document.Bytes, writing an
// error response and returning ok=false if the document can't be used.
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
	case errors.Is(err, ErrAdapterNotFound), errors.Is(err, ErrAdapterVersionNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	default:
		log.Printf("textract: internal error: %v", err)
		awsutil.WriteJSONError(w, http.StatusInternalServerError, "InternalServerError", "an internal error occurred")
	}
}
