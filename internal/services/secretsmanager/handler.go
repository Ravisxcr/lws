package secretsmanager

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"lws/pkg/awsutil"
)

// Handler binds Secrets Manager's JSON-protocol HTTP requests to Service
// calls and writes AWS JSON 1.1 response bodies.
type Handler struct {
	svc *Service
}

// NewHandler returns a Secrets Manager HTTP handler backed by svc.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// --- JSON request/response shapes, matching real Secrets Manager's
// AWS JSON 1.1 protocol shapes ---

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type createSecretRequest struct {
	Name               string     `json:"Name"`
	ClientRequestToken string     `json:"ClientRequestToken,omitempty"`
	Description        string     `json:"Description,omitempty"`
	KmsKeyId           string     `json:"KmsKeyId,omitempty"`
	SecretString       string     `json:"SecretString,omitempty"`
	SecretBinary       string     `json:"SecretBinary,omitempty"`
	Tags               []tagInput `json:"Tags,omitempty"`
}

type createSecretResponse struct {
	ARN       string `json:"ARN"`
	Name      string `json:"Name"`
	VersionId string `json:"VersionId,omitempty"`
}

type getSecretValueRequest struct {
	SecretId     string `json:"SecretId"`
	VersionId    string `json:"VersionId,omitempty"`
	VersionStage string `json:"VersionStage,omitempty"`
}

type getSecretValueResponse struct {
	ARN           string   `json:"ARN"`
	Name          string   `json:"Name"`
	VersionId     string   `json:"VersionId"`
	SecretString  string   `json:"SecretString,omitempty"`
	SecretBinary  string   `json:"SecretBinary,omitempty"`
	VersionStages []string `json:"VersionStages"`
	CreatedDate   float64  `json:"CreatedDate"`
}

type putSecretValueRequest struct {
	SecretId           string   `json:"SecretId"`
	ClientRequestToken string   `json:"ClientRequestToken,omitempty"`
	SecretString       string   `json:"SecretString,omitempty"`
	SecretBinary       string   `json:"SecretBinary,omitempty"`
	VersionStages      []string `json:"VersionStages,omitempty"`
}

type putSecretValueResponse struct {
	ARN           string   `json:"ARN"`
	Name          string   `json:"Name"`
	VersionId     string   `json:"VersionId"`
	VersionStages []string `json:"VersionStages"`
}

type describeSecretRequest struct {
	SecretId string `json:"SecretId"`
}

type describeSecretResponse struct {
	ARN                string              `json:"ARN"`
	Name               string              `json:"Name"`
	Description        string              `json:"Description,omitempty"`
	KmsKeyId           string              `json:"KmsKeyId,omitempty"`
	Tags               []tagInput          `json:"Tags,omitempty"`
	VersionIdsToStages map[string][]string `json:"VersionIdsToStages,omitempty"`
	CreatedDate        float64             `json:"CreatedDate"`
	LastChangedDate    float64             `json:"LastChangedDate"`
	LastAccessedDate   float64             `json:"LastAccessedDate,omitempty"`
	DeletedDate        *float64            `json:"DeletedDate,omitempty"`
}

type listSecretsRequest struct {
	MaxResults int64  `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type secretListEntry struct {
	ARN                    string              `json:"ARN"`
	Name                   string              `json:"Name"`
	Description            string              `json:"Description,omitempty"`
	KmsKeyId               string              `json:"KmsKeyId,omitempty"`
	Tags                   []tagInput          `json:"Tags,omitempty"`
	SecretVersionsToStages map[string][]string `json:"SecretVersionsToStages,omitempty"`
	CreatedDate            float64             `json:"CreatedDate"`
	LastChangedDate        float64             `json:"LastChangedDate"`
	DeletedDate            *float64            `json:"DeletedDate,omitempty"`
}

type listSecretsResponse struct {
	SecretList []secretListEntry `json:"SecretList"`
}

type deleteSecretRequest struct {
	SecretId                   string `json:"SecretId"`
	RecoveryWindowInDays       int64  `json:"RecoveryWindowInDays,omitempty"`
	ForceDeleteWithoutRecovery bool   `json:"ForceDeleteWithoutRecovery,omitempty"`
}

type deleteSecretResponse struct {
	ARN          string   `json:"ARN"`
	Name         string   `json:"Name"`
	DeletionDate *float64 `json:"DeletionDate,omitempty"`
}

type restoreSecretRequest struct {
	SecretId string `json:"SecretId"`
}

type restoreSecretResponse struct {
	ARN  string `json:"ARN"`
	Name string `json:"Name"`
}

type updateSecretRequest struct {
	SecretId           string `json:"SecretId"`
	ClientRequestToken string `json:"ClientRequestToken,omitempty"`
	Description        string `json:"Description,omitempty"`
	KmsKeyId           string `json:"KmsKeyId,omitempty"`
	SecretString       string `json:"SecretString,omitempty"`
	SecretBinary       string `json:"SecretBinary,omitempty"`
}

type updateSecretResponse struct {
	ARN       string `json:"ARN"`
	Name      string `json:"Name"`
	VersionId string `json:"VersionId,omitempty"`
}

type tagResourceRequest struct {
	SecretId string     `json:"SecretId"`
	Tags     []tagInput `json:"Tags"`
}

type untagResourceRequest struct {
	SecretId string   `json:"SecretId"`
	TagKeys  []string `json:"TagKeys"`
}

type listSecretVersionIdsRequest struct {
	SecretId          string `json:"SecretId"`
	IncludeDeprecated bool   `json:"IncludeDeprecated,omitempty"`
}

type secretVersionsListEntry struct {
	VersionId     string   `json:"VersionId"`
	VersionStages []string `json:"VersionStages"`
	CreatedDate   float64  `json:"CreatedDate"`
}

type listSecretVersionIdsResponse struct {
	ARN      string                    `json:"ARN"`
	Name     string                    `json:"Name"`
	Versions []secretVersionsListEntry `json:"Versions"`
}

// --- handlers ---

func (h *Handler) HandleCreateSecret(w http.ResponseWriter, r *http.Request) {
	var req createSecretRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "Name is required")
		return
	}
	secretBinary, ok := decodeBinary(w, req.SecretBinary)
	if !ok {
		return
	}

	snap, version, err := h.svc.CreateSecret(CreateSecretInput{
		Name:               req.Name,
		ClientRequestToken: req.ClientRequestToken,
		Description:        req.Description,
		KmsKeyId:           req.KmsKeyId,
		SecretString:       req.SecretString,
		SecretBinary:       secretBinary,
		Tags:               tagsFromInput(req.Tags),
	})
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	resp := createSecretResponse{ARN: snap.ARN, Name: snap.Name}
	if version != nil {
		resp.VersionId = version.VersionId
	}
	awsutil.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleGetSecretValue(w http.ResponseWriter, r *http.Request) {
	var req getSecretValueRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	snap, version, err := h.svc.GetSecretValue(req.SecretId, req.VersionId, req.VersionStage)
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	resp := getSecretValueResponse{
		ARN:           snap.ARN,
		Name:          snap.Name,
		VersionId:     version.VersionId,
		SecretString:  version.SecretString,
		VersionStages: version.Stages,
		CreatedDate:   unixTime(version.CreatedDate),
	}
	if len(version.SecretBinary) > 0 {
		resp.SecretBinary = base64.StdEncoding.EncodeToString(version.SecretBinary)
	}
	awsutil.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandlePutSecretValue(w http.ResponseWriter, r *http.Request) {
	var req putSecretValueRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	secretBinary, ok := decodeBinary(w, req.SecretBinary)
	if !ok {
		return
	}

	snap, version, err := h.svc.PutSecretValue(PutSecretValueInput{
		SecretId:           req.SecretId,
		ClientRequestToken: req.ClientRequestToken,
		SecretString:       req.SecretString,
		SecretBinary:       secretBinary,
		VersionStages:      req.VersionStages,
	})
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, putSecretValueResponse{
		ARN:           snap.ARN,
		Name:          snap.Name,
		VersionId:     version.VersionId,
		VersionStages: version.Stages,
	})
}

func (h *Handler) HandleDescribeSecret(w http.ResponseWriter, r *http.Request) {
	var req describeSecretRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	snap, err := h.svc.DescribeSecret(req.SecretId)
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, toDescribeSecretResponse(snap))
}

func (h *Handler) HandleListSecrets(w http.ResponseWriter, r *http.Request) {
	var req listSecretsRequest
	if !decodeBody(w, r, &req) {
		return
	}
	snaps := h.svc.ListSecrets()
	entries := make([]secretListEntry, 0, len(snaps))
	for _, snap := range snaps {
		entries = append(entries, secretListEntry{
			ARN:                    snap.ARN,
			Name:                   snap.Name,
			Description:            snap.Description,
			KmsKeyId:               snap.KmsKeyId,
			Tags:                   tagsToOutput(snap.Tags),
			SecretVersionsToStages: snap.VersionIdsToStage,
			CreatedDate:            unixTime(snap.CreatedDate),
			LastChangedDate:        unixTime(snap.LastChangedDate),
			DeletedDate:            optionalUnixTimePtr(snap.DeletedDate),
		})
	}
	awsutil.WriteJSON(w, http.StatusOK, listSecretsResponse{SecretList: entries})
}

func (h *Handler) HandleDeleteSecret(w http.ResponseWriter, r *http.Request) {
	var req deleteSecretRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	snap, err := h.svc.DeleteSecret(req.SecretId, req.RecoveryWindowInDays, req.ForceDeleteWithoutRecovery)
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, deleteSecretResponse{
		ARN:          snap.ARN,
		Name:         snap.Name,
		DeletionDate: optionalUnixTimePtr(snap.DeletedDate),
	})
}

func (h *Handler) HandleRestoreSecret(w http.ResponseWriter, r *http.Request) {
	var req restoreSecretRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	snap, err := h.svc.RestoreSecret(req.SecretId)
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, restoreSecretResponse{ARN: snap.ARN, Name: snap.Name})
}

func (h *Handler) HandleUpdateSecret(w http.ResponseWriter, r *http.Request) {
	var req updateSecretRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	secretBinary, ok := decodeBinary(w, req.SecretBinary)
	if !ok {
		return
	}

	snap, version, err := h.svc.UpdateSecret(UpdateSecretInput{
		SecretId:           req.SecretId,
		ClientRequestToken: req.ClientRequestToken,
		Description:        req.Description,
		KmsKeyId:           req.KmsKeyId,
		SecretString:       req.SecretString,
		SecretBinary:       secretBinary,
	})
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	resp := updateSecretResponse{ARN: snap.ARN, Name: snap.Name}
	if version != nil {
		resp.VersionId = version.VersionId
	}
	awsutil.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleTagResource(w http.ResponseWriter, r *http.Request) {
	var req tagResourceRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	if err := h.svc.TagResource(req.SecretId, tagsFromInput(req.Tags)); err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

func (h *Handler) HandleUntagResource(w http.ResponseWriter, r *http.Request) {
	var req untagResourceRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	if err := h.svc.UntagResource(req.SecretId, req.TagKeys); err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

func (h *Handler) HandleListSecretVersionIds(w http.ResponseWriter, r *http.Request) {
	var req listSecretVersionIdsRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.SecretId == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretId is required")
		return
	}
	snap, versions, err := h.svc.ListSecretVersionIds(req.SecretId)
	if err != nil {
		writeSecretsManagerError(w, err)
		return
	}
	entries := make([]secretVersionsListEntry, 0, len(versions))
	for _, v := range versions {
		entries = append(entries, secretVersionsListEntry{
			VersionId:     v.VersionId,
			VersionStages: v.Stages,
			CreatedDate:   unixTime(v.CreatedDate),
		})
	}
	awsutil.WriteJSON(w, http.StatusOK, listSecretVersionIdsResponse{
		ARN:      snap.ARN,
		Name:     snap.Name,
		Versions: entries,
	})
}

// --- shared helpers ---

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return false
	}
	return true
}

func decodeBinary(w http.ResponseWriter, encoded string) ([]byte, bool) {
	if encoded == "" {
		return nil, true
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "SecretBinary is not valid base64: "+err.Error())
		return nil, false
	}
	return raw, true
}

func tagsFromInput(tags []tagInput) map[string]string {
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		out[t.Key] = t.Value
	}
	return out
}

func tagsToOutput(tags map[string]string) []tagInput {
	out := make([]tagInput, 0, len(tags))
	for k, v := range tags {
		out = append(out, tagInput{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func unixTime(t time.Time) float64 {
	return float64(t.Unix())
}

// optionalUnixTime returns nil for a zero time.Time (never set), and the
// Unix-seconds value otherwise, so the JSON field is omitted rather than
// serialized as a large negative number.
func optionalUnixTime(t time.Time) *float64 {
	if t.IsZero() {
		return nil
	}
	v := unixTime(t)
	return &v
}

func optionalUnixTimePtr(t *time.Time) *float64 {
	if t == nil {
		return nil
	}
	return optionalUnixTime(*t)
}

func toDescribeSecretResponse(snap Snapshot) describeSecretResponse {
	return describeSecretResponse{
		ARN:                snap.ARN,
		Name:               snap.Name,
		Description:        snap.Description,
		KmsKeyId:           snap.KmsKeyId,
		Tags:               tagsToOutput(snap.Tags),
		VersionIdsToStages: snap.VersionIdsToStage,
		CreatedDate:        unixTime(snap.CreatedDate),
		LastChangedDate:    unixTime(snap.LastChangedDate),
		LastAccessedDate:   float64PtrOrZero(optionalUnixTime(snap.LastAccessedDate)),
		DeletedDate:        optionalUnixTimePtr(snap.DeletedDate),
	}
}

func float64PtrOrZero(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

// writeSecretsManagerError maps a Service error to the matching AWS JSON
// error shape/status.
func writeSecretsManagerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSecretNotFound), errors.Is(err, ErrVersionNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrSecretExists):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "ResourceExistsException", err.Error())
	case errors.Is(err, ErrInvalidRequest):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidRequestException", err.Error())
	case errors.Is(err, ErrInvalidParam):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
	default:
		awsutil.WriteJSONError(w, http.StatusInternalServerError, "InternalServiceError", err.Error())
	}
}
