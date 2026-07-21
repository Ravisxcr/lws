package textract

import (
	"encoding/json"
	"net/http"
	"time"

	"lws/pkg/awsutil"
)

func unixTime(t time.Time) float64 {
	return float64(t.Unix())
}

// --- CreateAdapter ---

type createAdapterRequest struct {
	AdapterName  string            `json:"AdapterName"`
	Description  string            `json:"Description,omitempty"`
	FeatureTypes []string          `json:"FeatureTypes"`
	AutoUpdate   string            `json:"AutoUpdate,omitempty"`
	Tags         map[string]string `json:"Tags,omitempty"`
}

type createAdapterResponse struct {
	AdapterId string `json:"AdapterId"`
}

func (h *Handler) HandleCreateAdapter(w http.ResponseWriter, r *http.Request) {
	var req createAdapterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	a, err := h.proc.CreateAdapter(CreateAdapterInput{
		AdapterName:  req.AdapterName,
		Description:  req.Description,
		FeatureTypes: req.FeatureTypes,
		AutoUpdate:   req.AutoUpdate,
		Tags:         req.Tags,
	})
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, createAdapterResponse{AdapterId: a.AdapterId})
}

// --- GetAdapter ---

type adapterIDRequest struct {
	AdapterId string `json:"AdapterId"`
}

type getAdapterResponse struct {
	AdapterId    string            `json:"AdapterId"`
	AdapterName  string            `json:"AdapterName"`
	CreationTime float64           `json:"CreationTime"`
	Description  string            `json:"Description,omitempty"`
	FeatureTypes []string          `json:"FeatureTypes"`
	AutoUpdate   string            `json:"AutoUpdate"`
	Tags         map[string]string `json:"Tags,omitempty"`
}

func (h *Handler) HandleGetAdapter(w http.ResponseWriter, r *http.Request) {
	var req adapterIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	a, err := h.proc.GetAdapter(req.AdapterId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, getAdapterResponse{
		AdapterId:    a.AdapterId,
		AdapterName:  a.AdapterName,
		CreationTime: unixTime(a.CreationTime),
		Description:  a.Description,
		FeatureTypes: a.FeatureTypes,
		AutoUpdate:   a.AutoUpdate,
		Tags:         a.Tags,
	})
}

// --- UpdateAdapter ---

type updateAdapterRequest struct {
	AdapterId   string `json:"AdapterId"`
	Description string `json:"Description,omitempty"`
	AutoUpdate  string `json:"AutoUpdate,omitempty"`
}

func (h *Handler) HandleUpdateAdapter(w http.ResponseWriter, r *http.Request) {
	var req updateAdapterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	a, err := h.proc.UpdateAdapter(UpdateAdapterInput{
		AdapterId:   req.AdapterId,
		Description: req.Description,
		AutoUpdate:  req.AutoUpdate,
	})
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, getAdapterResponse{
		AdapterId:    a.AdapterId,
		AdapterName:  a.AdapterName,
		CreationTime: unixTime(a.CreationTime),
		Description:  a.Description,
		FeatureTypes: a.FeatureTypes,
		AutoUpdate:   a.AutoUpdate,
	})
}

// --- DeleteAdapter ---

func (h *Handler) HandleDeleteAdapter(w http.ResponseWriter, r *http.Request) {
	var req adapterIDRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	if err := h.proc.DeleteAdapter(req.AdapterId); err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

// --- ListAdapters ---

type listAdaptersRequest struct {
	MaxResults int64  `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type adapterOverview struct {
	AdapterId    string   `json:"AdapterId"`
	AdapterName  string   `json:"AdapterName"`
	CreationTime float64  `json:"CreationTime"`
	FeatureTypes []string `json:"FeatureTypes,omitempty"`
}

type listAdaptersResponse struct {
	Adapters  []adapterOverview `json:"Adapters"`
	NextToken string            `json:"NextToken,omitempty"`
}

func (h *Handler) HandleListAdapters(w http.ResponseWriter, r *http.Request) {
	var req listAdaptersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	adapters := h.proc.ListAdapters()
	out := make([]adapterOverview, 0, len(adapters))
	for _, a := range adapters {
		out = append(out, adapterOverview{
			AdapterId:    a.AdapterId,
			AdapterName:  a.AdapterName,
			CreationTime: unixTime(a.CreationTime),
			FeatureTypes: a.FeatureTypes,
		})
	}
	awsutil.WriteJSON(w, http.StatusOK, listAdaptersResponse{Adapters: out})
}

// --- CreateAdapterVersion ---

type createAdapterVersionRequest struct {
	AdapterId     string            `json:"AdapterId"`
	DatasetConfig json.RawMessage   `json:"DatasetConfig,omitempty"`
	KMSKeyId      string            `json:"KMSKeyId,omitempty"`
	OutputConfig  json.RawMessage   `json:"OutputConfig,omitempty"`
	Tags          map[string]string `json:"Tags,omitempty"`
}

type createAdapterVersionResponse struct {
	AdapterId      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

func (h *Handler) HandleCreateAdapterVersion(w http.ResponseWriter, r *http.Request) {
	var req createAdapterVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	v, err := h.proc.CreateAdapterVersion(CreateAdapterVersionInput{
		AdapterId:     req.AdapterId,
		DatasetConfig: req.DatasetConfig,
		KMSKeyId:      req.KMSKeyId,
		OutputConfig:  req.OutputConfig,
		Tags:          req.Tags,
	})
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, createAdapterVersionResponse{AdapterId: v.AdapterId, AdapterVersion: v.AdapterVersion})
}

// --- GetAdapterVersion ---

type adapterVersionRequest struct {
	AdapterId      string `json:"AdapterId"`
	AdapterVersion string `json:"AdapterVersion"`
}

type getAdapterVersionResponse struct {
	AdapterId      string            `json:"AdapterId"`
	AdapterVersion string            `json:"AdapterVersion"`
	DatasetConfig  json.RawMessage   `json:"DatasetConfig,omitempty"`
	KMSKeyId       string            `json:"KMSKeyId,omitempty"`
	OutputConfig   json.RawMessage   `json:"OutputConfig,omitempty"`
	Status         string            `json:"Status"`
	StatusMessage  string            `json:"StatusMessage,omitempty"`
	CreationTime   float64           `json:"CreationTime"`
	FeatureTypes   []string          `json:"FeatureTypes,omitempty"`
	Tags           map[string]string `json:"Tags,omitempty"`
}

func (h *Handler) HandleGetAdapterVersion(w http.ResponseWriter, r *http.Request) {
	var req adapterVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	v, err := h.proc.GetAdapterVersion(req.AdapterId, req.AdapterVersion)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, getAdapterVersionResponse{
		AdapterId:      v.AdapterId,
		AdapterVersion: v.AdapterVersion,
		DatasetConfig:  v.DatasetConfig,
		KMSKeyId:       v.KMSKeyId,
		OutputConfig:   v.OutputConfig,
		Status:         v.Status,
		StatusMessage:  v.StatusMessage,
		CreationTime:   unixTime(v.CreationTime),
		FeatureTypes:   v.FeatureTypes,
		Tags:           v.Tags,
	})
}

// --- DeleteAdapterVersion ---

func (h *Handler) HandleDeleteAdapterVersion(w http.ResponseWriter, r *http.Request) {
	var req adapterVersionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	if err := h.proc.DeleteAdapterVersion(req.AdapterId, req.AdapterVersion); err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

// --- ListAdapterVersions ---

type listAdapterVersionsRequest struct {
	AdapterId  string `json:"AdapterId,omitempty"`
	MaxResults int64  `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type adapterVersionOverview struct {
	AdapterId      string   `json:"AdapterId"`
	AdapterVersion string   `json:"AdapterVersion"`
	CreationTime   float64  `json:"CreationTime"`
	FeatureTypes   []string `json:"FeatureTypes,omitempty"`
	Status         string   `json:"Status"`
}

type listAdapterVersionsResponse struct {
	AdapterVersions []adapterVersionOverview `json:"AdapterVersions"`
	NextToken       string                   `json:"NextToken,omitempty"`
}

func (h *Handler) HandleListAdapterVersions(w http.ResponseWriter, r *http.Request) {
	var req listAdapterVersionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	versions, err := h.proc.ListAdapterVersions(req.AdapterId)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	out := make([]adapterVersionOverview, 0, len(versions))
	for _, v := range versions {
		out = append(out, adapterVersionOverview{
			AdapterId:      v.AdapterId,
			AdapterVersion: v.AdapterVersion,
			CreationTime:   unixTime(v.CreationTime),
			FeatureTypes:   v.FeatureTypes,
			Status:         v.Status,
		})
	}
	awsutil.WriteJSON(w, http.StatusOK, listAdapterVersionsResponse{AdapterVersions: out})
}

// --- TagResource / UntagResource / ListTagsForResource ---

type tagResourceRequest struct {
	ResourceARN string            `json:"ResourceARN"`
	Tags        map[string]string `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type resourceARNRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsForResourceResponse struct {
	Tags map[string]string `json:"Tags,omitempty"`
}

func (h *Handler) HandleTagResource(w http.ResponseWriter, r *http.Request) {
	var req tagResourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	if err := h.proc.TagResource(req.ResourceARN, req.Tags); err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

func (h *Handler) HandleUntagResource(w http.ResponseWriter, r *http.Request) {
	var req untagResourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	if err := h.proc.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, struct{}{})
}

func (h *Handler) HandleListTagsForResource(w http.ResponseWriter, r *http.Request) {
	var req resourceARNRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterException", "malformed request body: "+err.Error())
		return
	}
	tags, err := h.proc.ListTagsForResource(req.ResourceARN)
	if err != nil {
		writeTextractError(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, listTagsForResourceResponse{Tags: tags})
}
