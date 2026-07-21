package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"lws/pkg/awsutil"
)

// Handler binds S3's REST HTTP requests (bucket/key in the URL path
type Handler struct {
	svc *Service
}

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type xmlOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

var localOwner = xmlOwner{ID: accountID, DisplayName: "lws"}

type listAllMyBucketsBucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}
type listAllMyBucketsResult struct {
	XMLName xml.Name                 `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   xmlOwner                 `xml:"Owner"`
	Buckets []listAllMyBucketsBucket `xml:"Buckets>Bucket"`
}

type locationConstraint struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
	Value   string   `xml:",chardata"`
}

type filterRuleXML struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}
type s3KeyFilterXML struct {
	FilterRules []filterRuleXML `xml:"FilterRule"`
}
type notificationFilterXML struct {
	S3Key s3KeyFilterXML `xml:"S3Key"`
}
type queueConfigurationXML struct {
	ID     string                 `xml:"Id,omitempty"`
	Queue  string                 `xml:"Queue"`
	Events []string               `xml:"Event"`
	Filter *notificationFilterXML `xml:"Filter,omitempty"`
}
type topicConfigurationXML struct {
	ID     string                 `xml:"Id,omitempty"`
	Topic  string                 `xml:"Topic"`
	Events []string               `xml:"Event"`
	Filter *notificationFilterXML `xml:"Filter,omitempty"`
}
type notificationConfigurationXML struct {
	XMLName             xml.Name                `xml:"http://s3.amazonaws.com/doc/2006-03-01/ NotificationConfiguration"`
	QueueConfigurations []queueConfigurationXML `xml:"QueueConfiguration,omitempty"`
	TopicConfigurations []topicConfigurationXML `xml:"TopicConfiguration,omitempty"`
}

type xmlContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}
type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}
type listBucketResultV2 struct {
	XMLName               xml.Name          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	IsTruncated           bool              `xml:"IsTruncated"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	Contents              []xmlContent      `xml:"Contents,omitempty"`
	CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
}
type listBucketResultV1 struct {
	XMLName        xml.Name          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []xmlContent      `xml:"Contents,omitempty"`
	CommonPrefixes []xmlCommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type deleteObjectsRequestObject struct {
	Key string `xml:"Key"`
}
type deleteObjectsRequest struct {
	XMLName xml.Name                     `xml:"Delete"`
	Objects []deleteObjectsRequestObject `xml:"Object"`
	Quiet   bool                         `xml:"Quiet"`
}
type deletedXML struct {
	Key string `xml:"Key"`
}
type deleteResult struct {
	XMLName xml.Name     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []deletedXML `xml:"Deleted,omitempty"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}
type tagSetXML struct {
	Tags []tagXML `xml:"Tag"`
}
type taggingXML struct {
	XMLName xml.Name  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ Tagging"`
	TagSet  tagSetXML `xml:"TagSet"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type completeMultipartUploadRequestPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}
type completeMultipartUploadRequest struct {
	XMLName xml.Name                             `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartUploadRequestPart `xml:"Part"`
}
type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	LastModified string `xml:"LastModified"`
}
type listPartsResult struct {
	XMLName  xml.Name  `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListPartsResult"`
	Bucket   string    `xml:"Bucket"`
	Key      string    `xml:"Key"`
	UploadId string    `xml:"UploadId"`
	Part     []partXML `xml:"Part,omitempty"`
}

type uploadXML struct {
	Key       string `xml:"Key"`
	UploadId  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}
type listMultipartUploadsResult struct {
	XMLName xml.Name    `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListMultipartUploadsResult"`
	Bucket  string      `xml:"Bucket"`
	Upload  []uploadXML `xml:"Upload,omitempty"`
}

// NewHandler returns an S3 HTTP handler backed by svc.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		if r.Method == http.MethodGet {
			h.handleListBuckets(w, r)
			return
		}
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method for the root path", "")
		return
	}

	bucket, key, hasKey := strings.Cut(path, "/")
	if !hasKey || key == "" {
		h.routeBucket(w, r, bucket)
		return
	}
	h.routeObject(w, r, bucket, key)
}

func (h *Handler) routeBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		switch {
		case q.Has("notification"):
			h.handlePutBucketNotificationConfiguration(w, r, bucket)
		default:
			h.handleCreateBucket(w, r, bucket)
		}
	case http.MethodDelete:
		h.handleDeleteBucket(w, r, bucket)
	case http.MethodHead:
		h.handleHeadBucket(w, r, bucket)
	case http.MethodGet:
		switch {
		case q.Has("location"):
			h.handleGetBucketLocation(w, r, bucket)
		case q.Has("notification"):
			h.handleGetBucketNotificationConfiguration(w, r, bucket)
		case q.Has("uploads"):
			h.handleListMultipartUploads(w, r, bucket)
		case q.Get("list-type") == "2":
			h.handleListObjectsV2(w, r, bucket)
		default:
			h.handleListObjects(w, r, bucket)
		}
	case http.MethodPost:
		if q.Has("delete") {
			h.handleDeleteObjects(w, r, bucket)
			return
		}
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "unsupported bucket POST request", bucket)
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method for a bucket path", bucket)
	}
}

func (h *Handler) routeObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	resource := bucket + "/" + key
	switch r.Method {
	case http.MethodPut:
		switch {
		case q.Has("partNumber") && q.Has("uploadId"):
			h.handleUploadPart(w, r, bucket, key)
		case q.Has("tagging"):
			h.handlePutObjectTagging(w, r, bucket, key)
		case r.Header.Get("X-Amz-Copy-Source") != "":
			h.handleCopyObject(w, r, bucket, key)
		default:
			h.handlePutObject(w, r, bucket, key)
		}
	case http.MethodGet:
		switch {
		case q.Has("uploadId"):
			h.handleListParts(w, r, bucket, key)
		case q.Has("tagging"):
			h.handleGetObjectTagging(w, r, bucket, key)
		default:
			h.handleGetObject(w, r, bucket, key)
		}
	case http.MethodHead:
		h.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		switch {
		case q.Has("uploadId"):
			h.handleAbortMultipartUpload(w, r, bucket, key)
		case q.Has("tagging"):
			h.handleDeleteObjectTagging(w, r, bucket, key)
		default:
			h.handleDeleteObject(w, r, bucket, key)
		}
	case http.MethodPost:
		switch {
		case q.Has("uploads"):
			h.handleCreateMultipartUpload(w, r, bucket, key)
		case q.Has("uploadId"):
			h.handleCompleteMultipartUpload(w, r, bucket, key)
		default:
			writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "unsupported object POST request", resource)
		}
	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported method for an object path", resource)
	}
}

// --- bucket handlers ---

func (h *Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	out := listAllMyBucketsResult{Owner: localOwner}
	for _, b := range h.svc.ListBuckets() {
		out.Buckets = append(out.Buckets, listAllMyBucketsBucket{
			Name:         b.Name,
			CreationDate: b.CreationDate.Format(time.RFC3339),
		})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if _, err := h.svc.CreateBucket(bucket); err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := h.svc.DeleteBucket(bucket); err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := h.svc.HeadBucket(bucket); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := h.svc.HeadBucket(bucket); err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	loc := region
	if loc == "us-east-1" {
		loc = "" // real S3 returns an empty LocationConstraint for the default region
	}
	awsutil.WriteXML(w, http.StatusOK, locationConstraint{Value: loc})
}

func (h *Handler) handleGetBucketNotificationConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg, err := h.svc.GetBucketNotificationConfiguration(bucket)
	if err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	out := notificationConfigurationXML{}
	for _, qc := range cfg.QueueConfigs {
		out.QueueConfigurations = append(out.QueueConfigurations, queueConfigurationXML{
			ID:     qc.ID,
			Queue:  qc.QueueArn,
			Events: qc.Events,
			Filter: filterToXML(qc.Filter),
		})
	}
	for _, tc := range cfg.TopicConfigs {
		out.TopicConfigurations = append(out.TopicConfigurations, topicConfigurationXML{
			ID:     tc.ID,
			Topic:  tc.TopicArn,
			Events: tc.Events,
			Filter: filterToXML(tc.Filter),
		})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handlePutBucketNotificationConfiguration(w http.ResponseWriter, r *http.Request, bucket string) {
	var req notificationConfigurationXML
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "failed to parse request body: "+err.Error(), bucket)
		return
	}
	cfg := &NotificationConfig{}
	for _, qc := range req.QueueConfigurations {
		cfg.QueueConfigs = append(cfg.QueueConfigs, QueueConfig{
			ID:       qc.ID,
			QueueArn: qc.Queue,
			Events:   qc.Events,
			Filter:   filterFromXML(qc.Filter),
		})
	}
	for _, tc := range req.TopicConfigurations {
		cfg.TopicConfigs = append(cfg.TopicConfigs, TopicConfig{
			ID:       tc.ID,
			TopicArn: tc.Topic,
			Events:   tc.Events,
			Filter:   filterFromXML(tc.Filter),
		})
	}
	if err := h.svc.PutBucketNotificationConfiguration(bucket, cfg); err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// filterToXML renders a KeyFilter as S3's Filter>S3Key>FilterRule shape,
// omitting the element entirely when the filter has no rules.
func filterToXML(f KeyFilter) *notificationFilterXML {
	if f.Prefix == "" && f.Suffix == "" {
		return nil
	}
	nf := &notificationFilterXML{}
	if f.Prefix != "" {
		nf.S3Key.FilterRules = append(nf.S3Key.FilterRules, filterRuleXML{Name: "prefix", Value: f.Prefix})
	}
	if f.Suffix != "" {
		nf.S3Key.FilterRules = append(nf.S3Key.FilterRules, filterRuleXML{Name: "suffix", Value: f.Suffix})
	}
	return nf
}

func filterFromXML(f *notificationFilterXML) KeyFilter {
	var kf KeyFilter
	if f == nil {
		return kf
	}
	for _, rule := range f.S3Key.FilterRules {
		switch strings.ToLower(rule.Name) {
		case "prefix":
			kf.Prefix = rule.Value
		case "suffix":
			kf.Suffix = rule.Value
		}
	}
	return kf
}

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	startAfter := q.Get("start-after")
	continuationToken := q.Get("continuation-token")
	if continuationToken != "" {
		startAfter = continuationToken
	}
	maxKeys, _ := strconv.Atoi(q.Get("max-keys"))

	res, err := h.svc.ListObjectsV2(bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	out := listBucketResultV2{
		Name:                  bucket,
		Prefix:                prefix,
		Delimiter:             delimiter,
		KeyCount:              len(res.Objects) + len(res.CommonPrefixes),
		MaxKeys:               maxKeysOrDefault(maxKeys),
		IsTruncated:           res.IsTruncated,
		ContinuationToken:     continuationToken,
		NextContinuationToken: res.NextMarker,
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, toXMLContent(o))
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xmlCommonPrefix{Prefix: p})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	marker := q.Get("marker")
	maxKeys, _ := strconv.Atoi(q.Get("max-keys"))

	res, err := h.svc.ListObjects(bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	out := listBucketResultV1{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		Delimiter:   delimiter,
		MaxKeys:     maxKeysOrDefault(maxKeys),
		IsTruncated: res.IsTruncated,
		NextMarker:  res.NextMarker,
	}
	for _, o := range res.Objects {
		out.Contents = append(out.Contents, toXMLContent(o))
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, xmlCommonPrefix{Prefix: p})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req deleteObjectsRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "failed to parse request body: "+err.Error(), bucket)
		return
	}
	keys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		keys = append(keys, o.Key)
	}
	if err := h.svc.DeleteObjects(bucket, keys); err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	out := deleteResult{}
	if !req.Quiet {
		for _, k := range keys {
			out.Deleted = append(out.Deleted, deletedXML{Key: k})
		}
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

// --- object handlers ---

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "failed to read request body: "+err.Error(), bucket+"/"+key)
		return
	}
	obj, err := h.svc.PutObject(bucket, key, data, r.Header.Get("Content-Type"), extractUserMetadata(r.Header))
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.Header().Set("ETag", quote(obj.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := h.svc.GetObject(bucket, key)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	setObjectHeaders(w, obj)

	data := obj.Data
	status := http.StatusOK
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		if start, end, ok := parseRange(rangeHeader, len(data)); ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			data = data[start : end+1]
			status = http.StatusPartialContent
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	obj, err := h.svc.HeadObject(bucket, key)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	setObjectHeaders(w, obj)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.Data)))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := h.svc.DeleteObject(bucket, key); err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", err.Error(), bucket+"/"+key)
		return
	}
	var newMetadata map[string]string
	newContentType := ""
	if r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		newMetadata = extractUserMetadata(r.Header)
		if newMetadata == nil {
			newMetadata = map[string]string{}
		}
		newContentType = r.Header.Get("Content-Type")
	}
	obj, err := h.svc.CopyObject(srcBucket, srcKey, bucket, key, newMetadata, newContentType)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, copyObjectResult{
		ETag:         quote(obj.ETag),
		LastModified: obj.LastModified.Format(time.RFC3339),
	})
}

// --- object tagging handlers ---

func (h *Handler) handlePutObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	var req taggingXML
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "failed to parse request body: "+err.Error(), bucket+"/"+key)
		return
	}
	tags := make(map[string]string, len(req.TagSet.Tags))
	for _, t := range req.TagSet.Tags {
		tags[t.Key] = t.Value
	}
	if err := h.svc.PutObjectTagging(bucket, key, tags); err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	tags, err := h.svc.GetObjectTagging(bucket, key)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	out := taggingXML{}
	for k, v := range tags {
		out.TagSet.Tags = append(out.TagSet.Tags, tagXML{Key: k, Value: v})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handleDeleteObjectTagging(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := h.svc.DeleteObjectTagging(bucket, key); err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- multipart upload handlers ---

func (h *Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	u, err := h.svc.CreateMultipartUpload(bucket, key, r.Header.Get("Content-Type"), extractUserMetadata(r.Header))
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, initiateMultipartUploadResult{Bucket: bucket, Key: key, UploadId: u.UploadID})
}

func (h *Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber <= 0 {
		writeS3Error(w, http.StatusBadRequest, "InvalidArgument", "partNumber must be a positive integer", bucket+"/"+key)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, http.StatusBadRequest, "InvalidRequest", "failed to read request body: "+err.Error(), bucket+"/"+key)
		return
	}
	p, err := h.svc.UploadPart(bucket, key, uploadID, partNumber, data)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.Header().Set("ETag", quote(p.ETag))
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	var req completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeS3Error(w, http.StatusBadRequest, "MalformedXML", "failed to parse request body: "+err.Error(), bucket+"/"+key)
		return
	}
	completed := make([]CompletedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		completed = append(completed, CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag})
	}
	obj, err := h.svc.CompleteMultipartUpload(bucket, key, uploadID, completed)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, completeMultipartUploadResult{
		Location: fmt.Sprintf("http://%s/%s/%s", r.Host, bucket, key),
		Bucket:   bucket,
		Key:      key,
		ETag:     quote(obj.ETag),
	})
}

func (h *Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	if err := h.svc.AbortMultipartUpload(bucket, key, uploadID); err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleListParts(w http.ResponseWriter, r *http.Request, bucket, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	parts, err := h.svc.ListParts(bucket, key, uploadID)
	if err != nil {
		writeServiceError(w, err, bucket+"/"+key)
		return
	}
	out := listPartsResult{Bucket: bucket, Key: key, UploadId: uploadID}
	for _, p := range parts {
		out.Part = append(out.Part, partXML{
			PartNumber:   p.PartNumber,
			ETag:         quote(p.ETag),
			Size:         int64(len(p.Data)),
			LastModified: p.LastModified.Format(time.RFC3339),
		})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

func (h *Handler) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string) {
	uploads, err := h.svc.ListMultipartUploads(bucket)
	if err != nil {
		writeServiceError(w, err, bucket)
		return
	}
	out := listMultipartUploadsResult{Bucket: bucket}
	for _, u := range uploads {
		out.Upload = append(out.Upload, uploadXML{Key: u.Key, UploadId: u.UploadID, Initiated: u.Initiated.Format(time.RFC3339)})
	}
	awsutil.WriteXML(w, http.StatusOK, out)
}

// --- helpers ---

func toXMLContent(o *Object) xmlContent {
	return xmlContent{
		Key:          o.Key,
		LastModified: o.LastModified.Format(time.RFC3339),
		ETag:         quote(o.ETag),
		Size:         int64(len(o.Data)),
		StorageClass: "STANDARD",
	}
}

func setObjectHeaders(w http.ResponseWriter, obj *Object) {
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", quote(obj.ETag))
	w.Header().Set("Last-Modified", obj.LastModified.Format(http.TimeFormat))
	for k, v := range obj.Metadata {
		// Set() canonicalizes to "X-Amz-Meta-Author"; real S3 (and boto3's
		// parsing of the response, which strips the literal prefix) expects
		// the wire header name to stay all-lowercase, so bypass Set/Add and
		// write the map key directly.
		w.Header()["x-amz-meta-"+k] = []string{v}
	}
}

// extractUserMetadata pulls x-amz-meta-* headers into a plain key/value map
// (with the prefix stripped), matching how S3 surfaces user metadata.
func extractUserMetadata(header http.Header) map[string]string {
	var metadata map[string]string
	for k := range header {
		if lower := strings.ToLower(k); strings.HasPrefix(lower, "x-amz-meta-") {
			if metadata == nil {
				metadata = make(map[string]string)
			}
			metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = header.Get(k)
		}
	}
	return metadata
}

// parseRange parses a single-range "bytes=start-end" Range header (including
func parseRange(header string, size int) (start, end int, ok bool) {
	header = strings.TrimPrefix(header, "bytes=")
	before, after, found := strings.Cut(header, "-")
	if !found {
		return 0, 0, false
	}
	if before == "" {
		n, err := strconv.Atoi(after)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, size - 1, true
	}
	start, err := strconv.Atoi(before)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if after == "" {
		return start, size - 1, true
	}
	end, err = strconv.Atoi(after)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

// parseCopySource parses the X-Amz-Copy-Source header ("/bucket/key" or
// "bucket/key", optionally URL-encoded) into its bucket and key parts.
func parseCopySource(src string) (bucket, key string, err error) {
	if src == "" {
		return "", "", errors.New("X-Amz-Copy-Source header is required")
	}
	if unescaped, uerr := url.QueryUnescape(src); uerr == nil {
		src = unescaped
	}
	src = strings.TrimPrefix(src, "/")
	b, k, found := strings.Cut(src, "/")
	if !found || b == "" || k == "" {
		return "", "", errors.New(`X-Amz-Copy-Source must be of the form "bucket/key"`)
	}
	return b, k, nil
}

func quote(etag string) string {
	return `"` + etag + `"`
}

func maxKeysOrDefault(n int) int {
	if n <= 0 {
		return 1000
	}
	return n
}

// writeS3Error writes a real-S3-shaped <Error> body (unlike SQS/SNS's
// <ErrorResponse> wrapper, S3's error root element is <Error> itself).
func writeS3Error(w http.ResponseWriter, status int, code, message, resource string) {
	log.Printf("s3: %s error %s: %s", code, message, resource)
	type s3ErrorXML struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		Resource  string   `xml:"Resource,omitempty"`
		RequestID string   `xml:"RequestId"`
	}
	body, err := xml.Marshal(s3ErrorXML{Code: code, Message: message, Resource: resource, RequestID: awsutil.NewRequestID()})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// writeServiceError maps a Service error to the matching AWS S3 error
// code/status.
func writeServiceError(w http.ResponseWriter, err error, resource string) {
	switch {
	case errors.Is(err, ErrBucketNotFound):
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", err.Error(), resource)
	case errors.Is(err, ErrBucketAlreadyExists):
		writeS3Error(w, http.StatusConflict, "BucketAlreadyExists", err.Error(), resource)
	case errors.Is(err, ErrBucketNotEmpty):
		writeS3Error(w, http.StatusConflict, "BucketNotEmpty", err.Error(), resource)
	case errors.Is(err, ErrKeyNotFound):
		writeS3Error(w, http.StatusNotFound, "NoSuchKey", err.Error(), resource)
	case errors.Is(err, ErrUploadNotFound):
		writeS3Error(w, http.StatusNotFound, "NoSuchUpload", err.Error(), resource)
	case errors.Is(err, ErrInvalidPart):
		writeS3Error(w, http.StatusBadRequest, "InvalidPart", err.Error(), resource)
	case errors.Is(err, ErrInvalidPartOrder):
		writeS3Error(w, http.StatusBadRequest, "InvalidPartOrder", err.Error(), resource)
	default:
		writeS3Error(w, http.StatusInternalServerError, "InternalError", err.Error(), resource)
	}
}
