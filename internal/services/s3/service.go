package s3

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Dummy account/region used to build AWS-shaped identifiers for a local
// emulator with no real AWS account behind it.
const (
	accountID = "000000000000"
	region    = "us-east-1"
)

var (
	ErrBucketNotFound      = errors.New("bucket does not exist")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket is not empty")
	ErrKeyNotFound         = errors.New("key does not exist")
	ErrUploadNotFound      = errors.New("upload does not exist")
	ErrInvalidPart         = errors.New("one or more specified parts could not be found or the ETag did not match")
	ErrInvalidPartOrder    = errors.New("part numbers must be in ascending order")
)

// CompletedPart identifies one part by number and the ETag the client
// received back from UploadPart, as sent in a CompleteMultipartUpload body.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// ListResult is the common shape returned by ListObjects/ListObjectsV2.
type ListResult struct {
	Objects        []*Object
	CommonPrefixes []string
	IsTruncated    bool
	NextMarker     string // continuation token (v2) or next marker (v1)
}

// TopicPublisher publishes an S3 event notification to an SNS topic;
// satisfied structurally by *sns.Service.
type TopicPublisher interface {
	Publish(topicArn, message, subject string) (string, error)
}

// QueuePublisher delivers an S3 event notification directly to an SQS
// queue; satisfied structurally by *sqs.Service.
type QueuePublisher interface {
	Enqueue(queueName, body string) error
}

// Service implements S3's core domain logic over the thread-safe Storage.
type Service struct {
	store  *Storage
	topics TopicPublisher
	queues QueuePublisher
}

// NewService returns an S3 service with an empty bucket registry. topics
// and queues (may be nil) back bucket event notifications configured via
// PutBucketNotificationConfiguration.
func NewService(topics TopicPublisher, queues QueuePublisher) *Service {
	return &Service{store: NewStorage(), topics: topics, queues: queues}
}

// --- buckets ---

func (s *Service) CreateBucket(name string) (*Bucket, error) {
	if name == "" {
		return nil, errors.New("bucket name is required")
	}
	b, ok := s.store.CreateBucket(name)
	if !ok {
		return nil, ErrBucketAlreadyExists
	}
	return b, nil
}

func (s *Service) HeadBucket(name string) error {
	if _, ok := s.store.GetBucket(name); !ok {
		return ErrBucketNotFound
	}
	return nil
}

func (s *Service) DeleteBucket(name string) error {
	ok, notEmpty := s.store.DeleteBucket(name)
	if !ok {
		return ErrBucketNotFound
	}
	if notEmpty {
		return ErrBucketNotEmpty
	}
	return nil
}

func (s *Service) ListBuckets() []*Bucket {
	return s.store.ListBuckets()
}

func (s *Service) getBucket(name string) (*Bucket, error) {
	b, ok := s.store.GetBucket(name)
	if !ok {
		return nil, ErrBucketNotFound
	}
	return b, nil
}

// PutBucketNotificationConfiguration replaces bucket's event notification
// config. A nil cfg (or one with no configs) clears it.
func (s *Service) PutBucketNotificationConfiguration(bucket string, cfg *NotificationConfig) error {
	b, err := s.getBucket(bucket)
	if err != nil {
		return err
	}
	b.SetNotificationConfig(cfg)
	return nil
}

// GetBucketNotificationConfiguration returns bucket's current event
// notification config, or an empty one if none has been set.
func (s *Service) GetBucketNotificationConfiguration(bucket string) (*NotificationConfig, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if cfg := b.NotificationConfig(); cfg != nil {
		return cfg, nil
	}
	return &NotificationConfig{}, nil
}

// --- objects ---

// PutObject stores data under key in bucket, computing its ETag (MD5) and
// recording contentType/metadata. Returns the stored object.
func (s *Service) PutObject(bucket, key string, data []byte, contentType string, metadata map[string]string) (*Object, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	sum := md5.Sum(data)
	obj := &Object{
		Key:          key,
		Data:         data,
		ContentType:  contentType,
		ETag:         hex.EncodeToString(sum[:]),
		LastModified: time.Now().UTC(),
		Metadata:     metadata,
	}
	b.Put(obj)
	s.notify(b, bucket, "ObjectCreated:Put", obj)
	return obj, nil
}

func (s *Service) GetObject(bucket, key string) (*Object, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	obj, ok := b.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	return obj, nil
}

// HeadObject is identical to GetObject except callers are expected to
// discard the object body; it exists as a distinct method for symmetry
// with the HTTP-level HeadObject operation.
func (s *Service) HeadObject(bucket, key string) (*Object, error) {
	return s.GetObject(bucket, key)
}

func (s *Service) DeleteObject(bucket, key string) error {
	b, err := s.getBucket(bucket)
	if err != nil {
		return err
	}
	if obj, ok := b.Get(key); ok {
		b.Delete(key)
		s.notify(b, bucket, "ObjectRemoved:Delete", obj)
	}
	return nil
}

// DeleteObjects deletes every key in keys, best-effort, matching real S3's
// bulk-delete semantics (deleting an absent key is not an error).
func (s *Service) DeleteObjects(bucket string, keys []string) error {
	b, err := s.getBucket(bucket)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if obj, ok := b.Get(key); ok {
			b.Delete(key)
			s.notify(b, bucket, "ObjectRemoved:Delete", obj)
		}
	}
	return nil
}

// CopyObject copies srcKey in srcBucket to dstKey in dstBucket. If
// newMetadata is non-nil, it replaces the source object's metadata and
// content type (the "REPLACE" metadata directive); otherwise both are
// copied from the source (the default "COPY" directive).
func (s *Service) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, newMetadata map[string]string, newContentType string) (*Object, error) {
	src, err := s.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	dst, err := s.getBucket(dstBucket)
	if err != nil {
		return nil, err
	}
	metadata := src.Metadata
	contentType := src.ContentType
	if newMetadata != nil {
		metadata = newMetadata
		contentType = newContentType
	}
	data := make([]byte, len(src.Data))
	copy(data, src.Data)
	obj := &Object{
		Key:          dstKey,
		Data:         data,
		ContentType:  contentType,
		ETag:         src.ETag,
		LastModified: time.Now().UTC(),
		Metadata:     metadata,
		Tags:         src.Tags,
	}
	dst.Put(obj)
	s.notify(dst, dstBucket, "ObjectCreated:Copy", obj)
	return obj, nil
}

// ListObjectsV2 lists objects in bucket matching prefix, grouping keys that
// share a delimiter-bounded segment into CommonPrefixes (matching real S3's
// "directory" emulation). Pagination resumes after startAfter (the
// continuation token) and stops once maxKeys objects+prefixes are emitted.
func (s *Service) ListObjectsV2(bucket, prefix, delimiter, startAfter string, maxKeys int) (*ListResult, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	return listObjects(b, prefix, delimiter, startAfter, maxKeys), nil
}

// ListObjects implements the legacy v1 ListObjects action, whose pagination
// cursor is a literal key ("Marker") rather than an opaque continuation
// token, but which is otherwise identical to ListObjectsV2.
func (s *Service) ListObjects(bucket, prefix, delimiter, marker string, maxKeys int) (*ListResult, error) {
	return s.ListObjectsV2(bucket, prefix, delimiter, marker, maxKeys)
}

func listObjects(b *Bucket, prefix, delimiter, startAfter string, maxKeys int) *ListResult {
	all := b.List()
	seenPrefixes := make(map[string]bool)
	res := &ListResult{}

	for _, obj := range all {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}

		key := obj.Key
		if delimiter != "" {
			rest := key[len(prefix):]
			if idx := strings.Index(rest, delimiter); idx != -1 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seenPrefixes[cp] {
					if len(res.Objects)+len(res.CommonPrefixes) >= maxKeys {
						res.IsTruncated = true
						res.NextMarker = key
						return res
					}
					seenPrefixes[cp] = true
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
				}
				continue
			}
		}

		if len(res.Objects)+len(res.CommonPrefixes) >= maxKeys {
			res.IsTruncated = true
			res.NextMarker = key
			return res
		}
		res.Objects = append(res.Objects, obj)
	}

	sort.Strings(res.CommonPrefixes)
	return res
}

// --- object tagging ---

func (s *Service) PutObjectTagging(bucket, key string, tags map[string]string) error {
	obj, err := s.GetObject(bucket, key)
	if err != nil {
		return err
	}
	obj.Tags = tags
	return nil
}

func (s *Service) GetObjectTagging(bucket, key string) (map[string]string, error) {
	obj, err := s.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	return obj.Tags, nil
}

func (s *Service) DeleteObjectTagging(bucket, key string) error {
	obj, err := s.GetObject(bucket, key)
	if err != nil {
		return err
	}
	obj.Tags = nil
	return nil
}

// --- multipart uploads ---

func (s *Service) CreateMultipartUpload(bucket, key, contentType string, metadata map[string]string) (*multipartUpload, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	u := &multipartUpload{
		UploadID:    uuid.NewString(),
		Key:         key,
		ContentType: contentType,
		Metadata:    metadata,
		Initiated:   time.Now().UTC(),
		parts:       make(map[int]*Part),
	}
	b.CreateUpload(u)
	return u, nil
}

func (s *Service) UploadPart(bucket, key, uploadID string, partNumber int, data []byte) (*Part, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	u, ok := b.GetUpload(uploadID)
	if !ok || u.Key != key {
		return nil, ErrUploadNotFound
	}
	sum := md5.Sum(data)
	p := &Part{
		PartNumber:   partNumber,
		ETag:         hex.EncodeToString(sum[:]),
		Data:         data,
		LastModified: time.Now().UTC(),
	}
	u.PutPart(p)
	return p, nil
}

func (s *Service) ListParts(bucket, key, uploadID string) ([]*Part, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	u, ok := b.GetUpload(uploadID)
	if !ok || u.Key != key {
		return nil, ErrUploadNotFound
	}
	return u.ListParts(), nil
}

func (s *Service) ListMultipartUploads(bucket string) ([]*multipartUpload, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	return b.ListUploads(), nil
}

func (s *Service) AbortMultipartUpload(bucket, key, uploadID string) error {
	b, err := s.getBucket(bucket)
	if err != nil {
		return err
	}
	u, ok := b.GetUpload(uploadID)
	if !ok || u.Key != key {
		return ErrUploadNotFound
	}
	b.RemoveUpload(uploadID)
	return nil
}

// CompleteMultipartUpload assembles the given parts (in the order the
// client listed them, which real S3 also requires to be ascending by part
// number) into a single object, validating each part's ETag against what
// was recorded at UploadPart time. The final ETag follows real S3's
// multipart convention: the hex MD5 of the concatenated raw MD5 digests of
// each part, suffixed with "-<part count>".
func (s *Service) CompleteMultipartUpload(bucket, key, uploadID string, completed []CompletedPart) (*Object, error) {
	b, err := s.getBucket(bucket)
	if err != nil {
		return nil, err
	}
	u, ok := b.GetUpload(uploadID)
	if !ok || u.Key != key {
		return nil, ErrUploadNotFound
	}

	var data []byte
	var digests []byte
	lastPartNumber := 0
	for _, cp := range completed {
		if cp.PartNumber <= lastPartNumber {
			return nil, ErrInvalidPartOrder
		}
		lastPartNumber = cp.PartNumber

		p, ok := u.GetPart(cp.PartNumber)
		if !ok || p.ETag != strings.Trim(cp.ETag, `"`) {
			return nil, ErrInvalidPart
		}
		data = append(data, p.Data...)
		digestBytes, _ := hex.DecodeString(p.ETag)
		digests = append(digests, digestBytes...)
	}

	sum := md5.Sum(digests)
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(sum[:]), len(completed))

	obj := &Object{
		Key:          key,
		Data:         data,
		ContentType:  u.ContentType,
		ETag:         etag,
		LastModified: time.Now().UTC(),
		Metadata:     u.Metadata,
	}
	b.Put(obj)
	b.RemoveUpload(uploadID)
	s.notify(b, bucket, "ObjectCreated:CompleteMultipartUpload", obj)
	return obj, nil
}

// --- event notifications ---

type eventRecord struct {
	EventVersion string        `json:"eventVersion"`
	EventSource  string        `json:"eventSource"`
	AWSRegion    string        `json:"awsRegion"`
	EventTime    string        `json:"eventTime"`
	EventName    string        `json:"eventName"`
	S3           eventRecordS3 `json:"s3"`
}
type eventRecordS3 struct {
	SchemaVersion string            `json:"s3SchemaVersion"`
	Bucket        eventRecordBucket `json:"bucket"`
	Object        eventRecordObject `json:"object"`
}
type eventRecordBucket struct {
	Name string `json:"name"`
	Arn  string `json:"arn"`
}
type eventRecordObject struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
	ETag string `json:"eTag"`
}
type eventNotification struct {
	Records []eventRecord `json:"Records"`
}

// notify publishes an S3 event notification (eventName without the "s3:"
// prefix, e.g. "ObjectCreated:Put") to every QueueConfig/TopicConfig on
// bucket whose Events and Filter match key. Best-effort: publish failures
// are logged, not returned, so they never fail the underlying object
// operation.
func (s *Service) notify(b *Bucket, bucketName, eventName string, obj *Object) {
	cfg := b.NotificationConfig()
	if cfg == nil {
		return
	}
	body, err := json.Marshal(eventNotification{Records: []eventRecord{{
		EventVersion: "2.1",
		EventSource:  "aws:s3",
		AWSRegion:    region,
		EventTime:    time.Now().UTC().Format(time.RFC3339),
		EventName:    eventName,
		S3: eventRecordS3{
			SchemaVersion: "1.0",
			Bucket:        eventRecordBucket{Name: bucketName, Arn: fmt.Sprintf("arn:aws:s3:::%s", bucketName)},
			Object:        eventRecordObject{Key: obj.Key, Size: int64(len(obj.Data)), ETag: obj.ETag},
		},
	}}})
	if err != nil {
		log.Printf("s3: failed to marshal event notification: %v", err)
		return
	}

	for _, qc := range cfg.QueueConfigs {
		if !matchesEvent(qc.Events, eventName) || !qc.Filter.Matches(obj.Key) {
			continue
		}
		if s.queues == nil {
			continue
		}
		if err := s.queues.Enqueue(queueNameFromArn(qc.QueueArn), string(body)); err != nil {
			log.Printf("s3: failed to deliver event notification to queue %s: %v", qc.QueueArn, err)
		}
	}
	for _, tc := range cfg.TopicConfigs {
		if !matchesEvent(tc.Events, eventName) || !tc.Filter.Matches(obj.Key) {
			continue
		}
		if s.topics == nil {
			continue
		}
		if _, err := s.topics.Publish(tc.TopicArn, string(body), "Amazon S3 Notification"); err != nil {
			log.Printf("s3: failed to deliver event notification to topic %s: %v", tc.TopicArn, err)
		}
	}
}

// matchesEvent reports whether eventName (e.g. "ObjectCreated:Put") is
// covered by any entry in configured (e.g. "s3:ObjectCreated:*" or
// "s3:ObjectCreated:Put").
func matchesEvent(configured []string, eventName string) bool {
	full := "s3:" + eventName
	for _, c := range configured {
		if c == full {
			return true
		}
		if strings.HasSuffix(c, ":*") && strings.HasPrefix(full, strings.TrimSuffix(c, "*")) {
			return true
		}
	}
	return false
}

// queueNameFromArn extracts the queue name from an
// "arn:aws:sqs:<region>:<account>:<name>" ARN.
func queueNameFromArn(arn string) string {
	if idx := strings.LastIndex(arn, ":"); idx != -1 {
		return arn[idx+1:]
	}
	return arn
}
