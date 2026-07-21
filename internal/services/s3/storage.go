// Package s3 emulates AWS S3 in-memory: buckets, objects, multipart
// uploads, and object tagging. Unlike SQS/SNS/Textract (Query or JSON
// protocol, dispatched by an Action/Target string), S3 uses a REST
// protocol where the bucket and key live in the URL path and the
// operation is selected by HTTP method plus query-string subresources.
package s3

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Object is a single stored object and its metadata.
type Object struct {
	Key          string
	Data         []byte
	ContentType  string
	ETag         string // hex MD5, unquoted; quoted when serialized to XML/headers
	LastModified time.Time
	Metadata     map[string]string // x-amz-meta-* user metadata
	Tags         map[string]string
}

// Part is a single uploaded part of an in-progress multipart upload.
type Part struct {
	PartNumber   int
	ETag         string
	Data         []byte
	LastModified time.Time
}

// multipartUpload tracks an in-progress CreateMultipartUpload until it is
// completed or aborted.
type multipartUpload struct {
	UploadID    string
	Key         string
	ContentType string
	Metadata    map[string]string
	Initiated   time.Time

	mu    sync.Mutex
	parts map[int]*Part
}

// KeyFilter restricts a notification configuration to keys matching a
// prefix and/or suffix (S3's Filter.S3Key.FilterRules).
type KeyFilter struct {
	Prefix string
	Suffix string
}

// Matches reports whether key satisfies every non-empty rule in f.
func (f KeyFilter) Matches(key string) bool {
	if f.Prefix != "" && !strings.HasPrefix(key, f.Prefix) {
		return false
	}
	if f.Suffix != "" && !strings.HasSuffix(key, f.Suffix) {
		return false
	}
	return true
}

// QueueConfig delivers matching events directly to an SQS queue.
type QueueConfig struct {
	ID       string
	QueueArn string
	Events   []string
	Filter   KeyFilter
}

// TopicConfig delivers matching events to an SNS topic.
type TopicConfig struct {
	ID       string
	TopicArn string
	Events   []string
	Filter   KeyFilter
}

// NotificationConfig is a bucket's event notification configuration, as set
// by PutBucketNotificationConfiguration.
type NotificationConfig struct {
	QueueConfigs []QueueConfig
	TopicConfigs []TopicConfig
}

// Bucket is a single S3 bucket: an immutable identity plus the mutable
// concurrency-protected state backing its objects and multipart uploads.
type Bucket struct {
	Name         string
	CreationDate time.Time

	mu           sync.RWMutex
	objects      map[string]*Object
	uploads      map[string]*multipartUpload
	notification *NotificationConfig
}

// Storage is the thread-safe registry of all buckets, keyed by name.
type Storage struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
}

// NewStorage returns an empty bucket registry.
func NewStorage() *Storage {
	return &Storage{buckets: make(map[string]*Bucket)}
}

// CreateBucket registers a new, empty bucket. ok is false if a bucket by
// that name already exists.
func (s *Storage) CreateBucket(name string) (b *Bucket, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.buckets[name]; exists {
		return nil, false
	}
	b = &Bucket{
		Name:         name,
		CreationDate: time.Now().UTC(),
		objects:      make(map[string]*Object),
		uploads:      make(map[string]*multipartUpload),
	}
	s.buckets[name] = b
	return b, true
}

// GetBucket looks up a bucket by name.
func (s *Storage) GetBucket(name string) (*Bucket, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.buckets[name]
	return b, ok
}

// DeleteBucket removes a bucket by name if it is empty. ok is false if the
// bucket does not exist; notEmpty is true if it exists but still holds
// objects or in-progress uploads.
func (s *Storage) DeleteBucket(name string) (ok bool, notEmpty bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, exists := s.buckets[name]
	if !exists {
		return false, false
	}
	b.mu.RLock()
	empty := len(b.objects) == 0 && len(b.uploads) == 0
	b.mu.RUnlock()
	if !empty {
		return true, true
	}
	delete(s.buckets, name)
	return true, false
}

// ListBuckets returns every bucket, sorted by name for deterministic output.
func (s *Storage) ListBuckets() []*Bucket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// --- per-bucket object operations ---

// Put inserts or overwrites an object.
func (b *Bucket) Put(obj *Object) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[obj.Key] = obj
}

// Get looks up an object by key.
func (b *Bucket) Get(key string) (*Object, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	o, ok := b.objects[key]
	return o, ok
}

// Delete removes an object by key. Deleting a nonexistent key is a no-op,
// matching real S3's idempotent DeleteObject semantics.
func (b *Bucket) Delete(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, key)
}

// SetNotificationConfig replaces the bucket's event notification config.
func (b *Bucket) SetNotificationConfig(cfg *NotificationConfig) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.notification = cfg
}

// NotificationConfig returns the bucket's current event notification config,
// or nil if none has been set.
func (b *Bucket) NotificationConfig() *NotificationConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.notification
}

// List returns every object, sorted by key for deterministic pagination.
func (b *Bucket) List() []*Object {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*Object, 0, len(b.objects))
	for _, o := range b.objects {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// --- per-bucket multipart upload operations ---

// CreateUpload registers a new in-progress multipart upload.
func (b *Bucket) CreateUpload(u *multipartUpload) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.uploads[u.UploadID] = u
}

// GetUpload looks up an in-progress multipart upload by ID.
func (b *Bucket) GetUpload(uploadID string) (*multipartUpload, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	u, ok := b.uploads[uploadID]
	return u, ok
}

// RemoveUpload deletes an in-progress multipart upload's bookkeeping,
// called once it is completed or aborted.
func (b *Bucket) RemoveUpload(uploadID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.uploads, uploadID)
}

// ListUploads returns every in-progress multipart upload, sorted by key
// then upload ID for deterministic output.
func (b *Bucket) ListUploads() []*multipartUpload {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]*multipartUpload, 0, len(b.uploads))
	for _, u := range b.uploads {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out
}

// PutPart inserts or overwrites a part of this upload.
func (u *multipartUpload) PutPart(p *Part) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.parts[p.PartNumber] = p
}

// ListParts returns every uploaded part, sorted by part number.
func (u *multipartUpload) ListParts() []*Part {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]*Part, 0, len(u.parts))
	for _, p := range u.parts {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	return out
}

// GetPart looks up a single part by number.
func (u *multipartUpload) GetPart(partNumber int) (*Part, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	p, ok := u.parts[partNumber]
	return p, ok
}
