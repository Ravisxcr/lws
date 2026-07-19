// Package sqs emulates AWS SQS standard queues in-memory. Each queue is
// backed by a buffered Go channel so the HTTP routing layer never blocks on
// queue capacity, per the project's concurrency strategy.
package sqs

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"
)

// queueCapacity bounds how many ready (undelivered) messages a queue can
// hold before SendMessage starts rejecting new messages.
const queueCapacity = 10000

// defaultVisibilityTimeout matches real SQS's default when a queue is
// created without an explicit VisibilityTimeout attribute.
const defaultVisibilityTimeout = 30 * time.Second

// MessageAttributeValue mirrors an SQS message attribute.
type MessageAttributeValue struct {
	DataType    string
	StringValue string
	BinaryValue []byte
}

// Message is a single SQS message body plus metadata.
type Message struct {
	MessageID         string
	Body              string
	MD5OfBody         string
	MessageAttributes map[string]MessageAttributeValue
	ReceiveCount      int
}

// inFlightRecord tracks a message that has been received but not yet
// deleted, along with the timer that will return it to the queue if the
// visibility timeout elapses before DeleteMessage is called.
type inFlightRecord struct {
	msg   *Message
	timer *time.Timer
}

// redrivePolicy mirrors the JSON shape SQS stores in a queue's
// RedrivePolicy attribute.
type redrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// Queue is a single SQS queue: an immutable identity (Name/URL/Arn) plus the
// mutable concurrency-protected state backing message delivery, attributes,
// and tags.
type Queue struct {
	Name string
	URL  string
	Arn  string

	CreatedTimestamp      time.Time
	LastModifiedTimestamp time.Time

	attrMu     sync.RWMutex
	Attributes map[string]string
	Tags       map[string]string
	// permissions maps a policy Label to the account IDs it grants access
	// to. The emulator has no IAM, so these are stored purely for API
	// compatibility (AddPermission/RemovePermission/round-trip) and never
	// enforced.
	permissions map[string]bool

	ready chan *Message

	inFlightMu sync.Mutex
	inFlight   map[string]*inFlightRecord
}

// VisibilityTimeout returns the queue's configured visibility timeout, or
// the AWS default of 30s if unset/invalid.
func (q *Queue) VisibilityTimeout() time.Duration {
	q.attrMu.RLock()
	defer q.attrMu.RUnlock()
	if v, ok := q.Attributes["VisibilityTimeout"]; ok {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultVisibilityTimeout
}

// redrivePolicy returns the queue's parsed RedrivePolicy attribute, if any
// is set and well-formed.
func (q *Queue) redrivePolicy() (redrivePolicy, bool) {
	q.attrMu.RLock()
	raw, ok := q.Attributes["RedrivePolicy"]
	q.attrMu.RUnlock()
	if !ok || raw == "" {
		return redrivePolicy{}, false
	}
	var rp redrivePolicy
	if err := json.Unmarshal([]byte(raw), &rp); err != nil || rp.DeadLetterTargetArn == "" {
		return redrivePolicy{}, false
	}
	return rp, true
}

// GetAttributes returns a copy of the queue's mutable attributes, safe for
// the caller to read without further locking.
func (q *Queue) GetAttributes() map[string]string {
	q.attrMu.RLock()
	defer q.attrMu.RUnlock()
	out := make(map[string]string, len(q.Attributes))
	for k, v := range q.Attributes {
		out[k] = v
	}
	return out
}

// SetAttributes merges attrs into the queue's attributes and bumps
// LastModifiedTimestamp.
func (q *Queue) SetAttributes(attrs map[string]string) {
	q.attrMu.Lock()
	defer q.attrMu.Unlock()
	if q.Attributes == nil {
		q.Attributes = make(map[string]string, len(attrs))
	}
	for k, v := range attrs {
		q.Attributes[k] = v
	}
	q.LastModifiedTimestamp = time.Now()
}

// GetTags returns a copy of the queue's tags.
func (q *Queue) GetTags() map[string]string {
	q.attrMu.RLock()
	defer q.attrMu.RUnlock()
	out := make(map[string]string, len(q.Tags))
	for k, v := range q.Tags {
		out[k] = v
	}
	return out
}

// SetTags merges tags into the queue's tag set.
func (q *Queue) SetTags(tags map[string]string) {
	q.attrMu.Lock()
	defer q.attrMu.Unlock()
	if q.Tags == nil {
		q.Tags = make(map[string]string, len(tags))
	}
	for k, v := range tags {
		q.Tags[k] = v
	}
}

// UntagKeys removes the given tag keys from the queue's tag set.
func (q *Queue) UntagKeys(keys []string) {
	q.attrMu.Lock()
	defer q.attrMu.Unlock()
	for _, k := range keys {
		delete(q.Tags, k)
	}
}

// AddPermission records a policy label as granted, for API round-trip only.
func (q *Queue) AddPermission(label string) {
	q.attrMu.Lock()
	defer q.attrMu.Unlock()
	if q.permissions == nil {
		q.permissions = make(map[string]bool)
	}
	q.permissions[label] = true
}

// RemovePermission removes a previously-added policy label, reporting
// whether it existed.
func (q *Queue) RemovePermission(label string) bool {
	q.attrMu.Lock()
	defer q.attrMu.Unlock()
	_, ok := q.permissions[label]
	delete(q.permissions, label)
	return ok
}

// ReadyCount returns how many messages are currently ready for delivery.
func (q *Queue) ReadyCount() int {
	return len(q.ready)
}

// InFlightCount returns how many messages are currently received but not
// yet deleted or expired.
func (q *Queue) InFlightCount() int {
	q.inFlightMu.Lock()
	defer q.inFlightMu.Unlock()
	return len(q.inFlight)
}

// stopAllTimers cancels every pending redelivery timer. Called when the
// queue is deleted so its timers don't leak.
func (q *Queue) stopAllTimers() {
	q.inFlightMu.Lock()
	defer q.inFlightMu.Unlock()
	for handle, rec := range q.inFlight {
		rec.timer.Stop()
		delete(q.inFlight, handle)
	}
}

// MoveTask tracks a StartMessageMoveTask request. The emulator performs the
// move synchronously (there's no background worker pool to speak of), so a
// task is always COMPLETED by the time StartMessageMoveTask returns.
type MoveTask struct {
	TaskHandle                       string
	SourceArn                        string
	DestinationArn                   string
	Status                           string // RUNNING, COMPLETED, CANCELLED, FAILED
	ApproximateNumberOfMessagesMoved int64
	StartedTimestamp                 int64
	FailureReason                    string
}

// Storage is the thread-safe registry of all queues, keyed by queue name.
type Storage struct {
	mu     sync.RWMutex
	queues map[string]*Queue

	tasksMu sync.Mutex
	tasks   map[string]*MoveTask
}

// NewStorage returns an empty queue registry.
func NewStorage() *Storage {
	return &Storage{
		queues: make(map[string]*Queue),
		tasks:  make(map[string]*MoveTask),
	}
}

// Create registers a new queue, or returns the existing one if a queue by
// that name already exists (CreateQueue is idempotent, matching real SQS
// behavior for repeated calls with the same name).
func (s *Storage) Create(name, url, arn string, attrs map[string]string) (q *Queue, created bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.queues[name]; ok {
		return existing, false
	}
	now := time.Now()
	q = &Queue{
		Name:                  name,
		URL:                   url,
		Arn:                   arn,
		CreatedTimestamp:      now,
		LastModifiedTimestamp: now,
		Attributes:            attrs,
		ready:                 make(chan *Message, queueCapacity),
		inFlight:              make(map[string]*inFlightRecord),
	}
	s.queues[name] = q
	return q, true
}

// Get looks up a queue by name.
func (s *Storage) Get(name string) (*Queue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	q, ok := s.queues[name]
	return q, ok
}

// GetByArn looks up a queue by its ARN.
func (s *Storage) GetByArn(arn string) (*Queue, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, q := range s.queues {
		if q.Arn == arn {
			return q, true
		}
	}
	return nil, false
}

// Delete removes a queue, stopping any pending redelivery timers.
func (s *Storage) Delete(name string) bool {
	s.mu.Lock()
	q, ok := s.queues[name]
	if ok {
		delete(s.queues, name)
	}
	s.mu.Unlock()
	if ok {
		q.stopAllTimers()
	}
	return ok
}

// List returns every queue, sorted by name for deterministic output.
func (s *Storage) List() []*Queue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Queue, 0, len(s.queues))
	for _, q := range s.queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// DeadLetterSources returns every queue whose RedrivePolicy targets dlqArn.
func (s *Storage) DeadLetterSources(dlqArn string) []*Queue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Queue
	for _, q := range s.queues {
		if rp, ok := q.redrivePolicy(); ok && rp.DeadLetterTargetArn == dlqArn {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SaveTask registers a move task.
func (s *Storage) SaveTask(t *MoveTask) {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	s.tasks[t.TaskHandle] = t
}

// GetTask looks up a move task by handle.
func (s *Storage) GetTask(handle string) (*MoveTask, bool) {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	t, ok := s.tasks[handle]
	return t, ok
}

// TasksForSource returns every move task started for the given source ARN,
// most recently started first.
func (s *Storage) TasksForSource(sourceArn string) []*MoveTask {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	var out []*MoveTask
	for _, t := range s.tasks {
		if t.SourceArn == sourceArn {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedTimestamp > out[j].StartedTimestamp })
	return out
}
