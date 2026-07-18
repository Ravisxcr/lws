// Package sqs emulates AWS SQS standard queues in-memory. Each queue is
// backed by a buffered Go channel so the HTTP routing layer never blocks on
// queue capacity, per the project's concurrency strategy.
package sqs

import (
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

// Queue is a single SQS queue: an immutable identity (Name/URL/Arn/
// Attributes) plus the mutable concurrency-protected state backing message
// delivery.
type Queue struct {
	Name       string
	URL        string
	Arn        string
	Attributes map[string]string

	ready chan *Message

	inFlightMu sync.Mutex
	inFlight   map[string]*inFlightRecord
}

// VisibilityTimeout returns the queue's configured visibility timeout, or
// the AWS default of 30s if unset/invalid.
func (q *Queue) VisibilityTimeout() time.Duration {
	if v, ok := q.Attributes["VisibilityTimeout"]; ok {
		if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return defaultVisibilityTimeout
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

// Storage is the thread-safe registry of all queues, keyed by queue name.
type Storage struct {
	mu     sync.RWMutex
	queues map[string]*Queue
}

// NewStorage returns an empty queue registry.
func NewStorage() *Storage {
	return &Storage{queues: make(map[string]*Queue)}
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
	q = &Queue{
		Name:       name,
		URL:        url,
		Arn:        arn,
		Attributes: attrs,
		ready:      make(chan *Message, queueCapacity),
		inFlight:   make(map[string]*inFlightRecord),
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
