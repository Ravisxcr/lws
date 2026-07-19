// Package sns emulates AWS SNS topics and subscriptions in-memory, guarded
// by sync.RWMutex-protected maps per the project's concurrency strategy.
package sns

import (
	"sort"
	"sync"
	"time"
)

// Subscription status values, matching the strings real SNS uses.
const (
	StatusPendingConfirmation = "PendingConfirmation"
	StatusConfirmed           = "Confirmed"
)

// Topic is a single SNS topic plus its mutable attributes, tags, and
// (unenforced, no-IAM-here) access policy labels.
type Topic struct {
	Name             string
	Arn              string
	CreatedTimestamp time.Time

	attrMu      sync.RWMutex
	Attributes  map[string]string
	Tags        map[string]string
	permissions map[string]bool
}

// GetAttributes returns a copy of the topic's mutable attributes.
func (t *Topic) GetAttributes() map[string]string {
	t.attrMu.RLock()
	defer t.attrMu.RUnlock()
	out := make(map[string]string, len(t.Attributes))
	for k, v := range t.Attributes {
		out[k] = v
	}
	return out
}

// SetAttribute sets a single named attribute (SetTopicAttributes takes one
// AttributeName/AttributeValue pair per call, matching real SNS).
func (t *Topic) SetAttribute(name, value string) {
	t.attrMu.Lock()
	defer t.attrMu.Unlock()
	if t.Attributes == nil {
		t.Attributes = make(map[string]string)
	}
	t.Attributes[name] = value
}

// GetTags returns a copy of the topic's tags.
func (t *Topic) GetTags() map[string]string {
	t.attrMu.RLock()
	defer t.attrMu.RUnlock()
	out := make(map[string]string, len(t.Tags))
	for k, v := range t.Tags {
		out[k] = v
	}
	return out
}

// SetTags merges tags into the topic's tag set.
func (t *Topic) SetTags(tags map[string]string) {
	t.attrMu.Lock()
	defer t.attrMu.Unlock()
	if t.Tags == nil {
		t.Tags = make(map[string]string, len(tags))
	}
	for k, v := range tags {
		t.Tags[k] = v
	}
}

// UntagKeys removes the given tag keys from the topic's tag set.
func (t *Topic) UntagKeys(keys []string) {
	t.attrMu.Lock()
	defer t.attrMu.Unlock()
	for _, k := range keys {
		delete(t.Tags, k)
	}
}

// AddPermission records a policy label as granted, for API round-trip only
// (the emulator has no IAM, so this is never enforced).
func (t *Topic) AddPermission(label string) {
	t.attrMu.Lock()
	defer t.attrMu.Unlock()
	if t.permissions == nil {
		t.permissions = make(map[string]bool)
	}
	t.permissions[label] = true
}

// RemovePermission removes a previously-added policy label, reporting
// whether it existed.
func (t *Topic) RemovePermission(label string) bool {
	t.attrMu.Lock()
	defer t.attrMu.Unlock()
	_, ok := t.permissions[label]
	delete(t.permissions, label)
	return ok
}

// Subscription binds a topic to a delivery endpoint. Only Protocol "sqs",
// "http", and "https" are actually delivered to (see service.go); other
// protocols are tracked for API completeness but not delivered.
//
// Arn is always assigned up front and used as the storage key, but it is
// only revealed to callers once the subscription is Confirmed — while
// Status is PendingConfirmation, PublicArn returns the literal
// "PendingConfirmation" string real SNS uses, matching AWS's behavior of
// requiring an out-of-band confirmation (a Token delivered to the endpoint)
// before the real ARN is usable.
type Subscription struct {
	Arn      string
	TopicArn string
	Protocol string
	Endpoint string
	Owner    string
	Status   string
	Token    string

	attrMu     sync.RWMutex
	attributes map[string]string
}

// PublicArn returns the ARN as it should be shown to API callers.
func (s *Subscription) PublicArn() string {
	if s.Status == StatusPendingConfirmation {
		return StatusPendingConfirmation
	}
	return s.Arn
}

// GetAttributes returns a copy of the subscription's mutable attributes.
func (s *Subscription) GetAttributes() map[string]string {
	s.attrMu.RLock()
	defer s.attrMu.RUnlock()
	out := make(map[string]string, len(s.attributes))
	for k, v := range s.attributes {
		out[k] = v
	}
	return out
}

// SetAttribute sets a single named attribute (e.g. RawMessageDelivery,
// FilterPolicy), matching real SNS's one-attribute-per-call semantics.
func (s *Subscription) SetAttribute(name, value string) {
	s.attrMu.Lock()
	defer s.attrMu.Unlock()
	if s.attributes == nil {
		s.attributes = make(map[string]string)
	}
	s.attributes[name] = value
}

// RawMessageDelivery reports whether raw message delivery is enabled for
// this subscription (default: false, matching real SNS).
func (s *Subscription) RawMessageDelivery() bool {
	s.attrMu.RLock()
	defer s.attrMu.RUnlock()
	return s.attributes["RawMessageDelivery"] == "true"
}

// Storage is the thread-safe registry of topics and subscriptions.
type Storage struct {
	mu            sync.RWMutex
	topics        map[string]*Topic        // key: topic ARN
	subscriptions map[string]*Subscription // key: subscription ARN (real, even while pending)
}

// NewStorage returns an empty topic/subscription registry.
func NewStorage() *Storage {
	return &Storage{
		topics:        make(map[string]*Topic),
		subscriptions: make(map[string]*Subscription),
	}
}

func (s *Storage) CreateTopic(name, arn string) *Topic {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.topics[arn]; ok {
		return existing
	}
	t := &Topic{Name: name, Arn: arn, CreatedTimestamp: time.Now()}
	s.topics[arn] = t
	return t
}

func (s *Storage) GetTopic(arn string) (*Topic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics[arn]
	return t, ok
}

func (s *Storage) DeleteTopic(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.topics[arn]; !ok {
		return false
	}
	delete(s.topics, arn)
	for subArn, sub := range s.subscriptions {
		if sub.TopicArn == arn {
			delete(s.subscriptions, subArn)
		}
	}
	return true
}

func (s *Storage) ListTopics() []*Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arn < out[j].Arn })
	return out
}

func (s *Storage) AddSubscription(sub *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.Arn] = sub
}

func (s *Storage) GetSubscription(arn string) (*Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[arn]
	return sub, ok
}

func (s *Storage) RemoveSubscription(arn string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[arn]; !ok {
		return false
	}
	delete(s.subscriptions, arn)
	return true
}

func (s *Storage) SubscriptionsForTopic(topicArn string) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Subscription, 0)
	for _, sub := range s.subscriptions {
		if sub.TopicArn == topicArn {
			out = append(out, sub)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arn < out[j].Arn })
	return out
}

// AllSubscriptions returns every subscription across every topic
// (ListSubscriptions, unlike ListSubscriptionsByTopic, is account-wide).
func (s *Storage) AllSubscriptions() []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		out = append(out, sub)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arn < out[j].Arn })
	return out
}

// FindPendingByToken locates a still-pending subscription on topicArn whose
// confirmation Token matches, for ConfirmSubscription.
func (s *Storage) FindPendingByToken(topicArn, token string) (*Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sub := range s.subscriptions {
		if sub.TopicArn == topicArn && sub.Status == StatusPendingConfirmation && sub.Token == token {
			return sub, true
		}
	}
	return nil, false
}
