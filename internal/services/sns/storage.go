// Package sns emulates AWS SNS topics and subscriptions in-memory, guarded
// by sync.RWMutex-protected maps per the project's concurrency strategy.
package sns

import "sync"

// Topic is a single SNS topic.
type Topic struct {
	Name string
	Arn  string
}

// Subscription binds a topic to a delivery endpoint. Only Protocol "sqs" is
// actually delivered to (see service.go); other protocols are tracked for
// API completeness but not delivered.
type Subscription struct {
	SubscriptionArn string
	TopicArn        string
	Protocol        string
	Endpoint        string
}

// Storage is the thread-safe registry of topics and subscriptions.
type Storage struct {
	mu            sync.RWMutex
	topics        map[string]*Topic        // key: topic ARN
	subscriptions map[string]*Subscription // key: subscription ARN
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
	t := &Topic{Name: name, Arn: arn}
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
	return out
}

func (s *Storage) AddSubscription(sub *Subscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptions[sub.SubscriptionArn] = sub
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
	return out
}
