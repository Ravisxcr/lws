package sns

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Dummy account/region used to build AWS-shaped ARNs for a local emulator
// with no real AWS account behind it.
const (
	accountID = "000000000000"
	region    = "us-east-1"
)

var (
	ErrTopicNotFound        = errors.New("topic does not exist")
	ErrSubscriptionNotFound = errors.New("subscription does not exist")
)

// QueuePublisher decouples sns from importing sqs directly; it is satisfied
// structurally by *sqs.Service.
type QueuePublisher interface {
	Enqueue(queueName, body string) error
}

// Service implements SNS's core domain logic: topics, subscriptions, and
// Publish (including SNS-to-SQS fan-out delivery).
type Service struct {
	store  *Storage
	queues QueuePublisher
}

// NewService returns an SNS service that delivers Protocol="sqs"
// subscriptions through queues.
func NewService(queues QueuePublisher) *Service {
	return &Service{store: NewStorage(), queues: queues}
}

func (s *Service) CreateTopic(name string) (*Topic, error) {
	if name == "" {
		return nil, errors.New("Name is required")
	}
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, accountID, name)
	return s.store.CreateTopic(name, arn), nil
}

func (s *Service) DeleteTopic(arn string) error {
	if !s.store.DeleteTopic(arn) {
		return ErrTopicNotFound
	}
	return nil
}

func (s *Service) ListTopics() []*Topic {
	return s.store.ListTopics()
}

func (s *Service) Subscribe(topicArn, protocol, endpoint string) (*Subscription, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return nil, ErrTopicNotFound
	}
	sub := &Subscription{
		SubscriptionArn: fmt.Sprintf("%s:%s", topicArn, uuid.NewString()),
		TopicArn:        topicArn,
		Protocol:        protocol,
		Endpoint:        endpoint,
	}
	s.store.AddSubscription(sub)
	return sub, nil
}

func (s *Service) Unsubscribe(subscriptionArn string) error {
	if !s.store.RemoveSubscription(subscriptionArn) {
		return ErrSubscriptionNotFound
	}
	return nil
}

func (s *Service) ListSubscriptionsByTopic(topicArn string) ([]*Subscription, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return nil, ErrTopicNotFound
	}
	return s.store.SubscriptionsForTopic(topicArn), nil
}

// notificationEnvelope is the standard AWS shape SNS wraps every delivered
// message in. Signature/SigningCertURL are shape-compatible placeholders —
// there is no real AWS account or key material behind this local emulator,
// so no genuine signing is performed.
type notificationEnvelope struct {
	Type             string `json:"Type"`
	MessageId        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Subject          string `json:"Subject,omitempty"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
	UnsubscribeURL   string `json:"UnsubscribeURL"`
}

// Publish delivers message to every subscriber of topicArn. Only
// Protocol="sqs" subscriptions are actually delivered (in-process,
// bypassing HTTP, via QueuePublisher.Enqueue); other protocols are logged
// and skipped. A delivery failure to one subscriber does not fail the
// whole Publish call, matching real SNS's fire-and-forget-per-subscriber
// semantics.
func (s *Service) Publish(topicArn, message, subject string) (string, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return "", ErrTopicNotFound
	}
	messageID := uuid.NewString()

	for _, sub := range s.store.SubscriptionsForTopic(topicArn) {
		if sub.Protocol != "sqs" {
			log.Printf("sns: delivery to protocol %q not implemented (subscription %s)", sub.Protocol, sub.SubscriptionArn)
			continue
		}
		queueName := queueNameFromArn(sub.Endpoint)
		if queueName == "" {
			log.Printf("sns: could not resolve queue name from endpoint %q (subscription %s)", sub.Endpoint, sub.SubscriptionArn)
			continue
		}
		envelope := notificationEnvelope{
			Type:             "Notification",
			MessageId:        messageID,
			TopicArn:         topicArn,
			Subject:          subject,
			Message:          message,
			Timestamp:        time.Now().UTC().Format(time.RFC3339),
			SignatureVersion: "1",
			Signature:        "EXAMPLElocalstacksignaturenotreal==",
			SigningCertURL:   "https://localhost/SimpleNotificationService-example.pem",
			UnsubscribeURL:   fmt.Sprintf("https://localhost/?Action=Unsubscribe&SubscriptionArn=%s", sub.SubscriptionArn),
		}
		body, err := json.Marshal(envelope)
		if err != nil {
			log.Printf("sns: failed to marshal notification envelope for subscription %s: %v", sub.SubscriptionArn, err)
			continue
		}
		if err := s.queues.Enqueue(queueName, string(body)); err != nil {
			log.Printf("sns: failed to deliver to queue %q (subscription %s): %v", queueName, sub.SubscriptionArn, err)
		}
	}

	return messageID, nil
}

// queueNameFromArn extracts the queue name from an SQS ARN
// ("arn:aws:sqs:region:account:name" -> "name").
func queueNameFromArn(arn string) string {
	idx := strings.LastIndex(arn, ":")
	if idx == -1 {
		return arn
	}
	return arn[idx+1:]
}
