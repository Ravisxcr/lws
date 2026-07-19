package sns

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
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
	ErrPermissionNotFound   = errors.New("permission label does not exist")
	ErrInvalidToken         = errors.New("invalid token for this subscription")
)

// QueuePublisher decouples sns from importing sqs directly; it is satisfied
// structurally by *sqs.Service.
type QueuePublisher interface {
	Enqueue(queueName, body string) error
}

// BatchResultError is a single failed entry within a PublishBatch call.
type BatchResultError struct {
	Id          string
	Code        string
	Message     string
	SenderFault bool
}

// Service implements SNS's core domain logic: topics, subscriptions, and
// Publish (including SNS-to-SQS fan-out delivery and best-effort HTTP/HTTPS
// delivery).
type Service struct {
	store      *Storage
	queues     QueuePublisher
	httpClient *http.Client
}

// NewService returns an SNS service that delivers Protocol="sqs"
// subscriptions through queues, and Protocol="http"/"https" subscriptions
// via real outbound HTTP POSTs.
func NewService(queues QueuePublisher) *Service {
	return &Service{
		store:      NewStorage(),
		queues:     queues,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}
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

// GetTopicAttributes returns the topic's attributes plus a handful of
// computed, always-present fields (TopicArn, Owner, SubscriptionsConfirmed,
// SubscriptionsPending, SubscriptionsDeleted), matching real SNS's shape.
func (s *Service) GetTopicAttributes(arn string) (map[string]string, error) {
	t, ok := s.store.GetTopic(arn)
	if !ok {
		return nil, ErrTopicNotFound
	}
	out := t.GetAttributes()
	out["TopicArn"] = arn
	out["Owner"] = accountID
	if _, ok := out["DisplayName"]; !ok {
		out["DisplayName"] = ""
	}
	if _, ok := out["Policy"]; !ok {
		out["Policy"] = "{}"
	}
	if _, ok := out["EffectiveDeliveryPolicy"]; !ok {
		out["EffectiveDeliveryPolicy"] = "{}"
	}
	var confirmed, pending int
	for _, sub := range s.store.SubscriptionsForTopic(arn) {
		if sub.Status == StatusConfirmed {
			confirmed++
		} else {
			pending++
		}
	}
	out["SubscriptionsConfirmed"] = strconv.Itoa(confirmed)
	out["SubscriptionsPending"] = strconv.Itoa(pending)
	out["SubscriptionsDeleted"] = "0"
	return out, nil
}

// SetTopicAttributes sets a single named attribute on the topic (e.g.
// DisplayName, Policy), matching real SNS's one-attribute-per-call API.
func (s *Service) SetTopicAttributes(arn, name, value string) error {
	t, ok := s.store.GetTopic(arn)
	if !ok {
		return ErrTopicNotFound
	}
	t.SetAttribute(name, value)
	return nil
}

// TagResource merges tags into a topic's tag set. Real SNS only supports
// tagging standard topics, which is all this emulator has.
func (s *Service) TagResource(resourceArn string, tags map[string]string) error {
	t, ok := s.store.GetTopic(resourceArn)
	if !ok {
		return ErrTopicNotFound
	}
	t.SetTags(tags)
	return nil
}

// UntagResource removes the given tag keys from a topic.
func (s *Service) UntagResource(resourceArn string, keys []string) error {
	t, ok := s.store.GetTopic(resourceArn)
	if !ok {
		return ErrTopicNotFound
	}
	t.UntagKeys(keys)
	return nil
}

// ListTagsForResource returns every tag on a topic.
func (s *Service) ListTagsForResource(resourceArn string) (map[string]string, error) {
	t, ok := s.store.GetTopic(resourceArn)
	if !ok {
		return nil, ErrTopicNotFound
	}
	return t.GetTags(), nil
}

// AddPermission records a policy label granting topic access. The emulator
// has no IAM, so this exists purely for API round-trip compatibility and is
// never enforced.
func (s *Service) AddPermission(topicArn, label string) error {
	t, ok := s.store.GetTopic(topicArn)
	if !ok {
		return ErrTopicNotFound
	}
	t.AddPermission(label)
	return nil
}

// RemovePermission removes a previously-added policy label.
func (s *Service) RemovePermission(topicArn, label string) error {
	t, ok := s.store.GetTopic(topicArn)
	if !ok {
		return ErrTopicNotFound
	}
	if !t.RemovePermission(label) {
		return ErrPermissionNotFound
	}
	return nil
}

// requiresConfirmation reports whether protocol needs an out-of-band
// ConfirmSubscription handshake before delivery starts. Real SNS
// auto-confirms AWS-internal protocols (sqs, lambda, firehose, application)
// since it can verify ownership itself; anything reachable only by sending
// a message to it (http/https/email/email-json/sms) requires the endpoint
// owner to confirm.
func requiresConfirmation(protocol string) bool {
	switch protocol {
	case "sqs", "lambda", "firehose", "application":
		return false
	default:
		return true
	}
}

// Subscribe creates a subscription. Protocols that require confirmation are
// created in PendingConfirmation status; for http/https endpoints, a real
// SubscriptionConfirmation POST (carrying the confirmation Token) is sent
// synchronously, matching real SNS's control-message delivery.
func (s *Service) Subscribe(topicArn, protocol, endpoint string) (*Subscription, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return nil, ErrTopicNotFound
	}
	sub := &Subscription{
		Arn:      fmt.Sprintf("%s:%s", topicArn, uuid.NewString()),
		TopicArn: topicArn,
		Protocol: protocol,
		Endpoint: endpoint,
		Owner:    accountID,
		Status:   StatusConfirmed,
	}
	if requiresConfirmation(protocol) {
		sub.Status = StatusPendingConfirmation
		sub.Token = uuid.NewString()
	}
	s.store.AddSubscription(sub)

	if sub.Status == StatusPendingConfirmation && isHTTPProtocol(protocol) {
		s.sendSubscriptionConfirmation(sub)
	}
	return sub, nil
}

// ConfirmSubscription completes the handshake started by Subscribe for a
// protocol that required confirmation, moving the subscription to
// Confirmed and returning it (now addressable by its real ARN).
func (s *Service) ConfirmSubscription(topicArn, token string) (*Subscription, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return nil, ErrTopicNotFound
	}
	sub, ok := s.store.FindPendingByToken(topicArn, token)
	if !ok {
		return nil, ErrInvalidToken
	}
	sub.Status = StatusConfirmed
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

// ListSubscriptions returns every subscription across every topic
// (account-wide, unlike ListSubscriptionsByTopic).
func (s *Service) ListSubscriptions() []*Subscription {
	return s.store.AllSubscriptions()
}

// GetSubscriptionAttributes returns the subscription's attributes plus a
// handful of computed, always-present fields.
func (s *Service) GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error) {
	sub, ok := s.store.GetSubscription(subscriptionArn)
	if !ok {
		return nil, ErrSubscriptionNotFound
	}
	out := sub.GetAttributes()
	out["SubscriptionArn"] = sub.PublicArn()
	out["TopicArn"] = sub.TopicArn
	out["Owner"] = sub.Owner
	out["Protocol"] = sub.Protocol
	out["Endpoint"] = sub.Endpoint
	if _, ok := out["RawMessageDelivery"]; !ok {
		out["RawMessageDelivery"] = "false"
	}
	out["PendingConfirmation"] = strconv.FormatBool(sub.Status == StatusPendingConfirmation)
	out["ConfirmationWasAuthenticated"] = strconv.FormatBool(sub.Status == StatusConfirmed)
	return out, nil
}

// SetSubscriptionAttributes sets a single named attribute on the
// subscription (e.g. RawMessageDelivery, FilterPolicy).
func (s *Service) SetSubscriptionAttributes(subscriptionArn, name, value string) error {
	sub, ok := s.store.GetSubscription(subscriptionArn)
	if !ok {
		return ErrSubscriptionNotFound
	}
	sub.SetAttribute(name, value)
	return nil
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

// subscriptionConfirmationEnvelope is the control message SNS sends to
// http/https endpoints when Subscribe requires confirmation.
type subscriptionConfirmationEnvelope struct {
	Type             string `json:"Type"`
	MessageId        string `json:"MessageId"`
	Token            string `json:"Token"`
	TopicArn         string `json:"TopicArn"`
	Message          string `json:"Message"`
	SubscribeURL     string `json:"SubscribeURL"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertURL   string `json:"SigningCertURL"`
}

// Publish delivers message to every confirmed subscriber of topicArn and
// returns the minted MessageId.
func (s *Service) Publish(topicArn, message, subject string) (string, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return "", ErrTopicNotFound
	}
	return s.publishOne(topicArn, message, subject), nil
}

// PublishBatchEntry is a single entry of a PublishBatch request.
type PublishBatchEntry struct {
	Id      string
	Message string
	Subject string
}

// PublishBatchResultEntry is a single successfully-published batch entry.
type PublishBatchResultEntry struct {
	Id        string
	MessageID string
}

// PublishBatch publishes up to 10 messages to a topic in one call, matching
// real SNS's partial-failure semantics: one bad entry doesn't fail the
// whole batch.
func (s *Service) PublishBatch(topicArn string, entries []PublishBatchEntry) ([]PublishBatchResultEntry, []BatchResultError, error) {
	if _, ok := s.store.GetTopic(topicArn); !ok {
		return nil, nil, ErrTopicNotFound
	}
	var oks []PublishBatchResultEntry
	var fails []BatchResultError
	for _, e := range entries {
		if e.Message == "" {
			fails = append(fails, BatchResultError{Id: e.Id, Code: "InvalidParameter", Message: "Message is required", SenderFault: true})
			continue
		}
		oks = append(oks, PublishBatchResultEntry{Id: e.Id, MessageID: s.publishOne(topicArn, e.Message, e.Subject)})
	}
	return oks, fails, nil
}

// publishOne mints a MessageId and fans a single message out to every
// confirmed subscriber of topicArn. A delivery failure to one subscriber
// does not fail the whole publish, matching real SNS's
// fire-and-forget-per-subscriber semantics.
func (s *Service) publishOne(topicArn, message, subject string) string {
	messageID := uuid.NewString()
	for _, sub := range s.store.SubscriptionsForTopic(topicArn) {
		s.deliverToSubscription(sub, topicArn, messageID, message, subject)
	}
	return messageID
}

func (s *Service) deliverToSubscription(sub *Subscription, topicArn, messageID, message, subject string) {
	if sub.Status != StatusConfirmed {
		return
	}
	switch sub.Protocol {
	case "sqs":
		queueName := queueNameFromArn(sub.Endpoint)
		if queueName == "" {
			log.Printf("sns: could not resolve queue name from endpoint %q (subscription %s)", sub.Endpoint, sub.Arn)
			return
		}
		body := message
		if !sub.RawMessageDelivery() {
			body = s.notificationBody(messageID, topicArn, subject, message, sub.Arn)
		}
		if err := s.queues.Enqueue(queueName, body); err != nil {
			log.Printf("sns: failed to deliver to queue %q (subscription %s): %v", queueName, sub.Arn, err)
		}
	case "http", "https":
		body := message
		if !sub.RawMessageDelivery() {
			body = s.notificationBody(messageID, topicArn, subject, message, sub.Arn)
		}
		s.postToEndpoint(sub.Endpoint, body, "Notification")
	default:
		log.Printf("sns: delivery to protocol %q not implemented (subscription %s)", sub.Protocol, sub.Arn)
	}
}

func (s *Service) notificationBody(messageID, topicArn, subject, message, subscriptionArn string) string {
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
		UnsubscribeURL:   fmt.Sprintf("https://localhost/?Action=Unsubscribe&SubscriptionArn=%s", subscriptionArn),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("sns: failed to marshal notification envelope for subscription %s: %v", subscriptionArn, err)
		return ""
	}
	return string(body)
}

// sendSubscriptionConfirmation POSTs the SubscriptionConfirmation control
// message to a pending http/https subscriber, synchronously and
// best-effort: a delivery failure here does not fail Subscribe, matching
// real SNS (which just retries delivery in the background).
func (s *Service) sendSubscriptionConfirmation(sub *Subscription) {
	envelope := subscriptionConfirmationEnvelope{
		Type:      "SubscriptionConfirmation",
		MessageId: uuid.NewString(),
		Token:     sub.Token,
		TopicArn:  sub.TopicArn,
		Message:   fmt.Sprintf("You have chosen to subscribe to the topic %s.\nTo confirm the subscription, call ConfirmSubscription with TopicArn=%s and Token=%s.", sub.TopicArn, sub.TopicArn, sub.Token),
		SubscribeURL: fmt.Sprintf("https://localhost/?Action=ConfirmSubscription&TopicArn=%s&Token=%s",
			sub.TopicArn, sub.Token),
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		SignatureVersion: "1",
		Signature:        "EXAMPLElocalstacksignaturenotreal==",
		SigningCertURL:   "https://localhost/SimpleNotificationService-example.pem",
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		log.Printf("sns: failed to marshal subscription confirmation for subscription %s: %v", sub.Arn, err)
		return
	}
	s.postToEndpoint(sub.Endpoint, string(body), "SubscriptionConfirmation")
}

// postToEndpoint delivers body to an http/https subscriber, logging (rather
// than failing the caller on) any error — matching real SNS's
// fire-and-forget delivery model.
func (s *Service) postToEndpoint(endpoint, body, messageType string) {
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader([]byte(body)))
	if err != nil {
		log.Printf("sns: failed to build request for endpoint %q: %v", endpoint, err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=UTF-8")
	req.Header.Set("x-amz-sns-message-type", messageType)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("sns: failed to deliver %s to endpoint %q: %v", messageType, endpoint, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("sns: endpoint %q rejected %s delivery with status %d", endpoint, messageType, resp.StatusCode)
	}
}

func isHTTPProtocol(protocol string) bool {
	return protocol == "http" || protocol == "https"
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
