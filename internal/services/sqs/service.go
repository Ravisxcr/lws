package sqs

import (
	"crypto/md5"
	"encoding/hex"
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
	ErrQueueNotFound   = errors.New("queue does not exist")
	ErrMessageNotFound = errors.New("receipt handle not found or already deleted")
	ErrQueueFull       = errors.New("queue is at capacity")
)

// ReceivedMessage pairs a Message with the receipt handle minted for this
// particular delivery.
type ReceivedMessage struct {
	*Message
	ReceiptHandle string
}

// Service implements SQS's core domain logic over the thread-safe Storage.
type Service struct {
	store *Storage
}

// NewService returns an SQS service with an empty queue registry.
func NewService() *Service {
	return &Service{store: NewStorage()}
}

// CreateQueue creates (or idempotently returns) a queue. host is the
// request's Host header, used to build a QueueUrl that matches wherever
// the emulator is actually listening, regardless of the PORT it was
// started with.
func (s *Service) CreateQueue(name string, attrs map[string]string, host string) (*Queue, error) {
	if name == "" {
		return nil, errors.New("QueueName is required")
	}
	arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", region, accountID, name)
	url := fmt.Sprintf("http://%s/%s/%s", host, accountID, name)
	q, _ := s.store.Create(name, url, arn, attrs)
	return q, nil
}

// GetQueueURL resolves a queue name to its URL.
func (s *Service) GetQueueURL(name string) (string, error) {
	q, ok := s.store.Get(name)
	if !ok {
		return "", ErrQueueNotFound
	}
	return q.URL, nil
}

// DeleteQueue removes a queue and stops its in-flight redelivery timers.
func (s *Service) DeleteQueue(name string) error {
	if !s.store.Delete(name) {
		return ErrQueueNotFound
	}
	return nil
}

// ListQueues returns every queue whose name has the given prefix (empty
// prefix matches all queues).
func (s *Service) ListQueues(prefix string) []*Queue {
	all := s.store.List()
	if prefix == "" {
		return all
	}
	out := make([]*Queue, 0, len(all))
	for _, q := range all {
		if strings.HasPrefix(q.Name, prefix) {
			out = append(out, q)
		}
	}
	return out
}

// SendMessage enqueues a new message onto the named queue's channel.
func (s *Service) SendMessage(queueName, body string, attrs map[string]MessageAttributeValue) (*Message, error) {
	q, ok := s.store.Get(queueName)
	if !ok {
		return nil, ErrQueueNotFound
	}
	sum := md5.Sum([]byte(body))
	msg := &Message{
		MessageID:         uuid.NewString(),
		Body:              body,
		MD5OfBody:         hex.EncodeToString(sum[:]),
		MessageAttributes: attrs,
	}
	select {
	case q.ready <- msg:
	default:
		return nil, ErrQueueFull
	}
	return msg, nil
}

// ReceiveMessage waits up to waitTime for at least one message to arrive,
// then greedily drains up to maxMessages already-ready messages without
// waiting further — matching real SQS long-poll semantics. Each returned
// message is marked in-flight and will be redelivered automatically if not
// deleted before the queue's visibility timeout elapses.
func (s *Service) ReceiveMessage(queueName string, maxMessages int, waitTime time.Duration) ([]*ReceivedMessage, error) {
	q, ok := s.store.Get(queueName)
	if !ok {
		return nil, ErrQueueNotFound
	}
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10 // real SQS caps ReceiveMessage at 10 per call
	}
	if waitTime < 0 {
		waitTime = 0
	}

	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	out := make([]*ReceivedMessage, 0, maxMessages)
	for len(out) < maxMessages {
		select {
		case msg := <-q.ready:
			out = append(out, s.markInFlight(q, msg))
		case <-timer.C:
			return out, nil
		}
	}
	return out, nil
}

// markInFlight mints a receipt handle for msg and schedules automatic
// redelivery if DeleteMessage isn't called before the visibility timeout.
func (s *Service) markInFlight(q *Queue, msg *Message) *ReceivedMessage {
	msg.ReceiveCount++
	handle := uuid.NewString()
	vt := q.VisibilityTimeout()

	timer := time.AfterFunc(vt, func() {
		q.inFlightMu.Lock()
		_, stillPending := q.inFlight[handle]
		if stillPending {
			delete(q.inFlight, handle)
		}
		q.inFlightMu.Unlock()

		if stillPending {
			select {
			case q.ready <- msg:
			default:
				log.Printf("sqs: failed to requeue message %s on queue %s after visibility timeout: queue full", msg.MessageID, q.Name)
			}
		}
	})

	q.inFlightMu.Lock()
	q.inFlight[handle] = &inFlightRecord{msg: msg, timer: timer}
	q.inFlightMu.Unlock()

	return &ReceivedMessage{Message: msg, ReceiptHandle: handle}
}

// DeleteMessage permanently acknowledges a message, cancelling its
// redelivery timer.
func (s *Service) DeleteMessage(queueName, receiptHandle string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.inFlightMu.Lock()
	rec, ok := q.inFlight[receiptHandle]
	if ok {
		delete(q.inFlight, receiptHandle)
	}
	q.inFlightMu.Unlock()
	if !ok {
		return ErrMessageNotFound
	}
	rec.timer.Stop()
	return nil
}

// Enqueue implements sns.QueuePublisher: it pushes a raw pre-built message
// body directly onto an existing queue, bypassing the HTTP layer entirely,
// for in-process SNS-to-SQS fan-out.
func (s *Service) Enqueue(queueName, body string) error {
	_, err := s.SendMessage(queueName, body, nil)
	return err
}
