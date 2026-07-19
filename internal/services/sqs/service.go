package sqs

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
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
	ErrQueueNotFound      = errors.New("queue does not exist")
	ErrMessageNotFound    = errors.New("receipt handle not found or already deleted")
	ErrQueueFull          = errors.New("queue is at capacity")
	ErrPermissionNotFound = errors.New("permission label does not exist")
	ErrTaskNotFound       = errors.New("message move task does not exist")
)

// ReceivedMessage pairs a Message with the receipt handle minted for this
// particular delivery.
type ReceivedMessage struct {
	*Message
	ReceiptHandle string
}

// BatchResultError is a single failed entry within a batch operation
// (SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch).
type BatchResultError struct {
	Id          string
	Code        string
	Message     string
	SenderFault bool
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

// SendMessageBatchEntry is a single entry of a SendMessageBatch request.
type SendMessageBatchEntry struct {
	Id                string
	MessageBody       string
	MessageAttributes map[string]MessageAttributeValue
}

// SendMessageBatchResultEntry is a single successfully-sent batch entry.
type SendMessageBatchResultEntry struct {
	Id        string
	MessageID string
	MD5OfBody string
}

// SendMessageBatch sends up to 10 messages in one call, matching real SQS's
// partial-failure semantics: one bad entry doesn't fail the whole batch.
func (s *Service) SendMessageBatch(queueName string, entries []SendMessageBatchEntry) ([]SendMessageBatchResultEntry, []BatchResultError, error) {
	if _, ok := s.store.Get(queueName); !ok {
		return nil, nil, ErrQueueNotFound
	}
	var oks []SendMessageBatchResultEntry
	var fails []BatchResultError
	for _, e := range entries {
		msg, err := s.SendMessage(queueName, e.MessageBody, e.MessageAttributes)
		if err != nil {
			fails = append(fails, BatchResultError{Id: e.Id, Code: "OverLimit", Message: err.Error(), SenderFault: true})
			continue
		}
		oks = append(oks, SendMessageBatchResultEntry{Id: e.Id, MessageID: msg.MessageID, MD5OfBody: msg.MD5OfBody})
	}
	return oks, fails, nil
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

	out := make([]*ReceivedMessage, 0, maxMessages)

	// Wait for the first message. A zero waitTime is a short poll: check
	// once without blocking, rather than racing a zero-duration timer
	// against the channel (which, with a message already buffered, would
	// nondeterministically pick the timer and drop the message).
	if waitTime == 0 {
		select {
		case msg := <-q.ready:
			out = append(out, s.markInFlight(q, msg))
		default:
			return out, nil
		}
	} else {
		timer := time.NewTimer(waitTime)
		defer timer.Stop()
		select {
		case msg := <-q.ready:
			out = append(out, s.markInFlight(q, msg))
		case <-timer.C:
			return out, nil
		}
	}

	// Greedily drain any additional already-ready messages without waiting further.
	for len(out) < maxMessages {
		select {
		case msg := <-q.ready:
			out = append(out, s.markInFlight(q, msg))
		default:
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
	timer := s.scheduleRedelivery(q, msg, handle, q.VisibilityTimeout())

	q.inFlightMu.Lock()
	q.inFlight[handle] = &inFlightRecord{msg: msg, timer: timer}
	q.inFlightMu.Unlock()

	return &ReceivedMessage{Message: msg, ReceiptHandle: handle}
}

// scheduleRedelivery arms a timer that, if the message at handle is still
// in flight when it fires, either moves the message to the queue's
// dead-letter queue (once ReceiveCount has passed the RedrivePolicy's
// MaxReceiveCount) or returns it to the same queue for redelivery.
func (s *Service) scheduleRedelivery(q *Queue, msg *Message, handle string, vt time.Duration) *time.Timer {
	return time.AfterFunc(vt, func() {
		q.inFlightMu.Lock()
		_, stillPending := q.inFlight[handle]
		if stillPending {
			delete(q.inFlight, handle)
		}
		q.inFlightMu.Unlock()

		if !stillPending {
			return
		}

		target := q
		if rp, ok := q.redrivePolicy(); ok && rp.MaxReceiveCount > 0 && msg.ReceiveCount >= rp.MaxReceiveCount {
			if dlq, ok := s.store.GetByArn(rp.DeadLetterTargetArn); ok {
				target = dlq
			}
		}

		select {
		case target.ready <- msg:
		default:
			log.Printf("sqs: failed to requeue message %s onto %s after visibility timeout: queue full", msg.MessageID, target.Name)
		}
	})
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

// DeleteMessageBatchEntry is a single entry of a DeleteMessageBatch request.
type DeleteMessageBatchEntry struct {
	Id            string
	ReceiptHandle string
}

// DeleteMessageBatch deletes up to 10 messages in one call.
func (s *Service) DeleteMessageBatch(queueName string, entries []DeleteMessageBatchEntry) ([]string, []BatchResultError, error) {
	if _, ok := s.store.Get(queueName); !ok {
		return nil, nil, ErrQueueNotFound
	}
	var oks []string
	var fails []BatchResultError
	for _, e := range entries {
		if err := s.DeleteMessage(queueName, e.ReceiptHandle); err != nil {
			fails = append(fails, BatchResultError{Id: e.Id, Code: "ReceiptHandleIsInvalid", Message: err.Error(), SenderFault: true})
			continue
		}
		oks = append(oks, e.Id)
	}
	return oks, fails, nil
}

// ChangeMessageVisibility resets how long a received-but-undeleted
// message stays hidden from other ReceiveMessage calls. A timeout of 0
// makes the message immediately visible again.
func (s *Service) ChangeMessageVisibility(queueName, receiptHandle string, visibilityTimeout int) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.inFlightMu.Lock()
	rec, ok := q.inFlight[receiptHandle]
	if !ok {
		q.inFlightMu.Unlock()
		return ErrMessageNotFound
	}
	rec.timer.Stop()
	if visibilityTimeout <= 0 {
		delete(q.inFlight, receiptHandle)
		q.inFlightMu.Unlock()
		select {
		case q.ready <- rec.msg:
		default:
			log.Printf("sqs: failed to make message %s immediately visible on %s: queue full", rec.msg.MessageID, q.Name)
		}
		return nil
	}
	rec.timer = s.scheduleRedelivery(q, rec.msg, receiptHandle, time.Duration(visibilityTimeout)*time.Second)
	q.inFlightMu.Unlock()
	return nil
}

// ChangeMessageVisibilityBatchEntry is a single entry of a
// ChangeMessageVisibilityBatch request.
type ChangeMessageVisibilityBatchEntry struct {
	Id                string
	ReceiptHandle     string
	VisibilityTimeout int
}

// ChangeMessageVisibilityBatch changes the visibility timeout of up to 10
// messages in one call.
func (s *Service) ChangeMessageVisibilityBatch(queueName string, entries []ChangeMessageVisibilityBatchEntry) ([]string, []BatchResultError, error) {
	if _, ok := s.store.Get(queueName); !ok {
		return nil, nil, ErrQueueNotFound
	}
	var oks []string
	var fails []BatchResultError
	for _, e := range entries {
		if err := s.ChangeMessageVisibility(queueName, e.ReceiptHandle, e.VisibilityTimeout); err != nil {
			fails = append(fails, BatchResultError{Id: e.Id, Code: "ReceiptHandleIsInvalid", Message: err.Error(), SenderFault: true})
			continue
		}
		oks = append(oks, e.Id)
	}
	return oks, fails, nil
}

// PurgeQueue discards every message currently on the queue, ready or
// in-flight.
func (s *Service) PurgeQueue(queueName string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.stopAllTimers()
	for {
		select {
		case <-q.ready:
		default:
			return nil
		}
	}
}

// GetQueueAttributes returns the requested queue attributes, plus every
// attribute if names is empty or contains "All".
func (s *Service) GetQueueAttributes(queueName string, names []string) (map[string]string, error) {
	q, ok := s.store.Get(queueName)
	if !ok {
		return nil, ErrQueueNotFound
	}

	all := q.GetAttributes()
	all["QueueArn"] = q.Arn
	if _, ok := all["VisibilityTimeout"]; !ok {
		all["VisibilityTimeout"] = strconv.Itoa(int(q.VisibilityTimeout().Seconds()))
	}
	all["ApproximateNumberOfMessages"] = strconv.Itoa(q.ReadyCount())
	all["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(q.InFlightCount())
	all["ApproximateNumberOfMessagesDelayed"] = "0"
	all["CreatedTimestamp"] = strconv.FormatInt(q.CreatedTimestamp.Unix(), 10)
	all["LastModifiedTimestamp"] = strconv.FormatInt(q.LastModifiedTimestamp.Unix(), 10)

	wantAll := len(names) == 0
	for _, n := range names {
		if n == "All" {
			wantAll = true
		}
	}
	if wantAll {
		return all, nil
	}
	out := make(map[string]string, len(names))
	for _, n := range names {
		if v, ok := all[n]; ok {
			out[n] = v
		}
	}
	return out, nil
}

// SetQueueAttributes merges attrs into the queue's attribute set (e.g.
// VisibilityTimeout, RedrivePolicy, Policy).
func (s *Service) SetQueueAttributes(queueName string, attrs map[string]string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.SetAttributes(attrs)
	return nil
}

// TagQueue merges tags into the queue's tag set.
func (s *Service) TagQueue(queueName string, tags map[string]string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.SetTags(tags)
	return nil
}

// UntagQueue removes the given tag keys from the queue.
func (s *Service) UntagQueue(queueName string, keys []string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.UntagKeys(keys)
	return nil
}

// ListQueueTags returns every tag on the queue.
func (s *Service) ListQueueTags(queueName string) (map[string]string, error) {
	q, ok := s.store.Get(queueName)
	if !ok {
		return nil, ErrQueueNotFound
	}
	return q.GetTags(), nil
}

// AddPermission records a policy label granting queue access. The emulator
// has no IAM, so this exists purely for API round-trip compatibility and
// is never enforced.
func (s *Service) AddPermission(queueName, label string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	q.AddPermission(label)
	return nil
}

// RemovePermission removes a previously-added policy label.
func (s *Service) RemovePermission(queueName, label string) error {
	q, ok := s.store.Get(queueName)
	if !ok {
		return ErrQueueNotFound
	}
	if !q.RemovePermission(label) {
		return ErrPermissionNotFound
	}
	return nil
}

// ListDeadLetterSourceQueuesByName returns every queue whose RedrivePolicy
// targets the named (dead-letter) queue.
func (s *Service) ListDeadLetterSourceQueuesByName(queueName string) ([]*Queue, error) {
	q, ok := s.store.Get(queueName)
	if !ok {
		return nil, ErrQueueNotFound
	}
	return s.store.DeadLetterSources(q.Arn), nil
}

// StartMessageMoveTask moves every ready message from the source queue
// (identified by ARN) onto the destination queue (also by ARN). If
// destinationArn is empty, the single queue whose RedrivePolicy targets
// sourceArn is used, matching real SQS's dead-letter-queue redrive
// shorthand. The emulator has no background task runner, so the move
// happens synchronously and the task is COMPLETED (or FAILED) by the time
// this returns.
func (s *Service) StartMessageMoveTask(sourceArn, destinationArn string) (*MoveTask, error) {
	srcQ, ok := s.store.GetByArn(sourceArn)
	if !ok {
		return nil, ErrQueueNotFound
	}

	var destQ *Queue
	if destinationArn != "" {
		destQ, ok = s.store.GetByArn(destinationArn)
		if !ok {
			return nil, ErrQueueNotFound
		}
	} else {
		sources := s.store.DeadLetterSources(sourceArn)
		if len(sources) != 1 {
			return nil, fmt.Errorf("cannot determine destination queue: %d queue(s) redrive to %s", len(sources), sourceArn)
		}
		destQ = sources[0]
		destinationArn = destQ.Arn
	}

	task := &MoveTask{
		TaskHandle:       uuid.NewString(),
		SourceArn:        sourceArn,
		DestinationArn:   destinationArn,
		Status:           "RUNNING",
		StartedTimestamp: time.Now().Unix(),
	}

	var moved int64
	for {
		select {
		case msg := <-srcQ.ready:
			select {
			case destQ.ready <- msg:
				moved++
			default:
				task.Status = "FAILED"
				task.FailureReason = "destination queue is at capacity"
				task.ApproximateNumberOfMessagesMoved = moved
				s.store.SaveTask(task)
				return task, nil
			}
		default:
			task.Status = "COMPLETED"
			task.ApproximateNumberOfMessagesMoved = moved
			s.store.SaveTask(task)
			return task, nil
		}
	}
}

// ListMessageMoveTasks returns move tasks started for the given source
// ARN, most recent first.
func (s *Service) ListMessageMoveTasks(sourceArn string) []*MoveTask {
	return s.store.TasksForSource(sourceArn)
}

// CancelMessageMoveTask cancels a still-running move task. Since
// StartMessageMoveTask completes synchronously, this only succeeds in the
// narrow window before the task has finished.
func (s *Service) CancelMessageMoveTask(taskHandle string) (*MoveTask, error) {
	t, ok := s.store.GetTask(taskHandle)
	if !ok {
		return nil, ErrTaskNotFound
	}
	if t.Status != "RUNNING" {
		return nil, fmt.Errorf("task %s is not running (status: %s)", taskHandle, t.Status)
	}
	t.Status = "CANCELLED"
	return t, nil
}

// Enqueue implements sns.QueuePublisher: it pushes a raw pre-built message
// body directly onto an existing queue, bypassing the HTTP layer entirely,
// for in-process SNS-to-SQS fan-out.
func (s *Service) Enqueue(queueName, body string) error {
	_, err := s.SendMessage(queueName, body, nil)
	return err
}
