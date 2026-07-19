package sqs

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"lws/pkg/awsutil"
)

// Handler binds SQS's Query-protocol HTTP requests to Service calls and
// writes AWS-compliant XML responses.
type Handler struct {
	svc *Service
}

// NewHandler returns an SQS HTTP handler backed by svc.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// --- XML response shapes, matching real SQS's Query-protocol responses ---

type CreateQueueResult struct {
	QueueUrl string `xml:"QueueUrl"`
}
type CreateQueueResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ CreateQueueResponse"`
	Result   CreateQueueResult        `xml:"CreateQueueResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type GetQueueUrlResult struct {
	QueueUrl string `xml:"QueueUrl"`
}
type GetQueueUrlResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ GetQueueUrlResponse"`
	Result   GetQueueUrlResult        `xml:"GetQueueUrlResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type SendMessageResult struct {
	MessageId string `xml:"MessageId"`
	MD5OfBody string `xml:"MD5OfMessageBody"`
}
type SendMessageResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageResponse"`
	Result   SendMessageResult        `xml:"SendMessageResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type AttributeXML struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}
type MessageAttributeValueXML struct {
	DataType    string `xml:"DataType"`
	StringValue string `xml:"StringValue,omitempty"`
}
type MessageAttributeXML struct {
	Name  string                   `xml:"Name"`
	Value MessageAttributeValueXML `xml:"Value"`
}
type MessageXML struct {
	MessageId        string                `xml:"MessageId"`
	ReceiptHandle    string                `xml:"ReceiptHandle"`
	MD5OfBody        string                `xml:"MD5OfBody"`
	Body             string                `xml:"Body"`
	Attribute        []AttributeXML        `xml:"Attribute,omitempty"`
	MessageAttribute []MessageAttributeXML `xml:"MessageAttribute,omitempty"`
}
type ReceiveMessageResult struct {
	Message []MessageXML `xml:"Message,omitempty"`
}
type ReceiveMessageResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ReceiveMessageResponse"`
	Result   ReceiveMessageResult     `xml:"ReceiveMessageResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type DeleteMessageResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteMessageResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type DeleteQueueResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteQueueResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type ListQueuesResult struct {
	QueueUrl []string `xml:"QueueUrl,omitempty"`
}
type ListQueuesResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueuesResponse"`
	Result   ListQueuesResult         `xml:"ListQueuesResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type ChangeMessageVisibilityResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ChangeMessageVisibilityResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type BatchResultErrorXML struct {
	Id          string `xml:"Id"`
	SenderFault bool   `xml:"SenderFault"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message,omitempty"`
}

type ChangeMessageVisibilityBatchResultEntryXML struct {
	Id string `xml:"Id"`
}
type ChangeMessageVisibilityBatchResult struct {
	ChangeMessageVisibilityBatchResultEntry []ChangeMessageVisibilityBatchResultEntryXML `xml:"ChangeMessageVisibilityBatchResultEntry,omitempty"`
	BatchResultErrorEntry                   []BatchResultErrorXML                        `xml:"BatchResultErrorEntry,omitempty"`
}
type ChangeMessageVisibilityBatchResponse struct {
	XMLName  xml.Name                           `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ChangeMessageVisibilityBatchResponse"`
	Result   ChangeMessageVisibilityBatchResult `xml:"ChangeMessageVisibilityBatchResult"`
	Metadata awsutil.ResponseMetadata           `xml:"ResponseMetadata"`
}

type DeleteMessageBatchResultEntryXML struct {
	Id string `xml:"Id"`
}
type DeleteMessageBatchResult struct {
	DeleteMessageBatchResultEntry []DeleteMessageBatchResultEntryXML `xml:"DeleteMessageBatchResultEntry,omitempty"`
	BatchResultErrorEntry         []BatchResultErrorXML              `xml:"BatchResultErrorEntry,omitempty"`
}
type DeleteMessageBatchResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ DeleteMessageBatchResponse"`
	Result   DeleteMessageBatchResult `xml:"DeleteMessageBatchResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type SendMessageBatchResultEntryXML struct {
	Id        string `xml:"Id"`
	MessageId string `xml:"MessageId"`
	MD5OfBody string `xml:"MD5OfMessageBody"`
}
type SendMessageBatchResult struct {
	SendMessageBatchResultEntry []SendMessageBatchResultEntryXML `xml:"SendMessageBatchResultEntry,omitempty"`
	BatchResultErrorEntry       []BatchResultErrorXML            `xml:"BatchResultErrorEntry,omitempty"`
}
type SendMessageBatchResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SendMessageBatchResponse"`
	Result   SendMessageBatchResult   `xml:"SendMessageBatchResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type PurgeQueueResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ PurgeQueueResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type GetQueueAttributesResult struct {
	Attribute []AttributeXML `xml:"Attribute,omitempty"`
}
type GetQueueAttributesResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ GetQueueAttributesResponse"`
	Result   GetQueueAttributesResult `xml:"GetQueueAttributesResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type SetQueueAttributesResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ SetQueueAttributesResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type TagQueueResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ TagQueueResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type UntagQueueResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ UntagQueueResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type TagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}
type ListQueueTagsResult struct {
	Tag []TagXML `xml:"Tag,omitempty"`
}
type ListQueueTagsResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListQueueTagsResponse"`
	Result   ListQueueTagsResult      `xml:"ListQueueTagsResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type AddPermissionResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ AddPermissionResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type RemovePermissionResponse struct {
	XMLName  xml.Name                 `xml:"http://queue.amazonaws.com/doc/2012-11-05/ RemovePermissionResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type ListDeadLetterSourceQueuesResult struct {
	QueueUrl []string `xml:"QueueUrl,omitempty"`
}
type ListDeadLetterSourceQueuesResponse struct {
	XMLName  xml.Name                         `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListDeadLetterSourceQueuesResponse"`
	Result   ListDeadLetterSourceQueuesResult `xml:"ListDeadLetterSourceQueuesResult"`
	Metadata awsutil.ResponseMetadata         `xml:"ResponseMetadata"`
}

type StartMessageMoveTaskResult struct {
	TaskHandle string `xml:"TaskHandle"`
}
type StartMessageMoveTaskResponse struct {
	XMLName  xml.Name                   `xml:"http://queue.amazonaws.com/doc/2012-11-05/ StartMessageMoveTaskResponse"`
	Result   StartMessageMoveTaskResult `xml:"StartMessageMoveTaskResult"`
	Metadata awsutil.ResponseMetadata   `xml:"ResponseMetadata"`
}

type MoveTaskXML struct {
	TaskHandle                       string `xml:"TaskHandle"`
	Status                           string `xml:"Status"`
	SourceArn                        string `xml:"SourceArn"`
	DestinationArn                   string `xml:"DestinationArn,omitempty"`
	ApproximateNumberOfMessagesMoved int64  `xml:"ApproximateNumberOfMessagesMoved"`
	StartedTimestamp                 int64  `xml:"StartedTimestamp"`
	FailureReason                    string `xml:"FailureReason,omitempty"`
}
type ListMessageMoveTasksResult struct {
	Result []MoveTaskXML `xml:"Result,omitempty"`
}
type ListMessageMoveTasksResponse struct {
	XMLName  xml.Name                   `xml:"http://queue.amazonaws.com/doc/2012-11-05/ ListMessageMoveTasksResponse"`
	Result   ListMessageMoveTasksResult `xml:"ListMessageMoveTasksResult"`
	Metadata awsutil.ResponseMetadata   `xml:"ResponseMetadata"`
}

type CancelMessageMoveTaskResult struct {
	ApproximateNumberOfMessagesMoved int64 `xml:"ApproximateNumberOfMessagesMoved"`
}
type CancelMessageMoveTaskResponse struct {
	XMLName  xml.Name                    `xml:"http://queue.amazonaws.com/doc/2012-11-05/ CancelMessageMoveTaskResponse"`
	Result   CancelMessageMoveTaskResult `xml:"CancelMessageMoveTaskResult"`
	Metadata awsutil.ResponseMetadata    `xml:"ResponseMetadata"`
}

// --- handlers ---

func (h *Handler) HandleCreateQueue(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("QueueName")
	if name == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueName is required")
		return
	}
	attrs := awsutil.ParseAttributePairs(r.Form, "Attribute")
	q, err := h.svc.CreateQueue(name, attrs, r.Host)
	if err != nil {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidParameterValue", err.Error())
		return
	}
	awsutil.WriteXML(w, http.StatusOK, CreateQueueResponse{
		Result:   CreateQueueResult{QueueUrl: q.URL},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleGetQueueUrl(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("QueueName")
	if name == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueName is required")
		return
	}
	url, err := h.svc.GetQueueURL(name)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, GetQueueUrlResponse{
		Result:   GetQueueUrlResult{QueueUrl: url},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSendMessage(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	body := r.FormValue("MessageBody")
	if queueName == "" || body == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl and MessageBody are required")
		return
	}
	attrs := parseMessageAttributes(r.Form)
	msg, err := h.svc.SendMessage(queueName, body, attrs)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, SendMessageResponse{
		Result:   SendMessageResult{MessageId: msg.MessageID, MD5OfBody: msg.MD5OfBody},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleReceiveMessage(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	maxMessages, _ := strconv.Atoi(r.FormValue("MaxNumberOfMessages"))
	waitSeconds, _ := strconv.Atoi(r.FormValue("WaitTimeSeconds"))

	received, err := h.svc.ReceiveMessage(queueName, maxMessages, time.Duration(waitSeconds)*time.Second)
	if err != nil {
		writeQueueError(w, err)
		return
	}

	messages := make([]MessageXML, 0, len(received))
	for _, rm := range received {
		mx := MessageXML{
			MessageId:     rm.MessageID,
			ReceiptHandle: rm.ReceiptHandle,
			MD5OfBody:     rm.MD5OfBody,
			Body:          rm.Body,
			Attribute: []AttributeXML{
				{Name: "ApproximateReceiveCount", Value: strconv.Itoa(rm.ReceiveCount)},
			},
		}
		for name, val := range rm.MessageAttributes {
			mx.MessageAttribute = append(mx.MessageAttribute, MessageAttributeXML{
				Name:  name,
				Value: MessageAttributeValueXML{DataType: val.DataType, StringValue: val.StringValue},
			})
		}
		messages = append(messages, mx)
	}

	awsutil.WriteXML(w, http.StatusOK, ReceiveMessageResponse{
		Result:   ReceiveMessageResult{Message: messages},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleDeleteMessage(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	handle := r.FormValue("ReceiptHandle")
	if queueName == "" || handle == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl and ReceiptHandle are required")
		return
	}
	if err := h.svc.DeleteMessage(queueName, handle); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, DeleteMessageResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.DeleteQueue(queueName); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, DeleteQueueResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListQueues(w http.ResponseWriter, r *http.Request) {
	prefix := r.FormValue("QueueNamePrefix")
	queues := h.svc.ListQueues(prefix)
	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}
	awsutil.WriteXML(w, http.StatusOK, ListQueuesResponse{
		Result:   ListQueuesResult{QueueUrl: urls},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleChangeMessageVisibility(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	handle := r.FormValue("ReceiptHandle")
	if queueName == "" || handle == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl and ReceiptHandle are required")
		return
	}
	vt, _ := strconv.Atoi(r.FormValue("VisibilityTimeout"))
	if err := h.svc.ChangeMessageVisibility(queueName, handle, vt); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, ChangeMessageVisibilityResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleChangeMessageVisibilityBatch(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	grouped := awsutil.ParseIndexedEntries(r.Form, "ChangeMessageVisibilityBatchRequestEntry")
	entries := make([]ChangeMessageVisibilityBatchEntry, 0, len(grouped))
	for _, idx := range awsutil.SortedIndexKeys(grouped) {
		e := grouped[idx]
		vt, _ := strconv.Atoi(e["VisibilityTimeout"])
		entries = append(entries, ChangeMessageVisibilityBatchEntry{Id: e["Id"], ReceiptHandle: e["ReceiptHandle"], VisibilityTimeout: vt})
	}
	oks, fails, err := h.svc.ChangeMessageVisibilityBatch(queueName, entries)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	result := ChangeMessageVisibilityBatchResult{}
	for _, id := range oks {
		result.ChangeMessageVisibilityBatchResultEntry = append(result.ChangeMessageVisibilityBatchResultEntry, ChangeMessageVisibilityBatchResultEntryXML{Id: id})
	}
	for _, f := range fails {
		result.BatchResultErrorEntry = append(result.BatchResultErrorEntry, BatchResultErrorXML{Id: f.Id, SenderFault: f.SenderFault, Code: f.Code, Message: f.Message})
	}
	awsutil.WriteXML(w, http.StatusOK, ChangeMessageVisibilityBatchResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleDeleteMessageBatch(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	grouped := awsutil.ParseIndexedEntries(r.Form, "DeleteMessageBatchRequestEntry")
	entries := make([]DeleteMessageBatchEntry, 0, len(grouped))
	for _, idx := range awsutil.SortedIndexKeys(grouped) {
		e := grouped[idx]
		entries = append(entries, DeleteMessageBatchEntry{Id: e["Id"], ReceiptHandle: e["ReceiptHandle"]})
	}
	oks, fails, err := h.svc.DeleteMessageBatch(queueName, entries)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	result := DeleteMessageBatchResult{}
	for _, id := range oks {
		result.DeleteMessageBatchResultEntry = append(result.DeleteMessageBatchResultEntry, DeleteMessageBatchResultEntryXML{Id: id})
	}
	for _, f := range fails {
		result.BatchResultErrorEntry = append(result.BatchResultErrorEntry, BatchResultErrorXML{Id: f.Id, SenderFault: f.SenderFault, Code: f.Code, Message: f.Message})
	}
	awsutil.WriteXML(w, http.StatusOK, DeleteMessageBatchResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSendMessageBatch(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	grouped := awsutil.ParseIndexedEntries(r.Form, "SendMessageBatchRequestEntry")
	entries := make([]SendMessageBatchEntry, 0, len(grouped))
	for _, idx := range awsutil.SortedIndexKeys(grouped) {
		e := grouped[idx]
		entries = append(entries, SendMessageBatchEntry{Id: e["Id"], MessageBody: e["MessageBody"]})
	}
	oks, fails, err := h.svc.SendMessageBatch(queueName, entries)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	result := SendMessageBatchResult{}
	for _, ok := range oks {
		result.SendMessageBatchResultEntry = append(result.SendMessageBatchResultEntry, SendMessageBatchResultEntryXML{Id: ok.Id, MessageId: ok.MessageID, MD5OfBody: ok.MD5OfBody})
	}
	for _, f := range fails {
		result.BatchResultErrorEntry = append(result.BatchResultErrorEntry, BatchResultErrorXML{Id: f.Id, SenderFault: f.SenderFault, Code: f.Code, Message: f.Message})
	}
	awsutil.WriteXML(w, http.StatusOK, SendMessageBatchResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandlePurgeQueue(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.PurgeQueue(queueName); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, PurgeQueueResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleGetQueueAttributes(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	names := awsutil.IndexedValues(r.Form, "AttributeName")
	attrs, err := h.svc.GetQueueAttributes(queueName, names)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	result := GetQueueAttributesResult{}
	for name, value := range attrs {
		result.Attribute = append(result.Attribute, AttributeXML{Name: name, Value: value})
	}
	sort.Slice(result.Attribute, func(i, j int) bool { return result.Attribute[i].Name < result.Attribute[j].Name })
	awsutil.WriteXML(w, http.StatusOK, GetQueueAttributesResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSetQueueAttributes(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	attrs := awsutil.ParseAttributePairs(r.Form, "Attribute")
	if err := h.svc.SetQueueAttributes(queueName, attrs); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, SetQueueAttributesResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleTagQueue(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.TagQueue(queueName, awsutil.ParseTagPairs(r.Form, "Tag")); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, TagQueueResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleUntagQueue(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.UntagQueue(queueName, awsutil.IndexedValues(r.Form, "TagKey")); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, UntagQueueResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListQueueTags(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	tags, err := h.svc.ListQueueTags(queueName)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	result := ListQueueTagsResult{}
	for k, v := range tags {
		result.Tag = append(result.Tag, TagXML{Key: k, Value: v})
	}
	sort.Slice(result.Tag, func(i, j int) bool { return result.Tag[i].Key < result.Tag[j].Key })
	awsutil.WriteXML(w, http.StatusOK, ListQueueTagsResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleAddPermission(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	label := r.FormValue("Label")
	if queueName == "" || label == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl and Label are required")
		return
	}
	if err := h.svc.AddPermission(queueName, label); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, AddPermissionResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleRemovePermission(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	label := r.FormValue("Label")
	if queueName == "" || label == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl and Label are required")
		return
	}
	if err := h.svc.RemovePermission(queueName, label); err != nil {
		writeQueueError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, RemovePermissionResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListDeadLetterSourceQueues(w http.ResponseWriter, r *http.Request) {
	queueName := queueNameFromURL(r.FormValue("QueueUrl"))
	if queueName == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "QueueUrl is required")
		return
	}
	queues, err := h.svc.ListDeadLetterSourceQueuesByName(queueName)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}
	awsutil.WriteXML(w, http.StatusOK, ListDeadLetterSourceQueuesResponse{
		Result:   ListDeadLetterSourceQueuesResult{QueueUrl: urls},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleStartMessageMoveTask(w http.ResponseWriter, r *http.Request) {
	sourceArn := r.FormValue("SourceArn")
	if sourceArn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "SourceArn is required")
		return
	}
	task, err := h.svc.StartMessageMoveTask(sourceArn, r.FormValue("DestinationArn"))
	if err != nil {
		writeMoveTaskError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, StartMessageMoveTaskResponse{
		Result:   StartMessageMoveTaskResult{TaskHandle: task.TaskHandle},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListMessageMoveTasks(w http.ResponseWriter, r *http.Request) {
	sourceArn := r.FormValue("SourceArn")
	if sourceArn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "SourceArn is required")
		return
	}
	tasks := h.svc.ListMessageMoveTasks(sourceArn)
	result := ListMessageMoveTasksResult{}
	for _, t := range tasks {
		result.Result = append(result.Result, MoveTaskXML{
			TaskHandle:                       t.TaskHandle,
			Status:                           t.Status,
			SourceArn:                        t.SourceArn,
			DestinationArn:                   t.DestinationArn,
			ApproximateNumberOfMessagesMoved: t.ApproximateNumberOfMessagesMoved,
			StartedTimestamp:                 t.StartedTimestamp,
			FailureReason:                    t.FailureReason,
		})
	}
	awsutil.WriteXML(w, http.StatusOK, ListMessageMoveTasksResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleCancelMessageMoveTask(w http.ResponseWriter, r *http.Request) {
	handle := r.FormValue("TaskHandle")
	if handle == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TaskHandle is required")
		return
	}
	task, err := h.svc.CancelMessageMoveTask(handle)
	if err != nil {
		writeMoveTaskError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, CancelMessageMoveTaskResponse{
		Result:   CancelMessageMoveTaskResult{ApproximateNumberOfMessagesMoved: task.ApproximateNumberOfMessagesMoved},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

// --- JSON-protocol handlers ---
//
// Modern AWS SDKs default to SQS's JSON protocol rather than the legacy
// Query protocol above: requests carry an X-Amz-Target header and a JSON
// body, and responses are flat JSON with no XML envelope.

type jsonMessageAttributeValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue,omitempty"`
}

type createQueueJSONRequest struct {
	QueueName  string            `json:"QueueName"`
	Attributes map[string]string `json:"Attributes,omitempty"`
}

func (h *Handler) HandleCreateQueueJSON(w http.ResponseWriter, r *http.Request) {
	var req createQueueJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	if req.QueueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueName is required")
		return
	}
	q, err := h.svc.CreateQueue(req.QueueName, req.Attributes, r.Host)
	if err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{"QueueUrl": q.URL})
}

type getQueueUrlJSONRequest struct {
	QueueName string `json:"QueueName"`
}

func (h *Handler) HandleGetQueueUrlJSON(w http.ResponseWriter, r *http.Request) {
	var req getQueueUrlJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	if req.QueueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueName is required")
		return
	}
	url, err := h.svc.GetQueueURL(req.QueueName)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{"QueueUrl": url})
}

type sendMessageJSONRequest struct {
	QueueUrl          string                               `json:"QueueUrl"`
	MessageBody       string                               `json:"MessageBody"`
	MessageAttributes map[string]jsonMessageAttributeValue `json:"MessageAttributes,omitempty"`
}

func (h *Handler) HandleSendMessageJSON(w http.ResponseWriter, r *http.Request) {
	var req sendMessageJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" || req.MessageBody == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl and MessageBody are required")
		return
	}
	var attrs map[string]MessageAttributeValue
	if len(req.MessageAttributes) > 0 {
		attrs = make(map[string]MessageAttributeValue, len(req.MessageAttributes))
		for name, v := range req.MessageAttributes {
			attrs[name] = MessageAttributeValue{DataType: v.DataType, StringValue: v.StringValue}
		}
	}
	msg, err := h.svc.SendMessage(queueName, req.MessageBody, attrs)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{
		"MessageId":        msg.MessageID,
		"MD5OfMessageBody": msg.MD5OfBody,
	})
}

type receiveMessageJSONRequest struct {
	QueueUrl            string `json:"QueueUrl"`
	MaxNumberOfMessages int    `json:"MaxNumberOfMessages"`
	WaitTimeSeconds     int    `json:"WaitTimeSeconds"`
}

type jsonMessage struct {
	MessageId         string                               `json:"MessageId"`
	ReceiptHandle     string                               `json:"ReceiptHandle"`
	MD5OfBody         string                               `json:"MD5OfBody"`
	Body              string                               `json:"Body"`
	Attributes        map[string]string                    `json:"Attributes,omitempty"`
	MessageAttributes map[string]jsonMessageAttributeValue `json:"MessageAttributes,omitempty"`
}

func (h *Handler) HandleReceiveMessageJSON(w http.ResponseWriter, r *http.Request) {
	var req receiveMessageJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	received, err := h.svc.ReceiveMessage(queueName, req.MaxNumberOfMessages, time.Duration(req.WaitTimeSeconds)*time.Second)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	messages := make([]jsonMessage, 0, len(received))
	for _, rm := range received {
		jm := jsonMessage{
			MessageId:     rm.MessageID,
			ReceiptHandle: rm.ReceiptHandle,
			MD5OfBody:     rm.MD5OfBody,
			Body:          rm.Body,
			Attributes:    map[string]string{"ApproximateReceiveCount": strconv.Itoa(rm.ReceiveCount)},
		}
		for name, val := range rm.MessageAttributes {
			if jm.MessageAttributes == nil {
				jm.MessageAttributes = make(map[string]jsonMessageAttributeValue, len(rm.MessageAttributes))
			}
			jm.MessageAttributes[name] = jsonMessageAttributeValue{DataType: val.DataType, StringValue: val.StringValue}
		}
		messages = append(messages, jm)
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{"Messages": messages})
}

type deleteMessageJSONRequest struct {
	QueueUrl      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

func (h *Handler) HandleDeleteMessageJSON(w http.ResponseWriter, r *http.Request) {
	var req deleteMessageJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" || req.ReceiptHandle == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl and ReceiptHandle are required")
		return
	}
	if err := h.svc.DeleteMessage(queueName, req.ReceiptHandle); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type deleteQueueJSONRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

func (h *Handler) HandleDeleteQueueJSON(w http.ResponseWriter, r *http.Request) {
	var req deleteQueueJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.DeleteQueue(queueName); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type listQueuesJSONRequest struct {
	QueueNamePrefix string `json:"QueueNamePrefix"`
}

func (h *Handler) HandleListQueuesJSON(w http.ResponseWriter, r *http.Request) {
	var req listQueuesJSONRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
			return
		}
	}
	queues := h.svc.ListQueues(req.QueueNamePrefix)
	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string][]string{"QueueUrls": urls})
}

type changeMessageVisibilityJSONRequest struct {
	QueueUrl          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout int    `json:"VisibilityTimeout"`
}

func (h *Handler) HandleChangeMessageVisibilityJSON(w http.ResponseWriter, r *http.Request) {
	var req changeMessageVisibilityJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" || req.ReceiptHandle == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl and ReceiptHandle are required")
		return
	}
	if err := h.svc.ChangeMessageVisibility(queueName, req.ReceiptHandle, req.VisibilityTimeout); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

// batchEntryJSON is a union of every field used across
// ChangeMessageVisibilityBatch/DeleteMessageBatch/SendMessageBatch entries;
// each action only reads the fields relevant to it.
type batchEntryJSON struct {
	Id                string                               `json:"Id"`
	ReceiptHandle     string                               `json:"ReceiptHandle,omitempty"`
	VisibilityTimeout int                                  `json:"VisibilityTimeout,omitempty"`
	MessageBody       string                               `json:"MessageBody,omitempty"`
	MessageAttributes map[string]jsonMessageAttributeValue `json:"MessageAttributes,omitempty"`
}

type batchRequestJSON struct {
	QueueUrl string           `json:"QueueUrl"`
	Entries  []batchEntryJSON `json:"Entries"`
}

type batchResultErrorJSON struct {
	Id          string `json:"Id"`
	SenderFault bool   `json:"SenderFault"`
	Code        string `json:"Code"`
	Message     string `json:"Message,omitempty"`
}

func idsToEntries(ids []string) []map[string]string {
	out := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, map[string]string{"Id": id})
	}
	return out
}

func toBatchResultErrorsJSON(fails []BatchResultError) []batchResultErrorJSON {
	out := make([]batchResultErrorJSON, 0, len(fails))
	for _, f := range fails {
		out = append(out, batchResultErrorJSON{Id: f.Id, SenderFault: f.SenderFault, Code: f.Code, Message: f.Message})
	}
	return out
}

func (h *Handler) HandleChangeMessageVisibilityBatchJSON(w http.ResponseWriter, r *http.Request) {
	var req batchRequestJSON
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	entries := make([]ChangeMessageVisibilityBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, ChangeMessageVisibilityBatchEntry{Id: e.Id, ReceiptHandle: e.ReceiptHandle, VisibilityTimeout: e.VisibilityTimeout})
	}
	oks, fails, err := h.svc.ChangeMessageVisibilityBatch(queueName, entries)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{
		"Successful": idsToEntries(oks),
		"Failed":     toBatchResultErrorsJSON(fails),
	})
}

func (h *Handler) HandleDeleteMessageBatchJSON(w http.ResponseWriter, r *http.Request) {
	var req batchRequestJSON
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	entries := make([]DeleteMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		entries = append(entries, DeleteMessageBatchEntry{Id: e.Id, ReceiptHandle: e.ReceiptHandle})
	}
	oks, fails, err := h.svc.DeleteMessageBatch(queueName, entries)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{
		"Successful": idsToEntries(oks),
		"Failed":     toBatchResultErrorsJSON(fails),
	})
}

func (h *Handler) HandleSendMessageBatchJSON(w http.ResponseWriter, r *http.Request) {
	var req batchRequestJSON
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	entries := make([]SendMessageBatchEntry, 0, len(req.Entries))
	for _, e := range req.Entries {
		var attrs map[string]MessageAttributeValue
		if len(e.MessageAttributes) > 0 {
			attrs = make(map[string]MessageAttributeValue, len(e.MessageAttributes))
			for name, v := range e.MessageAttributes {
				attrs[name] = MessageAttributeValue{DataType: v.DataType, StringValue: v.StringValue}
			}
		}
		entries = append(entries, SendMessageBatchEntry{Id: e.Id, MessageBody: e.MessageBody, MessageAttributes: attrs})
	}
	oks, fails, err := h.svc.SendMessageBatch(queueName, entries)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	successful := make([]map[string]string, 0, len(oks))
	for _, ok := range oks {
		successful = append(successful, map[string]string{"Id": ok.Id, "MessageId": ok.MessageID, "MD5OfMessageBody": ok.MD5OfBody})
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{
		"Successful": successful,
		"Failed":     toBatchResultErrorsJSON(fails),
	})
}

type purgeQueueJSONRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

func (h *Handler) HandlePurgeQueueJSON(w http.ResponseWriter, r *http.Request) {
	var req purgeQueueJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.PurgeQueue(queueName); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type getQueueAttributesJSONRequest struct {
	QueueUrl       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames,omitempty"`
}

func (h *Handler) HandleGetQueueAttributesJSON(w http.ResponseWriter, r *http.Request) {
	var req getQueueAttributesJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	attrs, err := h.svc.GetQueueAttributes(queueName, req.AttributeNames)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{"Attributes": attrs})
}

type setQueueAttributesJSONRequest struct {
	QueueUrl   string            `json:"QueueUrl"`
	Attributes map[string]string `json:"Attributes"`
}

func (h *Handler) HandleSetQueueAttributesJSON(w http.ResponseWriter, r *http.Request) {
	var req setQueueAttributesJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.SetQueueAttributes(queueName, req.Attributes); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type tagQueueJSONRequest struct {
	QueueUrl string            `json:"QueueUrl"`
	Tags     map[string]string `json:"Tags"`
}

func (h *Handler) HandleTagQueueJSON(w http.ResponseWriter, r *http.Request) {
	var req tagQueueJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.TagQueue(queueName, req.Tags); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type untagQueueJSONRequest struct {
	QueueUrl string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

func (h *Handler) HandleUntagQueueJSON(w http.ResponseWriter, r *http.Request) {
	var req untagQueueJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	if err := h.svc.UntagQueue(queueName, req.TagKeys); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type listQueueTagsJSONRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

func (h *Handler) HandleListQueueTagsJSON(w http.ResponseWriter, r *http.Request) {
	var req listQueueTagsJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	tags, err := h.svc.ListQueueTags(queueName)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{"Tags": tags})
}

type addPermissionJSONRequest struct {
	QueueUrl      string   `json:"QueueUrl"`
	Label         string   `json:"Label"`
	AWSAccountIds []string `json:"AWSAccountIds,omitempty"`
	Actions       []string `json:"Actions,omitempty"`
}

func (h *Handler) HandleAddPermissionJSON(w http.ResponseWriter, r *http.Request) {
	var req addPermissionJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" || req.Label == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl and Label are required")
		return
	}
	if err := h.svc.AddPermission(queueName, req.Label); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type removePermissionJSONRequest struct {
	QueueUrl string `json:"QueueUrl"`
	Label    string `json:"Label"`
}

func (h *Handler) HandleRemovePermissionJSON(w http.ResponseWriter, r *http.Request) {
	var req removePermissionJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" || req.Label == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl and Label are required")
		return
	}
	if err := h.svc.RemovePermission(queueName, req.Label); err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{})
}

type listDeadLetterSourceQueuesJSONRequest struct {
	QueueUrl string `json:"QueueUrl"`
}

func (h *Handler) HandleListDeadLetterSourceQueuesJSON(w http.ResponseWriter, r *http.Request) {
	var req listDeadLetterSourceQueuesJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	queueName := queueNameFromURL(req.QueueUrl)
	if queueName == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "QueueUrl is required")
		return
	}
	queues, err := h.svc.ListDeadLetterSourceQueuesByName(queueName)
	if err != nil {
		writeQueueErrorJSON(w, err)
		return
	}
	urls := make([]string, 0, len(queues))
	for _, q := range queues {
		urls = append(urls, q.URL)
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string][]string{"queueUrls": urls})
}

type startMessageMoveTaskJSONRequest struct {
	SourceArn      string `json:"SourceArn"`
	DestinationArn string `json:"DestinationArn,omitempty"`
}

func (h *Handler) HandleStartMessageMoveTaskJSON(w http.ResponseWriter, r *http.Request) {
	var req startMessageMoveTaskJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	if req.SourceArn == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "SourceArn is required")
		return
	}
	task, err := h.svc.StartMessageMoveTask(req.SourceArn, req.DestinationArn)
	if err != nil {
		writeMoveTaskErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]string{"TaskHandle": task.TaskHandle})
}

type listMessageMoveTasksJSONRequest struct {
	SourceArn string `json:"SourceArn"`
}

func (h *Handler) HandleListMessageMoveTasksJSON(w http.ResponseWriter, r *http.Request) {
	var req listMessageMoveTasksJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	if req.SourceArn == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "SourceArn is required")
		return
	}
	tasks := h.svc.ListMessageMoveTasks(req.SourceArn)
	results := make([]map[string]any, 0, len(tasks))
	for _, t := range tasks {
		results = append(results, map[string]any{
			"TaskHandle":                       t.TaskHandle,
			"Status":                           t.Status,
			"SourceArn":                        t.SourceArn,
			"DestinationArn":                   t.DestinationArn,
			"ApproximateNumberOfMessagesMoved": t.ApproximateNumberOfMessagesMoved,
			"StartedTimestamp":                 t.StartedTimestamp,
			"FailureReason":                    t.FailureReason,
		})
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]any{"Results": results})
}

type cancelMessageMoveTaskJSONRequest struct {
	TaskHandle string `json:"TaskHandle"`
}

func (h *Handler) HandleCancelMessageMoveTaskJSON(w http.ResponseWriter, r *http.Request) {
	var req cancelMessageMoveTaskJSONRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "malformed request body: "+err.Error())
		return
	}
	if req.TaskHandle == "" {
		awsutil.WriteJSONError(w, http.StatusBadRequest, "MissingParameter", "TaskHandle is required")
		return
	}
	task, err := h.svc.CancelMessageMoveTask(req.TaskHandle)
	if err != nil {
		writeMoveTaskErrorJSON(w, err)
		return
	}
	awsutil.WriteJSON(w, http.StatusOK, map[string]int64{"ApproximateNumberOfMessagesMoved": task.ApproximateNumberOfMessagesMoved})
}

// writeQueueErrorJSON maps a Service error to the matching AWS JSON
// protocol error code/status, mirroring writeQueueError below.
func writeQueueErrorJSON(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "QueueDoesNotExist", err.Error())
	case errors.Is(err, ErrMessageNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "ReceiptHandleIsInvalid", err.Error())
	case errors.Is(err, ErrQueueFull):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "OverLimit", err.Error())
	case errors.Is(err, ErrPermissionNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	default:
		awsutil.WriteJSONError(w, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// writeQueueError maps a Service error to the matching AWS error code/status.
func writeQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "QueueDoesNotExist", err.Error())
	case errors.Is(err, ErrMessageNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "ReceiptHandleIsInvalid", err.Error())
	case errors.Is(err, ErrQueueFull):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "OverLimit", err.Error())
	case errors.Is(err, ErrPermissionNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidParameterValue", err.Error())
	default:
		awsutil.WriteXMLError(w, http.StatusInternalServerError, "Receiver", "InternalError", err.Error())
	}
}

// queueNameFromURL extracts the trailing queue-name path segment from a
// QueueUrl parameter (SQS Query-protocol requests identify queues by URL
// for every action except CreateQueue/GetQueueUrl/ListQueues).
func queueNameFromURL(queueURL string) string {
	if queueURL == "" {
		return ""
	}
	idx := strings.LastIndex(queueURL, "/")
	if idx == -1 {
		return queueURL
	}
	return queueURL[idx+1:]
}

// parseMessageAttributes parses SendMessage's indexed
// MessageAttribute.N.Name / MessageAttribute.N.Value.DataType /
// MessageAttribute.N.Value.StringValue parameters.
func parseMessageAttributes(form url.Values) map[string]MessageAttributeValue {
	type partial struct {
		name, dataType, stringValue string
	}
	byIndex := map[string]*partial{}

	for key, vals := range form {
		if len(vals) == 0 {
			continue
		}
		rest, ok := strings.CutPrefix(key, "MessageAttribute.")
		if !ok {
			continue
		}
		idx, field, ok := strings.Cut(rest, ".")
		if !ok {
			continue
		}
		p, ok := byIndex[idx]
		if !ok {
			p = &partial{}
			byIndex[idx] = p
		}
		switch field {
		case "Name":
			p.name = vals[0]
		case "Value.DataType":
			p.dataType = vals[0]
		case "Value.StringValue":
			p.stringValue = vals[0]
		}
	}

	if len(byIndex) == 0 {
		return nil
	}
	out := make(map[string]MessageAttributeValue, len(byIndex))
	for _, p := range byIndex {
		if p.name == "" {
			continue
		}
		out[p.name] = MessageAttributeValue{DataType: p.dataType, StringValue: p.stringValue}
	}
	return out
}

// writeMoveTaskError maps a StartMessageMoveTask/CancelMessageMoveTask
// error to the matching AWS Query-protocol error code/status.
func writeMoveTaskError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "AWS.SimpleQueueService.NonExistentQueue", err.Error())
	case errors.Is(err, ErrTaskNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "ResourceNotFoundException", err.Error())
	default:
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidParameterValue", err.Error())
	}
}

// writeMoveTaskErrorJSON is writeMoveTaskError for the JSON protocol.
func writeMoveTaskErrorJSON(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", err.Error())
	case errors.Is(err, ErrTaskNotFound):
		awsutil.WriteJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	default:
		awsutil.WriteJSONError(w, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}
}
