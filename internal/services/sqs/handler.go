package sqs

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
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
	MessageId      string `xml:"MessageId"`
	MD5OfBody      string `xml:"MD5OfMessageBody"`
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
	Name  string                    `xml:"Name"`
	Value MessageAttributeValueXML  `xml:"Value"`
}
type MessageXML struct {
	MessageId        string                 `xml:"MessageId"`
	ReceiptHandle    string                 `xml:"ReceiptHandle"`
	MD5OfBody        string                 `xml:"MD5OfBody"`
	Body             string                 `xml:"Body"`
	Attribute        []AttributeXML         `xml:"Attribute,omitempty"`
	MessageAttribute []MessageAttributeXML  `xml:"MessageAttribute,omitempty"`
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

// writeQueueError maps a Service error to the matching AWS error code/status.
func writeQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "QueueDoesNotExist", err.Error())
	case errors.Is(err, ErrMessageNotFound):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "ReceiptHandleIsInvalid", err.Error())
	case errors.Is(err, ErrQueueFull):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "OverLimit", err.Error())
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
