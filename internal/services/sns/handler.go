package sns

import (
	"encoding/xml"
	"errors"
	"net/http"

	"lws/pkg/awsutil"
)

// Handler binds SNS's Query-protocol HTTP requests to Service calls and
// writes AWS-compliant XML responses.
type Handler struct {
	svc *Service
}

// NewHandler returns an SNS HTTP handler backed by svc.
func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

// --- XML response shapes, matching real SNS's Query-protocol responses ---

type CreateTopicResult struct {
	TopicArn string `xml:"TopicArn"`
}
type CreateTopicResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ CreateTopicResponse"`
	Result   CreateTopicResult        `xml:"CreateTopicResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type DeleteTopicResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ DeleteTopicResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type topicMember struct {
	TopicArn string `xml:"TopicArn"`
}
type ListTopicsResult struct {
	Topics []topicMember `xml:"Topics>member"`
}
type ListTopicsResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ListTopicsResponse"`
	Result   ListTopicsResult         `xml:"ListTopicsResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type SubscribeResult struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
}
type SubscribeResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ SubscribeResponse"`
	Result   SubscribeResult          `xml:"SubscribeResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type UnsubscribeResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ UnsubscribeResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type subscriptionMember struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
	Owner           string `xml:"Owner"`
	Protocol        string `xml:"Protocol"`
	Endpoint        string `xml:"Endpoint"`
	TopicArn        string `xml:"TopicArn"`
}
type ListSubscriptionsByTopicResult struct {
	Subscriptions []subscriptionMember `xml:"Subscriptions>member"`
}
type ListSubscriptionsByTopicResponse struct {
	XMLName  xml.Name                       `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ListSubscriptionsByTopicResponse"`
	Result   ListSubscriptionsByTopicResult `xml:"ListSubscriptionsByTopicResult"`
	Metadata awsutil.ResponseMetadata       `xml:"ResponseMetadata"`
}

type PublishResult struct {
	MessageId string `xml:"MessageId"`
}
type PublishResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ PublishResponse"`
	Result   PublishResult            `xml:"PublishResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

// --- handlers ---

func (h *Handler) HandleCreateTopic(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("Name")
	if name == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "Name is required")
		return
	}
	topic, err := h.svc.CreateTopic(name)
	if err != nil {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidParameter", err.Error())
		return
	}
	awsutil.WriteXML(w, http.StatusOK, CreateTopicResponse{
		Result:   CreateTopicResult{TopicArn: topic.Arn},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn is required")
		return
	}
	if err := h.svc.DeleteTopic(arn); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, DeleteTopicResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := h.svc.ListTopics()
	members := make([]topicMember, 0, len(topics))
	for _, t := range topics {
		members = append(members, topicMember{TopicArn: t.Arn})
	}
	awsutil.WriteXML(w, http.StatusOK, ListTopicsResponse{
		Result:   ListTopicsResult{Topics: members},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSubscribe(w http.ResponseWriter, r *http.Request) {
	topicArn := r.FormValue("TopicArn")
	protocol := r.FormValue("Protocol")
	endpoint := r.FormValue("Endpoint")
	if topicArn == "" || protocol == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and Protocol are required")
		return
	}
	sub, err := h.svc.Subscribe(topicArn, protocol, endpoint)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, SubscribeResponse{
		Result:   SubscribeResult{SubscriptionArn: sub.SubscriptionArn},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("SubscriptionArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "SubscriptionArn is required")
		return
	}
	if err := h.svc.Unsubscribe(arn); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, UnsubscribeResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListSubscriptionsByTopic(w http.ResponseWriter, r *http.Request) {
	topicArn := r.FormValue("TopicArn")
	if topicArn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn is required")
		return
	}
	subs, err := h.svc.ListSubscriptionsByTopic(topicArn)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	members := make([]subscriptionMember, 0, len(subs))
	for _, s := range subs {
		members = append(members, subscriptionMember{
			SubscriptionArn: s.SubscriptionArn,
			Owner:           accountID,
			Protocol:        s.Protocol,
			Endpoint:        s.Endpoint,
			TopicArn:        s.TopicArn,
		})
	}
	awsutil.WriteXML(w, http.StatusOK, ListSubscriptionsByTopicResponse{
		Result:   ListSubscriptionsByTopicResult{Subscriptions: members},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandlePublish(w http.ResponseWriter, r *http.Request) {
	topicArn := r.FormValue("TopicArn")
	message := r.FormValue("Message")
	subject := r.FormValue("Subject")
	if topicArn == "" || message == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and Message are required")
		return
	}
	messageID, err := h.svc.Publish(topicArn, message, subject)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, PublishResponse{
		Result:   PublishResult{MessageId: messageID},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

// writeSNSError maps a Service error to the matching AWS error code/status.
func writeSNSError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTopicNotFound):
		awsutil.WriteXMLError(w, http.StatusNotFound, "Sender", "NotFound", err.Error())
	case errors.Is(err, ErrSubscriptionNotFound):
		awsutil.WriteXMLError(w, http.StatusNotFound, "Sender", "NotFound", err.Error())
	default:
		awsutil.WriteXMLError(w, http.StatusInternalServerError, "Receiver", "InternalError", err.Error())
	}
}
