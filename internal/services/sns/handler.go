package sns

import (
	"encoding/xml"
	"errors"
	"net/http"
	"sort"

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

type attributeEntry struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}
type GetTopicAttributesResult struct {
	Attributes []attributeEntry `xml:"Attributes>entry"`
}
type GetTopicAttributesResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ GetTopicAttributesResponse"`
	Result   GetTopicAttributesResult `xml:"GetTopicAttributesResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type SetTopicAttributesResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ SetTopicAttributesResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

// TagResource and UntagResource have empty result shapes, but botocore's
// SNS model still declares a resultWrapper for them, so the empty
// <TagResourceResult/> element must be present (even though it carries no
// fields) or botocore's response parser errors with a KeyError.
type TagResourceResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ TagResourceResponse"`
	Result   struct{}                 `xml:"TagResourceResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type UntagResourceResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ UntagResourceResponse"`
	Result   struct{}                 `xml:"UntagResourceResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type tagMember struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}
type ListTagsForResourceResult struct {
	Tags []tagMember `xml:"Tags>member"`
}
type ListTagsForResourceResponse struct {
	XMLName  xml.Name                  `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ListTagsForResourceResponse"`
	Result   ListTagsForResourceResult `xml:"ListTagsForResourceResult"`
	Metadata awsutil.ResponseMetadata  `xml:"ResponseMetadata"`
}

type AddPermissionResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ AddPermissionResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}
type RemovePermissionResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ RemovePermissionResponse"`
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

type ConfirmSubscriptionResult struct {
	SubscriptionArn string `xml:"SubscriptionArn"`
}
type ConfirmSubscriptionResponse struct {
	XMLName  xml.Name                  `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ConfirmSubscriptionResponse"`
	Result   ConfirmSubscriptionResult `xml:"ConfirmSubscriptionResult"`
	Metadata awsutil.ResponseMetadata  `xml:"ResponseMetadata"`
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

type ListSubscriptionsResult struct {
	Subscriptions []subscriptionMember `xml:"Subscriptions>member"`
}
type ListSubscriptionsResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ ListSubscriptionsResponse"`
	Result   ListSubscriptionsResult  `xml:"ListSubscriptionsResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type GetSubscriptionAttributesResult struct {
	Attributes []attributeEntry `xml:"Attributes>entry"`
}
type GetSubscriptionAttributesResponse struct {
	XMLName  xml.Name                        `xml:"http://sns.amazonaws.com/doc/2010-03-31/ GetSubscriptionAttributesResponse"`
	Result   GetSubscriptionAttributesResult `xml:"GetSubscriptionAttributesResult"`
	Metadata awsutil.ResponseMetadata        `xml:"ResponseMetadata"`
}
type SetSubscriptionAttributesResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ SetSubscriptionAttributesResponse"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type PublishResult struct {
	MessageId string `xml:"MessageId"`
}
type PublishResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ PublishResponse"`
	Result   PublishResult            `xml:"PublishResult"`
	Metadata awsutil.ResponseMetadata `xml:"ResponseMetadata"`
}

type PublishBatchResultEntryXML struct {
	Id        string `xml:"Id"`
	MessageId string `xml:"MessageId"`
}
type BatchResultErrorXML struct {
	Id          string `xml:"Id"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
	SenderFault bool   `xml:"SenderFault"`
}
type PublishBatchResult struct {
	Successful []PublishBatchResultEntryXML `xml:"Successful>member"`
	Failed     []BatchResultErrorXML        `xml:"Failed>member"`
}
type PublishBatchResponse struct {
	XMLName  xml.Name                 `xml:"http://sns.amazonaws.com/doc/2010-03-31/ PublishBatchResponse"`
	Result   PublishBatchResult       `xml:"PublishBatchResult"`
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

func (h *Handler) HandleGetTopicAttributes(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn is required")
		return
	}
	attrs, err := h.svc.GetTopicAttributes(arn)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, GetTopicAttributesResponse{
		Result:   GetTopicAttributesResult{Attributes: attributeEntries(attrs)},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSetTopicAttributes(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	name := r.FormValue("AttributeName")
	if arn == "" || name == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and AttributeName are required")
		return
	}
	if err := h.svc.SetTopicAttributes(arn, name, r.FormValue("AttributeValue")); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, SetTopicAttributesResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleTagResource(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("ResourceArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "ResourceArn is required")
		return
	}
	if err := h.svc.TagResource(arn, awsutil.ParseTagPairs(r.Form, "Tags.member")); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, TagResourceResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleUntagResource(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("ResourceArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "ResourceArn is required")
		return
	}
	if err := h.svc.UntagResource(arn, awsutil.IndexedValues(r.Form, "TagKeys.member")); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, UntagResourceResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListTagsForResource(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("ResourceArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "ResourceArn is required")
		return
	}
	tags, err := h.svc.ListTagsForResource(arn)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	members := make([]tagMember, 0, len(tags))
	for k, v := range tags {
		members = append(members, tagMember{Key: k, Value: v})
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Key < members[j].Key })
	awsutil.WriteXML(w, http.StatusOK, ListTagsForResourceResponse{
		Result:   ListTagsForResourceResult{Tags: members},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleAddPermission(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	label := r.FormValue("Label")
	if arn == "" || label == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and Label are required")
		return
	}
	if err := h.svc.AddPermission(arn, label); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, AddPermissionResponse{
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleRemovePermission(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	label := r.FormValue("Label")
	if arn == "" || label == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and Label are required")
		return
	}
	if err := h.svc.RemovePermission(arn, label); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, RemovePermissionResponse{
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
		Result:   SubscribeResult{SubscriptionArn: sub.PublicArn()},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleConfirmSubscription(w http.ResponseWriter, r *http.Request) {
	topicArn := r.FormValue("TopicArn")
	token := r.FormValue("Token")
	if topicArn == "" || token == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn and Token are required")
		return
	}
	sub, err := h.svc.ConfirmSubscription(topicArn, token)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, ConfirmSubscriptionResponse{
		Result:   ConfirmSubscriptionResult{SubscriptionArn: sub.PublicArn()},
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
	awsutil.WriteXML(w, http.StatusOK, ListSubscriptionsByTopicResponse{
		Result:   ListSubscriptionsByTopicResult{Subscriptions: subscriptionMembers(subs)},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs := h.svc.ListSubscriptions()
	awsutil.WriteXML(w, http.StatusOK, ListSubscriptionsResponse{
		Result:   ListSubscriptionsResult{Subscriptions: subscriptionMembers(subs)},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleGetSubscriptionAttributes(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("SubscriptionArn")
	if arn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "SubscriptionArn is required")
		return
	}
	attrs, err := h.svc.GetSubscriptionAttributes(arn)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, GetSubscriptionAttributesResponse{
		Result:   GetSubscriptionAttributesResult{Attributes: attributeEntries(attrs)},
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

func (h *Handler) HandleSetSubscriptionAttributes(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("SubscriptionArn")
	name := r.FormValue("AttributeName")
	if arn == "" || name == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "SubscriptionArn and AttributeName are required")
		return
	}
	if err := h.svc.SetSubscriptionAttributes(arn, name, r.FormValue("AttributeValue")); err != nil {
		writeSNSError(w, err)
		return
	}
	awsutil.WriteXML(w, http.StatusOK, SetSubscriptionAttributesResponse{
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

func (h *Handler) HandlePublishBatch(w http.ResponseWriter, r *http.Request) {
	topicArn := r.FormValue("TopicArn")
	if topicArn == "" {
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "MissingParameter", "TopicArn is required")
		return
	}
	grouped := awsutil.ParseIndexedEntries(r.Form, "PublishBatchRequestEntries.member")
	entries := make([]PublishBatchEntry, 0, len(grouped))
	for _, idx := range awsutil.SortedIndexKeys(grouped) {
		e := grouped[idx]
		entries = append(entries, PublishBatchEntry{Id: e["Id"], Message: e["Message"], Subject: e["Subject"]})
	}
	oks, fails, err := h.svc.PublishBatch(topicArn, entries)
	if err != nil {
		writeSNSError(w, err)
		return
	}
	result := PublishBatchResult{}
	for _, ok := range oks {
		result.Successful = append(result.Successful, PublishBatchResultEntryXML{Id: ok.Id, MessageId: ok.MessageID})
	}
	for _, f := range fails {
		result.Failed = append(result.Failed, BatchResultErrorXML{Id: f.Id, Code: f.Code, Message: f.Message, SenderFault: f.SenderFault})
	}
	awsutil.WriteXML(w, http.StatusOK, PublishBatchResponse{
		Result:   result,
		Metadata: awsutil.ResponseMetadata{RequestID: awsutil.NewRequestID()},
	})
}

// --- shared response-building helpers ---

func subscriptionMembers(subs []*Subscription) []subscriptionMember {
	members := make([]subscriptionMember, 0, len(subs))
	for _, s := range subs {
		members = append(members, subscriptionMember{
			SubscriptionArn: s.PublicArn(),
			Owner:           s.Owner,
			Protocol:        s.Protocol,
			Endpoint:        s.Endpoint,
			TopicArn:        s.TopicArn,
		})
	}
	return members
}

func attributeEntries(attrs map[string]string) []attributeEntry {
	out := make([]attributeEntry, 0, len(attrs))
	for k, v := range attrs {
		out = append(out, attributeEntry{Key: k, Value: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// writeSNSError maps a Service error to the matching AWS error code/status.
func writeSNSError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTopicNotFound):
		awsutil.WriteXMLError(w, http.StatusNotFound, "Sender", "NotFound", err.Error())
	case errors.Is(err, ErrSubscriptionNotFound):
		awsutil.WriteXMLError(w, http.StatusNotFound, "Sender", "NotFound", err.Error())
	case errors.Is(err, ErrPermissionNotFound):
		awsutil.WriteXMLError(w, http.StatusNotFound, "Sender", "NotFound", err.Error())
	case errors.Is(err, ErrInvalidToken):
		awsutil.WriteXMLError(w, http.StatusBadRequest, "Sender", "InvalidParameter", err.Error())
	default:
		awsutil.WriteXMLError(w, http.StatusInternalServerError, "Receiver", "InternalError", err.Error())
	}
}
