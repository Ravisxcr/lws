package router

import (
	"net/http"

	"lws/internal/services"
)

// NewRouter builds the full request-dispatch table for lws, wiring every
// emulated AWS operation to its handler on the given service engine.
func NewRouter(eng *services.Engine) http.Handler {
	mux := NewMultiplexer()

	// Textract (JSON protocol)
	mux.RegisterJSONAction("Textract.DetectDocumentText", eng.TextractHandler.HandleDetectDocumentText)
	mux.RegisterJSONAction("Textract.AnalyzeDocument", eng.TextractHandler.HandleAnalyzeDocument)

	// SQS (Query protocol)
	mux.RegisterQueryAction("CreateQueue", eng.SQSHandler.HandleCreateQueue)
	mux.RegisterQueryAction("GetQueueUrl", eng.SQSHandler.HandleGetQueueUrl)
	mux.RegisterQueryAction("SendMessage", eng.SQSHandler.HandleSendMessage)
	mux.RegisterQueryAction("ReceiveMessage", eng.SQSHandler.HandleReceiveMessage)
	mux.RegisterQueryAction("DeleteMessage", eng.SQSHandler.HandleDeleteMessage)
	mux.RegisterQueryAction("DeleteQueue", eng.SQSHandler.HandleDeleteQueue)
	mux.RegisterQueryAction("ListQueues", eng.SQSHandler.HandleListQueues)

	// SNS (Query protocol)
	mux.RegisterQueryAction("CreateTopic", eng.SNSHandler.HandleCreateTopic)
	mux.RegisterQueryAction("DeleteTopic", eng.SNSHandler.HandleDeleteTopic)
	mux.RegisterQueryAction("ListTopics", eng.SNSHandler.HandleListTopics)
	mux.RegisterQueryAction("Subscribe", eng.SNSHandler.HandleSubscribe)
	mux.RegisterQueryAction("Unsubscribe", eng.SNSHandler.HandleUnsubscribe)
	mux.RegisterQueryAction("ListSubscriptionsByTopic", eng.SNSHandler.HandleListSubscriptionsByTopic)
	mux.RegisterQueryAction("Publish", eng.SNSHandler.HandlePublish)

	return mux
}
