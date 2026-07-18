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

	// SQS (JSON protocol) - current AWS SDKs default to this over Query.
	mux.RegisterJSONAction("AmazonSQS.CreateQueue", eng.SQSHandler.HandleCreateQueueJSON)
	mux.RegisterJSONAction("AmazonSQS.GetQueueUrl", eng.SQSHandler.HandleGetQueueUrlJSON)
	mux.RegisterJSONAction("AmazonSQS.SendMessage", eng.SQSHandler.HandleSendMessageJSON)
	mux.RegisterJSONAction("AmazonSQS.ReceiveMessage", eng.SQSHandler.HandleReceiveMessageJSON)
	mux.RegisterJSONAction("AmazonSQS.DeleteMessage", eng.SQSHandler.HandleDeleteMessageJSON)
	mux.RegisterJSONAction("AmazonSQS.DeleteQueue", eng.SQSHandler.HandleDeleteQueueJSON)
	mux.RegisterJSONAction("AmazonSQS.ListQueues", eng.SQSHandler.HandleListQueuesJSON)

	// SNS (Query protocol)
	mux.RegisterQueryAction("CreateTopic", eng.SNSHandler.HandleCreateTopic)
	mux.RegisterQueryAction("DeleteTopic", eng.SNSHandler.HandleDeleteTopic)
	mux.RegisterQueryAction("ListTopics", eng.SNSHandler.HandleListTopics)
	mux.RegisterQueryAction("Subscribe", eng.SNSHandler.HandleSubscribe)
	mux.RegisterQueryAction("Unsubscribe", eng.SNSHandler.HandleUnsubscribe)
	mux.RegisterQueryAction("ListSubscriptionsByTopic", eng.SNSHandler.HandleListSubscriptionsByTopic)
	mux.RegisterQueryAction("Publish", eng.SNSHandler.HandlePublish)

	// S3 (REST protocol) - bucket/key in the URL path, dispatched by HTTP
	// method and query-string subresource rather than an Action/Target.
	mux.RegisterRESTFallback(eng.S3Handler)

	return mux
}
