// Package services is the composition root tying together every emulated
// AWS service (SQS, SNS, Textract) into a single Engine consumed by the
// router.
package services

import (
	"lws/internal/services/sns"
	"lws/internal/services/sqs"
	"lws/internal/services/textract"
)

// Engine holds every service's domain logic and HTTP handler.
type Engine struct {
	SQSService *sqs.Service
	SQSHandler *sqs.Handler

	SNSService *sns.Service
	SNSHandler *sns.Handler

	TextractProcessor *textract.Processor
	TextractHandler   *textract.Handler
}

// NewEngine constructs every service and wires SNS's SQS fan-out to the
// same in-process SQS service the router dispatches to.
func NewEngine() *Engine {
	sqsSvc := sqs.NewService()
	sqsHandler := sqs.NewHandler(sqsSvc)

	snsSvc := sns.NewService(sqsSvc) // *sqs.Service satisfies sns.QueuePublisher
	snsHandler := sns.NewHandler(snsSvc)

	textractProc := textract.NewProcessor()
	textractHandler := textract.NewHandler(textractProc)

	return &Engine{
		SQSService: sqsSvc,
		SQSHandler: sqsHandler,

		SNSService: snsSvc,
		SNSHandler: snsHandler,

		TextractProcessor: textractProc,
		TextractHandler:   textractHandler,
	}
}
