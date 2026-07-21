// Package services is the composition root tying every emulated AWS service
// into a single Engine consumed by the router.
package services

import (
	"lws/internal/services/s3"
	"lws/internal/services/secretsmanager"
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

	S3Service *s3.Service
	S3Handler *s3.Handler

	SecretsManagerService *secretsmanager.Service
	SecretsManagerHandler *secretsmanager.Handler
}

// NewEngine constructs every service and wires SNS's SQS fan-out to the
// same in-process SQS service the router dispatches to.
func NewEngine() *Engine {
	sqsSvc := sqs.NewService()
	sqsHandler := sqs.NewHandler(sqsSvc)

	snsSvc := sns.NewService(sqsSvc) // *sqs.Service satisfies sns.QueuePublisher
	snsHandler := sns.NewHandler(snsSvc)

	s3Svc := s3.NewService()
	s3Handler := s3.NewHandler(s3Svc)

	textractProc := textract.NewProcessor()
	textractHandler := textract.NewHandler(textractProc, s3Svc, snsSvc) // *s3.Service satisfies textract.DocumentStore, *sns.Service satisfies textract.Notifier

	secretsManagerSvc := secretsmanager.NewService()
	secretsManagerHandler := secretsmanager.NewHandler(secretsManagerSvc)

	return &Engine{
		SQSService: sqsSvc,
		SQSHandler: sqsHandler,

		SNSService: snsSvc,
		SNSHandler: snsHandler,

		TextractProcessor: textractProc,
		TextractHandler:   textractHandler,

		S3Service: s3Svc,
		S3Handler: s3Handler,

		SecretsManagerService: secretsManagerSvc,
		SecretsManagerHandler: secretsManagerHandler,
	}
}
