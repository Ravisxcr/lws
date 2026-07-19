# lws

A local emulator for a subset of AWS services, written in Go. Useful for testing AWS-integrated code without hitting real AWS.

## Services

- **S3** — object storage
- **SQS** — queues (Query and JSON protocols)
- **SNS** — pub/sub
- **Secrets Manager** — secret storage
- **Textract** — document text/expense detection via Tesseract OCR (sync and async)

## Run

```sh
docker compose up
```

Serves on port `4566` (the LocalStack-standard port). Point your AWS SDK's endpoint at `http://localhost:4566`.

## Build locally

```sh
go build -o emulator ./emulator
PORT=8080 ./emulator
```

Requires OpenCV and Tesseract dev libraries on the host (see `Dockerfile`) for the Textract OCR path.

## Layout

- `emulator/` — entrypoint
- `internal/app/` — server bootstrap
- `internal/platform/router/` — request dispatch (AWS Query + JSON protocols)
- `internal/services/` — per-service handlers, business logic, storage
- `pkg/awsutil/` — shared AWS wire-format helpers
- `tests/` — integration tests (Python/pytest with boto3)
