#!/usr/bin/env python3
"""Smoke-test the lws emulator's SQS, SNS, and Textract endpoints via boto3.

Usage:
    python3 scripts/smoke_test.py [--endpoint http://localhost:4566]
"""

import argparse
import sys

import boto3

DUMMY_CREDS = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


def client(service: str, endpoint: str):
    return boto3.client(service, endpoint_url=endpoint, **DUMMY_CREDS)


def check_sqs(endpoint: str) -> None:
    sqs = client("sqs", endpoint)
    queue_url = sqs.create_queue(QueueName="smoke-test-queue")["QueueUrl"]
    sqs.send_message(QueueUrl=queue_url, MessageBody="hello from boto3")
    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1).get("Messages", [])
    assert messages and messages[0]["Body"] == "hello from boto3", "SQS round-trip failed"
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
    sqs.delete_queue(QueueUrl=queue_url)
    print("SQS: ok")


def check_sns(endpoint: str) -> None:
    sns = client("sns", endpoint)
    topic_arn = sns.create_topic(Name="smoke-test-topic")["TopicArn"]
    sns.publish(TopicArn=topic_arn, Message="hello from boto3")
    sns.delete_topic(TopicArn=topic_arn)
    print("SNS: ok")


def check_textract(endpoint: str) -> None:
    textract = client("textract", endpoint)
    # 1x1 transparent PNG - just checks the endpoint responds, not OCR accuracy.
    png_bytes = bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000a49444154789c6300010000050001"
        "0d0a2db40000000049454e44ae426082"
    )
    textract.detect_document_text(Document={"Bytes": png_bytes})
    print("Textract: ok")


def check_s3(endpoint: str) -> None:
    s3 = client("s3", endpoint)
    bucket = "smoke-test-bucket"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="smoke-test-key", Body=b"hello from boto3")
    body = s3.get_object(Bucket=bucket, Key="smoke-test-key")["Body"].read()
    assert body == b"hello from boto3", "S3 round-trip failed"
    s3.delete_object(Bucket=bucket, Key="smoke-test-key")
    s3.delete_bucket(Bucket=bucket)
    print("S3: ok")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://localhost:4566", help="lws base URL")
    args = parser.parse_args()

    checks = {"sqs": check_sqs, "sns": check_sns, "textract": check_textract, "s3": check_s3}
    failures = []
    for name, check in checks.items():
        try:
            check(args.endpoint)
        except Exception as exc:  # noqa: BLE001 - report every service, don't stop at first failure
            failures.append(name)
            print(f"{name}: FAILED - {exc}")

    if failures:
        print(f"\n{len(failures)}/{len(checks)} checks failed: {', '.join(failures)}")
        return 1

    print("\nAll checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
