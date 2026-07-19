"""Basic connectivity smoke tests for each emulated AWS service via boto3.

Run against a local emulator (LocalStack / lws):
    pytest tests/test_smoke.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_smoke.py --endpoint none --real-aws
"""


def test_sqs_round_trip(sqs, unique_name):
    queue_url = sqs.create_queue(QueueName=unique_name("smoke-test-queue"))["QueueUrl"]
    try:
        sqs.send_message(QueueUrl=queue_url, MessageBody="hello from boto3")
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1).get("Messages", [])
        assert messages and messages[0]["Body"] == "hello from boto3"
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
    finally:
        sqs.delete_queue(QueueUrl=queue_url)


def test_sns_publish(sns, unique_name):
    topic_arn = sns.create_topic(Name=unique_name("smoke-test-topic"))["TopicArn"]
    try:
        sns.publish(TopicArn=topic_arn, Message="hello from boto3")
    finally:
        sns.delete_topic(TopicArn=topic_arn)


def test_textract_detect_document_text(textract):
    # 1x1 transparent PNG - just checks the endpoint responds, not OCR accuracy.
    png_bytes = bytes.fromhex(
        "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000a49444154789c6300010000050001"
        "0d0a2db40000000049454e44ae426082"
    )
    resp = textract.detect_document_text(Document={"Bytes": png_bytes})
    assert "Blocks" in resp


def test_s3_round_trip(s3, unique_name):
    bucket = unique_name("smoke-test-bucket")
    s3.create_bucket(Bucket=bucket)
    try:
        s3.put_object(Bucket=bucket, Key="smoke-test-key", Body=b"hello from boto3")
        body = s3.get_object(Bucket=bucket, Key="smoke-test-key")["Body"].read()
        assert body == b"hello from boto3"
        s3.delete_object(Bucket=bucket, Key="smoke-test-key")
    finally:
        s3.delete_bucket(Bucket=bucket)
