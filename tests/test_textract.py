"""Tests synchronous (DetectDocumentText, AnalyzeDocument, AnalyzeExpense,
AnalyzeID), asynchronous (StartDocumentTextDetection, StartDocumentAnalysis,
StartExpenseAnalysis, StartLendingAnalysis), and Adapter/tagging Textract
APIs.

Run against a local emulator (LocalStack / lws):
    pytest tests/test_textract.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_textract.py --endpoint none --real-aws

Skip the async tests (they require S3):
    pytest tests/test_textract.py --skip-async

Use a real image file instead of the built-in synthetic PNG:
    pytest tests/test_textract.py --image path/to/document.png
"""

import json
import time
from pathlib import Path

import pytest

# Minimal 1-px transparent PNG — enough to exercise the endpoint without
# needing a real document.
MINIMAL_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000a49444154789c6300010000050001"
    "0d0a2db40000000049454e44ae426082"
)


@pytest.fixture(scope="session")
def image_bytes(image_path):
    if image_path:
        return Path(image_path).read_bytes()
    return MINIMAL_PNG


def _poll_job(get_fn, poll_interval: int = 2, max_polls: int = 30) -> str:
    for _ in range(max_polls):
        status = get_fn()["JobStatus"]
        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            return status
        time.sleep(poll_interval)
    return "TIMEOUT"


def test_detect_document_text(textract, image_bytes):
    resp = textract.detect_document_text(Document={"Bytes": image_bytes})
    assert "Blocks" in resp
    assert "DocumentMetadata" in resp


def test_analyze_document_forms(textract, image_bytes):
    resp = textract.analyze_document(Document={"Bytes": image_bytes}, FeatureTypes=["FORMS"])
    assert "Blocks" in resp


def test_analyze_document_tables(textract, image_bytes):
    resp = textract.analyze_document(Document={"Bytes": image_bytes}, FeatureTypes=["TABLES"])
    assert "Blocks" in resp


def test_analyze_document_forms_and_tables(textract, image_bytes):
    resp = textract.analyze_document(Document={"Bytes": image_bytes}, FeatureTypes=["FORMS", "TABLES"])
    assert "Blocks" in resp


def test_analyze_expense(textract, image_bytes):
    resp = textract.analyze_expense(Document={"Bytes": image_bytes})
    assert "ExpenseDocuments" in resp


def test_analyze_id(textract, image_bytes):
    resp = textract.analyze_id(DocumentPages=[{"Bytes": image_bytes}])
    assert "IdentityDocuments" in resp
    assert resp["IdentityDocuments"][0]["DocumentIndex"] == 1


class TestAsyncTextract:
    """StartDocumentTextDetection / StartDocumentAnalysis, which read from S3."""

    @pytest.fixture(scope="class")
    @classmethod
    def s3_object(cls, s3, unique_name, image_bytes, skip_async):
        if skip_async:
            pytest.skip("async Textract tests require S3 (--skip-async set)")
        bucket = unique_name("textract-test-bucket")
        key = "test-document.png"
        s3.create_bucket(Bucket=bucket)
        s3.put_object(Bucket=bucket, Key=key, Body=image_bytes)
        yield bucket, key
        s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)

    def test_start_document_text_detection(self, textract, s3_object):
        bucket, key = s3_object
        start_resp = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            ClientRequestToken=f"test-detect-{int(time.time())}",
            JobTag="pytest",
        )
        job_id = start_resp["JobId"]

        status = _poll_job(lambda: textract.get_document_text_detection(JobId=job_id))
        assert status == "SUCCEEDED", f"job ended with status {status}"

        result = textract.get_document_text_detection(JobId=job_id)
        assert "Blocks" in result

    def test_start_document_analysis(self, textract, s3_object):
        bucket, key = s3_object
        start_resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["FORMS", "TABLES"],
            ClientRequestToken=f"test-analyze-{int(time.time())}",
            JobTag="pytest",
        )
        job_id = start_resp["JobId"]

        status = _poll_job(lambda: textract.get_document_analysis(JobId=job_id))
        assert status == "SUCCEEDED", f"job ended with status {status}"

        result = textract.get_document_analysis(JobId=job_id)
        assert "Blocks" in result

    def test_start_expense_analysis(self, textract, s3_object):
        bucket, key = s3_object
        start_resp = textract.start_expense_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            ClientRequestToken=f"test-expense-{int(time.time())}",
            JobTag="pytest",
        )
        job_id = start_resp["JobId"]

        status = _poll_job(lambda: textract.get_expense_analysis(JobId=job_id))
        assert status == "SUCCEEDED", f"job ended with status {status}"

        result = textract.get_expense_analysis(JobId=job_id)
        assert "ExpenseDocuments" in result

    def test_start_lending_analysis(self, textract, s3_object):
        bucket, key = s3_object
        start_resp = textract.start_lending_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            ClientRequestToken=f"test-lending-{int(time.time())}",
            JobTag="pytest",
        )
        job_id = start_resp["JobId"]

        status = _poll_job(lambda: textract.get_lending_analysis(JobId=job_id))
        assert status == "SUCCEEDED", f"job ended with status {status}"

        result = textract.get_lending_analysis(JobId=job_id)
        assert "Results" in result

        summary = textract.get_lending_analysis_summary(JobId=job_id)
        assert "Summary" in summary
        assert "DocumentGroups" in summary["Summary"]

    def test_start_document_analysis_with_sns_notification(self, textract, sns, sqs, unique_name, s3_object):
        """StartDocumentAnalysis's NotificationChannel should fire an SNS
        completion notification, independently pollable from the job itself
        via GetDocumentAnalysis."""
        bucket, key = s3_object

        topic_arn = sns.create_topic(Name=unique_name("textract-notify-topic"))["TopicArn"]
        queue_url = sqs.create_queue(QueueName=unique_name("textract-notify-queue"))["QueueUrl"]
        try:
            queue_arn = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

            start_resp = textract.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
                FeatureTypes=["FORMS"],
                JobTag="sns-notify-test",
                NotificationChannel={
                    "RoleArn": "arn:aws:iam::000000000000:role/textract-role",
                    "SNSTopicArn": topic_arn,
                },
            )
            job_id = start_resp["JobId"]

            # Poll the job directly, same as any async caller would.
            status = _poll_job(lambda: textract.get_document_analysis(JobId=job_id))
            assert status == "SUCCEEDED", f"job ended with status {status}"

            # Separately, poll SQS for the SNS notification the Start call
            # fires once the job settles.
            notification = None
            for _ in range(10):
                messages = sqs.receive_message(
                    QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2
                ).get("Messages", [])
                if messages:
                    notification = json.loads(messages[0]["Body"])
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]["ReceiptHandle"])
                    break
            assert notification is not None, "no SNS notification received on the subscribed queue"

            message = json.loads(notification["Message"])
            assert message["JobId"] == job_id
            assert message["Status"] == "SUCCEEDED"
            assert message["API"] == "StartDocumentAnalysis"
            assert message["JobTag"] == "sns-notify-test"
        finally:
            sqs.delete_queue(QueueUrl=queue_url)
            sns.delete_topic(TopicArn=topic_arn)


class TestAdapters:
    """CreateAdapter/GetAdapter/UpdateAdapter/DeleteAdapter/ListAdapters and
    their nested Version + tagging operations. No real training happens —
    these exercise the emulator's in-memory CRUD/metadata behavior only."""

    def test_adapter_lifecycle(self, textract, unique_name):
        name = unique_name("adapter")
        created = textract.create_adapter(AdapterName=name, FeatureTypes=["QUERIES"])
        adapter_id = created["AdapterId"]

        got = textract.get_adapter(AdapterId=adapter_id)
        assert got["AdapterName"] == name

        listed = textract.list_adapters()
        assert any(a["AdapterId"] == adapter_id for a in listed["Adapters"])

        updated = textract.update_adapter(AdapterId=adapter_id, Description="updated")
        assert updated["Description"] == "updated"

        version = textract.create_adapter_version(
            AdapterId=adapter_id,
            DatasetConfig={"ManifestS3Object": {"Bucket": "bucket", "Name": "manifest.json"}},
            OutputConfig={"S3Bucket": "bucket", "S3Prefix": "output/"},
        )
        adapter_version = version["AdapterVersion"]

        got_version = textract.get_adapter_version(AdapterId=adapter_id, AdapterVersion=adapter_version)
        assert got_version["Status"] == "ACTIVE"

        listed_versions = textract.list_adapter_versions(AdapterId=adapter_id)
        assert any(v["AdapterVersion"] == adapter_version for v in listed_versions["AdapterVersions"])

        arn = f"arn:aws:textract:us-east-1:000000000000:adapter/{adapter_id}"
        textract.tag_resource(ResourceARN=arn, Tags={"env": "test"})
        tags = textract.list_tags_for_resource(ResourceARN=arn)
        assert tags["Tags"] == {"env": "test"}
        textract.untag_resource(ResourceARN=arn, TagKeys=["env"])
        tags = textract.list_tags_for_resource(ResourceARN=arn)
        assert tags.get("Tags", {}) == {}

        textract.delete_adapter_version(AdapterId=adapter_id, AdapterVersion=adapter_version)
        textract.delete_adapter(AdapterId=adapter_id)
