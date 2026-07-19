"""Tests both synchronous (DetectDocumentText, AnalyzeDocument, AnalyzeExpense)
and asynchronous (StartDocumentTextDetection, StartDocumentAnalysis) Textract
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
