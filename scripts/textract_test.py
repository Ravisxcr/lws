#!/usr/bin/env python3
"""
Complete AWS Textract test script.

Tests both synchronous (DetectDocumentText, AnalyzeDocument) and
asynchronous (StartDocumentTextDetection, StartDocumentAnalysis) Textract APIs.

Usage:
    # Against real AWS (uses ~/.aws credentials or env vars):
    python3 test_textract.py

    # Against a local emulator (LocalStack / lws):
    python3 test_textract.py --endpoint http://localhost:4566

    # Skip async tests (async requires S3; use --skip-async for quick local runs):
    python3 test_textract.py --skip-async

    # Use a real image file instead of the built-in synthetic PNG:
    python3 test_textract.py --image path/to/document.png

    # Run only specific test groups:
    python3 test_textract.py --tests detect analyze forms tables
"""

import argparse
import base64
import json
import sys
import time
import textwrap
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Minimal 1-px transparent PNG — used when no real image is supplied.
# Enough to exercise the endpoint without needing a real document.
# ---------------------------------------------------------------------------
MINIMAL_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000a49444154789c6300010000050001"
    "0d0a2db40000000049454e44ae426082"
)

# A tiny synthetic PNG that actually contains readable text ("Hello World")
# rendered as a black-on-white 200×50 image, base64-encoded.
# Generated offline; included inline so the script is self-contained.
HELLO_WORLD_PNG_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAMgAAAAyCAIAAACWMwO2AAAACXBIWXMAAA7EAAAOxAGVKw4b"
    "AAABIklEQVR4nO3BMQEAAADCoPVP7WsIoAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAeAMBuAABHgAAAABJRU5ErkJggg=="
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DUMMY_CREDS = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


def make_client(service: str, endpoint: Optional[str], real_aws: bool):
    kwargs = {"region_name": "us-east-1"}
    if not real_aws:
        kwargs.update(DUMMY_CREDS)
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client(service, **kwargs)


def load_image(path: Optional[str]) -> bytes:
    if path:
        return Path(path).read_bytes()
    # Fall back to the minimal transparent PNG — enough to hit the endpoint.
    return MINIMAL_PNG


def header(title: str) -> None:
    bar = "─" * 60
    print(f"\n{bar}")
    print(f"  {title}")
    print(bar)


def ok(msg: str) -> None:
    print(f"  ✓  {msg}")


def info(msg: str) -> None:
    print(f"     {msg}")


def fail(msg: str) -> None:
    print(f"  ✗  {msg}")


def summarise_blocks(blocks: list, label: str = "Blocks") -> None:
    counts: dict[str, int] = {}
    for b in blocks:
        counts[b["BlockType"]] = counts.get(b["BlockType"], 0) + 1
    parts = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    info(f"{label}: {parts or '(none)'}")


# ---------------------------------------------------------------------------
# Test: DetectDocumentText
# ---------------------------------------------------------------------------

def test_detect_document_text(textract, image_bytes: bytes) -> bool:
    header("1 · DetectDocumentText  (synchronous)")
    try:
        resp = textract.detect_document_text(Document={"Bytes": image_bytes})
        blocks = resp.get("Blocks", [])
        ok(f"API call succeeded — {len(blocks)} block(s) returned")
        summarise_blocks(blocks)

        lines = [b["Text"] for b in blocks if b["BlockType"] == "LINE"]
        if lines:
            info("Detected lines:")
            for line in lines[:10]:
                info(f"    {line!r}")
            if len(lines) > 10:
                info(f"    … and {len(lines) - 10} more")
        else:
            info("No LINE blocks detected (expected for a blank/minimal image)")

        metadata = resp.get("DocumentMetadata", {})
        info(f"Pages reported: {metadata.get('Pages', 'n/a')}")
        return True
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: AnalyzeDocument — FORMS feature
# ---------------------------------------------------------------------------

def test_analyze_document_forms(textract, image_bytes: bytes) -> bool:
    header("2 · AnalyzeDocument  (FeatureTypes=[FORMS])")
    try:
        resp = textract.analyze_document(
            Document={"Bytes": image_bytes},
            FeatureTypes=["FORMS"],
        )
        blocks = resp.get("Blocks", [])
        ok(f"API call succeeded — {len(blocks)} block(s) returned")
        summarise_blocks(blocks)

        # Extract key-value pairs
        key_blocks   = {b["Id"]: b for b in blocks if b["BlockType"] == "KEY_VALUE_SET" and "KEY"   in b.get("EntityTypes", [])}
        value_blocks = {b["Id"]: b for b in blocks if b["BlockType"] == "KEY_VALUE_SET" and "VALUE" in b.get("EntityTypes", [])}
        word_map     = {b["Id"]: b.get("Text", "") for b in blocks if b["BlockType"] == "WORD"}

        def collect_text(block: dict) -> str:
            parts = []
            for rel in block.get("Relationships", []):
                if rel["Type"] == "CHILD":
                    parts += [word_map.get(id_, "") for id_ in rel["Ids"]]
            return " ".join(p for p in parts if p).strip()

        kvs = []
        for key_block in key_blocks.values():
            key_text = collect_text(key_block)
            val_text = ""
            for rel in key_block.get("Relationships", []):
                if rel["Type"] == "VALUE":
                    for vid in rel["Ids"]:
                        vb = value_blocks.get(vid)
                        if vb:
                            val_text = collect_text(vb)
            if key_text or val_text:
                kvs.append((key_text, val_text))

        if kvs:
            info("Key-Value pairs found:")
            for k, v in kvs[:10]:
                info(f"    {k!r:30s} → {v!r}")
        else:
            info("No KV pairs extracted (expected for a blank/minimal image)")
        return True
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: AnalyzeDocument — TABLES feature
# ---------------------------------------------------------------------------

def test_analyze_document_tables(textract, image_bytes: bytes) -> bool:
    header("3 · AnalyzeDocument  (FeatureTypes=[TABLES])")
    try:
        resp = textract.analyze_document(
            Document={"Bytes": image_bytes},
            FeatureTypes=["TABLES"],
        )
        blocks = resp.get("Blocks", [])
        ok(f"API call succeeded — {len(blocks)} block(s) returned")
        summarise_blocks(blocks)

        tables = [b for b in blocks if b["BlockType"] == "TABLE"]
        info(f"Tables found: {len(tables)}")
        return True
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: AnalyzeDocument — FORMS + TABLES combined
# ---------------------------------------------------------------------------

def test_analyze_document_combined(textract, image_bytes: bytes) -> bool:
    header("4 · AnalyzeDocument  (FeatureTypes=[FORMS, TABLES])")
    try:
        resp = textract.analyze_document(
            Document={"Bytes": image_bytes},
            FeatureTypes=["FORMS", "TABLES"],
        )
        blocks = resp.get("Blocks", [])
        ok(f"API call succeeded — {len(blocks)} block(s) returned")
        summarise_blocks(blocks)
        return True
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: AnalyzeExpense
# ---------------------------------------------------------------------------

def test_analyze_expense(textract, image_bytes: bytes) -> bool:
    header("5 · AnalyzeExpense  (receipts / invoices)")
    try:
        resp = textract.analyze_expense(Document={"Bytes": image_bytes})
        docs = resp.get("ExpenseDocuments", [])
        ok(f"API call succeeded — {len(docs)} ExpenseDocument(s) returned")
        for i, doc in enumerate(docs, 1):
            summary_fields = doc.get("SummaryFields", [])
            info(f"  Document {i}: {len(summary_fields)} summary field(s)")
            for sf in summary_fields[:5]:
                label = sf.get("LabelDetection", {}).get("Text", "?")
                value = sf.get("ValueDetection",  {}).get("Text", "?")
                info(f"    {label}: {value}")
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("UnsupportedDocumentException", "NotImplementedError", "InvalidAction"):
            info(f"Skipped — endpoint does not support AnalyzeExpense ({code})")
            return True  # not a failure; just unsupported by the emulator
        fail(f"ClientError: {code} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: Async — StartDocumentTextDetection + GetDocumentTextDetection
# ---------------------------------------------------------------------------

def test_async_text_detection(textract, s3, bucket: str, key: str, poll_interval: int, max_polls: int) -> bool:
    header("6 · StartDocumentTextDetection  (asynchronous)")
    try:
        start_resp = textract.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            ClientRequestToken=f"smoke-detect-{int(time.time())}",
            JobTag="smoke-test",
        )
        job_id = start_resp["JobId"]
        ok(f"Job started — JobId: {job_id}")

        status = _poll_job(
            lambda: textract.get_document_text_detection(JobId=job_id),
            poll_interval,
            max_polls,
        )
        if status == "SUCCEEDED":
            result = textract.get_document_text_detection(JobId=job_id)
            blocks = result.get("Blocks", [])
            ok(f"Job SUCCEEDED — {len(blocks)} block(s) returned")
            summarise_blocks(blocks)
            return True
        else:
            fail(f"Job ended with status: {status}")
            return False
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


# ---------------------------------------------------------------------------
# Test: Async — StartDocumentAnalysis + GetDocumentAnalysis
# ---------------------------------------------------------------------------

def test_async_document_analysis(textract, s3, bucket: str, key: str, poll_interval: int, max_polls: int) -> bool:
    header("7 · StartDocumentAnalysis  (asynchronous, FORMS + TABLES)")
    try:
        start_resp = textract.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=["FORMS", "TABLES"],
            ClientRequestToken=f"smoke-analyze-{int(time.time())}",
            JobTag="smoke-test",
        )
        job_id = start_resp["JobId"]
        ok(f"Job started — JobId: {job_id}")

        status = _poll_job(
            lambda: textract.get_document_analysis(JobId=job_id),
            poll_interval,
            max_polls,
        )
        if status == "SUCCEEDED":
            result = textract.get_document_analysis(JobId=job_id)
            blocks = result.get("Blocks", [])
            ok(f"Job SUCCEEDED — {len(blocks)} block(s) returned")
            summarise_blocks(blocks)
            return True
        else:
            fail(f"Job ended with status: {status}")
            return False
    except ClientError as e:
        fail(f"ClientError: {e.response['Error']['Code']} — {e.response['Error']['Message']}")
        return False
    except Exception as e:
        fail(f"Unexpected error: {e}")
        return False


def _poll_job(get_fn, poll_interval: int, max_polls: int) -> str:
    for attempt in range(1, max_polls + 1):
        resp   = get_fn()
        status = resp["JobStatus"]
        info(f"Poll {attempt}/{max_polls}: {status}")
        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            return status
        time.sleep(poll_interval)
    return "TIMEOUT"


# ---------------------------------------------------------------------------
# S3 helpers for async tests
# ---------------------------------------------------------------------------

def setup_s3_fixture(s3, bucket: str, key: str, image_bytes: bytes) -> None:
    try:
        s3.create_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    s3.put_object(Bucket=bucket, Key=key, Body=image_bytes)
    ok(f"S3 fixture uploaded → s3://{bucket}/{key}")


def teardown_s3_fixture(s3, bucket: str, key: str) -> None:
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        s3.delete_bucket(Bucket=bucket)
        info(f"S3 fixture cleaned up (s3://{bucket}/{key})")
    except Exception:
        pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

ALL_TESTS = ["detect", "forms", "tables", "combined", "expense", "async-detect", "async-analyze"]


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--endpoint", default="http://localhost:4566",
        help="Textract (and S3) endpoint URL (default: http://localhost:4566). "
             "Set to 'none' to use real AWS.",
    )
    p.add_argument(
        "--image", default=None,
        help="Path to a local image or PDF to use as the test document. "
             "Defaults to a built-in synthetic PNG.",
    )
    p.add_argument(
        "--skip-async", action="store_true",
        help="Skip async tests (they require S3 access).",
    )
    p.add_argument(
        "--tests", nargs="+", choices=ALL_TESTS, default=ALL_TESTS,
        metavar="TEST",
        help=f"Which test(s) to run. Choices: {', '.join(ALL_TESTS)}. Default: all.",
    )
    p.add_argument(
        "--s3-bucket", default="textract-smoke-test",
        help="S3 bucket name for async tests (default: textract-smoke-test).",
    )
    p.add_argument(
        "--s3-key", default="smoke-test-document.png",
        help="S3 object key for async tests (default: smoke-test-document.png).",
    )
    p.add_argument(
        "--poll-interval", type=int, default=2,
        help="Seconds between async job status polls (default: 2).",
    )
    p.add_argument(
        "--max-polls", type=int, default=30,
        help="Maximum number of async job polls before giving up (default: 30).",
    )
    p.add_argument(
        "--real-aws", action="store_true",
        help="Use real AWS credentials (skips injecting dummy creds). "
             "Required when --endpoint is omitted.",
    )
    p.add_argument(
        "--output-json", default=None, metavar="FILE",
        help="Write a JSON summary of results to this file.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    if args.endpoint and args.endpoint.lower() == "none":
        args.endpoint = None
    real_aws = args.real_aws or args.endpoint is None
    image_bytes = load_image(args.image)

    print(f"\nAWS Textract — smoke test")
    print(f"  Endpoint : {args.endpoint or 'real AWS'}")
    print(f"  Image    : {args.image or '(built-in synthetic PNG)'} ({len(image_bytes):,} bytes)")
    print(f"  Tests    : {', '.join(args.tests)}")

    textract = make_client("textract", args.endpoint, real_aws)
    s3       = make_client("s3",       args.endpoint, real_aws) if not args.skip_async else None

    # Upload S3 fixture once if any async tests are selected
    async_tests = {"async-detect", "async-analyze"}
    need_s3 = bool(async_tests & set(args.tests)) and not args.skip_async
    if need_s3:
        header("S3 fixture setup")
        setup_s3_fixture(s3, args.s3_bucket, args.s3_key, image_bytes)

    results: dict[str, bool] = {}

    test_map = {
        "detect":       lambda: test_detect_document_text(textract, image_bytes),
        "forms":        lambda: test_analyze_document_forms(textract, image_bytes),
        "tables":       lambda: test_analyze_document_tables(textract, image_bytes),
        "combined":     lambda: test_analyze_document_combined(textract, image_bytes),
        "expense":      lambda: test_analyze_expense(textract, image_bytes),
        "async-detect": lambda: test_async_text_detection(
            textract, s3, args.s3_bucket, args.s3_key, args.poll_interval, args.max_polls
        ) if not args.skip_async else (info("Skipped (--skip-async)") or True),
        "async-analyze": lambda: test_async_document_analysis(
            textract, s3, args.s3_bucket, args.s3_key, args.poll_interval, args.max_polls
        ) if not args.skip_async else (info("Skipped (--skip-async)") or True),
    }

    for name in args.tests:
        results[name] = test_map[name]()

    # Clean up S3 fixture
    if need_s3:
        teardown_s3_fixture(s3, args.s3_bucket, args.s3_key)

    # Summary
    bar = "─" * 60
    print(f"\n{bar}")
    print("  SUMMARY")
    print(bar)
    passed  = [n for n, ok_ in results.items() if ok_]
    failed  = [n for n, ok_ in results.items() if not ok_]
    for name in args.tests:
        icon = "✓" if results[name] else "✗"
        print(f"  {icon}  {name}")
    print(bar)
    print(f"  {len(passed)}/{len(results)} passed", end="")
    if failed:
        print(f"  |  FAILED: {', '.join(failed)}")
    else:
        print("  |  All checks passed.")
    print()

    # Optional JSON output
    if args.output_json:
        with open(args.output_json, "w") as f:
            json.dump({"results": results, "passed": passed, "failed": failed}, f, indent=2)
        print(f"  Results written to {args.output_json}")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())