"""Shared pytest fixtures for the boto3-driven integration test suite.

By default, tests run against a local emulator (lws / LocalStack) at
http://localhost:4566 using dummy credentials. Point at real AWS with
``--endpoint none --real-aws``.
"""

import uuid
from typing import Optional

import boto3
import pytest

DUMMY_CREDS = {
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
    "region_name": "us-east-1",
}


def pytest_addoption(parser):
    parser.addoption(
        "--endpoint",
        default="http://localhost:4566",
        help="AWS endpoint URL to test against (default: http://localhost:4566). "
             "Set to 'none' to use real AWS.",
    )
    parser.addoption(
        "--real-aws",
        action="store_true",
        help="Use real AWS credentials (~/.aws or env vars) instead of dummy test creds.",
    )
    parser.addoption(
        "--skip-async",
        action="store_true",
        help="Skip async Textract tests (they require S3 access).",
    )
    parser.addoption(
        "--image",
        default=None,
        help="Path to a local image/PDF to use in Textract tests instead of the built-in synthetic PNG.",
    )
    parser.addoption(
        "--callback-host",
        default="localhost",
        help="Hostname the emulator can reach this test process's local HTTP server on, for "
             "SNS http(s) subscription tests. Use 'host.docker.internal' if the emulator runs "
             "in a container that isn't on the host network.",
    )


@pytest.fixture(scope="session")
def endpoint(pytestconfig) -> Optional[str]:
    value = pytestconfig.getoption("--endpoint")
    return None if value.lower() == "none" else value


@pytest.fixture(scope="session")
def real_aws(pytestconfig, endpoint) -> bool:
    return pytestconfig.getoption("--real-aws") or endpoint is None


@pytest.fixture(scope="session")
def skip_async(pytestconfig) -> bool:
    return pytestconfig.getoption("--skip-async")


@pytest.fixture(scope="session")
def image_path(pytestconfig) -> Optional[str]:
    return pytestconfig.getoption("--image")


@pytest.fixture(scope="session")
def callback_host(pytestconfig) -> str:
    return pytestconfig.getoption("--callback-host")


@pytest.fixture(scope="session")
def aws_client(endpoint, real_aws):
    def _make(service: str):
        kwargs = {"region_name": "us-east-1"}
        if not real_aws:
            kwargs.update(DUMMY_CREDS)
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        return boto3.client(service, **kwargs)

    return _make


@pytest.fixture(scope="session")
def sqs(aws_client):
    return aws_client("sqs")


@pytest.fixture(scope="session")
def sns(aws_client):
    return aws_client("sns")


@pytest.fixture(scope="session")
def s3(aws_client):
    return aws_client("s3")


@pytest.fixture(scope="session")
def textract(aws_client):
    return aws_client("textract")


@pytest.fixture(scope="session")
def secretsmanager(aws_client):
    return aws_client("secretsmanager")


@pytest.fixture(scope="session")
def unique_name():
    """Return a factory that generates collision-free resource names."""

    def _make(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    return _make
