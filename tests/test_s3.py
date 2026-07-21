"""Exercises every S3 operation this emulator implements: bucket lifecycle,
object CRUD, listing (v1 + v2, prefixes/delimiters/pagination), copy,
tagging, multipart upload, batch delete, and bucket event notifications
(QueueConfiguration/TopicConfiguration).

Run against a local emulator (LocalStack / lws):
    pytest tests/test_s3.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_s3.py --endpoint none --real-aws
"""

import json
import time

import pytest
from botocore.exceptions import ClientError


class TestBucketLifecycle:
    def test_create_head_list_delete_bucket(self, s3, unique_name):
        bucket = unique_name("s3-test-bucket")

        s3.create_bucket(Bucket=bucket)
        s3.head_bucket(Bucket=bucket)

        names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        assert bucket in names

        s3.get_bucket_location(Bucket=bucket)  # just confirm it doesn't error

        s3.delete_bucket(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket=bucket)

    def test_head_bucket_nonexistent(self, s3, unique_name):
        with pytest.raises(ClientError):
            s3.head_bucket(Bucket=unique_name("nonexistent-bucket"))

    def test_delete_bucket_nonexistent(self, s3, unique_name):
        with pytest.raises(ClientError) as exc_info:
            s3.delete_bucket(Bucket=unique_name("nonexistent-bucket"))
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_delete_bucket_not_empty(self, s3, unique_name):
        bucket = unique_name("s3-test-nonempty-bucket")
        s3.create_bucket(Bucket=bucket)
        try:
            s3.put_object(Bucket=bucket, Key="k", Body=b"data")
            with pytest.raises(ClientError) as exc_info:
                s3.delete_bucket(Bucket=bucket)
            assert exc_info.value.response["Error"]["Code"] == "BucketNotEmpty"
        finally:
            s3.delete_object(Bucket=bucket, Key="k")
            s3.delete_bucket(Bucket=bucket)


class TestObjectCRUD:
    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-object-bucket")
        s3.create_bucket(Bucket=name)
        yield name
        objs = s3.list_objects_v2(Bucket=name).get("Contents", [])
        for o in objs:
            s3.delete_object(Bucket=name, Key=o["Key"])
        s3.delete_bucket(Bucket=name)

    def test_put_get_head_delete_object(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="hello.txt",
            Body=b"hello world",
            ContentType="text/plain",
            Metadata={"author": "pytest"},
        )

        got = s3.get_object(Bucket=bucket, Key="hello.txt")
        assert got["Body"].read() == b"hello world"
        assert got["ContentType"] == "text/plain"
        assert got["Metadata"]["author"] == "pytest"

        head = s3.head_object(Bucket=bucket, Key="hello.txt")
        assert head["ContentType"] == "text/plain"
        assert head["Metadata"]["author"] == "pytest"

        s3.delete_object(Bucket=bucket, Key="hello.txt")
        with pytest.raises(ClientError) as exc_info:
            s3.get_object(Bucket=bucket, Key="hello.txt")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_delete_objects_batch(self, s3, bucket):
        keys = ["batch/a", "batch/b", "batch/c"]
        for k in keys:
            s3.put_object(Bucket=bucket, Key=k, Body=b"x")

        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]})
        assert {d["Key"] for d in resp.get("Deleted", [])} == set(keys)

        remaining = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        assert not remaining


class TestListing:
    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-listing-bucket")
        s3.create_bucket(Bucket=name)
        for key in ["a", "b", "c", "dir/x", "dir/y", "dir2/z"]:
            s3.put_object(Bucket=name, Key=key, Body=b"x")
        yield name
        for o in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=o["Key"])
        s3.delete_bucket(Bucket=name)

    def test_list_objects_v2_prefix_and_delimiter(self, s3, bucket):
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="dir", Delimiter="/")
        prefixes = {p["Prefix"] for p in resp.get("CommonPrefixes", [])}
        assert prefixes == {"dir/", "dir2/"}

        resp = s3.list_objects_v2(Bucket=bucket, Prefix="dir/", Delimiter="/")
        keys = {c["Key"] for c in resp.get("Contents", [])}
        assert keys == {"dir/x", "dir/y"}

    def test_list_objects_v2_pagination(self, s3, bucket):
        page1 = s3.list_objects_v2(Bucket=bucket, MaxKeys=2)
        assert len(page1["Contents"]) == 2
        assert page1["IsTruncated"] is True
        token = page1["NextContinuationToken"]

        page2 = s3.list_objects_v2(Bucket=bucket, MaxKeys=2, ContinuationToken=token)
        assert len(page2["Contents"]) > 0
        assert {c["Key"] for c in page1["Contents"]}.isdisjoint(
            {c["Key"] for c in page2["Contents"]}
        )

    def test_list_objects_v1_marker_pagination(self, s3, bucket):
        page1 = s3.list_objects(Bucket=bucket, MaxKeys=2)
        assert len(page1["Contents"]) == 2
        assert page1["IsTruncated"] is True
        marker = page1["NextMarker"]

        page2 = s3.list_objects(Bucket=bucket, MaxKeys=2, Marker=marker)
        assert len(page2["Contents"]) > 0


class TestCopyObject:
    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-copy-bucket")
        s3.create_bucket(Bucket=name)
        yield name
        for o in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=o["Key"])
        s3.delete_bucket(Bucket=name)

    def test_copy_object_default_directive(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="src", Body=b"copy me", ContentType="text/plain",
            Metadata={"tag": "original"},
        )
        s3.copy_object(Bucket=bucket, Key="dst", CopySource={"Bucket": bucket, "Key": "src"})

        dst = s3.get_object(Bucket=bucket, Key="dst")
        assert dst["Body"].read() == b"copy me"
        assert dst["Metadata"]["tag"] == "original"

    def test_copy_object_replace_directive(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="src2", Body=b"copy me too", ContentType="text/plain",
            Metadata={"tag": "original"},
        )
        s3.copy_object(
            Bucket=bucket,
            Key="dst2",
            CopySource={"Bucket": bucket, "Key": "src2"},
            MetadataDirective="REPLACE",
            Metadata={"tag": "replaced"},
            ContentType="application/json",
        )

        dst = s3.get_object(Bucket=bucket, Key="dst2")
        assert dst["Body"].read() == b"copy me too"
        assert dst["Metadata"]["tag"] == "replaced"
        assert dst["ContentType"] == "application/json"


class TestObjectTagging:
    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-tagging-bucket")
        s3.create_bucket(Bucket=name)
        s3.put_object(Bucket=name, Key="tagged", Body=b"x")
        yield name
        s3.delete_object(Bucket=name, Key="tagged")
        s3.delete_bucket(Bucket=name)

    def test_put_get_delete_object_tagging(self, s3, bucket):
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged",
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )

        tags = s3.get_object_tagging(Bucket=bucket, Key="tagged")["TagSet"]
        assert {"Key": "env", "Value": "test"} in tags

        s3.delete_object_tagging(Bucket=bucket, Key="tagged")
        tags_after = s3.get_object_tagging(Bucket=bucket, Key="tagged")["TagSet"]
        assert tags_after == []


class TestMultipartUpload:
    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-multipart-bucket")
        s3.create_bucket(Bucket=name)
        yield name
        for o in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=o["Key"])
        s3.delete_bucket(Bucket=name)

    def test_multipart_upload_complete(self, s3, bucket):
        key = "multipart/complete"
        upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

        part1 = b"a" * (5 * 1024 * 1024)  # S3 requires >=5MiB for non-final parts
        part2 = b"b" * 1024

        etag1 = s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=part1
        )["ETag"]
        etag2 = s3.upload_part(
            Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=part2
        )["ETag"]

        listed = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)["Parts"]
        assert [p["PartNumber"] for p in listed] == [1, 2]

        uploads = s3.list_multipart_uploads(Bucket=bucket)["Uploads"]
        assert any(u["UploadId"] == upload_id for u in uploads)

        s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": etag1},
                    {"PartNumber": 2, "ETag": etag2},
                ]
            },
        )

        obj = s3.get_object(Bucket=bucket, Key=key)
        assert obj["Body"].read() == part1 + part2
        assert "-2" in obj["ETag"]

    def test_multipart_upload_abort(self, s3, bucket):
        key = "multipart/aborted"
        upload_id = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"data")

        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

        uploads = s3.list_multipart_uploads(Bucket=bucket).get("Uploads", [])
        assert not any(u["UploadId"] == upload_id for u in uploads)


class TestBucketNotifications:
    """put/get_bucket_notification_configuration with SQS and SNS targets,
    plus s3:ObjectRemoved and Filter.S3Key prefix/suffix matching."""

    @pytest.fixture
    def bucket(self, s3, unique_name):
        name = unique_name("s3-test-notify-bucket")
        s3.create_bucket(Bucket=name)
        yield name
        for o in s3.list_objects_v2(Bucket=name).get("Contents", []):
            s3.delete_object(Bucket=name, Key=o["Key"])
        s3.put_bucket_notification_configuration(Bucket=name, NotificationConfiguration={})
        s3.delete_bucket(Bucket=name)

    def _drain(self, sqs, queue_url, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            messages = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=2
            ).get("Messages", [])
            if messages:
                return messages[0]
        return None

    def test_queue_configuration_object_created(self, s3, sqs, bucket, unique_name):
        queue_url = sqs.create_queue(QueueName=unique_name("s3-notify-queue"))["QueueUrl"]
        try:
            queue_arn = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]

            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "QueueConfigurations": [
                        {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:*"]}
                    ]
                },
            )

            got = s3.get_bucket_notification_configuration(Bucket=bucket)
            assert got["QueueConfigurations"][0]["QueueArn"] == queue_arn

            s3.put_object(Bucket=bucket, Key="triggers/notify.txt", Body=b"x")

            msg = self._drain(sqs, queue_url)
            assert msg is not None, "expected a notification on the queue after put_object"
            body = json.loads(msg["Body"])
            record = body["Records"][0]
            assert record["eventName"] == "ObjectCreated:Put"
            assert record["s3"]["bucket"]["name"] == bucket
            assert record["s3"]["object"]["key"] == "triggers/notify.txt"
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        finally:
            sqs.delete_queue(QueueUrl=queue_url)

    def test_queue_configuration_object_removed(self, s3, sqs, bucket, unique_name):
        queue_url = sqs.create_queue(QueueName=unique_name("s3-notify-remove-queue"))["QueueUrl"]
        try:
            queue_arn = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]

            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "QueueConfigurations": [
                        {"QueueArn": queue_arn, "Events": ["s3:ObjectRemoved:*"]}
                    ]
                },
            )

            s3.put_object(Bucket=bucket, Key="to-delete.txt", Body=b"x")
            assert self._drain(sqs, queue_url, timeout=3) is None  # Put shouldn't match Removed

            s3.delete_object(Bucket=bucket, Key="to-delete.txt")
            msg = self._drain(sqs, queue_url)
            assert msg is not None, "expected a notification on the queue after delete_object"
            record = json.loads(msg["Body"])["Records"][0]
            assert record["eventName"] == "ObjectRemoved:Delete"
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        finally:
            sqs.delete_queue(QueueUrl=queue_url)

    def test_topic_configuration_object_created(self, s3, sns, sqs, bucket, unique_name):
        topic_arn = sns.create_topic(Name=unique_name("s3-notify-topic"))["TopicArn"]
        queue_url = sqs.create_queue(QueueName=unique_name("s3-notify-topic-queue"))["QueueUrl"]
        try:
            queue_arn = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "TopicConfigurations": [
                        {"TopicArn": topic_arn, "Events": ["s3:ObjectCreated:*"]}
                    ]
                },
            )

            s3.put_object(Bucket=bucket, Key="via-sns.txt", Body=b"x")

            msg = self._drain(sqs, queue_url)
            assert msg is not None, "expected the SNS-forwarded notification on the queue"
            notification = json.loads(msg["Body"])
            record = json.loads(notification["Message"])["Records"][0]
            assert record["eventName"] == "ObjectCreated:Put"
            assert record["s3"]["object"]["key"] == "via-sns.txt"
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        finally:
            sqs.delete_queue(QueueUrl=queue_url)
            sns.delete_topic(TopicArn=topic_arn)

    def test_filter_prefix_and_suffix(self, s3, sqs, bucket, unique_name):
        queue_url = sqs.create_queue(QueueName=unique_name("s3-notify-filter-queue"))["QueueUrl"]
        try:
            queue_arn = sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]

            s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={
                    "QueueConfigurations": [
                        {
                            "QueueArn": queue_arn,
                            "Events": ["s3:ObjectCreated:*"],
                            "Filter": {
                                "Key": {
                                    "FilterRules": [
                                        {"Name": "prefix", "Value": "images/"},
                                        {"Name": "suffix", "Value": ".png"},
                                    ]
                                }
                            },
                        }
                    ]
                },
            )

            s3.put_object(Bucket=bucket, Key="images/not-a-match.txt", Body=b"x")
            assert self._drain(sqs, queue_url, timeout=3) is None, "non-matching key should not notify"

            s3.put_object(Bucket=bucket, Key="images/photo.png", Body=b"x")
            msg = self._drain(sqs, queue_url)
            assert msg is not None, "matching key should notify"
            record = json.loads(msg["Body"])["Records"][0]
            assert record["s3"]["object"]["key"] == "images/photo.png"
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
        finally:
            sqs.delete_queue(QueueUrl=queue_url)
