"""Exercises every operation in botocore's SQS API surface (see
https://docs.aws.amazon.com/boto3/latest/reference/services/sqs.html):
queue lifecycle, message send/receive/delete (single + batch), visibility
timeout changes, queue attributes, tags, permissions, dead-letter-queue
redrive, and message move tasks.

Run against a local emulator (LocalStack / lws):
    pytest tests/test_sqs.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_sqs.py --endpoint none --real-aws
"""

import json
import time

import pytest
from botocore.exceptions import ClientError


class TestSQSQueueLifecycle:
    """Walks a single queue through SQS's full API surface end to end.

    Tests run in file order (pytest's default) and share a class-scoped
    `state` dict, since e.g. dead-letter redrive depends on the queue and
    ARN created earlier in the class.
    """

    @pytest.fixture(scope="class")
    @classmethod
    def state(cls):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def cleanup(cls, sqs, state):
        yield
        for key in ("queue_url", "dlq_url"):
            url = state.get(key)
            if not url:
                continue
            try:
                sqs.delete_queue(QueueUrl=url)
            except ClientError:
                pass

    def test_create_queue_is_idempotent_and_discoverable(self, sqs, unique_name, state):
        queue_name = unique_name("sqs-test-queue")
        state["queue_name"] = queue_name

        queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        state["queue_url"] = queue_url

        same_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
        assert same_url == queue_url

        resolved = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
        assert resolved == queue_url

        urls = sqs.list_queues(QueueNamePrefix=queue_name)["QueueUrls"]
        assert queue_url in urls

    def test_send_receive_delete_message(self, sqs, state):
        queue_url = state["queue_url"]

        sent = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="hello from test_sqs.py",
            MessageAttributes={"Kind": {"DataType": "String", "StringValue": "greeting"}},
        )
        assert sent["MessageId"]

        received = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        assert received, "expected ReceiveMessage to return the sent message"
        msg = received[0]
        assert msg["Body"] == "hello from test_sqs.py"
        assert msg["MessageAttributes"]["Kind"]["StringValue"] == "greeting"

        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        empty = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0).get("Messages", [])
        assert not empty, "queue should be empty after DeleteMessage"

    def test_send_receive_delete_message_batch(self, sqs, state):
        queue_url = state["queue_url"]

        entries = [{"Id": str(i), "MessageBody": f"batch-message-{i}"} for i in range(3)]
        resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        assert len(resp.get("Successful", [])) == 3

        received = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
        ).get("Messages", [])
        assert len(received) == 3

        delete_entries = [{"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]} for m in received]
        del_resp = sqs.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)
        assert len(del_resp.get("Successful", [])) == 3

    def test_change_message_visibility(self, sqs, state):
        queue_url = state["queue_url"]

        sqs.send_message(QueueUrl=queue_url, MessageBody="visibility-test")
        [msg] = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)["Messages"]

        sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=30)

        sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"], VisibilityTimeout=0)
        again = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2).get("Messages", [])
        assert again, "VisibilityTimeout=0 should make the message immediately visible again"
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=again[0]["ReceiptHandle"])

    def test_change_message_visibility_batch(self, sqs, state):
        queue_url = state["queue_url"]

        for i in range(2):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"batch-visibility-{i}")
        received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=2, WaitTimeSeconds=2)["Messages"]
        entries = [
            {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 30}
            for i, m in enumerate(received)
        ]
        resp = sqs.change_message_visibility_batch(QueueUrl=queue_url, Entries=entries)
        assert len(resp.get("Successful", [])) == len(entries)

        # Make them visible again and clean up so later tests see an empty queue.
        for m in received:
            sqs.change_message_visibility(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"], VisibilityTimeout=0)
        leftovers = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
        ).get("Messages", [])
        for m in leftovers:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

    def test_get_and_set_queue_attributes(self, sqs, state):
        queue_url = state["queue_url"]

        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])["Attributes"]
        assert "QueueArn" in attrs
        state["queue_arn"] = attrs["QueueArn"]

        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "45"})
        updated = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"])["Attributes"]
        assert updated.get("VisibilityTimeout") == "45"

        # Restore the default so later tests aren't affected by a long timeout.
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "30"})

    def test_tag_list_untag_queue(self, sqs, state):
        queue_url = state["queue_url"]

        sqs.tag_queue(QueueUrl=queue_url, Tags={"env": "test", "owner": "test_sqs.py"})
        tags = sqs.list_queue_tags(QueueUrl=queue_url)["Tags"]
        assert tags.get("env") == "test"
        assert tags.get("owner") == "test_sqs.py"

        sqs.untag_queue(QueueUrl=queue_url, TagKeys=["owner"])
        tags_after = sqs.list_queue_tags(QueueUrl=queue_url).get("Tags", {})
        assert "owner" not in tags_after
        assert "env" in tags_after

    def test_add_and_remove_permission(self, sqs, state):
        queue_url = state["queue_url"]

        sqs.add_permission(
            QueueUrl=queue_url,
            Label="sqs-test-permission",
            AWSAccountIds=["000000000000"],
            Actions=["SendMessage"],
        )
        sqs.remove_permission(QueueUrl=queue_url, Label="sqs-test-permission")

        with pytest.raises(ClientError):
            sqs.remove_permission(QueueUrl=queue_url, Label="sqs-test-permission")

    def test_purge_queue(self, sqs, state):
        queue_url = state["queue_url"]

        for i in range(3):
            sqs.send_message(QueueUrl=queue_url, MessageBody=f"to-be-purged-{i}")
        sqs.purge_queue(QueueUrl=queue_url)
        remaining = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
        ).get("Messages", [])
        assert not remaining

    def test_dead_letter_queue_redrive(self, sqs, unique_name, state):
        queue_url = state["queue_url"]

        dlq_name = unique_name("sqs-test-dlq")
        dlq_url = sqs.create_queue(QueueName=dlq_name)["QueueUrl"]
        state["dlq_url"] = dlq_url
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        state["dlq_arn"] = dlq_arn

        redrive_policy = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1})
        sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={"RedrivePolicy": redrive_policy, "VisibilityTimeout": "1"},
        )

        sources = sqs.list_dead_letter_source_queues(QueueUrl=dlq_url)["queueUrls"]
        assert queue_url in sources

        sqs.send_message(QueueUrl=queue_url, MessageBody="doomed-message")
        first = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2).get("Messages", [])
        assert first, "expected to receive the message on the first attempt"

        time.sleep(2.5)  # let the 1s visibility timeout lapse past maxReceiveCount

        on_source = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2).get("Messages", [])
        on_dlq = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=2).get("Messages", [])
        assert not on_source, "message should have moved to the DLQ instead of being redelivered"
        assert on_dlq, "message did not move to the dead-letter queue after exceeding maxReceiveCount"
        sqs.delete_message(QueueUrl=dlq_url, ReceiptHandle=on_dlq[0]["ReceiptHandle"])

        # Restore the source queue's visibility timeout for the remaining tests.
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"VisibilityTimeout": "30"})

    def test_message_move_task(self, sqs, state):
        queue_url = state["queue_url"]
        queue_arn = state["queue_arn"]
        dlq_url = state["dlq_url"]
        dlq_arn = state["dlq_arn"]

        sqs.send_message(QueueUrl=dlq_url, MessageBody="redrive-me-back")

        task_handle = sqs.start_message_move_task(SourceArn=dlq_arn, DestinationArn=queue_arn)["TaskHandle"]

        tasks = sqs.list_message_move_tasks(SourceArn=dlq_arn)["Results"]
        assert any(t["TaskHandle"] == task_handle for t in tasks)

        moved = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2).get("Messages", [])
        assert moved, "message was not moved back onto the source queue"
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=moved[0]["ReceiptHandle"])

        # The emulator runs move tasks to completion synchronously, so by the
        # time we get here the task is always past the "RUNNING" window and
        # cancellation must be rejected.
        with pytest.raises(ClientError):
            sqs.cancel_message_move_task(TaskHandle=task_handle)

    def test_delete_queue(self, sqs, state):
        for key in ("queue_url", "dlq_url"):
            url = state.pop(key, None)
            if url:
                sqs.delete_queue(QueueUrl=url)

        with pytest.raises(ClientError):
            sqs.get_queue_url(QueueName=state["queue_name"])
