"""Exercises botocore's SNS API surface (see
https://docs.aws.amazon.com/boto3/latest/reference/services/sns.html):
topic lifecycle, attributes, tags, permissions, subscription lifecycle
(including the confirm-before-delivery handshake for endpoints that require
it), and Publish/PublishBatch fan-out to both SQS and HTTP(S) subscribers.

Mobile push (platform applications/endpoints) and SMS/phone-number
operations are out of scope: they model real device tokens and carrier
delivery that this local emulator has no meaningful way to simulate.

Run against a local emulator (LocalStack / lws):
    pytest tests/test_sns.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_sns.py --endpoint none --real-aws
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from botocore.exceptions import ClientError


class TestSNSTopicLifecycle:
    """Walks a single topic through SNS's topic-level API surface: create,
    attributes, tags, permissions, list, delete.

    Tests run in file order (pytest's default) and share a class-scoped
    `state` dict, since e.g. attribute checks depend on the topic created
    earlier in the class.
    """

    @pytest.fixture(scope="class")
    @classmethod
    def state(cls):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def cleanup(cls, sns, state):
        yield
        arn = state.get("topic_arn")
        if arn:
            try:
                sns.delete_topic(TopicArn=arn)
            except ClientError:
                pass

    def test_create_topic_is_idempotent_and_discoverable(self, sns, unique_name, state):
        topic_name = unique_name("sns-test-topic")
        state["topic_name"] = topic_name

        topic_arn = sns.create_topic(Name=topic_name)["TopicArn"]
        state["topic_arn"] = topic_arn

        same_arn = sns.create_topic(Name=topic_name)["TopicArn"]
        assert same_arn == topic_arn

        arns = [t["TopicArn"] for t in sns.list_topics()["Topics"]]
        assert topic_arn in arns

    def test_get_and_set_topic_attributes(self, sns, state):
        topic_arn = state["topic_arn"]

        attrs = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        assert attrs["TopicArn"] == topic_arn
        assert attrs["SubscriptionsConfirmed"] == "0"

        sns.set_topic_attributes(TopicArn=topic_arn, AttributeName="DisplayName", AttributeValue="lws test topic")
        updated = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
        assert updated["DisplayName"] == "lws test topic"

    def test_tag_list_untag_resource(self, sns, state):
        topic_arn = state["topic_arn"]

        sns.tag_resource(ResourceArn=topic_arn, Tags=[{"Key": "env", "Value": "test"}, {"Key": "owner", "Value": "test_sns.py"}])
        tags = {t["Key"]: t["Value"] for t in sns.list_tags_for_resource(ResourceArn=topic_arn)["Tags"]}
        assert tags.get("env") == "test"
        assert tags.get("owner") == "test_sns.py"

        sns.untag_resource(ResourceArn=topic_arn, TagKeys=["owner"])
        tags_after = {t["Key"]: t["Value"] for t in sns.list_tags_for_resource(ResourceArn=topic_arn)["Tags"]}
        assert "owner" not in tags_after
        assert "env" in tags_after

    def test_add_and_remove_permission(self, sns, state):
        topic_arn = state["topic_arn"]

        sns.add_permission(
            TopicArn=topic_arn,
            Label="sns-test-permission",
            AWSAccountId=["000000000000"],
            ActionName=["Publish"],
        )
        sns.remove_permission(TopicArn=topic_arn, Label="sns-test-permission")

        with pytest.raises(ClientError):
            sns.remove_permission(TopicArn=topic_arn, Label="sns-test-permission")

    def test_delete_topic(self, sns, state):
        topic_arn = state.pop("topic_arn")
        sns.delete_topic(TopicArn=topic_arn)

        with pytest.raises(ClientError):
            sns.get_topic_attributes(TopicArn=topic_arn)


class TestSNSToSQSFanOut:
    """Walks an SNS topic subscribed to an SQS queue through Subscribe,
    Publish/PublishBatch delivery (both wrapped and RawMessageDelivery),
    subscription attributes, ListSubscriptions(ByTopic), and Unsubscribe.
    """

    @pytest.fixture(scope="class")
    @classmethod
    def state(cls):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def cleanup(cls, sns, sqs, state):
        yield
        sub_arn = state.get("subscription_arn")
        if sub_arn:
            try:
                sns.unsubscribe(SubscriptionArn=sub_arn)
            except ClientError:
                pass
        queue_url = state.get("queue_url")
        if queue_url:
            try:
                sqs.delete_queue(QueueUrl=queue_url)
            except ClientError:
                pass
        topic_arn = state.get("topic_arn")
        if topic_arn:
            try:
                sns.delete_topic(TopicArn=topic_arn)
            except ClientError:
                pass

    def test_subscribe_sqs_is_auto_confirmed(self, sns, sqs, unique_name, state):
        topic_arn = sns.create_topic(Name=unique_name("sns-fanout-topic"))["TopicArn"]
        state["topic_arn"] = topic_arn

        queue_url = sqs.create_queue(QueueName=unique_name("sns-fanout-queue"))["QueueUrl"]
        state["queue_url"] = queue_url
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

        sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)["SubscriptionArn"]
        assert sub_arn != "PendingConfirmation"
        state["subscription_arn"] = sub_arn

        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert attrs["PendingConfirmation"] == "false"
        assert attrs["TopicArn"] == topic_arn
        assert attrs["Protocol"] == "sqs"

    def test_list_subscriptions(self, sns, state):
        topic_arn = state["topic_arn"]
        sub_arn = state["subscription_arn"]

        by_topic = [s["SubscriptionArn"] for s in sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]]
        assert sub_arn in by_topic

        all_subs = [s["SubscriptionArn"] for s in sns.list_subscriptions()["Subscriptions"]]
        assert sub_arn in all_subs

    def test_publish_delivers_wrapped_envelope(self, sns, sqs, state):
        topic_arn = state["topic_arn"]
        queue_url = state["queue_url"]

        sns.publish(TopicArn=topic_arn, Message="hello from test_sns.py", Subject="greeting")

        [msg] = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)["Messages"]
        envelope = json.loads(msg["Body"])
        assert envelope["Type"] == "Notification"
        assert envelope["Message"] == "hello from test_sns.py"
        assert envelope["Subject"] == "greeting"
        assert envelope["TopicArn"] == topic_arn
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

    def test_raw_message_delivery(self, sns, sqs, state):
        sub_arn = state["subscription_arn"]
        queue_url = state["queue_url"]

        sns.set_subscription_attributes(SubscriptionArn=sub_arn, AttributeName="RawMessageDelivery", AttributeValue="true")
        attrs = sns.get_subscription_attributes(SubscriptionArn=sub_arn)["Attributes"]
        assert attrs["RawMessageDelivery"] == "true"

        sns.publish(TopicArn=state["topic_arn"], Message="raw body, no envelope")

        [msg] = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)["Messages"]
        assert msg["Body"] == "raw body, no envelope"
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])

        # Restore wrapped delivery for later tests.
        sns.set_subscription_attributes(SubscriptionArn=sub_arn, AttributeName="RawMessageDelivery", AttributeValue="false")

    def test_publish_batch(self, sns, sqs, state):
        topic_arn = state["topic_arn"]
        queue_url = state["queue_url"]

        entries = [{"Id": str(i), "Message": f"batch-message-{i}"} for i in range(3)]
        resp = sns.publish_batch(TopicArn=topic_arn, PublishBatchRequestEntries=entries)
        assert len(resp.get("Successful", [])) == 3

        received = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2).get("Messages", [])
        assert len(received) == 3
        bodies = {json.loads(m["Body"])["Message"] for m in received}
        assert bodies == {"batch-message-0", "batch-message-1", "batch-message-2"}
        for m in received:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])

    def test_unsubscribe(self, sns, state):
        sub_arn = state.pop("subscription_arn")
        sns.unsubscribe(SubscriptionArn=sub_arn)

        with pytest.raises(ClientError):
            sns.get_subscription_attributes(SubscriptionArn=sub_arn)

        by_topic = [s["SubscriptionArn"] for s in sns.list_subscriptions_by_topic(TopicArn=state["topic_arn"])["Subscriptions"]]
        assert sub_arn not in by_topic


class _CapturingHandler(BaseHTTPRequestHandler):
    """Minimal HTTP endpoint that records every POST body it receives, so
    tests can inspect the SubscriptionConfirmation/Notification control
    messages SNS sends to http(s) subscribers.
    """

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        self.server.received.append(json.loads(body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, fmt, *args):
        pass  # silence default request logging


@pytest.fixture
def http_endpoint():
    """Starts a local HTTP server the emulator can deliver SNS control/data
    messages to, and returns its base URL plus the list of captured bodies.
    """
    server = HTTPServer(("0.0.0.0", 0), _CapturingHandler)
    server.received = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _wait_for(predicate, timeout=10, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class TestSNSHTTPSubscriptionConfirmation:
    """The http/https protocol is the one SNS protocol that requires the
    subscriber to actively confirm before delivery starts: Subscribe sends
    a SubscriptionConfirmation control message (carrying a Token) to the
    endpoint out-of-band, and only ConfirmSubscription(Token=...) flips the
    subscription to Confirmed. This walks that full handshake, then
    verifies Publish delivers a Notification to the now-confirmed endpoint.
    """

    def test_subscribe_confirm_publish_unsubscribe(self, sns, unique_name, http_endpoint, callback_host):
        topic_arn = sns.create_topic(Name=unique_name("sns-http-topic"))["TopicArn"]
        try:
            endpoint_url = f"http://{callback_host}:{http_endpoint.server_port}/"

            sub_arn = sns.subscribe(TopicArn=topic_arn, Protocol="http", Endpoint=endpoint_url)["SubscriptionArn"]
            assert sub_arn == "PendingConfirmation"

            assert _wait_for(lambda: len(http_endpoint.received) >= 1), "SubscriptionConfirmation was never delivered"
            confirmation = http_endpoint.received[0]
            assert confirmation["Type"] == "SubscriptionConfirmation"
            assert confirmation["TopicArn"] == topic_arn
            token = confirmation["Token"]

            confirmed_arn = sns.confirm_subscription(TopicArn=topic_arn, Token=token)["SubscriptionArn"]
            assert confirmed_arn != "PendingConfirmation"

            attrs = sns.get_subscription_attributes(SubscriptionArn=confirmed_arn)["Attributes"]
            assert attrs["PendingConfirmation"] == "false"

            subs = [s["SubscriptionArn"] for s in sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]]
            assert confirmed_arn in subs

            sns.publish(TopicArn=topic_arn, Message="hello over http", Subject="greeting")
            assert _wait_for(lambda: len(http_endpoint.received) >= 2), "Notification was never delivered"
            notification = http_endpoint.received[1]
            assert notification["Type"] == "Notification"
            assert notification["Message"] == "hello over http"

            sns.unsubscribe(SubscriptionArn=confirmed_arn)
            with pytest.raises(ClientError):
                sns.get_subscription_attributes(SubscriptionArn=confirmed_arn)
        finally:
            sns.delete_topic(TopicArn=topic_arn)
