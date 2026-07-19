"""Exercises botocore's Secrets Manager API surface (see
https://docs.aws.amazon.com/boto3/latest/guide/secrets-manager.html):
secret lifecycle (create/describe/list/update/delete/restore), versioned
values (get/put, including AWSCURRENT/AWSPREVIOUS stage rotation and
ClientRequestToken idempotency), and tags.

Rotation (RotateSecret / Lambda-backed rotation) and cross-region
replication are out of scope: they model infrastructure (a rotation
Lambda, secondary regions) this local emulator has no meaningful way to
simulate.

Run against a local emulator (LocalStack / lws):
    pytest tests/test_secretsmanager.py

Run against real AWS (uses ~/.aws credentials or env vars):
    pytest tests/test_secretsmanager.py --endpoint none --real-aws
"""

import pytest
from botocore.exceptions import ClientError


class TestSecretLifecycle:
    """Walks a single secret through Secrets Manager's full API surface:
    create, describe, get/put value (with version-stage rotation), update,
    tags, list, delete, restore, and permanent deletion.

    Tests run in file order (pytest's default) and share a class-scoped
    `state` dict, since e.g. version checks depend on the secret created
    earlier in the class.
    """

    @pytest.fixture(scope="class")
    @classmethod
    def state(cls):
        return {}

    @pytest.fixture(scope="class", autouse=True)
    @classmethod
    def cleanup(cls, secretsmanager, state):
        yield
        name = state.get("secret_name")
        if name:
            try:
                secretsmanager.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
            except ClientError:
                pass

    def test_create_secret_is_discoverable(self, secretsmanager, unique_name, state):
        secret_name = unique_name("sm-test-secret")
        state["secret_name"] = secret_name

        created = secretsmanager.create_secret(
            Name=secret_name,
            Description="lws test secret",
            SecretString="s3cr3t-v1",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        state["arn"] = created["ARN"]
        state["version_id_1"] = created["VersionId"]
        assert created["Name"] == secret_name

        with pytest.raises(ClientError) as exc_info:
            secretsmanager.create_secret(Name=secret_name, SecretString="dup")
        assert exc_info.value.response["Error"]["Code"] == "ResourceExistsException"

    def test_get_secret_value_returns_current_version(self, secretsmanager, state):
        resp = secretsmanager.get_secret_value(SecretId=state["secret_name"])
        assert resp["SecretString"] == "s3cr3t-v1"
        assert resp["VersionId"] == state["version_id_1"]
        assert "AWSCURRENT" in resp["VersionStages"]

        by_arn = secretsmanager.get_secret_value(SecretId=state["arn"])
        assert by_arn["SecretString"] == "s3cr3t-v1"

    def test_describe_secret(self, secretsmanager, state):
        desc = secretsmanager.describe_secret(SecretId=state["secret_name"])
        assert desc["ARN"] == state["arn"]
        assert desc["Name"] == state["secret_name"]
        assert desc["Description"] == "lws test secret"
        assert state["version_id_1"] in desc["VersionIdsToStages"]
        assert "AWSCURRENT" in desc["VersionIdsToStages"][state["version_id_1"]]

    def test_put_secret_value_creates_new_current_version_and_demotes_previous(self, secretsmanager, state):
        put = secretsmanager.put_secret_value(SecretId=state["secret_name"], SecretString="s3cr3t-v2")
        state["version_id_2"] = put["VersionId"]
        assert put["VersionId"] != state["version_id_1"]
        assert "AWSCURRENT" in put["VersionStages"]

        current = secretsmanager.get_secret_value(SecretId=state["secret_name"])
        assert current["SecretString"] == "s3cr3t-v2"
        assert current["VersionId"] == state["version_id_2"]

        previous = secretsmanager.get_secret_value(
            SecretId=state["secret_name"], VersionStage="AWSPREVIOUS"
        )
        assert previous["SecretString"] == "s3cr3t-v1"
        assert previous["VersionId"] == state["version_id_1"]

        by_version_id = secretsmanager.get_secret_value(
            SecretId=state["secret_name"], VersionId=state["version_id_1"]
        )
        assert by_version_id["SecretString"] == "s3cr3t-v1"

    def test_put_secret_value_is_idempotent_per_client_request_token(self, secretsmanager, state):
        token = "lws-test-fixed-token-" + "0" * 16
        first = secretsmanager.put_secret_value(
            SecretId=state["secret_name"], SecretString="s3cr3t-v3", ClientRequestToken=token
        )
        second = secretsmanager.put_secret_value(
            SecretId=state["secret_name"], SecretString="s3cr3t-v3", ClientRequestToken=token
        )
        assert first["VersionId"] == second["VersionId"] == token

    def test_list_secret_version_ids(self, secretsmanager, state):
        versions = {
            v["VersionId"] for v in secretsmanager.list_secret_version_ids(SecretId=state["secret_name"])["Versions"]
        }
        assert state["version_id_1"] in versions
        assert state["version_id_2"] in versions

    def test_update_secret(self, secretsmanager, state):
        updated = secretsmanager.update_secret(
            SecretId=state["secret_name"], Description="updated description"
        )
        assert updated["Name"] == state["secret_name"]

        desc = secretsmanager.describe_secret(SecretId=state["secret_name"])
        assert desc["Description"] == "updated description"

    def test_tag_and_untag_resource(self, secretsmanager, state):
        secretsmanager.tag_resource(
            SecretId=state["secret_name"], Tags=[{"Key": "owner", "Value": "test_secretsmanager.py"}]
        )
        desc = secretsmanager.describe_secret(SecretId=state["secret_name"])
        tags = {t["Key"]: t["Value"] for t in desc["Tags"]}
        assert tags.get("owner") == "test_secretsmanager.py"
        assert tags.get("env") == "test"

        secretsmanager.untag_resource(SecretId=state["secret_name"], TagKeys=["owner"])
        desc_after = secretsmanager.describe_secret(SecretId=state["secret_name"])
        tags_after = {t["Key"]: t["Value"] for t in desc_after["Tags"]}
        assert "owner" not in tags_after
        assert "env" in tags_after

    def test_list_secrets(self, secretsmanager, state):
        names = [s["Name"] for s in secretsmanager.list_secrets()["SecretList"]]
        assert state["secret_name"] in names

    def test_delete_and_restore_secret(self, secretsmanager, state):
        deleted = secretsmanager.delete_secret(SecretId=state["secret_name"])
        assert deleted["Name"] == state["secret_name"]
        assert "DeletionDate" in deleted

        with pytest.raises(ClientError) as exc_info:
            secretsmanager.get_secret_value(SecretId=state["secret_name"])
        assert exc_info.value.response["Error"]["Code"] == "InvalidRequestException"

        restored = secretsmanager.restore_secret(SecretId=state["secret_name"])
        assert restored["Name"] == state["secret_name"]

        # Restored secrets are usable again.
        resp = secretsmanager.get_secret_value(SecretId=state["secret_name"])
        assert resp["SecretString"] == "s3cr3t-v3"

    def test_force_delete_without_recovery(self, secretsmanager, state):
        secretsmanager.delete_secret(SecretId=state["secret_name"], ForceDeleteWithoutRecovery=True)
        state.pop("secret_name")

        with pytest.raises(ClientError) as exc_info:
            secretsmanager.describe_secret(SecretId=state["arn"])
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestSecretBinaryValue:
    """SecretBinary is the blob-valued counterpart to SecretString; this
    checks it round-trips through create_secret/get_secret_value/
    put_secret_value distinctly from the string path above.
    """

    def test_binary_secret_round_trips(self, secretsmanager, unique_name):
        secret_name = unique_name("sm-binary-secret")
        try:
            secretsmanager.create_secret(Name=secret_name, SecretBinary=b"\x00\x01binary-payload\xff")
            resp = secretsmanager.get_secret_value(SecretId=secret_name)
            assert resp["SecretBinary"] == b"\x00\x01binary-payload\xff"
            assert "SecretString" not in resp

            secretsmanager.put_secret_value(SecretId=secret_name, SecretBinary=b"updated-binary")
            updated = secretsmanager.get_secret_value(SecretId=secret_name)
            assert updated["SecretBinary"] == b"updated-binary"
        finally:
            secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)


class TestSecretNotFound:
    """Operations against a nonexistent SecretId should fail with
    ResourceNotFoundException, matching real Secrets Manager.
    """

    @pytest.mark.parametrize(
        "call",
        [
            lambda sm, name: sm.get_secret_value(SecretId=name),
            lambda sm, name: sm.describe_secret(SecretId=name),
            lambda sm, name: sm.delete_secret(SecretId=name),
            lambda sm, name: sm.put_secret_value(SecretId=name, SecretString="x"),
        ],
    )
    def test_operation_on_missing_secret_raises_not_found(self, secretsmanager, unique_name, call):
        missing_name = unique_name("sm-does-not-exist")
        with pytest.raises(ClientError) as exc_info:
            call(secretsmanager, missing_name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
