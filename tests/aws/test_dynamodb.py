import logging
from datetime import datetime, timezone

import mock
import pytest

from exodus_gw.aws import dynamodb
from exodus_gw.settings import Environment, Settings, get_environment

NOW_UTC = str(datetime.now(timezone.utc))


@pytest.mark.parametrize(
    "delete,expected_request",
    [
        (
            False,
            {
                "my-table": [
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {"S": "/some/path"},
                                "object_key": {
                                    "S": "0bacfc5268f9994065dd858ece3359fd"
                                    "7a99d82af5be84202b8e84c2a5b07ffa"
                                },
                                "from_date": {"S": NOW_UTC},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {"S": "/other/path"},
                                "object_key": {
                                    "S": "e448a4330ff79a1b20069d436fae9480"
                                    "6a0e2e3a6b309cd31421ef088c6439fb"
                                },
                                "from_date": {"S": NOW_UTC},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {"S": "/to/repomd.xml"},
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                    "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": NOW_UTC},
                                "content_type": {"S": None},
                            }
                        }
                    },
                ],
            },
        ),
        (
            True,
            {
                "my-table": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {"S": "/some/path"},
                                "from_date": {"S": NOW_UTC},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {"S": "/other/path"},
                                "from_date": {"S": NOW_UTC},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {"S": "/to/repomd.xml"},
                                "from_date": {"S": NOW_UTC},
                            }
                        }
                    },
                ],
            },
        ),
    ],
    ids=["Put", "Delete"],
)
def test_batch_write(
    mock_boto3_client, fake_publish, delete, expected_request
):
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)

    request = ddb.create_request(fake_publish.items, delete=delete)

    # Represent successful write/delete of all items to the table.
    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    ddb.batch_write(request)

    # Should've requested write of all items.
    mock_boto3_client.batch_write_item.assert_called_once_with(
        RequestItems=expected_request
    )


def test_batch_write_item_limit(mock_boto3_client, fake_publish, caplog):
    items = fake_publish.items * 9
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)

    request = ddb.create_request(items)

    with pytest.raises(ValueError) as exc_info:
        ddb.batch_write(request)

    assert "Cannot process more than 25 items per request" in caplog.text
    assert str(exc_info.value) == "Request contains too many items (27)"


@pytest.mark.parametrize("delete", [False, True], ids=["Put", "Delete"])
def test_write_batch(delete, mock_boto3_client, fake_publish, caplog):
    caplog.set_level(logging.INFO, logger="exodus-gw")

    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}
    mock_boto3_client.query.return_value = {
        "Items": [{"config": {"S": '{"origin_alias": []}'}}]
    }

    expected_msg = "Items successfully %s" % "deleted" if delete else "written"

    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
    ddb.write_batch(fake_publish.items, delete)

    assert expected_msg in caplog.text


@mock.patch("exodus_gw.aws.dynamodb.DynamoDB.batch_write")
def test_write_batch_put_fail(mock_batch_write, fake_publish, caplog):
    caplog.set_level(logging.INFO, logger="exodus-gw")
    mock_batch_write.return_value = {
        "UnprocessedItems": {
            "my-table": [
                {"PutRequest": {"Item": {"web_uri": {"S": "/some/path"}}}},
            ]
        }
    }

    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
    with pytest.raises(RuntimeError) as exc_info:
        ddb.write_batch(fake_publish.items)
        assert "One or more writes were unsuccessful" in str(exc_info)


@mock.patch("exodus_gw.aws.dynamodb.DynamoDB.batch_write")
def test_write_batch_delete_fail(mock_batch_write, fake_publish, caplog):
    mock_batch_write.return_value = {
        "UnprocessedItems": {
            "my-table": [
                {"PutRequest": {"Key": {"web_uri": {"S": "/some/path"}}}},
            ]
        }
    }

    with pytest.raises(RuntimeError) as exc_info:
        ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
        ddb.write_batch(fake_publish.items, delete=True)

    assert (
        "Unprocessed items:\n\t%s"
        % str(
            {
                "my-table": [
                    {"PutRequest": {"Key": {"web_uri": {"S": "/some/path"}}}},
                ]
            }
        )
        in caplog.text
    )
    assert "Deletion failed" in str(exc_info.value)


@pytest.mark.parametrize("delete", [False, True], ids=["Put", "Delete"])
def test_write_batch_excs(mock_boto3_client, fake_publish, delete, caplog):
    mock_boto3_client.batch_write_item.side_effect = ValueError()

    expected_msg = "Exception while %s" % "deleting" if delete else "writing"

    with pytest.raises(ValueError):
        ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
        ddb.write_batch(fake_publish.items, delete)

    assert expected_msg in caplog.text
