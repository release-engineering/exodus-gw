import json
import logging
import re
from datetime import datetime, timedelta, timezone

import mock
import pytest
from botocore.exceptions import EndpointConnectionError

from exodus_gw.models import Publish, Item
from exodus_gw.aws import dynamodb
from exodus_gw.settings import Settings

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
                                # Note these timestamps come from the canned values
                                # on fake_publish.items
                                "from_date": {"S": "2023-10-04 03:52:00"},
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
                                "from_date": {"S": "2023-10-04 03:52:01"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/repomd.xml"
                                },
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                    "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/repomd.xml"
                                },
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                         "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/.__exodus_autoindex"
                                },
                                "object_key": {
                                    "S": "5891b5b522d5df086d0ff0b110fbd9d2"
                                    "1bb4fc7163af34d08286a2e846f6be03"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/.__exodus_autoindex"
                                },
                                "object_key": {
                                    "S": "5891b5b522d5df086d0ff0b110fbd9d2"
                                    "1bb4fc7163af34d08286a2e846f6be03"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
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
                                "from_date": {"S": "2023-10-04 03:52:00"},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {"S": "/other/path"},
                                "from_date": {"S": "2023-10-04 03:52:01"},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/repomd.xml"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/repomd.xml"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/.__exodus_autoindex"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                            }
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/.__exodus_autoindex"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
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
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC, mirror_writes=True)

    request = ddb.create_requests(fake_publish.items, delete=delete)[0]

    # Represent successful write/delete of all items to the table.
    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    ddb.batch_write(request)

    # Should've requested write of all items.
    mock_boto3_client.batch_write_item.assert_called_once_with(
        RequestItems=expected_request
    )


@pytest.mark.parametrize(
    "mirror,expected_request",
    [
        (
            True,
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
                                # Note these timestamps come from the canned values
                                # on fake_publish.items
                                "from_date": {"S": "2023-10-04 03:52:00"},
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
                                "from_date": {"S": "2023-10-04 03:52:01"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/repomd.xml"
                                },
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                    "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/repomd.xml"
                                },
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                         "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/.__exodus_autoindex"
                                },
                                "object_key": {
                                    "S": "5891b5b522d5df086d0ff0b110fbd9d2"
                                    "1bb4fc7163af34d08286a2e846f6be03"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1/repo/.__exodus_autoindex"
                                },
                                "object_key": {
                                    "S": "5891b5b522d5df086d0ff0b110fbd9d2"
                                    "1bb4fc7163af34d08286a2e846f6be03"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                ],
            },
        ),
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
                                # Note these timestamps come from the canned values
                                # on fake_publish.items
                                "from_date": {"S": "2023-10-04 03:52:00"},
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
                                "from_date": {"S": "2023-10-04 03:52:01"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/repomd.xml"
                                },
                                "object_key": {
                                    "S": "3f449eb3b942af58e9aca4c1cffdef89"
                                         "c3f1552c20787ae8c966767a1fedd3a5"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "web_uri": {
                                    "S": "/content/testproduct/1.1.0/repo/.__exodus_autoindex"
                                },
                                "object_key": {
                                    "S": "5891b5b522d5df086d0ff0b110fbd9d2"
                                         "1bb4fc7163af34d08286a2e846f6be03"
                                },
                                "from_date": {"S": "2023-10-04 03:52:02"},
                                "content_type": {"S": None},
                            }
                        }
                    },
                ],
            },
        ),
    ],
    ids=["Mirror-Enabled", "Mirror-Disabled"],
)
def test_batch_write_mirror_configurable(
    mock_boto3_client, fake_publish, mirror, expected_request
):
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC, mirror_writes=mirror)

    request = ddb.create_requests(fake_publish.items, delete=False)[0]

    # Represent successful write/delete of all items to the table.
    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    ddb.batch_write(request)

    # Should've requested write of all items.
    mock_boto3_client.batch_write_item.assert_called_once_with(
        RequestItems=expected_request
    )


def test_write_mirror(mock_boto3_client):
    expected_request = {'my-table': [
            # publish.items[0] both sides of the alias are mirrored.
            {'PutRequest': {
                'Item': {'from_date': {'S': '2023-10-04 03:52:00'},
                         'web_uri': {
                             'S': '/content/dist/rhel8/8.5/x86_64/baseos/os/repodata/abc123-primary.xml.gz'},
                         'object_key': {
                             'S': '0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa'},
                         'content_type': {'S': None}}}}
            , {'PutRequest': {
            'Item': {'from_date': {'S': '2023-10-04 03:52:00'}, 'web_uri': {
                'S': '/content/dist/rhel8/8/x86_64/baseos/os/repodata/abc123-primary.xml.gz'},
                     'object_key': {
                         'S': '0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa'},
                     'content_type': {'S': None}}}},
            # publish.items[1] no alias matches, so it's just the provided uri
            {'PutRequest': {
                'Item': {'from_date': {'S': '2023-10-04 03:52:01'},
                         'web_uri': {
                             'S': '/content/dist/rhel9/9/x86_64/baseos/os/repodata/abc-primary.xml.gz'},
                         'object_key': {
                             'S': 'e448a4330ff79a1b20069d436fae94806a0e2e3a6b309cd31421ef088c6439fb'},
                         'content_type': {'S': None}}}},
            # publish.items[2], dest of the alias, so no mirroring occurs.
            {'PutRequest': {
                'Item': {'from_date': {'S': '2023-10-04 03:52:02'},
                         'web_uri': {
                             'S': '/content/dist/rhel8/8.5/aarch64/appstream/debug/repodata/xyz-primary.xml.gz'},
                         'object_key': {
                             'S': '3f449eb3b942af58e9aca4c1cffdef89c3f1552c20787ae8c966767a1fedd3a5'},
                         'content_type': {'S': None}}}},
            # publish.items[3] RHUI is not mirrored.
            {'PutRequest': {
                'Item': {'from_date': {'S': '2023-10-04 03:52:02'},
                         'web_uri': {
                             'S': '/content/dist/rhel8/rhui/8/aarch64/appstream/debug/repodata/ijk-primary.xml.gz'},
                         'object_key': {
                             'S': '5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03'},
                         'content_type': {'S': None}}}},
        ]
    }
    publish = Publish(
        id="123e4567-e89b-12d3-a456-426614174000",
        env="test",
        state="PENDING",
    )
    publish.items = [
        # Matches the 8 => 8.5 alias in conftest.py, should mirror
        Item(
            web_uri="/content/dist/rhel8/8/x86_64/baseos/os/repodata/abc123-primary.xml.gz",
            object_key="0bacfc5268f9994065dd858ece3359fd7a99d82af5be84202b8e84c2a5b07ffa",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 0),
        ),
        # Doesn't match any aliases, so no mirroring.
        Item(
            web_uri="/content/dist/rhel9/9/x86_64/baseos/os/repodata/abc-primary.xml.gz",
            object_key="e448a4330ff79a1b20069d436fae94806a0e2e3a6b309cd31421ef088c6439fb",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 1),
        ),
        # The destination side of 8 => 8.5, should not mirror.
        Item(
            web_uri="/content/dist/rhel8/8.5/aarch64/appstream/debug/repodata/xyz-primary.xml.gz",
            object_key="3f449eb3b942af58e9aca4c1cffdef89c3f1552c20787ae8c966767a1fedd3a5",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 2),
        ),
        # RHUI aliases are not aliased on write, so we expect the same uri
        # with no mirroring. rhsm-pulp is configured upstream to block
        # writing directly to rhui, this test is here to document the
        # current behaviour.
        Item(
            web_uri="/content/dist/rhel8/rhui/8/aarch64/appstream/debug/repodata/ijk-primary.xml.gz",
            object_key="5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
            publish_id=publish.id,
            updated=datetime(2023, 10, 4, 3, 52, 2),
        ),
    ]
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC, mirror_writes=True)

    request = ddb.create_requests(publish.items, delete=False)[0]

    # Represent successful write/delete of all items to the table.
    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    ddb.batch_write(request)

    # Should've requested write of all items.
    mock_boto3_client.batch_write_item.assert_called_once_with(
        RequestItems=expected_request
    )


def test_create_requests_splits_batches(mock_boto3_client, fake_publish, caplog):
    # With mirroring, requests could end up with more than the allowed number
    # of items.
    items = fake_publish.items * 9
    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
    # Test that the generated requests are split into groups of no more than 25
    requests = ddb.create_requests(items)

    for request in requests:
        assert len(request["my-table"]) <= 25


def test_batch_write_deadline(mock_boto3_client, fake_publish, caplog):
    """Ensure deadline is respected by backoff/retry.

    With a deadline set, retries' max_time is assigned however many seconds
    remain before the deadline is reached.
    """

    caplog.set_level(logging.DEBUG, logger="exodus-gw")

    # Set deadline to 2 seconds from the start of the test.
    deadline = datetime.utcnow() + timedelta(seconds=2)

    ddb = dynamodb.DynamoDB(
        env="test",
        settings=Settings(),
        from_date=NOW_UTC,
        deadline=deadline,
    )
    request = ddb.create_requests(items=fake_publish.items)[0]

    # Ensure eternally unsuccessful write of all items to the table.
    # This would ordinarily exhaust all tries defined in settings (default 20).
    mock_boto3_client.batch_write_item.return_value = {
        "UnprocessedItems": {fake_publish.items[-1]}
    }

    ddb.batch_write(request=request)

    # It should report remaining time.
    # This indicates that the max_time was dynamically generated.
    # We won't specify number of seconds, as it can vary.
    assert "Remaining time for batch_write:" in caplog.text
    # It should report backing off at least once.
    assert "Backing off _batch_write(...)" in caplog.text
    # It should report giving up...
    last_rec = json.loads(caplog.text.splitlines()[-1])
    assert "Giving up _batch_write(...)" in last_rec["message"]
    # ...and it should've given up immediately past the deadline.
    giveup_time = datetime.strptime(last_rec["time"], "%Y-%m-%d %H:%M:%S.%f")
    assert (giveup_time.timestamp() - deadline.timestamp()) < 0.1


@pytest.mark.parametrize("delete", [False, True], ids=["Put", "Delete"])
def test_write_batch(delete, mock_boto3_client, fake_publish, caplog):
    caplog.set_level(logging.DEBUG, logger="exodus-gw")

    mock_boto3_client.batch_write_item.return_value = {"UnprocessedItems": {}}
    mock_boto3_client.query.return_value = {
        "Items": [{"config": {"S": '{"origin_alias": []}'}}]
    }

    expected_msg = "Items successfully %s" % "deleted" if delete else "written"

    ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
    ddb.write_batch(fake_publish.items, delete)

    assert expected_msg in caplog.text


@mock.patch("exodus_gw.aws.dynamodb.DynamoDB.batch_write")
def test_write_batch_put_fail(
    mock_batch_write, mock_boto3_client, fake_publish, caplog
):
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
def test_write_batch_delete_fail(
    mock_batch_write, mock_boto3_client, fake_publish, caplog
):
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
        "\"message\": \"Unprocessed items:\\n\\t{'my-table': [{'PutRequest': {'Key': {'web_uri': {'S': '/some/path'}}}}]}\", "
        '"event": "publish", '
        '"success": false' in caplog.text
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
    assert mock_boto3_client.batch_write_item.call_count == 1


def test_write_batch_endpoint_connection_error(
    mock_boto3_client, fake_publish, caplog
):
    num_retries = 20
    mock_boto3_client.batch_write_item.side_effect = EndpointConnectionError(
        endpoint_url="fake-url"
    )

    caplog.set_level(logging.DEBUG, logger="exodus-gw")

    with mock.patch("time.sleep"):
        with pytest.raises(EndpointConnectionError):
            ddb = dynamodb.DynamoDB("test", Settings(), NOW_UTC)
            ddb.write_batch(fake_publish.items)

    p = re.compile(
        r"Backing off _batch_write\(\.\.\.\) for [0-9]+[.]?[0-9]+s \(botocore\.exceptions\.EndpointConnectionError: Could not connect to the endpoint URL: \\\"fake-url\\\"\)"
    )
    assert len(p.findall(caplog.text)) == num_retries - 1
    assert (
        f'Giving up _batch_write(...) after {num_retries} tries (botocore.exceptions.EndpointConnectionError: Could not connect to the endpoint URL: \\"fake-url\\")'
        in caplog.text
    )
    assert mock_boto3_client.batch_write_item.call_count == num_retries
