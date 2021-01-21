import pytest

from exodus_gw.aws import dynamodb


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete,expected_request",
    [
        (
            False,
            [
                {
                    "PutRequest": {
                        "Item": {
                            "web_uri": {"S": "/some/path"},
                            "object_key": {"S": "abcde"},
                            "from_date": {"S": "2021-01-01T00:00:00.0"},
                        }
                    }
                },
                {
                    "PutRequest": {
                        "Item": {
                            "web_uri": {"S": "/other/path"},
                            "object_key": {"S": "a1b2"},
                            "from_date": {"S": "2021-01-01T00:00:00.0"},
                        }
                    }
                },
            ],
        ),
        (
            True,
            [
                {
                    "DeleteRequest": {
                        "Key": {
                            "web_uri": {"S": "/some/path"},
                            "object_key": {"S": "abcde"},
                            "from_date": {"S": "2021-01-01T00:00:00.0"},
                        }
                    }
                },
                {
                    "DeleteRequest": {
                        "Key": {
                            "web_uri": {"S": "/other/path"},
                            "object_key": {"S": "a1b2"},
                            "from_date": {"S": "2021-01-01T00:00:00.0"},
                        }
                    }
                },
            ],
        ),
    ],
    ids=["Put", "Delete"],
)
async def test_batch_write(
    mock_aws_client, mock_publish, delete, expected_request
):
    """Ensure batch_write/delete delegates correctly to DynamoDB."""

    # Represent successful write/delete of all items to the table.
    mock_aws_client.batch_write_item.return_value = {"UnprocessedItems": {}}
    await dynamodb.batch_write("test", mock_publish.items, delete)

    # Should've requested write of all items.
    mock_aws_client.batch_write_item.assert_called_once_with(
        RequestItems={"my-table": expected_request}
    )


@pytest.mark.asyncio
async def test_batch_write_item_limit(mock_aws_client, mock_publish, caplog):
    """Ensure batch_write does not accept more than 25 items."""

    items = mock_publish.items * 13

    with pytest.raises(ValueError) as exc_info:
        await dynamodb.batch_write("test", items)

    assert "Cannot process more than 25 items" in caplog.text
    assert str(exc_info.value) == "Received too many items (26)"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete,expected_msg",
    [
        (False, "Exception while writing 2 items to table 'my-table'"),
        (True, "Exception while deleting 2 items from table 'my-table'"),
    ],
    ids=["Put", "Delete"],
)
async def test_batch_write_excs(
    mock_aws_client, mock_publish, delete, expected_msg, caplog
):
    """Ensure messages are emitted for exceptions."""

    mock_aws_client.batch_write_item.side_effect = ValueError()

    with pytest.raises(ValueError):
        await dynamodb.batch_write("test", mock_publish.items, delete)

    assert expected_msg in caplog.text
