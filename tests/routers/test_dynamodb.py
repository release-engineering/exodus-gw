import mock
import pytest

from exodus_gw.routers import dynamodb

TEST_ITEMS = [
    {"web_uri": {"S": "/example/one"}, "from_date": {"S": "2021-01-01"}},
    {"web_uri": {"S": "/example/two"}, "from_date": {"S": "2021-01-01"}},
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete,expected_request",
    [
        (False, {"PutRequest": {"Item": item} for item in TEST_ITEMS}),
        (True, {"DeleteRequest": {"Key": item} for item in TEST_ITEMS}),
    ],
    ids=["Put", "Delete"],
)
async def test_batch_write(mock_aws_client, delete, expected_request):
    """Ensure batch_write/delete delegates correctly to DynamoDB."""

    # Represent successful write/delete of all items to the table.
    mock_aws_client.batch_write_item.return_value = {"UnprocessedItems": {}}

    await dynamodb.batch_write("test", TEST_ITEMS, delete)

    # Should've requested write of all items.
    mock_aws_client.batch_write_item.assert_called_once_with(
        RequestItems={"my-table": [expected_request]}
    )


@pytest.mark.asyncio
async def test_batch_write_retry(mock_aws_client):
    """Ensure batch_write is retried with unprocessed items."""

    items = [
        {"web_uri": {"S": "/example/one"}, "from_date": {"S": "2021-01-01"}},
        {"web_uri": {"S": "/example/two"}, "from_date": {"S": "2021-01-01"}},
    ]

    # Represent successful write of all items after one retry.
    mock_aws_client.batch_write_item.side_effect = [
        {
            "UnprocessedItems": {
                "my-table": [{"PutRequest": {"Item": items[1]}}]
            }
        },
        {"UnprocessedItems": {}},
    ]

    await dynamodb.batch_write("test", items)

    # Should've requested write of all items, then retried one.
    mock_aws_client.batch_write_item.assert_has_calls(
        [
            mock.call(
                RequestItems={
                    "my-table": [
                        {"PutRequest": {"Item": item} for item in items}
                    ]
                }
            ),
            mock.call(
                RequestItems={"my-table": [{"PutRequest": {"Item": items[1]}}]}
            ),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
async def test_batch_write_item_limit(mock_aws_client, caplog):
    """Ensure batch_write does not accept more than 25 items."""

    items = TEST_ITEMS * 13

    with pytest.raises(ValueError) as exc:
        await dynamodb.batch_write("test", items)

    assert "Cannot process more than 25 items" in caplog.text
    assert str(exc.value) == "Received too many items (26)"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delete,expected_msg",
    [
        (False, "Exception while writing 2 items to table 'my-table'"),
        (True, "Exception while deleting 2 items from table 'my-table'"),
    ],
    ids=["Put", "Delete"],
)
async def test_batch_write_excs(mock_aws_client, delete, expected_msg, caplog):
    """Ensure messages are emitted for exceptions."""

    items = [
        {"web_uri": {"S": "/example/one"}, "from_date": {"S": "2021-01-01"}},
        {"web_uri": {"S": "/example/two"}, "from_date": {"S": "2021-01-01"}},
    ]

    mock_aws_client.batch_write_item.side_effect = ValueError()

    with pytest.raises(ValueError):
        await dynamodb.batch_write("test", items, delete)

    assert expected_msg in caplog.text
