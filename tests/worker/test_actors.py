import mock
import pytest
from fastapi import HTTPException

from exodus_gw import worker


@pytest.mark.parametrize(
    "env",
    [
        "test",
        "test2",
        "test3",
    ],
)
@mock.patch("exodus_gw.worker.publish.Session")
@mock.patch("exodus_gw.worker.publish.write_batches")
@mock.patch("exodus_gw.worker.publish.get_publish_by_id")
def test_commit_publish(
    mock_get_publish,
    mock_write_batches,
    mock_alch_session,
    env,
    mock_publish,
    mock_db_session,
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.return_value = True
    mock_alch_session.return_value = mock_db_session

    worker.publish.commit(
        env=env,
        publish_id=mock_publish.id,
    )

    # Should write repomd.xml file separately after other items.
    mock_write_batches.assert_has_calls(
        calls=[
            mock.call(env, mock_publish.items[:2]),
            mock.call(env, [mock_publish.items[2]]),
        ],
        any_order=False,
    )


@mock.patch("exodus_gw.worker.publish.get_publish_by_id")
def test_commit_publish_env_doesnt_exist(mock_publish):
    env = "foo"

    with pytest.raises(HTTPException) as exc_info:
        worker.publish.commit(
            env=env,
            publish_id=mock_publish.id,
        )

    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Invalid environment='foo'"


@mock.patch("exodus_gw.worker.publish.Session")
@mock.patch("exodus_gw.worker.publish.write_batches")
@mock.patch("exodus_gw.worker.publish.get_publish_by_id")
def test_commit_publish_write_failed(
    mock_get_publish,
    mock_write_batches,
    mock_alch_session,
    mock_publish,
    mock_db_session,
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.side_effect = [False, True]
    mock_alch_session.return_value = mock_db_session

    worker.publish.commit(
        env="test",
        publish_id=mock_publish.id,
    )

    mock_write_batches.assert_has_calls(
        calls=[
            mock.call("test", mock_publish.items[:2]),
            mock.call("test", mock_publish.items[:2], delete=True),
        ],
        any_order=False,
    )


@mock.patch("exodus_gw.worker.publish.Session")
@mock.patch("exodus_gw.worker.publish.write_batches")
@mock.patch("exodus_gw.worker.publish.get_publish_by_id")
def test_commit_publish_entry_point_files_failed(
    mock_get_publish,
    mock_write_batches,
    mock_alch_session,
    mock_publish,
    mock_db_session,
):
    mock_get_publish.return_value = mock_publish
    mock_write_batches.side_effect = [True, False, True]
    mock_alch_session.return_value = mock_db_session

    worker.publish.commit(
        env="test",
        publish_id=mock_publish.id,
    )

    mock_write_batches.assert_has_calls(
        calls=[
            mock.call("test", mock_publish.items[:2]),
            mock.call("test", [mock_publish.items[2]]),
            mock.call("test", mock_publish.items, delete=True),
        ],
        any_order=False,
    )
