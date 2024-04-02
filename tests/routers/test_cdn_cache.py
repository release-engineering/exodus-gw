from fastapi.testclient import TestClient

from exodus_gw.main import app
from exodus_gw.models.dramatiq import DramatiqMessage


def test_flush_cache_denied(auth_header, caplog):
    """flush-cache denies request if user is missing role"""
    with TestClient(app) as client:
        response = client.post(
            "/test/cdn-flush",
            json=[
                {"web_uri": "/path1"},
                {"web_uri": "/path2"},
            ],
            headers=auth_header(roles=["irrelevant-role"]),
        )

    # It should be forbidden
    assert response.status_code == 403

    # Should have been an "Access denied" event
    assert "Access denied; path=/test/cdn-flush" in caplog.text


def test_flush_cache_typical(auth_header, db):
    """flush-cache enqueues actor as expected in typical case"""

    with TestClient(app) as client:
        response = client.post(
            "/test/cdn-flush",
            json=[
                {"web_uri": "/path1"},
                {"web_uri": "/path2"},
            ],
            headers=auth_header(roles=["test-cdn-flusher"]),
        )

    # It should have succeeded
    assert response.status_code == 200

    # Should have given us some NOT_STARTED task
    task = response.json()
    assert task["state"] == "NOT_STARTED"

    # Check the enqueued messages...
    messages: list[DramatiqMessage] = db.query(DramatiqMessage).all()

    # It should have enqueued one message
    assert len(messages) == 1

    message = messages[0]

    # Should be the message corresponding to the returned task
    assert task["id"] == message.id

    # Should be a message for the expected actor with
    # expected args
    assert message.actor == "flush_cdn_cache"

    kwargs = message.body["kwargs"]
    assert kwargs["env"] == "test"
    assert kwargs["paths"] == ["/path1", "/path2"]
