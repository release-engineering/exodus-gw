import json
from uuid import UUID


def test_publish(wait_for_api, db_connection) -> None:
    request_session, api_url = wait_for_api

    def test_one_publish_object() -> None:
        response = request_session.post(f"{api_url}/dev/publish")
        assert response.status_code == 200
        publish_id = response.json()["id"]
        assert UUID(publish_id, version=4)
        # Validate the publish object stored in the database
        results = db_connection.query(
            f"SELECT * FROM \"publishes\" WHERE id='{publish_id}';"
        )
        # Validate the publish object exists and having a unique id
        assert len(results) == 1

    n = 3
    for _ in range(n):
        test_one_publish_object()

    # Validate all publish objects stored in the database
    results = db_connection.query('SELECT id FROM "publishes";')
    assert len(results) == n


def test_update_publish(wait_for_api, db_connection) -> None:
    request_session, api_url = wait_for_api

    response = request_session.post(f"{api_url}/dev/publish")
    publish_id = response.json()["id"]
    body = [
        {"uri": "/some/path", "object_key": "abcde"},
        {"uri": "/other/path", "object_key": "a1b2"},
    ]
    response = request_session.put(
        url=f"{api_url}/dev/publish/{publish_id}", data=json.dumps(body)
    )
    results = db_connection.query(
        f"SELECT * FROM \"items\" WHERE publish_id='{publish_id}';"
    )
    assert len(results) == 2
    # Exclude the last item which is the publish_id
    assert results[0][:-1] == ("/some/path", "abcde")
    assert results[1][:-1] == ("/other/path", "a1b2")
