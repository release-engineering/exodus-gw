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
            f"SELECT id FROM \"publishes\" WHERE id='{publish_id}';"
        )
        # Validate the publish object exists and having a unique id
        assert len(results) == 1

    n = 3
    for _ in range(n):
        test_one_publish_object()

    # Validate all publish objects stored in the database
    results = db_connection.query('SELECT id FROM "publishes";')
    assert len(results) == n
