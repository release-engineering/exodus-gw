from exodus_gw import publish


def test_create_publish_id(mocker):
    mocker.patch(
        "uuid.uuid4", return_value="f6486570-ed20-11ea-9114-94b86d596988",
    )

    assert (
        publish.create_publish_id() == "f6486570-ed20-11ea-9114-94b86d596988"
    )
