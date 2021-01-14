from exodus_gw.models import Item


def test_Item_init():
    item = Item(
        web_uri="/some/path",
        object_key="abcde",
        from_date="2021-01-01T00:00:00.0",
        publish_id="123e4567-e89b-12d3-a456-426614174000",
    )
    assert item.web_uri == "/some/path"
    assert item.object_key == "abcde"
    assert item.from_date == "2021-01-01T00:00:00.0"
    assert item.publish_id == "123e4567-e89b-12d3-a456-426614174000"
