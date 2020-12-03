from exodus_gw.models import Item


def test_Item_init():
    item = Item(
        uri="/some/path",
        object_key="abcde",
        publish_id="123e4567-e89b-12d3-a456-426614174000",
    )
    assert item.uri == "/some/path"
    assert item.object_key == "abcde"
    assert item.publish_id == "123e4567-e89b-12d3-a456-426614174000"
