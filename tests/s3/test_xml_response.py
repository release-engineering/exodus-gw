import textwrap

from exodus_gw.s3.util import xml_response


def test_typical_response():
    """xml_response creates response objects with the expected content."""

    response = xml_response("SomeOperation", Foo="Bar", Baz=123)

    # It should be successful
    assert response.status_code == 200

    # It should be XML
    assert response.media_type == "application/xml"

    # It should look like this
    body = response.body.decode("utf-8")
    expected = textwrap.dedent(
        """
            <?xml version='1.0' encoding='UTF-8'?>
            <SomeOperation><Foo>Bar</Foo><Baz>123</Baz></SomeOperation>
        """
    ).strip()
    assert body == expected
