import mock

from exodus_gw.aws.util import content_md5


def test_content_md5():
    request = mock.MagicMock()
    request.headers = {
        "Content-Length": 1258,
        "Content-MD5": "28d4b03bdd69e77917638c3ef6b8720b",
    }

    assert content_md5(request) == "28d4b03bdd69e77917638c3ef6b8720b"


def test_empty_content_md5():
    """When the Content-Length header is 0, no Content-MD5 is present
    and content_md5 returns equivalent value for no content.
    """

    request = mock.MagicMock()
    request.headers = {"Content-Length": 0}

    assert content_md5(request) == "1B2M2Y8AsgTpgAmY7PhCfg=="
