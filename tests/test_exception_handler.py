import pytest

import mock
from fastapi import HTTPException

from exodus_gw.main import custom_http_exception_handler

TEST_KEY = "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint, media_type",
    [
        ("/upload/foo/%s" % TEST_KEY, "xml"),
        ("/foo/publish/%s" % TEST_KEY, "json"),
        ("/healthcheck", "json"),
        ("/whoami", "json"),
    ],
    ids=["upload", "publish", "healthcheck", "whoami"],
)
async def test_custom_http_exception_handler(endpoint, media_type):
    # Verify that HTTPExceptions raised are handled according to endpoint.
    # Expand list of endpoints as needed.

    request = mock.Mock(scope={"path": endpoint})
    err = HTTPException(status_code=600, detail="testing response")
    response = await custom_http_exception_handler(request, err)

    assert response.media_type == "application/%s" % media_type
