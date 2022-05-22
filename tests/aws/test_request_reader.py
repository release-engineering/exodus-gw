import pytest

from exodus_gw.aws.util import RequestReader


class FakeRequest:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self):
        for chunk in self._chunks:
            yield chunk


async def test_iterate_stream():
    request = FakeRequest([b"first ", b"second ", b"third"])

    reader = RequestReader.get_reader(request)

    # Can read the request
    content = b"".join([chunk async for chunk in reader])
    assert content == b"first second third"

    # Can read the request a second time
    content_reread = b"".join([chunk async for chunk in reader])
    assert content == content_reread


def test_no_blocking_operations():
    """read blocking API is not available."""

    request = FakeRequest([])

    reader = RequestReader.get_reader(request)
    with pytest.raises(NotImplementedError):
        reader.read(123)
