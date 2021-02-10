import pytest
from fastapi import HTTPException

from exodus_gw.aws.util import validate_object_key


def test_invalid_object_key():
    with pytest.raises(HTTPException) as exc_info:
        validate_object_key(key="foooooo")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Invalid object key: 'foooooo'"
