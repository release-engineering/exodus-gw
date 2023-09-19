from uuid import uuid4

from exodus_gw.main import request_id_validator


def test_request_id_validator():
    uuid = uuid4().hex[:8]
    assert request_id_validator(uuid)
