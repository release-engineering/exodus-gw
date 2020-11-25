from typing import Tuple

import pg
import pytest
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@pytest.fixture(scope="session")
def wait_for_api(
    session_scoped_container_getter,
) -> Tuple[requests.Session, str]:
    """Wait for the api from exodus-gw to become responsive"""
    request_session = requests.Session()
    retries = Retry(
        total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
    )
    request_session.mount("http://", HTTPAdapter(max_retries=retries))

    service = session_scoped_container_getter.get("app").network_info[0]
    api_url = f"http://{service.hostname}:{service.host_port}"
    assert request_session.get(f"{api_url}/healthcheck")
    return request_session, api_url


@pytest.fixture(scope="session")
def db_connection():
    return pg.DB(
        user="exodus-gw",
        passwd="exodus-gw",
        host="localhost",
        dbname="exodus-gw",
        port=5432,
    )
