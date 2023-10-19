import json
import logging
from base64 import b64decode
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from freezegun import freeze_time

from exodus_gw.main import app
from exodus_gw.routers import cdn
from exodus_gw.settings import get_environment


@freeze_time("2022-02-16")
def test_generate_cf_cookies(monkeypatch, dummy_private_key, caplog):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    caplog.set_level(logging.DEBUG, "exodus-gw")

    env = get_environment("test")
    expiration = datetime.now(timezone.utc) + timedelta(seconds=720)
    parsed_url = urlparse(env.cdn_url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    out = cdn.cf_cookie(f"{base_url}/content/*", env, expiration, "tester")

    assert out == {
        "CloudFront-Key-Pair-Id": "XXXXXXXXXXXXXX",
        "CloudFront-Policy": "eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xv"
        "Y2FsaG9zdDo4MDQ5L2NvbnRlbnQvKiIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iO"
        "nsiQVdTOkVwb2NoVGltZSI6MTY0NDk3MDMyMH19fV19",
        "CloudFront-Signature": "H15eGmXpn91K3zIDLFBcDaA77srpmJe21G5RBcoXlShWq"
        "EnbMP4CZJAnMtYtqV~0ccjp8EMICkCCeY2RYXp4cmI~0B5--SZrc5GW~RJ51f7EoTwJ93"
        "aH7fcGE0d9H~lazmZz27dq6-RGZKnsBv6S~uKOVo9vC1upSDIjQ3~n~H0_",
    }
    assert (
        "Generated cookie for: user=tester, key=XXXXXXXXXXXXXX, "
        "resource=http://localhost:8049/content/*, "
        "expires=2022-02-16 00:12:00+00:00, "
        f"policy={out['CloudFront-Policy']}"
    ) in caplog.text

    assert json.loads(b64decode(out["CloudFront-Policy"])) == {
        "Statement": [
            {
                "Resource": "http://localhost:8049/content/*",
                "Condition": {"DateLessThan": {"AWS:EpochTime": 1644970320}},
            }
        ]
    }


@freeze_time("2022-02-16")
def test_cdn_redirect_(monkeypatch, dummy_private_key, caplog):
    caplog.set_level(logging.DEBUG, "exodus-gw")
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    with TestClient(app) as client:
        get_r = client.get("/test/cdn/some/url", follow_redirects=False)
        head_r = client.head("/test/cdn/some/url", follow_redirects=False)

    expected_cookies = (
        "WyJDbG91ZEZyb250LUtleS1QYWlyLUlkPVhYWFhYWFhYWFhYWFhYOyBTZWN1cmU7IEh0d"
        "HBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWFpbj1odHRwOi8vbG9jYWxob3N0OjgwNDk7IF"
        "BhdGg9L2NvbnRlbnQvKjsgTWF4LUFnZT0xODAwIiwgIkNsb3VkRnJvbnQtUG9saWN5PWV"
        "5SlRkR0YwWlcxbGJuUWlPbHQ3SWxKbGMyOTFjbU5sSWpvaWFIUjBjRG92TDJ4dlkyRnNh"
        "Rzl6ZERvNE1EUTVMMk52Ym5SbGJuUXZLaUlzSWtOdmJtUnBkR2x2YmlJNmV5SkVZWFJsV"
        "EdWemMxUm9ZVzRpT25zaVFWZFRPa1Z3YjJOb1ZHbHRaU0k2TVRZME5EazNNVFF3TUgxOW"
        "ZWMTk7IFNlY3VyZTsgSHR0cE9ubHk7IFNhbWVTaXRlPWxheDsgRG9tYWluPWh0dHA6Ly9"
        "sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vY29udGVudC8qOyBNYXgtQWdlPTE4MDAiLCAiQ2xv"
        "dWRGcm9udC1TaWduYXR1cmU9UjZrLUJHdk5nSlJzMy1HVzBCdmd0ZlF3RUJ0Z1R4R3Z5W"
        "WRXVTAzZlBDR2xRTUY4RVpWRFdJcVRkSE9Qem5yQ3RuVFlWWnZ2Y3c0VX5WRXNTaWY3NV"
        "c0eC1xZW9xbn5ZSDlZVmFnMnZOQVlwQkV0d21JMDhNcnZOUGlyQ2dPYmNMdnlXMU9yeEx"
        "oSFZEeWNzQWRGRmFudW91RUF5a0J0c0dNbkh2bVJvR1JBXzsgU2VjdXJlOyBIdHRwT25s"
        "eTsgU2FtZVNpdGU9bGF4OyBEb21haW49aHR0cDovL2xvY2FsaG9zdDo4MDQ5OyBQYXRoP"
        "S9jb250ZW50Lyo7IE1heC1BZ2U9MTgwMCIsICJDbG91ZEZyb250LUtleS1QYWlyLUlkPV"
        "hYWFhYWFhYWFhYWFhYOyBTZWN1cmU7IEh0dHBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWF"
        "pbj1odHRwOi8vbG9jYWxob3N0OjgwNDk7IFBhdGg9L29yaWdpbi8qOyBNYXgtQWdlPTE4"
        "MDAiLCAiQ2xvdWRGcm9udC1Qb2xpY3k9ZXlKVGRHRjBaVzFsYm5RaU9sdDdJbEpsYzI5M"
        "WNtTmxJam9pYUhSMGNEb3ZMMnh2WTJGc2FHOXpkRG80TURRNUwyOXlhV2RwYmk4cUlpd2"
        "lRMjl1WkdsMGFXOXVJanA3SWtSaGRHVk1aWE56VkdoaGJpSTZleUpCVjFNNlJYQnZZMmh"
        "VYVcxbElqb3hOalEwT1RjeE5EQXdmWDE5WFgwXzsgU2VjdXJlOyBIdHRwT25seTsgU2Ft"
        "ZVNpdGU9bGF4OyBEb21haW49aHR0cDovL2xvY2FsaG9zdDo4MDQ5OyBQYXRoPS9vcmlna"
        "W4vKjsgTWF4LUFnZT0xODAwIiwgIkNsb3VkRnJvbnQtU2lnbmF0dXJlPVFoSFFDZlNWUH"
        "lGOXJwbDNBY1ZyYkJ0eFBZN3c4Rm55Y0k2V3dKTE5Od0h2MEoxYmM0NGhUQjhpSW53Skd"
        "IU1U0b3hGQnlFSFUyeFl4akxQSjdSdkJjNGllM3NSQmNXV00wb0hjd0lOdHA2U0FHWVRP"
        "U3FCN3NDNlZMblIxfmN3SThkcTRGTEZpZElVVElDSGpFbk8taDVrUnBYaTFXbXpsZGVvc"
        "GZ1ZHR-WV87IFNlY3VyZTsgSHR0cE9ubHk7IFNhbWVTaXRlPWxheDsgRG9tYWluPWh0dH"
        "A6Ly9sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vb3JpZ2luLyo7IE1heC1BZ2U9MTgwMCJd"
    )
    expected_url = (
        "http://localhost:8049/_/cookie/some/url?"
        "Expires=1644971400&"
        "Signature=QXdMBQNyDLYeIsJzV7bKHnqYQSErcz9OYdJTuIYKVHCDaDiqPOUjqkSXX4f"
        "m7A-Fi2roZSlWhyd4emrlC8hvNdPLZb3-7LHMVqau1QK9qFlhZz~aP1i4~Zud-kTot4JO"
        "4ewE8LdCkQL1pda-on~wVTXhiAtB7EaX8aR3dnBZmYo_&"
        f"Set-Cookies={expected_cookies}&"
        "Key-Pair-Id=XXXXXXXXXXXXXX"
    )

    assert get_r.status_code == 302
    assert get_r.headers["location"] == expected_url

    assert head_r.status_code == 302
    assert head_r.headers["location"] == expected_url

    # Sanity check CloudFront cookies
    cookies = (
        expected_cookies.replace("-", "+").replace("_", "=").replace("~", "/")
    )
    cookies = json.loads(b64decode(cookies))
    assert cookies == [
        "CloudFront-Key-Pair-Id=XXXXXXXXXXXXXX; Secure; HttpOnly; "
        "SameSite=lax; Domain=http://localhost:8049; Path=/content/*; Max-Age=1800",
        "CloudFront-Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2F"
        "saG9zdDo4MDQ5L2NvbnRlbnQvKiIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsi"
        "QVdTOkVwb2NoVGltZSI6MTY0NDk3MTQwMH19fV19; Secure; HttpOnly; "
        "SameSite=lax; Domain=http://localhost:8049; Path=/content/*; Max-Age=1800",
        "CloudFront-Signature=R6k-BGvNgJRs3-GW0BvgtfQwEBtgTxGvyYdWU03fPCGlQMF8"
        "EZVDWIqTdHOPznrCtnTYVZvvcw4U~VEsSif75W4x-qeoqn~YH9YVag2vNAYpBEtwmI08M"
        "rvNPirCgObcLvyW1OrxLhHVDycsAdFFanuouEAykBtsGMnHvmRoGRA_; Secure; "
        "HttpOnly; SameSite=lax; Domain=http://localhost:8049; Path=/content/*; "
        "Max-Age=1800",
        "CloudFront-Key-Pair-Id=XXXXXXXXXXXXXX; Secure; "
        "HttpOnly; SameSite=lax; Domain=http://localhost:8049; Path=/origin/*; "
        "Max-Age=1800",
        "CloudFront-Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2F"
        "saG9zdDo4MDQ5L29yaWdpbi8qIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJB"
        "V1M6RXBvY2hUaW1lIjoxNjQ0OTcxNDAwfX19XX0_; Secure; HttpOnly; "
        "SameSite=lax; Domain=http://localhost:8049; Path=/origin/*; Max-Age=1800",
        "CloudFront-Signature=QhHQCfSVPyF9rpl3AcVrbBtxPY7w8FnycI6WwJLNNwHv0J1b"
        "c44hTB8iInwJGHSU4oxFByEHU2xYxjLPJ7RvBc4ie3sRBcWWM0oHcwINtp6SAGYTOSqB7"
        "sC6VLnR1~cwI8dq4FLFidIUTICHjEnO-h5kRpXi1Wmzldeopfudt~Y_; Secure; "
        "HttpOnly; SameSite=lax; Domain=http://localhost:8049; Path=/origin/*; "
        "Max-Age=1800",
    ]
    # Sanity check at least one policy
    content_policy = b64decode(
        cookies[1].split("=", maxsplit=1)[1].split(";")[0]
    )
    assert json.loads(content_policy) == {
        "Statement": [
            {
                "Resource": "http://localhost:8049/content/*",
                "Condition": {"DateLessThan": {"AWS:EpochTime": 1644971400}},
            }
        ]
    }


def test_sign_url_without_private_key():
    env = get_environment("test")

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env, "tester")

    assert "Missing private key for CDN access" in str(exc_info)


def test_sign_url_without_key_id(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    env = get_environment("test")
    env.cdn_key_id = None

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env, "tester")

    assert "Missing key ID for CDN access" in str(exc_info)


def test_sign_url_without_cdn_url(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    env = get_environment("test")
    env.cdn_url = None

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env, "tester")

    assert "Missing cdn_url, nowhere to redirect request" in str(exc_info)
