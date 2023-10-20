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

    expected_cookies = "WyJDbG91ZEZyb250LUtleS1QYWlyLUlkPVhYWFhYWFhYWFhYWFhYOyBTZWN1cmU7IEh0dHBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWFpbj1sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vY29udGVudC87IE1heC1BZ2U9NDMyMDAiLCAiQ2xvdWRGcm9udC1Qb2xpY3k9ZXlKVGRHRjBaVzFsYm5RaU9sdDdJbEpsYzI5MWNtTmxJam9pYUhSMGNEb3ZMMnh2WTJGc2FHOXpkRG80TURRNUwyTnZiblJsYm5RdktpSXNJa052Ym1ScGRHbHZiaUk2ZXlKRVlYUmxUR1Z6YzFSb1lXNGlPbnNpUVZkVE9rVndiMk5vVkdsdFpTSTZNVFkwTlRBeE1qZ3dNSDE5ZlYxOTsgU2VjdXJlOyBIdHRwT25seTsgU2FtZVNpdGU9bGF4OyBEb21haW49bG9jYWxob3N0OjgwNDk7IFBhdGg9L2NvbnRlbnQvOyBNYXgtQWdlPTQzMjAwIiwgIkNsb3VkRnJvbnQtU2lnbmF0dXJlPU5XUGZnb3REdTJEa0g0ZjRkNjhlVWtMTk5hVmZKR2hpenp4UlJleGI1NVh0Y0o3Qzk2cEF4ekd3cX56UWJoNndyMHhhMlh4Zll3UjV5dEs1MmJXQ3JCTGJWVHI5WWd0M2Z3Z3FDZTl1cWl1dnJoU3V-WDd3Z0VPbkVvT053Sng2WGw1VkFERU4yYXBVblBMQ1hJVEQybXYtNnJDaFhmemdaMXg0UER5OGo4MF87IFNlY3VyZTsgSHR0cE9ubHk7IFNhbWVTaXRlPWxheDsgRG9tYWluPWxvY2FsaG9zdDo4MDQ5OyBQYXRoPS9jb250ZW50LzsgTWF4LUFnZT00MzIwMCIsICJDbG91ZEZyb250LUtleS1QYWlyLUlkPVhYWFhYWFhYWFhYWFhYOyBTZWN1cmU7IEh0dHBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWFpbj1sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vb3JpZ2luLzsgTWF4LUFnZT00MzIwMCIsICJDbG91ZEZyb250LVBvbGljeT1leUpUZEdGMFpXMWxiblFpT2x0N0lsSmxjMjkxY21ObElqb2lhSFIwY0RvdkwyeHZZMkZzYUc5emREbzRNRFE1TDI5eWFXZHBiaThxSWl3aVEyOXVaR2wwYVc5dUlqcDdJa1JoZEdWTVpYTnpWR2hoYmlJNmV5SkJWMU02UlhCdlkyaFVhVzFsSWpveE5qUTFNREV5T0RBd2ZYMTlYWDBfOyBTZWN1cmU7IEh0dHBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWFpbj1sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vb3JpZ2luLzsgTWF4LUFnZT00MzIwMCIsICJDbG91ZEZyb250LVNpZ25hdHVyZT1NaW8za2w5enpCZXE2WUtjREY0aFdHNGlIRFhnLWRwSnV-VmtkWklYZVhPM0lsZzE3OTZUWlFBZGpLLWN6Tm5aQzBUNWVmVzNEbGlKQWVMSmhYd351MVZoTkpSQ0lvUTZmTGJDVnV4MVRHMzAtUC1FVzR-a1JmU2dlWjV2RVcydTBNWXpsQ0pNZndZSUoxQ1ZlejlMdTJ3a2NIMjFQTkNjc2liS25tTmZjbk1fOyBTZWN1cmU7IEh0dHBPbmx5OyBTYW1lU2l0ZT1sYXg7IERvbWFpbj1sb2NhbGhvc3Q6ODA0OTsgUGF0aD0vb3JpZ2luLzsgTWF4LUFnZT00MzIwMCJd"
    expected_url = (
        "http://localhost:8049/_/cookie/some/url?"
        "Expires=1644971400&"
        "Signature=QXdMBQNyDLYeIsJzV7bKHnqYQSErcz9OYdJTuIYKVHCDaDiqPOUjqkSXX4fm7A-Fi2roZSlWhyd4emrlC8hvNdPLZb3-7LHMVqau1QK9qFlhZz~aP1i4~Zud-kTot4JO4ewE8LdCkQL1pda-on~wVTXhiAtB7EaX8aR3dnBZmYo_&"
        f"CloudFront-Cookies={expected_cookies}&"
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
        "SameSite=lax; Domain=localhost:8049; Path=/content/; Max-Age=43200",
        "CloudFront-Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2F"
        "saG9zdDo4MDQ5L2NvbnRlbnQvKiIsIkNvbmRpdGlvbiI6eyJEYXRlTGVzc1RoYW4iOnsi"
        "QVdTOkVwb2NoVGltZSI6MTY0NTAxMjgwMH19fV19; Secure; HttpOnly; "
        "SameSite=lax; Domain=localhost:8049; Path=/content/; Max-Age=43200",
        "CloudFront-Signature=NWPfgotDu2DkH4f4d68eUkLNNaVfJGhizzxRRexb55XtcJ7C"
        "96pAxzGwq~zQbh6wr0xa2XxfYwR5ytK52bWCrBLbVTr9Ygt3fwgqCe9uqiuvrhSu~X7wg"
        "EOnEoONwJx6Xl5VADEN2apUnPLCXITD2mv-6rChXfzgZ1x4PDy8j80_; Secure; "
        "HttpOnly; SameSite=lax; Domain=localhost:8049; Path=/content/; "
        "Max-Age=43200",
        "CloudFront-Key-Pair-Id=XXXXXXXXXXXXXX; Secure; "
        "HttpOnly; SameSite=lax; Domain=localhost:8049; Path=/origin/; "
        "Max-Age=43200",
        "CloudFront-Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2FsaG9zdDo4MDQ5L29yaWdpbi8qIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJBV1M6RXBvY2hUaW1lIjoxNjQ1MDEyODAwfX19XX0_; Secure; HttpOnly; "
        "SameSite=lax; Domain=localhost:8049; Path=/origin/; Max-Age=43200",
        "CloudFront-Signature=Mio3kl9zzBeq6YKcDF4hWG4iHDXg-dpJu~VkdZIXeXO3Ilg1"
        "796TZQAdjK-czNnZC0T5efW3DliJAeLJhXw~u1VhNJRCIoQ6fLbCVux1TG30-P-EW4~kR"
        "fSgeZ5vEW2u0MYzlCJMfwYIJ1CVez9Lu2wkcH21PNCcsibKnmNfcnM_; Secure; "
        "HttpOnly; SameSite=lax; Domain=localhost:8049; Path=/origin/; "
        "Max-Age=43200",
    ]
    # Sanity check at least one policy
    content_policy = b64decode(
        cookies[1].split("=", maxsplit=1)[1].split(";")[0]
    )
    assert json.loads(content_policy) == {
        "Statement": [
            {
                "Resource": "http://localhost:8049/content/*",
                "Condition": {"DateLessThan": {"AWS:EpochTime": 1645012800}},
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
