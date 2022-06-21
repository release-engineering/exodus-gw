import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from freezegun import freeze_time

from exodus_gw.main import app
from exodus_gw.routers import cdn
from exodus_gw.settings import get_environment


@freeze_time("2022-02-16")
def test_cdn_redirect_(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    with TestClient(app) as client:
        get_r = client.get("/test/cdn/some/url", allow_redirects=False)
        head_r = client.head("/test/cdn/some/url", allow_redirects=False)

    expected_url = (
        "http://localhost:8049/_/cookie/some/url?Expires=1644971400&"
        "Signature=QXdMBQNyDLYeIsJzV7bKHnqYQSErcz9OYdJTuIYKVHCDaDiqP"
        "OUjqkSXX4fm7A-Fi2roZSlWhyd4emrlC8hvNdPLZb3-7LHMVqau1QK9qFlh"
        "Zz~aP1i4~Zud-kTot4JO4ewE8LdCkQL1pda-on~wVTXhiAtB7EaX8aR3dnB"
        "ZmYo_&Key-Pair-Id=XXXXXXXXXXXXXX"
    )

    assert get_r.ok
    assert get_r.status_code == 302
    assert get_r.headers["location"] == expected_url

    assert head_r.ok
    assert head_r.status_code == 302
    assert head_r.headers["location"] == expected_url


@freeze_time("2022-02-16")
def test_sign_url_with_query(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    env = get_environment("test")

    # URL-parameter separator should be "&" when a query string is given.
    signed_url = cdn.sign_url("?cdest=some-file&ckey=a1bc3d4", 60, env)
    expected_url = (
        "http://localhost:8049/_/cookie/?cdest=some-file&ckey=a1bc3d4&"
        "Expires=1644969660&Signature=G1abxXXex82KUjdKSB3Pf6v~3GNnu-tU"
        "KRiLq5QgowWwC13AkRcy92olUC6kMy1NTtnqzK4b4Fzs8pAOYtWVJil3bMqv6"
        "omweBJ7LSsW0KTv4XlWwPYeV5aD8nQ26HRGspkmmoVibmFgvRtWgf06v70ynK"
        "aE4wZexdMvbT9RCis_&Key-Pair-Id=XXXXXXXXXXXXXX"
    )

    assert signed_url == expected_url


def test_sign_url_without_private_key():
    env = get_environment("test")

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env)

    assert "Missing private key for CDN access" in str(exc_info)


def test_sign_url_without_key_id(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    env = get_environment("test")
    env.cdn_key_id = None

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env)

    assert "Missing key ID for CDN access" in str(exc_info)


def test_sign_url_without_cdn_url(monkeypatch, dummy_private_key):
    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    env = get_environment("test")
    env.cdn_url = None

    with pytest.raises(HTTPException) as exc_info:
        cdn.sign_url("some/uri", 60, env)

    assert "Missing cdn_url, nowhere to redirect request" in str(exc_info)
