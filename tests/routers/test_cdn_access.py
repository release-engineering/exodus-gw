from fastapi.testclient import TestClient
from freezegun import freeze_time

from exodus_gw.main import app


@freeze_time("2023-04-20")
def test_cdn_access_typical(
    monkeypatch, dummy_private_key, auth_header, caplog
):
    """cdn-access endpoint returns valid access info in a typical scenario."""

    monkeypatch.setenv("EXODUS_GW_CDN_PRIVATE_KEY_TEST", dummy_private_key)

    with TestClient(app) as client:
        response = client.get(
            "/test/cdn-access?expire_days=60",
            headers=auth_header(roles=["test-cdn-consumer"]),
        )

    # It should have succeeded
    assert response.status_code == 200

    # It should have generated exactly this output.
    assert response.json() == {
        "cookie": (
            "CloudFront-Key-Pair-Id=XXXXXXXXXXXXXX; "
            "CloudFront-Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2FsaG9zdDo4MDQ5LyoiLCJDb25kaXRpb24iOnsiRGF0ZUxlc3NUaGFuIjp7IkFXUzpFcG9jaFRpbWUiOjE2ODcxMzI4MDB9fX1dfQ__; "
            "CloudFront-Signature=P-6pNRKHOOoK6~mKgK~bOb2LXLtepgJuFO4rwzUPKBrrTO2bBhmhAyIzA~W3rOFGWbd~IJ8ZHAKvYKXeg0e4-KI5lkPhM1uzyoqLdmewnfvKUB4TIesEms7JBBabSaiA6plc5cHLN08nql9TGUApBWYM6oycF-0tVBWr4AzBdxU_"
        ),
        "expires": "2023-06-19T00:00Z",
        "url": "http://localhost:8049",
    }

    # It should have logged about the cookie generation.
    expected_message = (
        "Generated cookie for: user=fake-user, key=XXXXXXXXXXXXXX, "
        "resource=http://localhost:8049/*, expires=2023-06-19 00:00:00, "
        "policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cDovL2xvY2FsaG9zdDo4MDQ5LyoiLCJDb25kaXRpb24iOnsiRGF0ZUxlc3NUaGFuIjp7IkFXUzpFcG9jaFRpbWUiOjE2ODcxMzI4MDB9fX1dfQ__"
    )
    assert expected_message in caplog.text


def test_cdn_access_unauthed(auth_header):
    """cdn-access endpoint forbids usage if caller is missing needed role."""

    with TestClient(app) as client:
        response = client.get(
            "/test/cdn-access?expire_days=60",
            headers=auth_header(roles=["some-unrelated-role"]),
        )

    # It should have been forbidden.
    assert response.status_code == 403

    # For this reason.
    assert response.json() == {
        "detail": "this operation requires role 'test-cdn-consumer'"
    }


def test_cdn_access_bad_expiry(monkeypatch, auth_header):
    """cdn-access endpoint fails if caller requests expiration date out of range."""

    monkeypatch.setenv("EXODUS_GW_CDN_MAX_EXPIRE_DAYS", "100")

    with TestClient(app) as client:
        response = client.get(
            "/test/cdn-access?expire_days=6000",
            headers=auth_header(roles=["test-cdn-consumer"]),
        )

    # It should have failed
    assert response.status_code == 400

    # It should have generated exactly this output.
    assert response.json() == {
        "detail": "An expire_days option from 1 to 100 must be provided"
    }
