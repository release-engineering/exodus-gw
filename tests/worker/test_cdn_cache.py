import pathlib
from datetime import datetime, timedelta

import fastpurge
import pytest
from dramatiq.middleware import CurrentMessage
from more_executors import f_return
from sqlalchemy.orm import Session

from exodus_gw.models.service import Task
from exodus_gw.settings import load_settings
from exodus_gw.worker import flush_cdn_cache


class FakeFastPurgeClient:
    # Minimal fake for a fastpurge.FastPurgeClient which records
    # purged URLs and always succeeds.
    INSTANCE = None

    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self._purged_urls = []
        FakeFastPurgeClient.INSTANCE = self

    def purge_by_url(self, urls):
        self._purged_urls.extend(urls)
        return f_return({"fake": "response"})


@pytest.fixture(autouse=True)
def fake_fastpurge_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fastpurge, "FastPurgeClient", FakeFastPurgeClient)
    yield
    FakeFastPurgeClient.INSTANCE = None


@pytest.fixture
def fake_message_id(monkeypatch: pytest.MonkeyPatch) -> str:
    class FakeMessage:
        @property
        def message_id(self):
            return "3ce55238-f7d7-46d1-a302-c79674108dc9"

    monkeypatch.setattr(
        CurrentMessage, "get_current_message", lambda: FakeMessage()
    )

    return FakeMessage().message_id


def test_flush_cdn_cache_bad_task(
    db: Session,
    caplog: pytest.LogCaptureFixture,
    fake_message_id: str,
):
    """flush_cdn_cache bails out if no appropriate task exists."""
    settings = load_settings()

    # It should run to completion...
    flush_cdn_cache(
        paths=["/foo", "/bar"],
        env="test",
        settings=settings,
    )

    # ...but it should complain
    assert "Task in unexpected state" in caplog.text


def test_flush_cdn_cache_expired_task(
    db: Session,
    caplog: pytest.LogCaptureFixture,
    fake_message_id: str,
):
    """flush_cdn_cache bails out if task has passed the deadline."""
    settings = load_settings()

    task = Task(id=fake_message_id)
    task.deadline = datetime.utcnow() - timedelta(hours=3)
    task.state = "NOT_STARTED"
    db.add(task)
    db.commit()

    # It should run to completion...
    flush_cdn_cache(
        paths=["/foo", "/bar"],
        env="test",
        settings=settings,
    )

    # ...but it should complain
    assert "Task exceeded deadline" in caplog.text

    # And the task should be marked as failed
    db.refresh(task)
    assert task.state == "FAILED"


def test_flush_cdn_cache_fastpurge_disabled(
    db: Session,
    caplog: pytest.LogCaptureFixture,
    fake_message_id: str,
):
    """flush_cdn_cache succeeds but does nothing if fastpurge is not configured."""
    settings = load_settings()

    task = Task(id=fake_message_id)
    task.state = "NOT_STARTED"
    db.add(task)
    db.commit()

    # It should run to completion...
    flush_cdn_cache(
        paths=["/foo", "/bar"],
        env="test",
        settings=settings,
    )

    # And the task should have succeeded
    db.refresh(task)
    assert task.state == "COMPLETE"

    # But it didn't actually touch the fastpurge API and the logs
    # should tell us about this
    assert "fastpurge is not enabled" in caplog.text
    assert "Skipped flush" in caplog.text


def test_flush_cdn_cache_typical(
    db: Session,
    caplog: pytest.LogCaptureFixture,
    fake_message_id: str,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """flush_cdn_cache performs expected cache flushes in
    a typical usage scenario.
    """

    # Write an ini file with some fastpurge stuff under our control.
    conf_path = tmp_path / "exodus-gw.ini"
    conf_path.write_text(
        """

[env.cachetest]
aws_profile = cachetest
bucket = my-bucket
table = my-table
config_table = my-config

cdn_url = http://localhost:8049/_/cookie
cdn_key_id = XXXXXXXXXXXXXX

cache_flush_urls =
    https://cdn1.example.com
    https://cdn2.example.com/root

cache_flush_arl_templates =
    S/=/123/4567/{ttl}/cdn1.example.com/{path} cid=///
    S/=/234/6677/{ttl}/cdn2.example.com/other/{path} x/y/z

"""
    )

    # Make load_settings use our config file above.
    monkeypatch.setenv("EXODUS_GW_INI_PATH", str(conf_path))

    # Provide some fastpurge credentials
    monkeypatch.setenv("EXODUS_GW_FASTPURGE_HOST_CACHETEST", "fphost")
    monkeypatch.setenv("EXODUS_GW_FASTPURGE_CLIENT_TOKEN_CACHETEST", "ctok")
    monkeypatch.setenv("EXODUS_GW_FASTPURGE_CLIENT_SECRET_CACHETEST", "csec")
    monkeypatch.setenv("EXODUS_GW_FASTPURGE_ACCESS_TOKEN_CACHETEST", "atok")

    settings = load_settings()

    task = Task(id=fake_message_id)
    task.state = "NOT_STARTED"
    db.add(task)
    db.commit()

    # It should run to completion...
    flush_cdn_cache(
        paths=[
            # Paths here are chosen to exercise:
            # - different TTL values for different types of file
            # - leading "/" vs no leading "/" - both should be tolerated
            "/path/one/repodata/repomd.xml",
            "path/two/listing",
            "third/path",
        ],
        env="cachetest",
        settings=settings,
    )

    # The task should have succeeded
    db.refresh(task)
    assert task.state == "COMPLETE"

    # Check how it used the fastpurge client
    fp_client = FakeFastPurgeClient.INSTANCE

    # It should have created a client
    assert fp_client

    # It should have provided the credentials from env vars
    assert fp_client._kwargs["auth"] == {
        "access_token": "atok",
        "client_secret": "csec",
        "client_token": "ctok",
        "host": "fphost",
    }

    # It should have flushed cache for all the expected URLs,
    # using both the CDN root URLs and the ARL templates
    assert sorted(fp_client._purged_urls) == [
        # Used the ARL templates. Note the different TTL values
        # for different paths.
        "S/=/123/4567/10m/cdn1.example.com/path/two/listing cid=///",
        "S/=/123/4567/30d/cdn1.example.com/third/path cid=///",
        "S/=/123/4567/4h/cdn1.example.com/path/one/repodata/repomd.xml cid=///",
        "S/=/234/6677/10m/cdn2.example.com/other/path/two/listing x/y/z",
        "S/=/234/6677/30d/cdn2.example.com/other/third/path x/y/z",
        "S/=/234/6677/4h/cdn2.example.com/other/path/one/repodata/repomd.xml x/y/z",
        # Used the CDN URL which didn't have a leading path.
        "https://cdn1.example.com/path/one/repodata/repomd.xml",
        "https://cdn1.example.com/path/two/listing",
        "https://cdn1.example.com/third/path",
        # Used the CDN URL which had a leading path.
        "https://cdn2.example.com/root/path/one/repodata/repomd.xml",
        "https://cdn2.example.com/root/path/two/listing",
        "https://cdn2.example.com/root/third/path",
    ]
