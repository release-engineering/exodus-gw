import gzip
from asyncio import StreamReader
from collections.abc import Mapping

import pytest
from botocore.exceptions import ClientError
from pytest import LogCaptureFixture
from sqlalchemy.orm import Session

from exodus_gw.models import Item, Publish
from exodus_gw.schemas import PublishStates
from exodus_gw.settings import load_settings
from exodus_gw.worker.autoindex import AutoindexEnricher, autoindex_partial

# Some minimal valid yum repodata XML used in later tests.
SAMPLE_REPOMD_XML = b"""
<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
  <revision>1659419679</revision>
  <data type="primary">
    <checksum type="sha256">3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129</checksum>
    <open-checksum type="sha256">ad4149ec99b72282ab4891ea5d224db02cc3d7e0ad5c1bdaba56c21cbd4ab132</open-checksum>
    <location href="repodata/3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129-primary.xml.gz"/>
    <timestamp>1659419679</timestamp>
    <size>598</size>
    <open-size>1127</open-size>
  </data>
</repomd>
""".strip()

SAMPLE_PRIMARY_XML = b"""
<?xml version="1.0" encoding="UTF-8"?>
<metadata xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm" packages="1">
<package type="rpm">
  <name>walrus</name>
  <arch>noarch</arch>
  <version epoch="0" ver="5.21" rel="1"/>
  <checksum type="sha256" pkgid="YES">e837a635cc99f967a70f34b268baa52e0f412c1502e08e924ff5b09f1f9573f2</checksum>
  <summary>A dummy package of walrus</summary>
  <description>A dummy package of walrus</description>
  <packager></packager>
  <url>http://tstrachota.fedorapeople.org</url>
  <time file="1659419641" build="1331831368"/>
  <size package="2445" installed="42" archive="296"/>
  <location href="pkgs/w/walrus-5.21-1.noarch.rpm"/>
  <format>
    <rpm:license>GPLv2</rpm:license>
    <rpm:vendor></rpm:vendor>
    <rpm:group>Internet/Applications</rpm:group>
    <rpm:buildhost>smqe-ws15</rpm:buildhost>
    <rpm:sourcerpm>walrus-5.21-1.src.rpm</rpm:sourcerpm>
    <rpm:header-range start="872" end="2293"/>
    <rpm:provides>
      <rpm:entry name="walrus" flags="EQ" epoch="0" ver="5.21" rel="1"/>
    </rpm:provides>
  </format>
</package>
</metadata>
""".strip()


class FakeS3Getter:
    """An implementation of s3.get_object returning canned responses."""

    def __init__(
        self,
        expected_bucket: str,
        responses: Mapping[str, tuple[bytes, Mapping[str, str]]],
    ):
        self.expected_bucket = expected_bucket
        self.responses = responses

    def __call__(self, Bucket: str, Key: str):
        assert Bucket == self.expected_bucket

        (content, headers) = self.responses[Key]

        reader = StreamReader()
        reader.feed_data(content)
        reader.feed_eof()

        return {
            "ResponseMetadata": {
                "HTTPHeaders": headers or {},
            },
            "Body": reader,
        }


@pytest.fixture
def mixed_publish(db: Session, mock_aws_client):
    # Fixture yields a valid publish object with mixed content of various types.
    publish = Publish(env="test", state="PENDING")
    db.add(publish)
    db.commit()

    db.add_all(
        [
            # Arrange for the publish to contain some items which would
            # trigger index generation.
            #
            # A file repo
            Item(
                publish_id=publish.id,
                web_uri="/some/file-repo/PULP_MANIFEST",
                object_key="key1",
            ),
            # A deleted entrypoint on a file repo, should be ignored
            Item(
                publish_id=publish.id,
                web_uri="/some/deleted-file-repo/PULP_MANIFEST",
                object_key="absent",
            ),
            # A file repo where an index already exists (usually would mean
            # we were interrupted partway through before)
            Item(
                publish_id=publish.id,
                web_uri="/some/other-file-repo/PULP_MANIFEST",
                object_key="key1",
            ),
            Item(
                publish_id=publish.id,
                web_uri="/some/other-file-repo/.__exodus_autoindex",
                object_key="existing-index-key",
            ),
            # A valid yum repo
            Item(
                publish_id=publish.id,
                web_uri="/some/yum-repo/repodata/repomd.xml",
                object_key="key2",
            ),
            Item(
                publish_id=publish.id,
                web_uri="/some/yum-repo/repodata/3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129-primary.xml.gz",
                object_key="key3",
            ),
            # A deleted entrypoint on a yum repo, should be ignored
            Item(
                publish_id=publish.id,
                web_uri="/some/deleted-yum-repo/repodata/repomd.xml",
                object_key="absent",
            ),
            # An invalid yum repo (we will set up a corrupt primary XML later)
            Item(
                publish_id=publish.id,
                web_uri="/some/invalid-yum-repo/repodata/repomd.xml",
                object_key="key2",
            ),
            Item(
                publish_id=publish.id,
                web_uri="/some/invalid-yum-repo/repodata/3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129-primary.xml.gz",
                object_key="key4",
            ),
        ]
    )

    # Arrange for some matching content to be available in S3
    mock_aws_client.get_object.side_effect = FakeS3Getter(
        expected_bucket="my-bucket",
        responses={
            # valid PULP_MANIFEST
            "key1": (
                (
                    b"somefile,fa687b8f847b5301b6da817fdbe612558aa69c65584ec5781f3feb0c19ff8f24,379584512\r\n"
                    b"anotherfile,eab749310c95b4751ef9df7d7906ae0b8021c8e0dbc280c3efc8e967d5e60e71,4324327424\r\n"
                ),
                {"content-type": "text/plain"},
            ),
            # valid yum repo - repomd.xml
            "key2": (SAMPLE_REPOMD_XML, {"content-type": "text/xml"}),
            # valid yum repo - primary xml (compressed)
            "key3": (
                gzip.compress(SAMPLE_PRIMARY_XML),
                {"content-type": "application/x-gzip"},
            ),
            # invalid yum repo - primary xml (compressed)
            "key4": (
                gzip.compress(
                    b"hmm, this doesn't seem the right format, does it?"
                ),
                {"content-type": "application/x-gzip"},
            ),
        },
    )

    # Arrange for put_object to have some basic successful responses
    async def put_object(Key, **kwargs):
        return {"ETag": Key}

    mock_aws_client.put_object.side_effect = put_object

    # head_object will be called per each object we're about to upload.
    # There are 5 in total. We'll make it so there's a mix of successful
    # calls and 404 errors (missing objects).
    mock_aws_client.head_object.side_effect = [
        {},
        ClientError(
            {"Error": {"Code": "404"}},
            "HeadObject",
        ),
        {},
        ClientError(
            {"Error": {"Code": "404"}},
            "HeadObject",
        ),
        {},
    ]

    db.commit()

    return publish


async def test_enricher_empty(db: Session, caplog: LogCaptureFixture):
    """AutoindexEnricher should succeed but do nothing on an empty publish."""

    caplog.set_level("INFO", "exodus-gw")

    publish = Publish(env="test", state="PENDING")
    db.add(publish)
    db.commit()

    settings = load_settings()
    enricher = AutoindexEnricher(publish, "test", settings)

    # It should run to completion
    await enricher.run()

    # But it shouldn't have added anything
    db.refresh(publish)
    assert list(publish.items) == []

    # And this is the reason why
    assert "Found 0 path(s) eligible for autoindex" in caplog.text


async def test_enricher_disabled(db: Session, caplog: LogCaptureFixture):
    """AutoindexEnricher should not do anything if disabled by settings."""

    caplog.set_level("DEBUG", "exodus-gw")

    publish = Publish(env="test", state="PENDING")
    db.add(publish)
    db.commit()

    settings = load_settings()
    settings.autoindex_filename = ""
    enricher = AutoindexEnricher(publish, "test", settings)

    # It should run to completion
    await enricher.run()

    # But it shouldn't have added anything
    db.refresh(publish)
    assert list(publish.items) == []

    # And this is the reason why
    assert "autoindex is disabled" in caplog.text


async def test_enricher_mixed(
    db: Session,
    caplog: LogCaptureFixture,
    mixed_publish: Publish,
    mock_aws_client,
):
    """AutoindexEnricher should generate indexes for supported content types."""

    caplog.set_level("DEBUG", "exodus-gw")

    settings = load_settings()
    enricher = AutoindexEnricher(mixed_publish, "test", settings)

    # It should run to completion
    await enricher.run()

    # Have a look at the items on the publish now...
    uri_to_key = {}
    db.refresh(mixed_publish)
    for item in mixed_publish.items:
        uri_to_key[item.web_uri] = item.object_key

    # All these items should exist (see the added indexes)
    assert sorted(uri_to_key.keys()) == [
        # Deleted items still exist on the publish, but they obviously
        # don't get any autoindex created.
        "/some/deleted-file-repo/PULP_MANIFEST",
        "/some/deleted-yum-repo/repodata/repomd.xml",
        # added index for the pulp file repo
        "/some/file-repo/.__exodus_autoindex",
        "/some/file-repo/PULP_MANIFEST",
        # did not add any indexes for the invalid-yum-repo because it was invalid
        "/some/invalid-yum-repo/repodata/3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129-primary.xml.gz",
        "/some/invalid-yum-repo/repodata/repomd.xml",
        # this repo already had an index file
        "/some/other-file-repo/.__exodus_autoindex",
        "/some/other-file-repo/PULP_MANIFEST",
        # added indexes for the valid yum repo.
        # It needs to add an index at every level of the "directory" tree, mirroring
        # the structure found in the repodata.
        "/some/yum-repo/.__exodus_autoindex",
        "/some/yum-repo/pkgs/.__exodus_autoindex",
        "/some/yum-repo/pkgs/w/.__exodus_autoindex",
        "/some/yum-repo/repodata/.__exodus_autoindex",
        "/some/yum-repo/repodata/3a7a286e13883d497b2e3c7029ceb7c372ff2529bbfa22d0c890285ce6aa3129-primary.xml.gz",
        "/some/yum-repo/repodata/repomd.xml",
    ]

    # The index which already existed should have been kept as-is
    assert (
        uri_to_key["/some/other-file-repo/.__exodus_autoindex"]
        == "existing-index-key"
    )

    # It should log a per-repo summary which includes how many items
    # were uploaded
    assert (
        "autoindex of /some/yum-repo: generated 4, uploaded 2 item(s)"
        in caplog.text
    )

    # It should log a global summary also, covering all items in the publish.
    # This log does not have access to the upload count.
    assert "autoindex complete: generated 5 item(s)" in caplog.text

    # It should warn for the repo which was skipped
    assert (
        "autoindex for /some/invalid-yum-repo skipped due to invalid content"
        in caplog.text
    )

    # Correctness of index generation is tested properly in repo-autoindex,
    # but lets at least sanity check one added piece of content...
    key = uri_to_key["/some/yum-repo/pkgs/w/.__exodus_autoindex"]

    # Find the s3 upload call for that key
    put_content = None
    for put_call in mock_aws_client.put_object.mock_calls:
        if put_call.kwargs.get("Key") == key:
            assert put_content is None, f"object {key} put more than once!?"
            assert put_call.kwargs["Bucket"] == "my-bucket"
            put_content = put_call.kwargs["Body"]

    assert put_content is not None, f"missing put_object for {key}"

    # It should have a link to the package in this directory
    assert (
        b'<a href="walrus-5.21-1.noarch.rpm">walrus-5.21-1.noarch.rpm</a>'
        in put_content
    )


async def test_enricher_head_errors(
    db: Session, caplog: LogCaptureFixture, mock_aws_client
):
    """AutoindexEnricher should propagate errors during S3 HEAD requests."""

    caplog.set_level("DEBUG", "exodus-gw")

    publish = Publish(env="test", state="PENDING")
    db.add(publish)
    db.commit()

    db.add_all(
        [
            Item(
                publish_id=publish.id,
                web_uri="/some/file-repo/PULP_MANIFEST",
                object_key="key1",
            ),
        ]
    )

    # Arrange for some matching content to be available in S3
    mock_aws_client.get_object.side_effect = FakeS3Getter(
        expected_bucket="my-bucket",
        responses={
            # valid PULP_MANIFEST
            "key1": (
                (
                    b"somefile,fa687b8f847b5301b6da817fdbe612558aa69c65584ec5781f3feb0c19ff8f24,379584512\r\n"
                    b"anotherfile,eab749310c95b4751ef9df7d7906ae0b8021c8e0dbc280c3efc8e967d5e60e71,4324327424\r\n"
                ),
                {"content-type": "text/plain"},
            ),
        },
    )

    head_error = ClientError(
        {"Error": {"Code": "403"}},
        "HeadObject",
    )

    mock_aws_client.head_object.side_effect = head_error

    db.commit()

    settings = load_settings()
    enricher = AutoindexEnricher(publish, "test", settings)

    # It should fail
    with pytest.raises(ClientError) as excinfo:
        await enricher.run()

    # Should have propagated exactly the original error
    assert excinfo.value is head_error


def test_autoindex_partial(
    db: Session,
    mixed_publish: Publish,
):
    """autoindex_partial should generate indexes for just the requested entry points."""

    # First note all the URIs present on the publish so we can
    # see what's added later.
    initial_uris = set([i.web_uri for i in mixed_publish.items])

    # It should succeed
    settings = load_settings()
    autoindex_partial(
        publish_id=mixed_publish.id,
        entrypoint_paths=["/some/yum-repo/repodata/repomd.xml"],
        settings=settings,
    )

    # What was added?
    added_uris = set()
    db.refresh(mixed_publish)
    for item in mixed_publish.items:
        if item.web_uri not in initial_uris:
            added_uris.add(item.web_uri)

    # It should have added exactly what we expect:
    # ONLY the indexes for the one entry point we passed in.
    assert sorted(added_uris) == [
        "/some/yum-repo/.__exodus_autoindex",
        "/some/yum-repo/pkgs/.__exodus_autoindex",
        "/some/yum-repo/pkgs/w/.__exodus_autoindex",
        "/some/yum-repo/repodata/.__exodus_autoindex",
    ]


def test_autoindex_partial_bailout(
    db: Session,
    mixed_publish: Publish,
    caplog: pytest.LogCaptureFixture,
):
    """autoindex_partial should do nothing if publish is in unexpected state."""

    # Check how many items there were prior to invoking the actor.
    initial_count = len(mixed_publish.items)

    # Simulate what happens if the publish already got committed by the time
    # the actor runs.
    mixed_publish.state = PublishStates.committed
    db.commit()

    # It should succeed
    settings = load_settings()
    autoindex_partial(
        publish_id=mixed_publish.id,
        entrypoint_paths=["/some/yum-repo/repodata/repomd.xml"],
        settings=settings,
    )

    # But it should not have added any items
    db.refresh(mixed_publish)
    assert len(mixed_publish.items) == initial_count

    # And it should have logged about this
    assert (
        "Dropping autoindex request for publish in state 'COMMITTED'"
        in caplog.text
    )
