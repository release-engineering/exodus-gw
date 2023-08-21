import gzip
import hashlib
import logging
from time import monotonic
from typing import AsyncGenerator, Generator, Optional

from repo_autoindex import ContentError, Fetcher, autoindex
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from exodus_gw.aws.client import S3ClientWrapper
from exodus_gw.models import Item, Publish
from exodus_gw.settings import Environment, Settings, get_environment

LOG = logging.getLogger("exodus-gw")


def object_key(content: bytes) -> str:
    hasher = hashlib.sha256()
    hasher.update(content)
    return hasher.hexdigest().lower()


class PublishContentFetcher:
    # An implementation of repo_autoindex.Fetcher capable of fetching content
    # from the current publish in progress, for the purpose of index generation.

    def __init__(
        self,
        db: Session,
        publish: Publish,
        s3_client,
        environment: Environment,
    ):
        self.db = db
        self.publish = publish
        self.s3_client = s3_client
        self.environment = environment

    async def __call__(self, uri: str) -> Optional[str]:
        LOG.debug("Requested to fetch: %s", uri, extra={"event": "publish"})

        matched = (
            self.db.query(Item)
            .filter(Item.publish_id == self.publish.id, Item.web_uri == uri)
            .all()
        )
        if not matched:
            LOG.debug("%s: no content available", uri)
            return None

        item: Item = matched[0]
        key: Optional[str] = item.object_key
        LOG.debug(
            "%s can be fetched from %s", uri, key, extra={"event": "publish"}
        )
        response = await self.s3_client.get_object(
            Bucket=self.environment.bucket, Key=key
        )
        LOG.debug("S3 response: %s", response, extra={"event": "publish"})

        content_type: str = response["ResponseMetadata"]["HTTPHeaders"][
            "content-type"
        ]
        content: bytes = await response["Body"].read()

        if uri.endswith(".gz") and content_type in (
            "binary/octet-stream",
            "application/octet-stream",
            "application/x-gzip",
        ):
            content = gzip.decompress(content)

        return content.decode("utf-8")


class AutoindexEnricher:
    # This object manages the process of enriching a Publish object with
    # automatically generated index pages.
    #
    # It should be provided a Publish which may or may not already contain
    # autoindex pages. When run() completes, the Publish will have had any
    # applicable autoindexes added to it.

    def __init__(self, publish: Publish, env_name: str, settings: Settings):
        self.publish = publish
        self.env_name = env_name
        self.env = get_environment(env_name, settings)
        self.settings = settings

        ins = inspect(publish)
        assert ins
        # The object returned by inspect() leads mypy to believe that Session
        # is Optional[Session]. Typed as Session here to contain the complaint.
        # Otherwise all db calls trigger mypy, as None, which is included in
        # Optional (Union[x, None]), doesn't have a 'query' attr.
        self.db: Session = ins.session  # type: ignore

        self.item_query = self.db.query(Item).filter(
            Item.publish_id == publish.id
        )

    @property
    def repomd_xml_items(self) -> list[Item]:
        return self.item_query.filter(
            Item.web_uri.like("%/repodata/repomd.xml")
        ).all()

    @property
    def pulp_manifest_items(self) -> list[Item]:
        return self.item_query.filter(
            Item.web_uri.like("%/PULP_MANIFEST")
        ).all()

    @property
    def repo_base_uris(self) -> Generator[str, None, None]:
        yielded = set()

        for item in self.repomd_xml_items:
            repomd_uri: str = item.web_uri[: -len("/repodata/repomd.xml")]
            if repomd_uri not in yielded:
                yielded.add(repomd_uri)
                yield repomd_uri

        for item in self.pulp_manifest_items:
            manifest_uri: str = item.web_uri[: -len("/PULP_MANIFEST")]
            if manifest_uri not in yielded:
                yielded.add(manifest_uri)
                yield manifest_uri

    @property
    def uris_for_autoindex(self) -> list[str]:
        out = []

        for repo_base_uri in self.repo_base_uris:
            index_uri = f"{repo_base_uri}/{self.settings.autoindex_filename}"
            if self.item_query.filter(Item.web_uri == index_uri).count():
                LOG.debug(
                    "Index at %s already exists",
                    index_uri,
                    extra={"event": "publish"},
                )
            else:
                LOG.debug(
                    "Should generate index for %s",
                    repo_base_uri,
                    extra={"event": "publish"},
                )
                out.append(repo_base_uri)

        return out

    def fetcher_for_client(self, s3_client) -> PublishContentFetcher:
        return PublishContentFetcher(
            db=self.db,
            publish=self.publish,
            s3_client=s3_client,
            environment=self.env,
        )

    async def autoindex_items(
        self, s3_client, fetcher: Fetcher, base_uri: str
    ) -> AsyncGenerator[Item, None]:
        """Given a base_uri pointing at a possible content repository (e.g. yum repo)
        in the current publish, generates and yields Items pointing at static HTML
        indexes for the repository.

        Includes the side effect of uploading the index content to S3.
        """
        before = monotonic()
        count = 0

        async for idx in autoindex(
            base_uri,
            fetcher=fetcher,
        ):
            count += 1

            index_uri_components = [base_uri]
            if idx.relative_dir:
                index_uri_components.append(idx.relative_dir)
            index_uri_components.append(self.settings.autoindex_filename)
            web_uri = "/".join(index_uri_components)

            content_bytes = idx.content.encode("utf-8")
            content_key = object_key(content_bytes)

            response = await s3_client.put_object(
                Body=content_bytes,
                ContentLength=len(content_bytes),
                Bucket=self.env.bucket,
                Key=content_key,
            )

            LOG.info(
                "Uploaded autoindex %s => %s (ETag: %s)",
                web_uri,
                content_key,
                response.get("ETag"),
                extra={"event": "publish", "success": True},
            )

            item = Item(
                web_uri=web_uri,
                object_key=content_key,
                content_type="text/html; charset=UTF-8",
                publish_id=self.publish.id,
            )
            yield item

        duration = monotonic() - before
        LOG.info(
            "autoindex of %s generated %s item(s) in %.02f second(s)",
            base_uri,
            count,
            duration,
            extra={"event": "publish", "success": True},
        )

    async def run(self):
        if not self.settings.autoindex_filename:
            LOG.debug("autoindex is disabled", extra={"event": "publish"})
            return

        LOG.info("Starting autoindex", extra={"event": "publish"})

        before = monotonic()
        count = 0

        uris = self.uris_for_autoindex
        LOG.info(
            "Found %d path(s) eligible for autoindex",
            len(uris),
            extra={"event": "publish"},
        )

        async with S3ClientWrapper(profile=self.env.aws_profile) as s3_client:
            fetcher = self.fetcher_for_client(s3_client)
            for base_uri in uris:
                try:
                    async for item in self.autoindex_items(
                        s3_client, fetcher, base_uri
                    ):
                        self.db.add(item)
                        # We commit after generation of each object so that, if
                        # interrupted, we won't lose the progress made so far.
                        self.db.commit()
                        count += 1
                except ContentError:
                    # If we get here it means an index couldn't be generated due to
                    # problems in the content being published; for example, a yum repo
                    # with corrupt metadata. We don't want publish to be blocked here
                    # as it is not the job of this service to validate published
                    # content. We'll warn and continue, meaning that index generation
                    # is best-effort.
                    LOG.warning(
                        "autoindex for %s skipped due to invalid content",
                        base_uri,
                        exc_info=True,
                        extra={"event": "publish"},
                    )

        duration = monotonic() - before
        LOG.info(
            "autoindex complete: generated %s item(s) in %.02f second(s)",
            count,
            duration,
            extra={"event": "publish", "success": True},
        )
