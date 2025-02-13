import gzip
import json
import logging
from datetime import datetime
from itertools import islice
from threading import Lock
from typing import Any

import backoff
from botocore.exceptions import EndpointConnectionError

from .. import models
from ..aws.client import DynamoDBClientWrapper
from ..aws.util import uri_alias
from ..settings import Environment, Settings, get_environment

LOG = logging.getLogger("exodus-gw")


class DynamoDB:
    def __init__(
        self,
        env: str,
        settings: Settings,
        from_date: str,
        env_obj: Environment | None = None,
        deadline: datetime | None = None,
        mirror_writes: bool = False
    ):
        self.env = env
        self.settings = settings
        self.from_date = from_date
        self.env_obj = env_obj or get_environment(env)
        self.deadline = deadline
        self.mirror_writes = mirror_writes
        self.client = DynamoDBClientWrapper(self.env_obj.aws_profile).client
        self._lock = Lock()
        self._definitions = None

    @property
    def definitions(self):
        if self._definitions is None:
            # This class is used from multiple threads, which could be
            # competing to load definitions. Make sure we load only
            # a single time.
            with self._lock:
                if self._definitions is None:
                    self._definitions = self.query_definitions()
        return self._definitions

    def _aliases(
        self, alias_types: list[str]
    ) -> list[tuple[str, str, list[str]]]:
        out: list[tuple[str, str, list[str]]] = []

        for k, v in self.definitions.items():
            if k in alias_types:
                for alias in v:
                    out.append(
                        (
                            alias["src"],
                            alias["dest"],
                            alias.get("exclude_paths") or [],
                        )
                    )

        return out

    @property
    def aliases_for_write(self) -> list[tuple[str, str, list[str]]]:
        # Aliases used when writing items to DynamoDB.
        #
        # Note that these aliases are traversed only in the src => dest
        # direction, which intentionally results in a kind of
        # canonicalization of paths at publish time.
        #
        # Example:
        #
        #   Given an alias of:
        #     src:  /content/dist/rhel8/8
        #     dest: /content/dist/rhel8/8.8
        #
        # If /content/dist/rhel8/8/foo is published, we write
        # an item with path /content/dist/rhel8/8.8/foo and DO NOT
        # write /content/dist/rhel8/8/foo.
        #
        # Note also that rhui_alias is not included here.
        # It's not needed because, in practice, all of the content
        # accessed via those aliases is always *published* on the
        # destination side of an alias.
        #
        # Example:
        #
        #   Given an alias of:
        #     src:  /content/dist/rhel8/rhui
        #     dest: /content/dist/rhel8
        #
        # Content is always published under the /content/dist/rhel8 paths,
        # therefore there is no need to resolve the above alias at publish
        # time.
        #
        # It is possible that processing rhui_alias here would not be
        # incorrect, but also possible that adding it might have some
        # unintended effects.
        return self._aliases(["origin_alias", "releasever_alias"])

    @property
    def aliases_for_flush(self) -> list[tuple[str, str, list[str]]]:
        # Aliases used when flushing cache.
        out = self._aliases(["origin_alias", "releasever_alias", "rhui_alias"])

        # When calculating paths for cache flush, the aliases should be resolved
        # in *both* directions, so also return inverted copies of the aliases.
        #
        # Example:
        #
        #   Given an alias of:
        #     src:  /content/dist/rhel8/8
        #     dest: /content/dist/rhel8/8.8
        #
        #   - If /content/dist/rhel8/8/foo is published, then
        #     /content/dist/rhel8/8.8/foo should also have cache flushed
        #     (src => dest)
        #   - If /content/dist/rhel8/8.8/foo is published, then
        #     /content/dist/rhel8/8/foo should also have cache flushed
        #     (dest => src)
        #
        # In practice, while it seems technically correct to do this for all aliases,
        # this is mainly needed for RHUI content.
        #
        # Example:
        #
        #   Given an alias of:
        #     src:  /content/dist/rhel8/rhui
        #     dest: /content/dist/rhel8
        #
        # We are always publishing content only on the destination side of
        # this alias. If we don't traverse the alias from dest => src then we
        # will miss the fact that /content/dist/rhel8/rhui paths should also
        # have cache flushed.
        out = out + [
            (dest, src, exclusions) for (src, dest, exclusions) in out
        ]

        return out

    @property
    def aliases_for_config_update(self) -> list[tuple[str, str, list[str]]]:
        # Used for determining which aliases were updated during config
        # deployment. Unliked alias_for_flush these aliases should only be in
        # the forward direction. This avoids urls being unnecessarily flushed.
        #
        # Example:
        #
        #   Given the aliases:
        #      src : /content/dist/rhel/workstation/5/5Client
        #      dest: /content/dist/rhel/workstation/5/5.11
        #
        #      src : /content/dist/rhel/workstation/5/5Workstation
        #      dest: /content/dist/rhel/workstation/5/5.11
        #
        #  Since both aliases point to the same destination, if we checked for
        #  alias updates in both directions, it would appear that the alias is
        #  getting updated. i.e: the code would see .../5/5.11 pointing to
        #  .../5/5Client, and then see .../5/5.11 pointing to .../5/5Workstation
        #  and will wrongly believe this is because the alias has changed.

        return self._aliases(
            ["origin_alias", "releasever_alias", "rhui_alias"]
        )

    def query_definitions(self) -> dict[str, Any]:
        """Query the definitions in the config_table. If definitions are found, return them. Otherwise,
        return a valid empty configuration."""

        # If a query result is not found, return a reasonable default object representing a valid
        # empty configuration.
        out: dict[str, Any] = {
            "listing": {},
            "origin_alias": [],
            "releasever_alias": [],
            "rhui_alias": [],
        }

        LOG.info(
            "Loading exodus-config as at %s.",
            self.from_date,
            extra={"event": "publish"},
        )

        query_result = self.client.query(
            TableName=self.env_obj.config_table,
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression="config_id = :id and from_date <= :d",
            ExpressionAttributeValues={
                ":id": {"S": "exodus-config"},
                ":d": {"S": self.from_date},
            },
        )
        if query_result.get("Items"):
            item = query_result["Items"][0]
            if item_bytes := item["config"].get("B"):
                # new-style: config is compressed and stored as bytes
                item_json = gzip.decompress(item_bytes).decode()
            else:
                # old-style, config was stored as JSON string.
                # Consider deleting this code path in 2025
                item_json = item["config"]["S"]
            out = json.loads(item_json)
        return out

    def uris_for_item(self, item) -> list[str]:
        """Returns all URIs to be written for the given item.

        In practice, always returns either one or two URIs depending on
        configured aliases and other settings, though the caller should
        assume any number of URIs.
        """

        # Resolve aliases. We only write to the deepest path
        # after all alias resolution, hence only using the
        # first result from uri_alias.
        uris = [uri_alias(item.web_uri, self.aliases_for_write)[0]]

        # We only want to mirror writes for release ver aliases. Recalculating
        # the aliases completely is a bit inefficient, but I'd rather not
        # duplicate any alias logic.
        if self.mirror_writes and \
                uri_alias(
                    item.web_uri, self._aliases(["releasever_alias"])
                )[0] != item.web_uri:
            uris.append(item.web_uri)
        return uris

    def create_requests(
        self,
        items: list[models.Item],
        delete: bool = False,
    ):
        """Create the dictionary structure expected by batch_write_item."""
        table_name = self.env_obj.table
        request: dict[str, list[Any]] = {table_name: []}

        # Used to split request into requests < 25 items, to keep boto happy.
        # in reality, it'll currently only be two requests.
        request_list = []
        url_count = 0
        for item in items:
            # Items carry their own from_date. This effectively resolves
            # conflicts in the case of two publishes updating the same web_uri
            # at the same time; commits can be done in either order, and after
            # both commits complete, the "winner" is whoever had the latest
            # updated timestamp.
            from_date = str(item.updated)

            for web_uri in self.uris_for_item(item):
                # request is too large
                if url_count == self.settings.write_batch_size:
                    url_count = 0
                    request_list.append(request)
                    request = {table_name: []}
                if delete:
                    request[table_name].append(
                        {
                            "DeleteRequest": {
                                "Key": {
                                    "from_date": {"S": from_date},
                                    "web_uri": {"S": web_uri},
                                }
                            }
                        }
                    )
                else:
                    request[table_name].append(
                        {
                            "PutRequest": {
                                "Item": {
                                    "from_date": {"S": from_date},
                                    "web_uri": {"S": web_uri},
                                    "object_key": {"S": item.object_key},
                                    "content_type": {"S": item.content_type},
                                }
                            }
                        }
                    )
                url_count += 1

        request_list.append(request)
        return request_list

    def create_config_request(self, config):
        request = {
            self.env_obj.config_table: [
                {
                    "PutRequest": {
                        "Item": {
                            "from_date": {"S": self.from_date},
                            "config_id": {"S": "exodus-config"},
                            "config": {
                                "B": gzip.compress(json.dumps(config).encode())
                            },
                        }
                    }
                },
            ]
        }
        return request

    def batch_write(self, request: dict[str, Any]):
        """Wrapper for batch_write_item with retries and item count validation.

        Item limit of 25 is, at this time, imposed by AWS's boto3 library.
        """

        def _max_time():
            # Calculates the retry time limit at runtime, in seconds, based on
            # the deadline, should one be set.
            #
            # E.g., if it's currently 12:20 and there's a deadline set for
            # 12:46, backoff will only permit retrying for 1560 seconds before
            # failing, regardless of the next calculated wait time.
            #
            # The backoff wait generator won't sleep longer than the remaining
            # time. github.com/litl/backoff/blob/master/backoff/_common.py#L34

            if self.deadline is not None:
                now = datetime.utcnow()
                diff = self.deadline.timestamp() - now.timestamp()
                LOG.debug("Remaining time for batch_write: %ds", diff)
                return diff

        @backoff.on_exception(
            wait_gen=backoff.expo,
            exception=EndpointConnectionError,
            max_tries=self.settings.write_max_tries,
            max_time=_max_time,
            logger=LOG,
            backoff_log_level=logging.DEBUG,
        )
        @backoff.on_predicate(
            wait_gen=backoff.expo,
            predicate=lambda response: response["UnprocessedItems"],
            max_tries=self.settings.write_max_tries,
            max_time=_max_time,
            logger=LOG,
            backoff_log_level=logging.DEBUG,
        )
        def _batch_write(req):
            response = self.client.batch_write_item(RequestItems=req)
            return response

        item_count = len(request.get(self.env_obj.table, []))

        if item_count > 25:
            LOG.error(
                "Cannot process more than 25 items per request",
                extra={"event": "publish", "success": False},
            )
            raise ValueError(
                "Request contains too many items (%s)" % item_count
            )

        return _batch_write(request)

    def get_batches(self, items: list[models.Item]):
        """Divide the publish items into batches of size 'write_batch_size'."""
        it = iter(items)
        batches = list(
            iter(lambda: tuple(islice(it, self.settings.write_batch_size)), ())
        )
        return batches

    def write_batch(self, items: list[models.Item], delete: bool = False):
        """Submit a batch of given items for writing via batch_write."""

        requests = self.create_requests(list(items), delete)
        for request in requests:
            try:

                response = self.batch_write(request)
            except Exception:
                LOG.exception(
                    "Exception while %s items on table '%s'",
                    ("deleting" if delete else "writing"),
                    self.env_obj.table,
                    extra={"event": "publish", "success": False},
                )
                raise
            # Raise immediately for put requests.
            # Collect unprocessed items for delete requests and resume deleting.
            if response["UnprocessedItems"]:
                if delete:
                    LOG.error(
                        "Unprocessed items:\n\t%s",
                        (response["UnprocessedItems"]),
                        extra={"event": "publish", "success": False},
                    )
                    raise RuntimeError(
                        "Deletion failed\nSee error log for details"
                    )

                raise RuntimeError("One or more writes were unsuccessful")

            LOG.debug(
                "Items successfully %s",
                "deleted" if delete else "written",
                extra={"event": "publish", "success": True},
            )

    def write_config(self, config):
        request = self.create_config_request(config)
        self.batch_write(request)
        # As well as writing to the DB, update our own local copy
        # so that methods using the config are consistent with what
        # we've just written.
        self._definitions = config
