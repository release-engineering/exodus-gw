import json
import logging
from itertools import islice
from threading import Lock
from typing import Any, Dict, List, Optional

import backoff

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
        env_obj: Optional[Environment] = None,
    ):
        self.env = env
        self.env_obj = env_obj or get_environment(env)
        self.settings = settings
        self.from_date = from_date
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

    def query_definitions(self) -> Dict[str, Any]:
        """Query the definitions in the config_table. If definitions are found, return them. Otherwise,
        return an empty dictionary."""

        # Return an empty dict if a query result is not found
        out: Dict[str, Any] = {}

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
            out = json.loads(item["config"]["S"])
        return out

    def create_request(
        self,
        items: List[models.Item],
        delete: bool = False,
    ):
        """Create the dictionary structure expected by batch_write_item."""
        table_name = self.env_obj.table
        request: Dict[str, List[Any]] = {table_name: []}

        uri_aliases = []
        for k, v in self.definitions.items():
            # Exclude rhui aliases (for now? RHELDST-18849).
            if k in ("origin_alias", "releasever_alias"):
                uri_aliases.extend(v)

        for item in items:
            web_uri = uri_alias(item.web_uri, uri_aliases)

            if delete:
                request[table_name].append(
                    {
                        "DeleteRequest": {
                            "Key": {
                                "from_date": {"S": self.from_date},
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
                                "from_date": {"S": self.from_date},
                                "web_uri": {"S": web_uri},
                                "object_key": {"S": item.object_key},
                                "content_type": {"S": item.content_type},
                            }
                        }
                    }
                )
        return request

    def create_config_request(self, config):
        request = {
            self.env_obj.config_table: [
                {
                    "PutRequest": {
                        "Item": {
                            "from_date": {"S": self.from_date},
                            "config_id": {"S": "exodus-config"},
                            "config": {"S": json.dumps(config)},
                        }
                    }
                },
            ]
        }
        return request

    def batch_write(self, request: Dict[str, Any]):
        """Wrapper for batch_write_item with retries and item count validation.

        Item limit of 25 is, at this time, imposed by AWS's boto3 library.
        """

        @backoff.on_predicate(
            wait_gen=backoff.expo,
            predicate=lambda response: response["UnprocessedItems"],
            max_tries=self.settings.write_max_tries,
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

    def get_batches(self, items: List[models.Item]):
        """Divide the publish items into batches of size 'write_batch_size'."""
        it = iter(items)
        batches = list(
            iter(lambda: tuple(islice(it, self.settings.write_batch_size)), ())
        )
        return batches

    def write_batch(self, items: List[models.Item], delete: bool = False):
        """Submit a batch of given items for writing via batch_write."""

        request = self.create_request(list(items), delete)
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
