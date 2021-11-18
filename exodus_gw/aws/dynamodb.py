import json
import logging
from itertools import islice
from typing import Any, Dict, List, Optional

import backoff

from .. import models
from ..aws.client import DynamoDBClientWrapper as ddb_client
from ..aws.util import uri_alias
from ..settings import Environment, Settings, get_environment

LOG = logging.getLogger("exodus-gw")

# TODO: this module is using some settings evaluated at import time.
# This means changing settings (e.g. during tests) won't have the desired
# effect. As this code should be executed from the dramatiq workers, is
# there a way to tie Settings lifecycle to the lifecycle of the worker?


@backoff.on_predicate(
    wait_gen=backoff.expo,
    predicate=lambda response: response["UnprocessedItems"],
    max_tries=Settings().max_tries,
)
def batch_write(env_obj: Environment, request: Dict[str, Any]):
    """Wrapper for batch_write_item with retries and item count validation.

    Item limit of 25 is, at this time, imposed by AWS's boto3 library.
    """

    item_count = len(request.get(env_obj.table, []))

    if item_count > 25:
        LOG.error("Cannot process more than 25 items per request")
        raise ValueError("Request contains too many items (%s)" % item_count)

    with ddb_client(profile=env_obj.aws_profile) as ddb:
        response = ddb.batch_write_item(RequestItems=request)

    return response


def query_definitions(env_obj: Environment, from_date: str):
    out: Dict[str, Any] = {}

    table = env_obj.config_table
    aws_profile = env_obj.aws_profile

    with ddb_client(profile=aws_profile) as ddb:
        query_result = ddb.query(
            TableName=table,
            Limit=1,
            ScanIndexForward=False,
            KeyConditionExpression="config_id = :id and from_date <= :d",
            ExpressionAttributeValues={
                ":id": {"S": "exodus-config"},
                ":d": {"S": from_date},
            },
        )
        if query_result["Items"]:
            item = query_result["Items"][0]
            out = json.loads(item["config"]["S"])
    return out


def create_request(
    env_obj: Environment,
    items: List[models.Item],
    from_date: str,
    definitions: Optional[Dict[str, List[Any]]] = None,
    delete: bool = False,
):
    """Create the dictionary structure expected by batch_write_item."""

    table_name = env_obj.table

    request: Dict[str, List[Any]] = {table_name: []}
    definitions = definitions or {}

    for item in items:
        # Resolve aliases relating to origin, e.g. content/origin <=> origin
        web_uri = uri_alias(item.web_uri, definitions.get("origin_alias"))

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
                        }
                    }
                }
            )
    return request


def write_batches(
    env: str, items: List[models.Item], from_date: str, delete: bool = False
):
    """Submit batches of given items for writing via batch_write."""

    env_obj = get_environment(env)
    settings = Settings()

    it = iter(items)
    batches = list(iter(lambda: tuple(islice(it, settings.batch_size)), ()))
    unprocessed_items = []
    definitions = query_definitions(env_obj, from_date)

    for batch in batches:
        request = create_request(
            env_obj, list(batch), from_date, definitions, delete
        )

        try:
            response = batch_write(env_obj, request)
        except Exception:
            LOG.exception(
                "Exception while %s %s items on table '%s'",
                ("deleting" if delete else "writing"),
                len(batch),
                env_obj.table,
            )
            raise

        # Abort immediately for put requests.
        # Collect and log unprocessed items for delete requests.
        if response["UnprocessedItems"]:
            if delete:
                unprocessed_items.append(response["UnprocessedItems"])
                continue

            LOG.info("One or more writes were unsuccessful")
            return False

    if unprocessed_items:
        LOG.error(
            "Unprocessed items:\n\t%s",
            ("\n\t".join([str(item) for item in unprocessed_items])),
        )
        raise RuntimeError("Deletion failed\nSee error log for details")

    LOG.info("Items successfully %s", "deleted" if delete else "written")
    return True
