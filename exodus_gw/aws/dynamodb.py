import logging
from itertools import islice
from typing import List

import backoff

from .. import models
from ..aws.client import DynamoDBClientWrapper as ddb_client
from ..settings import Settings, get_environment

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
def batch_write(env: str, request: dict):
    """Wrapper for batch_write_item with retries and item count validation.

    Item limit of 25 is, at this time, imposed by AWS's boto3 library.
    """

    environment = get_environment(env)
    item_count = len(request[environment.table])

    if item_count > 25:
        LOG.error("Cannot process more than 25 items per request")
        raise ValueError("Request contains too many items (%s)" % item_count)

    with ddb_client(profile=environment.aws_profile) as ddb:
        response = ddb.batch_write_item(RequestItems=request)

    return response


def create_request(env: str, items: List[models.Item], delete: bool = False):
    """Create the dictionary structure expected by batch_write_item."""

    table = get_environment(env).table

    if delete:
        req_type = "DeleteRequest"
        item_type = "Key"
    else:
        req_type = "PutRequest"
        item_type = "Item"

    return {table: [{req_type: {item_type: item.aws_fmt}} for item in items]}


def write_batches(env: str, items: List[models.Item], delete: bool = False):
    """Submit batches of given items for writing via batch_write."""

    environment = get_environment(env)
    settings = Settings()

    it = iter(items)
    batches = list(iter(lambda: tuple(islice(it, settings.batch_size)), ()))
    unprocessed_items = []

    for batch in batches:
        request = create_request(env, list(batch), delete)

        try:
            response = batch_write(env, request)
        except Exception:
            LOG.exception(
                "Exception while %s %s items on table '%s'",
                ("deleting" if delete else "writing"),
                len(batch),
                environment.table,
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
