import logging
from typing import List

import dramatiq

from ..aws.client import DynamoDBClientWrapper as ddb_client
from ..settings import get_settings, get_environment

LOG = logging.getLogger("exodus-gw")


class IncompleteBatchWrite(Exception):
    """Response from batch_write contains unprocessed items."""

    pass


def retry_batch_write(retries_so_far, exception):
    """Determine if dramatiq actor should retry batch_write."""

    limit_not_exceeded = retries_so_far <= int(get_settings.max_write_retires)
    incomplete = isinstance(exception, IncompleteBatchWrite)

    return limit_not_exceeded and incomplete


@dramatiq.actor(retry_when=retry_batch_write)
async def batch_write(env: str, items: List[dict], delete: bool = False):
    """Write or delete up to 25 items on the given environment's table.

    Item limit of 25 is, at this time, imposed by AWS's boto3 library.
    """

    if len(items) > 25:
        LOG.error("Cannot process more than 25 items")
        raise ValueError("Received too many items (%s)" % len(items))

    env_obj = get_environment(env)
    profile = env_obj.aws_profile
    table = env_obj.table

    if delete:
        request = {table: [{"DeleteRequest": {"Key": item} for item in items}]}
        exc_msg = "Exception while deleting %s items from table '%s'"
    else:
        request = {table: [{"PutRequest": {"Item": item} for item in items}]}
        exc_msg = "Exception while writing %s items to table '%s'"

    try:
        async with ddb_client(profile=profile) as ddb:
            response = await ddb.batch_write_item(RequestItems=request)
    except Exception:
        LOG.exception(exc_msg, len(items), table)
        raise

    if response["UnprocessedItems"]:
        raise IncompleteBatchWrite

    return response
