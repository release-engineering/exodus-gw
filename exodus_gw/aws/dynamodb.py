import logging
from typing import List

from ..aws.client import DynamoDBClientWrapper as ddb_client
from ..settings import get_environment

LOG = logging.getLogger("exodus-gw")


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

    return response
