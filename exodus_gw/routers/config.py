"""APIs for retrieving configuration used by the CDN."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter

from exodus_gw.aws.dynamodb import DynamoDB

from .. import auth, deps, schemas
from ..settings import Environment, Settings

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "config", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.get(
    "/{env}/config",
    response_model=schemas.Config,
    summary="Get CDN configuration",
    status_code=200,
    responses={
        200: {
            "listing": {
                "/content/dist/rhel/server": {
                    "values": ["7"],
                    "var": "releasever",
                },
            },
            "origin_alias": [
                {"src": "/content/origin", "dest": "/origin"},
                {"src": "/origin/rpm", "dest": "/origin/rpms"},
            ],
            "releasever_alias": [
                {
                    "dest": "/content/dist/rhel-alt/server/7/7.9",
                    "src": "/content/dist/rhel-alt/server/7/7Server",
                },
            ],
            "rhui_alias": [
                {
                    "dest": "/content/dist/rhel8",
                    "src": "/content/dist/rhel8/rhui",
                },
            ],
        }
    },
    dependencies=[auth.needs_role("config-consumer")],
)
def config_get(
    settings: Settings = deps.settings, env: Environment = deps.env
):
    """Retrieves current CDN configuration data for use by Exodus components.

    **Required roles**: `{env}-config-consumer`
    """

    from_date = str(datetime.now(timezone.utc))
    ddb = DynamoDB(env.name, settings, from_date)

    return dict(ddb.definitions.items())
