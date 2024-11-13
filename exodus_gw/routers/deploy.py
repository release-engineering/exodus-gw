"""APIs for adjusting configuration used by the CDN."""

import logging
from typing import Any

from fastapi import APIRouter, Body
from sqlalchemy.orm import Session

from .. import auth, deps, models, schemas, settings
from .config import CONFIG_SCHEMA, config_post

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "deploy", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


@router.post(
    "/{env}/deploy-config",
    response_model=schemas.Task,
    dependencies=[auth.needs_role("config-deployer")],
    responses={200: {"description": "Deployment enqueued"}},
    deprecated=True,
    openapi_extra={
        "requestBody": {
            "description": (
                "Configuration data for the CDN. "
                "Will replace the previously deployed configuration."
            ),
            "content": {
                "application/json": {
                    "schema": CONFIG_SCHEMA,
                }
            },
        }
    },
)
def deploy_config(
    config: dict[str, Any] = Body(
        ...,
        examples=[
            {
                "listing": {
                    "/content/dist/rhel8": {
                        "var": "releasever",
                        "values": [
                            "8",
                            "8.0",
                            "8.1",
                            "8.2",
                            "8.3",
                            "8.4",
                            "8.5",
                        ],
                    },
                },
                "origin_alias": [
                    {
                        "src": "/content/origin",
                        "dest": "/origin",
                        "exclude_paths": []
                    },
                    {
                        "src": "/origin/rpm",
                        "dest": "/origin/rpms",
                        "exclude_paths": ["/iso/"]
                    },
                ],
                "releasever_alias": [
                    {
                        "dest": "/content/dist/rhel8/8.5",
                        "src": "/content/dist/rhel8/8",
                        "exclude_paths": ["/files/", "/images/", "/iso/"]
                    },

                ],
                "rhui_alias": [
                    {
                        "dest": "/content/dist/rhel8",
                        "src": "/content/dist/rhel8/rhui",
                        "exclude_paths": ["/files/", "/images/", "/iso/"]
                    },
                ],
            }
        ],
    ),
    env: settings.Environment = deps.env,
    db: Session = deps.db,
) -> models.Task:
    """Deploys CDN configuration data for use by Exodus components.

    **Required roles**: `{env}-config-deployer`

    Deployment occurs asynchronously. This API returns a Task object
    which may be used to monitor the progress of the deployment.

    This endpoint is deprecated, use /{env}/config instead.
    """
    return config_post(config, env, db)
