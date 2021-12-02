"""APIs for adjusting data consumed by components of the CDN.

## Deploy Config

The deploy_config API deploys configuration/definition data to a
DynamoDB table where it can be accessed via queries.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict

import jsonschema
from fastapi import APIRouter, Body, HTTPException
from sqlalchemy.orm import Session

from .. import auth, deps, models, schemas, settings, worker

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "deploy", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])


# Paths segments (e.g., "/dist" in "/content/dist/rhel") may contain
# any number of alphanumeric characters, dollars ($), hyphens (-), or
# underscores (_). Periods (.) are also allowed when acompanying any
# other permitted character.
#
# Not allowed are multiple slashes (e.g., "///") or segments containing
# only periods (e.g., "/../").
PATH_PATTERN = r"^((?!/\.+/)(/[\w\$\.\-]+))*$"

ALIAS_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "src": {"type": "string", "pattern": PATH_PATTERN},
            "dest": {"type": "string", "pattern": PATH_PATTERN},
        },
    },
    "uniqueItems": True,
}

CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "listing": {
            "type": "object",
            "patternProperties": {
                PATH_PATTERN: {
                    "type": "object",
                    "properties": {
                        "var": {
                            "type": "string",
                            "enum": ["releasever", "basearch"],
                        },
                        "values": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                            "uniqueItems": True,
                        },
                    },
                }
            },
            "additionalProperties": False,
        },
        "origin_alias": ALIAS_SCHEMA,
        "releasever_alias": ALIAS_SCHEMA,
        "rhui_alias": ALIAS_SCHEMA,
    },
    # All above properties are required by consumers of this data.
    "required": ["listing", "origin_alias", "releasever_alias", "rhui_alias"],
    # Restrict properties to only those required.
    "additionalProperties": False,
}


@router.post(
    "/{env}/deploy-config",
    response_model=schemas.Task,
    dependencies=[auth.needs_role("config-deployer")],
)
def deploy_config(
    config: Dict[str, Any] = Body(
        ...,
        example={
            "listing": {
                "/content/dist/rhel8": {
                    "var": "releasever",
                    "values": ["8", "8.0", "8.1", "8.2", "8.3", "8.4", "8.5"],
                },
            },
            "origin_alias": [
                {"src": "/content/origin", "dest": "/origin"},
                {"src": "/origin/rpm", "dest": "/origin/rpms"},
            ],
            "releasever_alias": [
                {
                    "dest": "/content/dist/rhel8/8.5",
                    "src": "/content/dist/rhel8/8",
                },
            ],
            "rhui_alias": [
                {
                    "dest": "/content/dist/rhel8",
                    "src": "/content/dist/rhel8/rhui",
                },
            ],
        },
    ),
    env: settings.Environment = deps.env,
    db: Session = deps.db,
) -> models.Task:
    """Deploys CDN configuration data for use by Exodus components.

    **Required roles**: `{env}-config-deployer`

    Deployment occurs asynchronously. This API returns a Task object
    which may be used to monitor the progress of the deployment.
    """

    try:
        jsonschema.validate(config, CONFIG_SCHEMA)
    except jsonschema.ValidationError as exc_info:
        LOG.error("Invalid config", exc_info=exc_info)
        raise HTTPException(
            status_code=400, detail="Invalid configuration structure"
        ) from exc_info

    msg = worker.deploy_config.send(
        config=config,
        env=env.name,
        from_date=str(datetime.now(timezone.utc)),
    )

    LOG.info(
        "Enqueued configuration deployment at %s: %s",
        msg.kwargs["from_date"],
        msg.message_id,
    )

    task = models.Task(
        id=msg.message_id,
        state="NOT_STARTED",
    )
    db.add(task)

    return task
