"""APIs for retrieving and deploying configuration used by the CDN."""

import logging
from datetime import datetime, timezone
from typing import Any

import jsonschema
from fastapi import APIRouter, Body, HTTPException
from sqlalchemy.orm import Session

from exodus_gw.aws.dynamodb import DynamoDB

from .. import auth, deps, models, schemas, worker
from ..settings import Environment, Settings

LOG = logging.getLogger("exodus-gw")

openapi_tag = {"name": "config", "description": __doc__}

router = APIRouter(tags=[openapi_tag["name"]])

# Paths segments (e.g., "/dist" in "/content/dist/rhel") may contain
# any number of alphanumeric characters, dollars ($), hyphens (-), or
# underscores (_). Periods (.) are also allowed when accompanying any
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
            "src": {
                "type": "string",
                "pattern": PATH_PATTERN,
                "description": "Path being aliased from, relative to CDN root.",
            },
            "dest": {
                "type": "string",
                "pattern": PATH_PATTERN,
                "description": "Target of the alias, relative to CDN root.",
            },
            "exclude_paths": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Paths for which alias will not be resolved, "
                "treated as an unanchored regex.",
            },
        },
    },
    "uniqueItems": True,
}


def alias_schema(description):
    out = ALIAS_SCHEMA.copy()
    out["description"] = description
    return out


CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "listing": {
            "type": "object",
            "description": (
                "A mapping from paths to a yum variable name & list of values, "
                "used in generating 'listing' responses."
            ),
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
        "origin_alias": alias_schema("Aliases relating to /origin."),
        "releasever_alias": alias_schema(
            "Aliases relating to $releasever variables."
        ),
        "rhui_alias": alias_schema("Aliases relating to RHUI."),
    },
    # All above properties are required by consumers of this data.
    "required": ["listing", "origin_alias", "releasever_alias", "rhui_alias"],
    # Restrict properties to only those required.
    "additionalProperties": False,
}


@router.post(
    "/{env}/config",
    response_model=schemas.Task,
    dependencies=[auth.needs_role("config-deployer")],
    responses={200: {"description": "Deployment enqueued"}},
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
def config_post(
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
                    },
                    {
                        "src": "/origin/rpm",
                        "dest": "/origin/rpms",
                    },
                ],
                "releasever_alias": [
                    {
                        "dest": "/content/dist/rhel8/8.5",
                        "src": "/content/dist/rhel8/8",
                        "exclude_paths": ["/files/", "/iso/"],
                    },
                ],
                "rhui_alias": [
                    {
                        "dest": "/content/dist/rhel8",
                        "src": "/content/dist/rhel8/rhui",
                    },
                ],
            }
        ],
    ),
    env: Environment = deps.env,
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
        LOG.error(
            "Invalid config",
            exc_info=exc_info,
            extra={"event": "deploy", "success": False},
        )
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
        extra={"event": "deploy", "success": True},
    )

    task = models.Task(
        id=msg.message_id,
        state="NOT_STARTED",
    )
    db.add(task)

    return task


@router.get(
    "/{env}/config",
    response_model=schemas.Config,
    response_model_exclude_none=True,
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
                    "exclude_paths": ["/files/", "/iso/"],
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
