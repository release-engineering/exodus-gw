import configparser
import os
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from pydantic import BaseSettings


class Environment(object):
    def __init__(self, name, aws_profile, bucket, table):
        self.name = name
        self.aws_profile = aws_profile
        self.bucket = bucket
        self.table = table


class MigrationMode(str, Enum):
    upgrade = "upgrade"
    model = "model"
    none = "none"


class Settings(BaseSettings):
    # Settings for the server.
    #
    # Most settings defined here can be overridden by an environment variable
    # of the same name, prefixed with "EXODUS_GW_". Please add doc strings only
    # for those (and not for other computed fields, like 'environments'.)

    call_context_header: str = "X-RhApiPlatform-CallContext"
    """Name of the header from which to extract call context (for authentication
    and authorization).
    """

    log_config: Dict[str, Any] = {
        "version": 1,
        "incremental": True,
        "disable_existing_loggers": False,
    }
    """Logging configuration in dictConfig schema."""

    environments: List[Environment] = []
    # List of environment objects derived from exodus-gw.ini.

    db_service_user: str = "exodus-gw"
    """db service user name"""
    db_service_pass: str = "exodus-gw"
    """db service user password"""
    db_service_host: str = "exodus-gw-db"
    """db service host"""
    db_service_port: str = "5432"
    """db service port"""

    db_url: Optional[str] = None
    """Connection string for database. If set, overrides the ``db_service_*`` settings."""

    db_reset: bool = False
    """If set to True, drop all DB tables during startup.

    This setting is intended for use during development.
    """

    db_migration_mode: MigrationMode = MigrationMode.upgrade
    """Adjusts the DB migration behavior when the exodus-gw service starts.

    Valid values are:

        upgrade (default)
            Migrate the DB to ``db_migration_revision`` (default latest) when
            the service starts up.

            This is the default setting and should be left enabled for typical
            production use.

        model
            Don't use migrations. Instead, attempt to initialize the database
            from the current version of the internal sqlalchemy model.

            This is intended for use during development while prototyping
            schema changes.

        none
            Don't perform any DB initialization at all.
    """

    db_migration_revision: str = "head"
    """If ``db_migration_mode`` is ``upgrade``, this setting can be used to override
    the target revision when migrating the DB.
    """

    batch_size: int = 25
    """Maximum number of items to write at one time"""
    max_tries: int = 20
    """Maximum attempts to write to DynamoDB table."""

    entry_point_files: List[str] = [
        "repomd.xml",
        "repomd.xml.asc",
        "PULP_MANIFEST",
    ]
    """List of file names that should be saved for last when publishing."""

    class Config:
        env_prefix = "exodus_gw_"


def load_settings() -> Settings:
    """Return the currently active settings for the server.

    This function will load settings from config files and environment
    variables. It is intended to be called once at application startup.

    Request handler functions should access settings via ``app.state.settings``.
    """

    settings = Settings()
    config = configparser.ConfigParser()

    config.read(
        [
            os.path.join(os.path.dirname(__file__), "../exodus-gw.ini"),
            "/opt/app/config/exodus-gw.ini",
        ]
    )

    for logger in config["loglevels"] if "loglevels" in config else []:
        settings.log_config.setdefault("loggers", {})

        log_config = settings.log_config
        dest = log_config if logger == "root" else log_config["loggers"]

        dest.update({logger: {"level": config.get("loglevels", logger)}})

    for env in [sec for sec in config.sections() if sec.startswith("env.")]:
        aws_profile = config.get(env, "aws_profile", fallback=None)
        bucket = config.get(env, "bucket", fallback=None)
        table = config.get(env, "table", fallback=None)
        settings.environments.append(
            Environment(
                name=env.replace("env.", ""),
                aws_profile=aws_profile,
                bucket=bucket,
                table=table,
            )
        )

    return settings


def get_environment(env: str, settings: Settings = None):
    """Return the corresponding environment object for the given environment
    name.
    """

    settings = settings or load_settings()

    for env_obj in settings.environments:
        if env_obj.name == env:
            return env_obj

    raise HTTPException(
        status_code=404, detail="Invalid environment=%s" % repr(env)
    )
