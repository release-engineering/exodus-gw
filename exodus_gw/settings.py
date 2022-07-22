import configparser
import os
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from pydantic import BaseSettings


class Environment(object):
    def __init__(
        self,
        name,
        aws_profile,
        bucket,
        table,
        config_table,
        cdn_url,
        cdn_key_id,
    ):
        self.name = name
        self.aws_profile = aws_profile
        self.bucket = bucket
        self.table = table
        self.config_table = config_table
        self.cdn_url = cdn_url
        self.cdn_key_id = cdn_key_id

    @property
    def cdn_private_key(self):
        return os.getenv("EXODUS_GW_CDN_PRIVATE_KEY_%s" % self.name.upper())


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

    ini_path: Optional[str] = None
    """Path to an exodus-gw.ini config file with additional settings."""

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

    item_yield_size: int = 5000
    """Number of publish items to load from the service DB at one time."""

    write_batch_size: int = 25
    """Maximum number of items to write to the DynamoDB table at one time."""
    write_max_tries: int = 20
    """Maximum write attempts to the DynamoDB table."""

    publish_timeout: int = 24
    """Maximum amount of time (in hours) between updates to a pending publish before
    it will be considered abandoned. Defaults to one day.
    """

    history_timeout: int = 24 * 14
    """Maximum amount of time (in hours) to retain historical data for publishes and
    tasks. Publishes and tasks in a terminal state will be erased after this time has
    passed. Defaults to two weeks.
    """

    task_deadline: int = 2
    """Maximum amount of time (in hours) a task should remain viable. Defaults to two
    hours.
    """

    actor_time_limit: int = 30 * 60000
    """Maximum amount of time (in milliseconds) actors may run."""

    entry_point_files: List[str] = [
        "repomd.xml",
        "repomd.xml.asc",
        "PULP_MANIFEST",
    ]
    """List of file names that should be saved for last when publishing."""

    config_cache_ttl: int = 2
    """Time (in minutes) config is expected to live in components that consume it.

    Determines the delay for deployment task completion to allow for
    existing caches to expire and the newly deployed config to take effect.
    """

    worker_keepalive_timeout: int = 60 * 5
    """Background worker keepalive timeout, in seconds. If a worker fails to update its
    status within this time period, it is assumed dead.

    This setting affects how quickly the system can recover from issues such as a worker
    process being killed unexpectedly.
    """

    worker_keepalive_interval: int = 60
    """How often, in seconds, should background workers update their status."""

    cron_cleanup: str = "0 */12 * * *"
    """cron-style schedule for cleanup task.

    exodus-gw will run a cleanup task approximately according to this schedule, removing old
    data from the system."""

    scheduler_interval: int = 15
    """How often, in minutes, exodus-gw should check if a scheduled task is ready to run.

    Note that the cron rules applied to each scheduled task are only as accurate as this
    interval allows, i.e. each rule may be triggered up to ``scheduler_interval`` minutes late.
    """

    scheduler_delay: int = 5
    """Delay, in minutes, after exodus-gw workers start up before any scheduled tasks
    should run."""

    cdn_signature_timeout: int = 60 * 30
    """Time (in seconds) signed URLs remain valid."""

    s3_pool_size: int = 3
    """Number of S3 clients to cache"""

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

    # Try to find config here by default...
    filenames = [
        os.path.join(os.path.dirname(__file__), "../exodus-gw.ini"),
        "/opt/app/config/exodus-gw.ini",
    ]

    # ...but also allow pointing at a specific config file if this path
    # has been set. Note that putting this at the end gives it the highest
    # precedence, as the behavior is to load all the existing files in
    # order with each one potentially overriding settings from the prior.
    if settings.ini_path:
        filenames.append(settings.ini_path)

    config.read(filenames)

    for logger in config["loglevels"] if "loglevels" in config else []:
        settings.log_config.setdefault("loggers", {})

        log_config = settings.log_config
        dest = log_config if logger == "root" else log_config["loggers"]

        dest.update({logger: {"level": config.get("loglevels", logger)}})

    for env in [sec for sec in config.sections() if sec.startswith("env.")]:
        aws_profile = config.get(env, "aws_profile", fallback=None)
        bucket = config.get(env, "bucket", fallback=None)
        table = config.get(env, "table", fallback=None)
        config_table = config.get(env, "config_table", fallback=None)
        cdn_url = config.get(env, "cdn_url", fallback=None)
        cdn_key_id = config.get(env, "cdn_key_id", fallback=None)

        settings.environments.append(
            Environment(
                name=env.replace("env.", ""),
                aws_profile=aws_profile,
                bucket=bucket,
                table=table,
                config_table=config_table,
                cdn_url=cdn_url,
                cdn_key_id=cdn_key_id,
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
