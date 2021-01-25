import configparser
import os
from functools import lru_cache
from typing import List

from fastapi import HTTPException
from pydantic import BaseSettings


class Environment(object):
    def __init__(self, name, aws_profile, bucket, table):
        self.name = name
        self.aws_profile = aws_profile
        self.bucket = bucket
        self.table = table


class Settings(BaseSettings):
    # Settings for the server.
    #
    # Each setting defined here can be overridden by an environment variable
    # of the same name, prefixed with "EXODUS_GW_".

    call_context_header: str = "X-RhApiPlatform-CallContext"
    """Name of the header from which to extract call context (for authentication
    and authorization).
    """

    log_config: dict = {
        "version": 1,
        "incremental": True,
        "disable_existing_loggers": False,
    }
    """Logging configuration in dictConfig schema."""

    environments: List[Environment] = []
    """List of environment objects derived from exodus-gw.ini."""

    db_service_user: str = "exodus-gw"
    """db service user name"""
    db_service_pass: str = "exodus-gw"
    """db service user password"""
    db_service_host: str = "exodus-gw-db"
    """db service host"""
    db_service_port: str = "5432"
    """db service port"""

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


@lru_cache()
def get_settings() -> Settings:
    """Return the currently active settings for the server.

    This function is intended for use with fastapi.Depends.

    Settings are loaded the first time this function is called, and cached
    afterward.
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


def get_environment(env: str):
    """Return the corresponding environment object for the given environment
    name.
    """

    settings = get_settings()

    for env_obj in settings.environments:
        if env_obj.name == env:
            return env_obj

    raise HTTPException(
        status_code=404, detail="Invalid environment=%s" % repr(env)
    )
