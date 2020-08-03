from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Settings for the server.

    Each setting defined here can be overridden by an environment variable
    of the same name, prefixed with "EXODUS_GW_".
    """

    call_context_header: str = "X-RhApiPlatform-CallContext"
    """Name of the header from which to extract call context (for authentication
    and authorization).
    """

    class Config:
        env_prefix = "exodus_gw_"


@lru_cache()
def get_settings() -> Settings:
    """Return the currently active settings for the server.

    This function is intended for use with fastapi.Depends.

    Settings are loaded the first time this function is called, and cached
    afterward.
    """

    return Settings()
