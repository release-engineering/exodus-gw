import logging.config
from fastapi import FastAPI

from .settings import get_settings
from .database import get_db

db = get_db()

app = FastAPI(title="exodus-gw")


@app.on_event("startup")
def configure_loggers():
    settings = get_settings()
    logging.config.dictConfig(settings.log_config)
