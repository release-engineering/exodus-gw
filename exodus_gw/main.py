import logging.config

from fastapi import FastAPI
from fastapi.exception_handlers import http_exception_handler
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import models
from .aws.util import xml_response
from .database import engine
from .routers import gateway, s3
from .settings import get_settings

app = FastAPI(title="exodus-gw")
app.include_router(gateway.router)
app.include_router(s3.router)


@app.exception_handler(StarletteHTTPException)
async def custom_http_exception_handler(request, exc):
    # Override HTTPException to produce XML error responses for the
    # given endpoints.

    path = request.scope.get("path")

    if path.startswith("/upload"):
        return xml_response(
            "Error", Code=exc.status_code, Message=exc.detail, Endpoint=path
        )

    return await http_exception_handler(request, exc)


@app.on_event("startup")
def configure_loggers():
    settings = get_settings()
    logging.config.dictConfig(settings.log_config)

    root = logging.getLogger()
    if not root.hasHandlers():
        fmtr = logging.Formatter(
            fmt="[%(asctime)s] [%(process)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S %z",
        )
        hdlr = logging.StreamHandler()
        hdlr.setFormatter(fmtr)
        root.addHandler(hdlr)


@app.on_event("startup")
def db_init() -> None:
    models.Base.metadata.create_all(bind=engine)
