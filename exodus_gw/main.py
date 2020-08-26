#!/usr/bin/env python3

from fastapi import FastAPI

app = FastAPI(title="exodus-gw")


@app.get("/healthcheck")
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}
