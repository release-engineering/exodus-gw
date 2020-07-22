from .app import app


@app.get("/healthcheck")
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"200": "OK"}
