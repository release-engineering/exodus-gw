from .app import app


@app.get("/healthcheck")
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}
