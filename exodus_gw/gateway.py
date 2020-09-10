from .app import app
from .publish import create_publish_id


@app.get("/healthcheck")
def healthcheck():
    """Returns a successful response if the service is running."""
    return {"detail": "exodus-gw is running"}


@app.post("/{env}/publish")
def publish(env: str):
    """WIP: Returns a new, empty publish id"""
    if env not in ["dev", "qa", "stage", "prod"]:
        return {"error": "environment {0} not found".format(env)}
    create_publish_id()
    return {"detail": "Created Publish Id"}
