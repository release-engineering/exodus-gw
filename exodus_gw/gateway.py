from fastapi import FastAPI

app = FastAPI()


@app.get("/healthcheck")
def healthcheck():
    return {"200": "OK"}
