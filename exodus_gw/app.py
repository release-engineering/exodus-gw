from fastapi import FastAPI

from .database import get_db

db = get_db()

app = FastAPI(title="exodus-gw")
