from uuid import UUID

from pydantic import BaseModel


class Publish(BaseModel):
    id: UUID

    class Config:
        orm_mode = True
