from typing import List
from uuid import UUID

from pydantic import BaseModel


class ItemBase(BaseModel):
    uri: str
    object_key: str


class Item(ItemBase):
    publish_id: UUID

    class Config:
        orm_mode = True


class PublishBase(BaseModel):
    id: UUID


class Publish(PublishBase):
    items: List[Item] = []

    class Config:
        orm_mode = True
