from os.path import join
from typing import List
from uuid import UUID

from fastapi import Path
from pydantic import BaseModel, Field, root_validator

PathEnv = Path(
    ...,
    title="environment",
    description="[Environment](#section/Environments) on which to operate.",
)

PathPublishId = Path(
    ...,
    title="publish ID",
    description="UUID of an existing publish object.",
)


class ItemBase(BaseModel):
    web_uri: str = Field(
        ...,
        description="URI, relative to CDN root, which shall be used to expose this object.",
    )
    object_key: str = Field(
        ...,
        description=(
            "Key of blob to be exposed; should be the SHA256 checksum of a previously uploaded "
            "piece of content, in lowercase hex-digest form."
        ),
    )

    from_date: str


class Item(ItemBase):
    publish_id: UUID = Field(
        ..., description="Unique ID of publish object containing this item."
    )

    class Config:
        orm_mode = True


class PublishBase(BaseModel):
    id: UUID = Field(..., description="Unique ID of publish object.")


class Publish(PublishBase):
    env: str = Field(
        ..., description="""Environment to which this publish belongs."""
    )
    links: dict = Field(
        {}, description="""URL links related to this publish."""
    )
    items: List[Item] = Field(
        [],
        description="""All items (pieces of content) included in this publish.""",
    )

    @root_validator
    @classmethod
    def make_links(cls, values):
        _self = join("/", values["env"], "publish", str(values["id"]))
        values["links"] = {"self": _self, "commit": join(_self, "commit")}
        return values

    class Config:
        orm_mode = True
