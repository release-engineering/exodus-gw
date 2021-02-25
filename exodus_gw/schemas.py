from datetime import datetime
from enum import Enum
from os.path import join
from typing import Dict, List
from uuid import UUID

from fastapi import Path
from pydantic import BaseModel, Field, root_validator

PathPublishId = Path(
    ...,
    title="publish ID",
    description="UUID of an existing publish object.",
)

PathTaskId = Path(
    ..., title="task ID", description="UUID of an existing task object."
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


class Item(ItemBase):
    publish_id: UUID = Field(
        ..., description="Unique ID of publish object containing this item."
    )

    class Config:
        orm_mode = True


class PublishStates(str, Enum):
    pending = "PENDING"
    committing = "COMMITTING"
    committed = "COMMITTED"
    failed = "FAILED"


class PublishBase(BaseModel):
    id: UUID = Field(..., description="Unique ID of publish object.")


class Publish(PublishBase):
    env: str = Field(
        ..., description="""Environment to which this publish belongs."""
    )
    state: PublishStates = Field(
        ..., description="Current state of this publish."
    )
    updated: datetime = Field(
        None,
        description="DateTime of last update to this publish. None if never updated.",
    )
    links: Dict[str, str] = Field(
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


class TaskStates(str, Enum):
    not_started = "NOT_STARTED"
    in_progress = "IN_PROGRESS"
    complete = "COMPLETE"
    failed = "FAILED"


class Task(BaseModel):
    id: UUID = Field(..., description="Unique ID of task object.")
    publish_id: UUID = Field(
        ..., description="Unique ID of publish object handled by this task."
    )
    state: TaskStates = Field(..., description="Current state of this task.")
    updated: datetime = Field(
        None,
        description="DateTime of last update to this task. None if never updated.",
    )
    links: Dict[str, str] = Field(
        {}, description="""URL links related to this task."""
    )

    @root_validator
    @classmethod
    def make_links(cls, values):
        values["links"] = {"self": join("/task", str(values["id"]))}
        return values

    class Config:
        orm_mode = True


class MessageResponse(BaseModel):
    detail: str = Field(
        ..., description="A human-readable message with additional info."
    )


class EmptyResponse(BaseModel):
    """An empty object."""
