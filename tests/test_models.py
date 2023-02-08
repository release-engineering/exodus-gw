import uuid
from datetime import datetime

from exodus_gw.models import Item, Publish, Task


def test_Item_init():
    item = Item(
        web_uri="/some/path",
        object_key="abcde",
        link_to="",
        publish_id="123e4567-e89b-12d3-a456-426614174000",
    )
    assert item.web_uri == "/some/path"
    assert item.object_key == "abcde"
    assert item.publish_id == "123e4567-e89b-12d3-a456-426614174000"


def test_publish_task_before_update(db):
    """Changing object states updates timestamp."""

    publish_id = "11224567-e89b-12d3-a456-426614174000"
    publish = Publish(id=publish_id, env="test", state="PENDING")

    task_id = "8d8a4692-c89b-4b57-840f-b3f0166148d2"
    task = Task(
        id=task_id,
        publish_id=publish_id,
        state="NOT_STARTED",
    )

    db.add(publish)
    db.add(task)
    db.commit()

    # Updated should initially be null
    assert publish.updated is None
    assert task.updated is None

    # Change state of publish and task
    publish.state = "COMMITTING"
    task.state = "IN_PROGRESS"
    db.commit()

    # Updated should now hold datetime objects
    p_updated = publish.updated
    assert isinstance(p_updated, datetime)

    t_updated = task.updated
    assert isinstance(t_updated, datetime)

    # Change state of publish and task again
    publish.state = "COMMITTED"
    task.state = "COMPLETE"
    db.commit()

    # Updated should now hold different datetime objects
    assert isinstance(publish.updated, datetime)
    assert p_updated != publish.updated

    assert isinstance(task.updated, datetime)
    assert t_updated != task.updated
