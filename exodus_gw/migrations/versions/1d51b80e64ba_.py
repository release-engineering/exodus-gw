"""Adds columns supporting phase1 commit

Revision ID: 1d51b80e64ba
Revises: 0d88322fe0b3
Create Date: 2023-10-02 11:44:04.604593

"""

import sqlalchemy as sa
from alembic import op

from exodus_gw.migrations.test import tested_by

# revision identifiers, used by Alembic.
revision = "1d51b80e64ba"
down_revision = "0d88322fe0b3"
branch_labels = None
depends_on = None


def upgrade_testdata():
    # Make a commit_task exist so we can verify it's transformed
    # into phase2 commit
    task_id = "41400ff1-9198-4b35-b24e-a71a29957ae1"
    publish_id = "f7a38eb1-0d75-4245-a4ef-3dfd02d8129f"
    op.bulk_insert(
        sa.table(
            "tasks",
            sa.column("id", sa.Uuid(as_uuid=False)),
            sa.column("state", sa.String()),
            sa.column("type", sa.String()),
        ),
        [
            {
                "id": task_id,
                "state": "NOT_STARTED",
                "type": "commit",
            },
        ],
    )
    op.bulk_insert(
        sa.table(
            "commit_tasks",
            sa.column("id", sa.Uuid(as_uuid=False)),
            sa.column("publish_id", sa.Uuid(as_uuid=False)),
        ),
        [
            {
                "id": task_id,
                "publish_id": publish_id,
            },
        ],
    )

    # and make some items exist too, which will be marked dirty
    op.bulk_insert(
        sa.table(
            "items",
            sa.column("id", sa.Uuid(as_uuid=False)),
            sa.column("web_uri", sa.String()),
            sa.column("object_key", sa.String()),
            sa.column("publish_id", sa.Uuid(as_uuid=False)),
        ),
        [
            {
                "id": "f021da4d-5c3b-483f-af8d-85117fb64b2c",
                "publish_id": publish_id,
                "web_uri": "/foo",
                "object_key": "a1b2c3",
            },
            {
                "id": "9dafa529-03e6-4412-85db-f681ea98d75d",
                "publish_id": publish_id,
                "web_uri": "/bar",
                "object_key": "a1b2c3",
            },
        ],
    )


@tested_by(upgrade_testdata)
def upgrade():
    op.add_column(
        "commit_tasks",
        sa.Column(
            "commit_mode", sa.String(), nullable=False, server_default="phase2"
        ),
    )
    op.add_column(
        "items",
        sa.Column(
            "dirty", sa.Boolean(), nullable=False, server_default="TRUE"
        ),
    )


def downgrade():
    op.drop_column("items", "dirty")
    op.drop_column("commit_tasks", "commit_mode")
