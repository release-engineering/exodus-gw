"""Split commit_tasks from tasks

Revision ID: 0d88322fe0b3
Revises: 6461bad8ed91
Create Date: 2023-09-29 10:04:28.122074

"""

import sqlalchemy as sa
from alembic import op

from exodus_gw.migrations.test import tested_by

# revision identifiers, used by Alembic.
revision = "0d88322fe0b3"
down_revision = "6461bad8ed91"
branch_labels = None
depends_on = None


def upgrade_testdata():
    # Ensure that some publish and non-publish tasks exist,
    # as both are expected to be split out to different tables.
    op.bulk_insert(
        sa.table(
            "tasks",
            sa.column("id", sa.Uuid(as_uuid=False)),
            sa.column("publish_id", sa.Uuid(as_uuid=False)),
            sa.column("state", sa.String()),
        ),
        [
            # two tasks associated with a publish, should be
            # moved to commit_tasks
            {
                "id": "fb870e73-ac62-4eb7-a75e-917b758a5d64",
                "publish_id": "f7a38eb1-0d75-4245-a4ef-3dfd02d8129f",
                "state": "NOT_STARTED",
            },
            {
                "id": "c99702b6-b0a6-48e5-9cf2-9fabd65d3d72",
                "publish_id": "e7c4d0b0-d158-49a3-a87a-3aeb7ce94e0d",
                "state": "IN_PROGRESS",
            },
            # two tasks not associated with a publish
            {
                "id": "0f31777e-8171-4f83-99bb-0464d7f7cec8",
                "publish_id": None,
                "state": "NOT_STARTED",
            },
            {
                "id": "b09f079a-8e5a-42d0-adc9-30a3a3c89d55",
                "publish_id": None,
                "state": "IN_PROGRESS",
            },
        ],
    )


@tested_by(upgrade_testdata)
def upgrade():
    op.create_table(
        "commit_tasks",
        sa.Column("id", sa.Uuid(as_uuid=False), nullable=False),
        sa.Column("publish_id", sa.Uuid(as_uuid=False), nullable=False),
        sa.ForeignKeyConstraint(
            ["id"],
            ["tasks.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Add type column as nullable initially, then we'll populate the
    # actual values and set non-nullable.
    op.add_column(
        "tasks",
        sa.Column("type", sa.String(), nullable=True),
    )
    op.execute("UPDATE tasks SET type='task' WHERE publish_id IS NULL")
    op.execute("UPDATE tasks SET type='commit' WHERE publish_id IS NOT NULL")

    # It looks odd that we're doing a batch here with only one operation,
    # but it's needed for sqlite compatibility
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.alter_column(
            "type", existing_type=sa.String(), nullable=False
        )

    # With the commit_tasks table being created, we should now move over
    # all publish_id values to that table.
    op.execute("""
        INSERT INTO commit_tasks (id, publish_id)
        SELECT id, publish_id FROM tasks WHERE tasks.type='commit'
        """)

    op.drop_column("tasks", "publish_id")


def downgrade():
    # NOTE: there is no attempt to move publish_id info back into tasks
    # during downgrade, the info is simply lost.
    # We'll just wipe all tasks to avoid any incoherency.
    op.execute("DELETE FROM tasks")

    op.drop_column("tasks", "type")
    op.add_column(
        "tasks",
        sa.Column(
            "publish_id",
            sa.Uuid(as_uuid=False),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_table("commit_tasks")
