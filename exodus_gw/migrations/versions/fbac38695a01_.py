"""Add updated timestamp to items

Revision ID: fbac38695a01
Revises: 1d51b80e64ba
Create Date: 2023-10-04 13:41:25.588710

"""

from datetime import datetime

import sqlalchemy as sa
from alembic import op

from exodus_gw.migrations.test import tested_by

# revision identifiers, used by Alembic.
revision = "fbac38695a01"
down_revision = "1d51b80e64ba"
branch_labels = None
depends_on = None


# Basic declaration of items table, pre and post migration.
def columns_pre():
    return [
        sa.column("id", sa.Uuid(as_uuid=False)),
        sa.column("web_uri", sa.String()),
        sa.column("object_key", sa.String()),
        sa.column("publish_id", sa.Uuid(as_uuid=False)),
    ]


items_pre = sa.table("items", *columns_pre())

items_post = sa.table(
    "items", *(columns_pre() + [sa.column("updated", sa.DateTime())])
)


def upgrade_testdata():
    publish_id = "f7a38eb1-0d75-4245-a4ef-3dfd02d8129f"

    op.bulk_insert(
        items_pre,
        [
            {
                "id": "6d9ae1af-0f26-4491-8f05-762d0f3c540d",
                "publish_id": publish_id,
                "web_uri": "/foo-updated",
                "object_key": "a1b2c3",
            },
            {
                "id": "68f376fb-4f36-4efc-bed7-a289c4fe43f1",
                "publish_id": publish_id,
                "web_uri": "/bar-updated",
                "object_key": "a1b2c3",
            },
        ],
    )


@tested_by(upgrade_testdata)
def upgrade():
    # Add updated initially as nullable, fill in any missing values, then make
    # it not-nullable.
    op.add_column(
        "items",
        sa.Column("updated", sa.DateTime(), nullable=True),
    )

    # Any items which existed at time of migration will have their updated
    # timestamp set to the time at which the migration runs.
    op.execute(
        items_post.update()
        .where(items_post.c.updated == None)
        .values(updated=datetime.utcnow())
    )

    # Now everything has a value, make it not nullable.
    with op.batch_alter_table("items") as batch_op:
        batch_op.alter_column(
            "updated", existing_type=sa.DateTime(), nullable=False
        )


def downgrade():
    op.drop_column("items", "updated")
