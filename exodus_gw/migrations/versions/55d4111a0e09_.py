"""Remove from_date column from items

Revision ID: 55d4111a0e09
Revises: c164c7b69e55
Create Date: 2021-02-23 10:47:33.420762

"""
import os

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "55d4111a0e09"
down_revision = "c164c7b69e55"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("items") as batch_op:
        batch_op.drop_column("from_date")


def downgrade():
    # clean all items first to avoid crashing on null state
    op.execute("DELETE FROM items")

    # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
    # on sqlite < 3.32.0
    recreate = (
        "always"
        if "sqlite" in os.environ.get("EXODUS_GW_DB_URL", "")
        else "auto"
    )

    with op.batch_alter_table("items", recreate=recreate) as batch_op:
        batch_op.add_column(
            sa.Column(
                "from_date", sa.VARCHAR(), autoincrement=False, nullable=False
            ),
        )
