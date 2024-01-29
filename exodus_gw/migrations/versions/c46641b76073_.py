"""Add link_to column to items table and make it and object_key nullable

Revision ID: c46641b76073
Revises: 55d4111a0e09
Create Date: 2021-11-03 13:00:30.443526

"""

import os

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c46641b76073"
down_revision = "55d4111a0e09"
branch_labels = None
depends_on = None


def upgrade():
    # clean all items first to avoid crashing on null actor
    op.execute("DELETE FROM items")

    # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
    # on sqlite < 3.32.0
    recreate = (
        "always"
        if "sqlite" in os.environ.get("EXODUS_GW_DB_URL", "")
        else "auto"
    )

    with op.batch_alter_table("items", recreate=recreate) as batch_op:
        batch_op.add_column(sa.Column("link_to", sa.String(), nullable=True))
        batch_op.alter_column(
            "object_key", existing_type=sa.VARCHAR(), nullable=True
        )
    # ### end Alembic commands ###


def downgrade():
    with op.batch_alter_table("items") as batch_op:
        batch_op.alter_column(
            "object_key", existing_type=sa.VARCHAR(), nullable=False
        )
        batch_op.drop_column("link_to")
    # ### end Alembic commands ###
