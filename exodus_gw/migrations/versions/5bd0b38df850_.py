"""stop using timezone for column updated and deadline

Revision ID: 5bd0b38df850
Revises: 8b70b7e9c7fc
Create Date: 2022-08-19 13:57:38.431351

"""
import os

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5bd0b38df850"
down_revision = "8b70b7e9c7fc"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DELETE FROM tasks")
    op.execute("DELETE FROM publishes")

    # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
    # on sqlite < 3.32.0
    recreate = (
        "always"
        if "sqlite" in os.environ.get("EXODUS_GW_DB_URL", "")
        else "auto"
    )

    with op.batch_alter_table("tasks", recreate=recreate) as batch_op:
        batch_op.alter_column("updated", type_=sa.DateTime(), nullable=True)
        batch_op.alter_column("deadline", type_=sa.DateTime(), nullable=True)
    with op.batch_alter_table("publishes", recreate=recreate) as batch_op:
        batch_op.alter_column("updated", type_=sa.DateTime(), nullable=True)


def downgrade():
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.alter_column(
            "updated", type_=sa.DateTime(timezone=True), nullable=True
        )
        batch_op.alter_column(
            "deadline", type_=sa.DateTime(timezone=True), nullable=True
        )
    with op.batch_alter_table("publishes") as batch_op:
        batch_op.alter_column(
            "updated", type_=sa.DateTime(timezone=True), nullable=True
        )
