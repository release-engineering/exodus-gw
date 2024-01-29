"""Add actor to dramatiq messages

Revision ID: a60131dd10c4
Revises: 0a3a709da247
Create Date: 2021-02-15 11:35:58.851579

"""

import os

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a60131dd10c4"
down_revision = "0a3a709da247"
branch_labels = None
depends_on = None


def upgrade():
    # clean all messages first to avoid crashing on null actor
    op.execute("DELETE FROM dramatiq_messages")

    # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
    # on sqlite < 3.32.0
    recreate = (
        "always"
        if "sqlite" in os.environ.get("EXODUS_GW_DB_URL", "")
        else "auto"
    )

    with op.batch_alter_table(
        "dramatiq_messages", recreate=recreate
    ) as batch_op:
        batch_op.add_column(
            sa.Column(
                "actor",
                sa.String(),
                nullable=False,
            )
        )


def downgrade():
    with op.batch_alter_table("dramatiq_messages") as batch_op:
        batch_op.drop_column("actor")
