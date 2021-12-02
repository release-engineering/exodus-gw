"""Make task publish_id nullable to support uses other than publishing

Revision ID: be804e93d51d
Revises: c46641b76073
Create Date: 2021-11-17 18:15:24.393374

"""
import os

from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "be804e93d51d"
down_revision = "c46641b76073"
branch_labels = None
depends_on = None


def upgrade():
    # clean all tasks first to avoid crashing on null actor
    op.execute("DELETE FROM tasks")

    # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
    # on sqlite < 3.32.0
    recreate = (
        "always"
        if "sqlite" in os.environ.get("EXODUS_GW_DB_URL", "")
        else "auto"
    )

    with op.batch_alter_table("tasks", recreate=recreate) as batch_op:
        batch_op.alter_column(
            "publish_id", existing_type=postgresql.UUID(), nullable=True
        )


def downgrade():
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.alter_column(
            "publish_id", existing_type=postgresql.UUID(), nullable=False
        )
