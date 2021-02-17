"""Add state column to publishes

Revision ID: 854e06069e65
Revises: a60131dd10c4
Create Date: 2021-02-18 11:06:59.636314

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "854e06069e65"
down_revision = "a60131dd10c4"
branch_labels = None
depends_on = None


def upgrade():
    # clean all publishes first to avoid crashing on null state
    op.execute("DELETE FROM publishes")

    with op.batch_alter_table(
        "publishes",
        # recreate='always' to avoid "Cannot add a NOT NULL column with default value NULL"
        # on sqlite < 3.32.0
        recreate="always",
    ) as batch_op:
        batch_op.add_column(
            sa.Column(
                "state",
                sa.String(),
                nullable=False,
            )
        )


def downgrade():
    with op.batch_alter_table("publishes") as batch_op:
        batch_op.drop_column("state")
