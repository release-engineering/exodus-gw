"""Add published_paths table

Revision ID: 979ec567eb91
Revises: fbac38695a01
Create Date: 2024-04-22 09:59:36.854652
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "979ec567eb91"
down_revision = "fbac38695a01"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "published_paths",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("env", sa.String(), nullable=False),
        sa.Column("web_uri", sa.String(), nullable=False),
        sa.Column("updated", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "env", "web_uri", name="published_paths_env_web_uri_key"
        ),
    )


def downgrade():
    op.drop_table("published_paths")
