"""add unique constraint on (publish_id, web_uri)

Revision ID: 6461bad8ed91
Revises: 5bd0b38df850
Create Date: 2023-02-02 14:02:05.437573

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "6461bad8ed91"
down_revision = "5bd0b38df850"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("items") as batch_op:
        batch_op.create_unique_constraint(
            "items_publish_id_web_uri_key", ["publish_id", "web_uri"]
        )


def downgrade():
    with op.batch_alter_table("items") as batch_op:
        batch_op.drop_constraint(
            "items_publish_id_web_uri_key", type_="unique"
        )
