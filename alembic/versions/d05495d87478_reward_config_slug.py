"""reward_config slug

Revision ID: d05495d87478
Revises: 962cc8859572
Create Date: 2023-01-09 13:26:08.763102

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d05495d87478"
down_revision = "962cc8859572"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("reward_config", sa.Column("slug", sa.String(), nullable=False))
    op.create_index(op.f("ix_reward_config_slug"), "reward_config", ["slug"], unique=False)
    op.create_unique_constraint("slug_retailer_unq", "reward_config", ["slug", "retailer_id"])


def downgrade() -> None:
    op.drop_constraint("slug_retailer_unq", "reward_config", type_="unique")
    op.drop_index(op.f("ix_reward_config_slug"), table_name="reward_config")
    op.drop_column("reward_config", "slug")
