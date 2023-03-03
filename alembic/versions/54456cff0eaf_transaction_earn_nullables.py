"""transaction earn nullables

Revision ID: 54456cff0eaf
Revises: ce2b5ff75fa5
Create Date: 2023-03-03 15:48:33.877129

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "54456cff0eaf"
down_revision = "ce2b5ff75fa5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE transaction_earn SET earn_amount=0 WHERE earn_amount IS NULL;")
    op.alter_column("transaction_earn", "earn_amount", existing_type=sa.INTEGER(), nullable=False)


def downgrade() -> None:
    op.alter_column("transaction_earn", "earn_amount", existing_type=sa.INTEGER(), nullable=True)
    op.execute("UPDATE transaction_earn SET earn_amount=NULL WHERE earn_amount=0;")
