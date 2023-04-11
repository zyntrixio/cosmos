"""add purchase prompt email type

Revision ID: 445163cbf0f3
Revises: a17cdfcce810
Create Date: 2023-04-05 18:00:55.645212

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "445163cbf0f3"
down_revision = "a17cdfcce810"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(None, "email_template", ["template_id"])

    conn = op.get_bind()
    email_type = sa.Table("email_type", sa.MetaData(), autoload_with=conn)
    conn.execute(
        sa.insert(email_type).values(
            slug="PURCHASE_PROMPT",
            required_fields="purchase_prompt_days: integer",
            send_email_params_fn="cosmos.accounts.send_email_params_gen.get_purchase_prompt_params",
        )
    )


def downgrade() -> None:
    op.drop_constraint(None, "email_template", type_="unique")

    conn = op.get_bind()
    email_type = sa.Table("email_type", sa.MetaData(), autoload_with=conn)
    conn.execute(sa.delete(email_type).where(email_type.c.slug == "PURCHASE_PROMPT"))
