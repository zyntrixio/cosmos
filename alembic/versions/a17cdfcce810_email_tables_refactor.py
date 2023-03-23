"""email tables refactor

Revision ID: a17cdfcce810
Revises: ce2b5ff75fa5
Create Date: 2023-03-21 13:36:52.665051

"""
import sqlalchemy as sa

from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "a17cdfcce810"
down_revision = "ce2b5ff75fa5"
branch_labels = None
depends_on = None

email_type_id_fk = "email_template_email_type_id_fkey"
template_types = ["WELCOME_EMAIL", "REWARD_ISSUANCE", "BALANCE_RESET"]
emailtemplatetypes_enum = postgresql.ENUM(*template_types, name="emailtemplatetypes")


def get_tables(conn: sa.engine.Connection) -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return (
        sa.Table("email_type", metadata, autoload_with=conn),
        sa.Table("email_template", metadata, autoload_with=conn),
    )


def migrate_types(conn: sa.engine.Connection) -> None:
    EmailType, EmailTemplate = get_tables(conn)  # noqa: N806
    conn.execute(
        EmailType.insert(),
        [{"slug": value} for value in template_types],
    )
    conn.execute(
        EmailType.update()
        .values(send_email_params_fn="cosmos.accounts.send_email_params_gen.get_balance_reset_nudge_params")
        .where(EmailType.c.slug == "BALANCE_RESET")
    )

    type_slug_to_id_map = sa.future.select(EmailType.c.slug, EmailType.c.id).cte("type_slug_to_id_map")
    conn.execute(
        EmailTemplate.update()
        .values(email_type_id=type_slug_to_id_map.c.id)
        .where(EmailTemplate.c.type == sa.cast(type_slug_to_id_map.c.slug, emailtemplatetypes_enum))
    )


def migrate_types_downgrade(conn: sa.engine.Connection) -> None:
    EmailType, EmailTemplate = get_tables(conn)  # noqa: N806
    type_slug_to_id_map = sa.future.select(EmailType.c.slug, EmailType.c.id).cte("type_slug_to_id_map")
    conn.execute(
        EmailTemplate.update()
        .values(type=sa.cast(type_slug_to_id_map.c.slug, emailtemplatetypes_enum))
        .where(EmailTemplate.c.email_type_id == type_slug_to_id_map.c.id)
    )


def upgrade() -> None:
    op.create_table(
        "email_type",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("send_email_params_fn", sa.String(), nullable=True),
        sa.Column("required_fields", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_email_type_slug"), "email_type", ["slug"], unique=True)
    op.create_table(
        "account_holder_email",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', CURRENT_TIMESTAMP)"), nullable=False
        ),
        sa.Column("account_holder_id", sa.BigInteger(), nullable=False),
        sa.Column("email_type_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_id", sa.BigInteger(), nullable=True),
        sa.Column("retry_task_id", sa.BigInteger(), nullable=True),
        sa.Column("message_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("current_status", sa.String(), nullable=True),
        sa.Column("allow_re_send", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["account_holder_id"], ["account_holder.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["campaign_id"], ["campaign.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["email_type_id"], ["email_type.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["retry_task_id"], ["retry_task.retry_task_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_account_holder_email_account_holder_id"), "account_holder_email", ["account_holder_id"], unique=False
    )
    op.create_index(op.f("ix_account_holder_email_campaign_id"), "account_holder_email", ["campaign_id"], unique=False)
    op.create_index(
        op.f("ix_account_holder_email_allow_re_send"), "account_holder_email", ["allow_re_send"], unique=False
    )
    op.create_index(
        op.f("ix_account_holder_email_email_type_id"), "account_holder_email", ["email_type_id"], unique=False
    )
    op.create_index(op.f("ix_account_holder_email_message_uuid"), "account_holder_email", ["message_uuid"], unique=True)
    op.create_index(
        op.f("ix_account_holder_email_retry_task_id"), "account_holder_email", ["retry_task_id"], unique=True
    )
    op.add_column("email_template", sa.Column("required_fields_values", sa.Text(), nullable=True))
    op.add_column("email_template", sa.Column("email_type_id", sa.BigInteger(), nullable=True))
    op.drop_constraint("type_retailer_unq", "email_template", type_="unique")
    op.create_unique_constraint("type_retailer_unq", "email_template", ["email_type_id", "retailer_id"])
    op.create_foreign_key(
        email_type_id_fk, "email_template", "email_type", ["email_type_id"], ["id"], ondelete="CASCADE"
    )

    conn = op.get_bind()

    migrate_types(conn)

    op.alter_column("email_template", "email_type_id", nullable=False)
    op.drop_column("email_template", "type")
    emailtemplatetypes_enum.drop(bind=conn, checkfirst=False)


def downgrade() -> None:
    conn = op.get_bind()
    emailtemplatetypes_enum.create(bind=conn, checkfirst=False)
    op.add_column("email_template", sa.Column("type", emailtemplatetypes_enum, autoincrement=False, nullable=True))

    migrate_types_downgrade(conn)

    op.alter_column("email_template", "type", nullable=False)
    op.drop_constraint(email_type_id_fk, "email_template", type_="foreignkey")
    op.drop_constraint("type_retailer_unq", "email_template", type_="unique")
    op.create_unique_constraint("type_retailer_unq", "email_template", ["type", "retailer_id"])
    op.drop_column("email_template", "email_type_id")
    op.drop_column("email_template", "required_fields_values")
    op.drop_index(op.f("ix_account_holder_email_retry_task_id"), table_name="account_holder_email")
    op.drop_index(op.f("ix_account_holder_email_message_uuid"), table_name="account_holder_email")
    op.drop_index(op.f("ix_account_holder_email_email_type_id"), table_name="account_holder_email")
    op.drop_index(op.f("ix_account_holder_email_allow_re_send"), table_name="account_holder_email")
    op.drop_index(op.f("ix_account_holder_email_account_holder_id"), table_name="account_holder_email")
    op.drop_table("account_holder_email")
    op.drop_index(op.f("ix_email_type_slug"), table_name="email_type")
    op.drop_table("email_type")
