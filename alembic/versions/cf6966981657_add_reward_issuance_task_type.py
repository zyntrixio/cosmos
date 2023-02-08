"""add reward issuance task type

Revision ID: cf6966981657
Revises: d05495d87478
Create Date: 2023-02-09 14:38:53.763764

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "cf6966981657"
down_revision = "d05495d87478"
branch_labels = None
depends_on = None


reward_issuance_task_name = "reward-issuance"
key_type_list = [
    {"name": "campaign_id", "type": "INTEGER"},
    {"name": "account_holder_id", "type": "INTEGER"},
    {"name": "reward_config_id", "type": "INTEGER"},
    {"name": "pending_reward_id", "type": "STRING"},
    {"name": "reason", "type": "STRING"},
    {"name": "agent_state_params_raw", "type": "STRING"},
]


def get_tables(conn: sa.engine.Connection) -> tuple[sa.Table, sa.Table]:
    metadata = sa.MetaData()
    return (
        sa.Table("task_type", metadata, autoload_with=conn),
        sa.Table("task_type_key", metadata, autoload_with=conn),
    )


def upgrade() -> None:
    conn = op.get_bind()
    task_type, task_type_key = get_tables(conn)

    task_type_id = conn.execute(
        sa.insert(task_type).values(
            name=reward_issuance_task_name,
            path="cosmos.rewards.tasks.issuance.issue_reward",
            error_handler_path="cosmos.core.tasks.error_handlers.default_handler",
            queue_name="cosmos:default",
        )
    ).inserted_primary_key[0]

    op.bulk_insert(
        task_type_key,
        [key_type | {"task_type_id": task_type_id} for key_type in key_type_list],
    )


def downgrade() -> None:
    conn = op.get_bind()
    task_type, task_type_key = get_tables(conn)

    conn.execute(
        sa.delete(task_type_key).where(
            task_type_key.c.task_type_id == task_type.c.task_type_id,
            task_type.c.name == reward_issuance_task_name,
        )
    )
    conn.execute(sa.delete(task_type).where(task_type.c.name == reward_issuance_task_name))
