from alembic import op


def create_check_constaints() -> None:
    op.create_check_constraint(
        constraint_name="balance_lifespan_positive_int_or_null_check",
        table_name="retailer",
        condition="balance_lifespan IS NULL OR balance_lifespan > 0",
    )
    op.create_check_constraint(
        constraint_name="processed_null_or_true_check",
        table_name="transaction",
        condition="processed IS NULL OR processed IS TRUE",
    )
