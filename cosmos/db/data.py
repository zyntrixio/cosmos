from collections import namedtuple

import sqlalchemy as sa

QUEUE_NAME = "cosmos:default"
STRING = "STRING"
INTEGER = "INTEGER"
JSON = "JSON"

TaskTypeKeyData = namedtuple("TaskTypeKeyData", ["name", "type"])
TaskTypeData = namedtuple("TaskTypeData", ["name", "path", "error_handler_path", "keys"])
task_type_data = [
    TaskTypeData(
        name="account-holder-activation",
        path="cosmos.accounts.tasks.account_holder.account_holder_activation",
        error_handler_path="cosmos.accounts.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="welcome_email_retry_task_id", type=INTEGER),
            TaskTypeKeyData(name="callback_retry_task_id", type=INTEGER),
            TaskTypeKeyData(name="third_party_identifier", type=STRING),
            TaskTypeKeyData(name="channel", type=STRING),
        ],
    ),
    TaskTypeData(
        name="send-email",
        path="cosmos.accounts.tasks.account_holder.send_email",
        error_handler_path="cosmos.accounts.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="retailer_id", type=INTEGER),
            TaskTypeKeyData(name="template_type", type=STRING),
            TaskTypeKeyData(name="extra_params", type=JSON),
        ],
    ),
    TaskTypeData(
        name="enrolment-callback",
        path="cosmos.accounts.tasks.account_holder.enrolment_callback",
        error_handler_path="cosmos.accounts.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="callback_url", type=STRING),
            TaskTypeKeyData(name="third_party_identifier", type=STRING),
        ],
    ),
]


EmailTemplateKeyData = namedtuple("EmailTemplateKeyData", ["name", "display_name", "description"])
email_template_key_data = [
    EmailTemplateKeyData(
        name="first_name",
        display_name="First name",
        description="Account holder first name",
    ),
    EmailTemplateKeyData(
        name="last_name",
        display_name="Last name",
        description="Account holder last name",
    ),
    EmailTemplateKeyData(
        name="account_number",
        display_name="Account number",
        description="Account holder number",
    ),
    EmailTemplateKeyData(
        name="marketing_opt_out_link",
        display_name="Marketing opt out link",
        description="Account holder marketing opt out link",
    ),
    EmailTemplateKeyData(
        name="reward_url",
        display_name="Reward URL",
        description="Associated URL on account holder reward",
    ),
    EmailTemplateKeyData(
        name="current_balance",
        display_name="Current balance",
        description="Current Account Holder balance value.",
    ),
    EmailTemplateKeyData(
        name="balance_reset_date",
        display_name="Balance reset date",
        description="Account Holder Balance reset date in DD/MM/YY format.",
    ),
    EmailTemplateKeyData(
        name="datetime",
        display_name="Datetime",
        description="Account Holder Balance last read time in HH:MM DD/MM/YY (24 hours) format.",
    ),
]


def add_fetch_types(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    fetch_type = sa.Table("fetch_type", metadata, autoload_with=conn)
    conn.execute(
        fetch_type.insert(),
        [
            {
                "name": "PRE_LOADED",
                "required_fields": "validity_days: integer",
                "path": "TBC",
            },
            {
                "name": "JIGSAW_EGIFT",
                "required_fields": "transaction_value: integer",
                "path": "TBC",
            },
        ],
    )


def create_task_data(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    task_type = sa.Table("task_type", metadata, autoload_with=conn)
    task_type_key = sa.Table("task_type_key", metadata, autoload_with=conn)
    for data in task_type_data:
        inserted_obj = conn.execute(
            task_type.insert().values(
                name=data.name,
                path=data.path,
                error_handler_path=data.error_handler_path,
                queue_name=QUEUE_NAME,
            )
        )
        task_type_id = inserted_obj.inserted_primary_key[0]
        for key in data.keys:
            conn.execute(task_type_key.insert().values(name=key.name, type=key.type, task_type_id=task_type_id))


def load_email_template_key_data(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    email_template_key = sa.Table("email_template_key", metadata, autoload_with=conn)
    for data in email_template_key_data:
        conn.execute(
            email_template_key.insert().values(
                name=data.name,
                display_name=data.display_name,
                description=data.description,
            )
        )


def load_data(conn: sa.engine.Connection, metadata: sa.MetaData) -> None:
    add_fetch_types(conn, metadata)
    create_task_data(conn, metadata)
    load_email_template_key_data(conn, metadata)
