STRING = "STRING"
INTEGER = "INTEGER"

QUEUE_NAME = "cosmos:default"

TaskTypeKeyData = namedtuple("TaskTypeKeyData", ["name", "type"])
TaskTypeData = namedtuple("TaskTypeData", ["name", "path", "error_handler_path", "keys"])
task_type_data = [
    TaskTypeData(
        name="account-holder-activation",
        path="polaris.tasks.account_holder.account_holder_activation",
        error_handler_path="polaris.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="welcome_email_retry_task_id", type=INTEGER),
            TaskTypeKeyData(name="callback_retry_task_id", type=INTEGER),
        ],
    ),
    TaskTypeData(
        name="send-welcome-email",
        path="polaris.tasks.account_holder.send_welcome_email",
        error_handler_path="polaris.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
        ],
    ),
    TaskTypeData(
        name="enrolment-callback",
        path="polaris.tasks.account_holder.enrolment_callback",
        error_handler_path="polaris.tasks.error_handlers.handle_retry_task_request_error",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="callback_url", type=STRING),
            TaskTypeKeyData(name="third_party_identifier", type=STRING),
        ],
    ),
    TaskTypeData(
        name="create-campaign-balances",
        path="polaris.tasks.account_holder.create_campaign_balances",
        error_handler_path="polaris.tasks.error_handlers.default_handler",
        keys=[
            TaskTypeKeyData(name="retailer_slug", type=STRING),
            TaskTypeKeyData(name="campaign_slug", type=STRING),
        ],
    ),
    TaskTypeData(
        name="anonymise-account-holder",
        path="polaris.tasks.account_holder.anonymise_account_holder_data",
        error_handler_path="polaris.tasks.error_handlers.default_handler",
        keys=[
            TaskTypeKeyData(name="account_holder_id", type=INTEGER),
            TaskTypeKeyData(name="retailer_id", type=INTEGER),
        ],
    ),
]


def add_task_data() -> None:
    metadata = sa.MetaData()
    conn = op.get_bind()
    TaskType = sa.Table("task_type", metadata, autoload_with=conn)
    TaskTypeKey = sa.Table("task_type_key", metadata, autoload_with=conn)
    for data in task_type_data:
        inserted_obj = conn.execute(
            TaskType.insert().values(
                name=data.name,
                path=data.path,
                error_handler_path=data.error_handler_path,
                queue_name=QUEUE_NAME,
            )
        )
        task_type_id = inserted_obj.inserted_primary_key[0]
        for key in data.keys:
            conn.execute(TaskTypeKey.insert().values(name=key.name, type=key.type, task_type_id=task_type_id))
