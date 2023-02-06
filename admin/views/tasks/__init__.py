from flask_admin import Admin
from retry_tasks_lib.admin.views import (
    RetryTaskAdminBase,
    TaskTypeAdminBase,
    TaskTypeKeyAdminBase,
    TaskTypeKeyValueAdminBase,
)
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue

from cosmos.db.session import scoped_db_session


def register_tasks_admin(admin: "Admin") -> None:
    tasks_menu_title = "Tasks"
    admin.add_view(
        RetryTaskAdminBase(
            RetryTask,
            scoped_db_session,
            "Tasks",
            endpoint="tasks",
            category=tasks_menu_title,
        )
    )
    admin.add_view(
        TaskTypeAdminBase(
            TaskType,
            scoped_db_session,
            "Task Types",
            endpoint="task-types",
            category=tasks_menu_title,
        )
    )
    admin.add_view(
        TaskTypeKeyAdminBase(
            TaskTypeKey,
            scoped_db_session,
            "Task Type Keys",
            endpoint="task-type-keys",
            category=tasks_menu_title,
        )
    )
    admin.add_view(
        TaskTypeKeyValueAdminBase(
            TaskTypeKeyValue,
            scoped_db_session,
            "Task Type Key Values",
            endpoint="task-type-key-values",
            category=tasks_menu_title,
        )
    )
