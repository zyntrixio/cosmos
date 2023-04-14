import logging

from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from retry_tasks_lib.db.models import RetryTask

logger = logging.getLogger(__name__)


def get_accounts_queueable_task_ids(
    activation_tasks: "Sequence[RetryTask]", eligible_account_ids: set[int]
) -> list[int]:
    # Get the number of tasks for each account holder id
    num_tasks_by_acc_holder_id: defaultdict[int, int] = defaultdict(int)
    for task in activation_tasks:
        num_tasks_by_acc_holder_id[task.get_params().get("account_holder_id")] += 1

    # Are there any eligible account holder ids with more than one task?
    account_ids_with_more_than_one_task = {
        account_holder_id for account_holder_id, num_tasks in num_tasks_by_acc_holder_id.items() if num_tasks != 1
    }
    # Does every eligible account id have at least one task?
    account_ids_with_no_tasks = {
        eligible_account_id
        for eligible_account_id in eligible_account_ids
        if eligible_account_id not in num_tasks_by_acc_holder_id
    }
    if account_ids_with_more_than_one_task:
        logger.error(
            "Extra activation tasks found for account holder ids: %s", sorted(account_ids_with_more_than_one_task)
        )
    if account_ids_with_no_tasks:
        logger.error(
            "Insufficient activation tasks found for account holder ids: %s", sorted(account_ids_with_no_tasks)
        )

    return [
        activation_task.retry_task_id
        for activation_task in activation_tasks
        if activation_task.get_params().get("account_holder_id") not in account_ids_with_more_than_one_task
    ]
