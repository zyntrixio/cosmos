import json
import logging

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy import select

from cosmos.core.activity.tasks import sync_send_activity
from cosmos.core.tasks import send_request_with_metrics
from cosmos.rewards.activity.enums import ActivityType

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback
    from uuid import UUID

    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from cosmos.db.models import AccountHolder, Campaign, RewardConfig
    from cosmos.rewards.schemas import IssuanceTaskParams


class BaseAgent(ABC):
    logger = logging.getLogger("agents")

    AGENT_STATE_PARAMS_RAW_KEY = "agent_state_params_raw"

    def __init__(
        self,
        db_session: "Session",
        *,
        campaign: "Campaign",
        reward_config: "RewardConfig",
        account_holder: "AccountHolder",
        config: dict,
        retry_task: "RetryTask",
        task_params: "IssuanceTaskParams",
    ) -> None:
        self.db_session = db_session
        self.reward_config = reward_config
        self.campaign = campaign
        self.account_holder = account_holder
        self.config = config
        self.retry_task = retry_task
        self.task_params = task_params
        self.send_request = send_request_with_metrics
        self._agent_state_params_raw_instance: TaskTypeKeyValue | None = None
        self.agent_state_params: dict = {}
        self._load_agent_state_params_raw_instance()

    def __enter__(self) -> "BaseAgent":
        return self

    def __exit__(self, exc_type: type, exc_value: Exception, exc_traceback: "Traceback") -> None:  # noqa: B027
        pass

    def _load_agent_state_params_raw_instance(self) -> None:
        try:
            self._agent_state_params_raw_instance = self.db_session.scalar(
                select(TaskTypeKeyValue).where(
                    TaskTypeKeyValue.retry_task_id == self.retry_task.retry_task_id,
                    TaskTypeKeyValue.task_type_key_id == TaskTypeKey.task_type_key_id,
                    TaskTypeKey.task_type_id == self.retry_task.task_type_id,
                    TaskTypeKey.name == self.AGENT_STATE_PARAMS_RAW_KEY,
                )
            )
            if self._agent_state_params_raw_instance:
                self.agent_state_params = json.loads(self._agent_state_params_raw_instance.value)

        except Exception as ex:  # noqa: BLE001
            raise AgentError(
                "Error while loading the agent_state_params_raw TaskTypeValue "
                f"for RetryTask: {self.retry_task.retry_task_id}."
            ) from ex

    def set_agent_state_params(self, value: dict) -> None:
        try:
            self.agent_state_params = value
            parsed_val = json.dumps(value)

            if self._agent_state_params_raw_instance is None:
                self._agent_state_params_raw_instance = TaskTypeKeyValue(
                    retry_task_id=self.retry_task.retry_task_id,
                    value=parsed_val,
                    task_type_key_id=self.db_session.execute(
                        select(TaskTypeKey.task_type_key_id).where(
                            TaskTypeKey.task_type_id == self.retry_task.task_type_id,
                            TaskTypeKey.name == self.AGENT_STATE_PARAMS_RAW_KEY,
                        )
                    ).scalar_one(),
                )
                self.db_session.add(self._agent_state_params_raw_instance)
            else:
                self._agent_state_params_raw_instance.value = parsed_val

            self.db_session.commit()

        except Exception as ex:  # noqa: BLE001 # pragma: no cover
            raise AgentError(
                "Error while saving the agent_state_params_raw TaskTypeValue "
                f"for RetryTask: {self.retry_task.retry_task_id}."
            ) from ex

    def _send_issued_reward_activity(self, reward_uuid: "UUID", issued_date: datetime) -> None:  # pragma: no cover

        sync_send_activity(
            ActivityType.get_issued_reward_status_activity_data(
                account_holder_uuid=str(self.account_holder.account_holder_uuid),
                retailer=self.reward_config.retailer,
                reward_slug=self.reward_config.slug,
                activity_timestamp=issued_date,
                reward_uuid=str(reward_uuid),
                pending_reward_uuid=self.task_params.pending_reward_uuid,
                campaign=self.campaign,
                reason=self.task_params.reason,
            ),
            routing_key=ActivityType.REWARD_STATUS.value,
        )

    @abstractmethod
    def issue_reward(self) -> int | None:  # pragma: no cover
        ...

    @abstractmethod
    def fetch_balance(self) -> int:  # pragma: no cover
        ...


class AgentError(Exception):
    pass
