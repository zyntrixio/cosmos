from dataclasses import dataclass
from typing import TYPE_CHECKING

from cron_descriptor import get_description
from flask import redirect, url_for
from flask_admin import AdminIndexView, expose

from admin.views.model_views import UserSessionMixin
from cosmos.accounts.config import account_settings
from cosmos.rewards.config import reward_settings

if TYPE_CHECKING:
    from werkzeug.wrappers import Response


@dataclass
class ScheduleConfig:
    name: str
    schedule: str
    description: str


class CosmosAdminPanelIndexView(AdminIndexView, UserSessionMixin):
    def _build_schedule_config_data(self, name: str, schedule: str) -> ScheduleConfig:
        return ScheduleConfig(name=name, schedule=schedule, description=get_description(schedule))

    @property
    def scheduler_configs(self) -> list[ScheduleConfig]:
        return [
            self._build_schedule_config_data(
                "Reward Imports and Updates Schedule",
                reward_settings.BLOB_IMPORT_SCHEDULE,
            ),
            self._build_schedule_config_data("Pending Rewards Schedule", reward_settings.PENDING_REWARDS_SCHEDULE),
            self._build_schedule_config_data(
                "Balance Reset Schedule",
                account_settings.RESET_BALANCES_SCHEDULE,
            ),
            self._build_schedule_config_data(
                "Balance Reset Nudge Schedule",
                account_settings.RESET_BALANCE_NUDGES_SCHEDULE,
            ),
            self._build_schedule_config_data(
                "Customer Purchase Prompt Schedule",
                account_settings.PURCHASE_PROMPT_SCHEDULE,
            ),
        ]

    @expose("/")
    def index(self) -> "Response":
        if not self.user_info or self.user_session_expired:
            return redirect(url_for("auth_views.login"))
        return self.render("admin/index.html", scheduler_configs=self.scheduler_configs)
