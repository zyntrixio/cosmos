from datetime import UTC, datetime
from typing import TYPE_CHECKING

from flask import Markup, flash, redirect, url_for
from flask_admin.actions import action
from flask_admin.contrib.sqla.filters import FilterEqual
from sqlalchemy import or_
from sqlalchemy.future import select
from sqlalchemy.orm import Query, joinedload

from admin.activity_utils.enums import ActivityType
from admin.helpers.custom_formatters import account_holder_export_repr, account_holder_repr, reward_file_log_format
from admin.views.campaign_reward.validators import validate_required_fields_values_yaml, validate_retailer_fetch_type
from admin.views.model_views import BaseModelView
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.db.models import Retailer, Reward, RewardConfig, RewardRule

if TYPE_CHECKING:
    from werkzeug.wrappers import Response


class RewardStatusFilter(FilterEqual):
    def get_filters_from_status(self, value: str) -> tuple:
        now = datetime.now(tz=UTC).replace(tzinfo=None)

        match Reward.RewardStatuses(value):
            case Reward.RewardStatuses.UNALLOCATED:
                return (Reward.account_holder_id.is_(None),)
            case Reward.RewardStatuses.ISSUED:
                return (
                    Reward.account_holder_id.is_not(None),
                    Reward.redeemed_date.is_(None),
                    Reward.cancelled_date.is_(None),
                    or_(
                        Reward.expiry_date > now,
                        Reward.expiry_date.is_(None),
                    ),
                )
            case Reward.RewardStatuses.CANCELLED:
                return (
                    Reward.account_holder_id.is_not(None),
                    Reward.cancelled_date.is_not(None),
                )
            case Reward.RewardStatuses.REDEEMED:
                return (
                    Reward.account_holder_id.is_not(None),
                    Reward.redeemed_date.is_not(None),
                    Reward.cancelled_date.is_(None),
                )
            case Reward.RewardStatuses.EXPIRED:
                return (
                    Reward.account_holder_id.is_not(None),
                    Reward.expiry_date <= now,
                    Reward.redeemed_date.is_(None),
                )
            case _:
                raise ValueError(f"unexpected status value {value}")

    def apply(self, query: Query, value: str, alias: str | None = None) -> Query:  # noqa: ARG002
        return query.where(*self.get_filters_from_status(value))


class RewardConfigAdmin(BaseModelView):
    column_filters = ("retailer.slug",)
    form_excluded_columns = ("rewards", "reward_rules", "active", "created_at", "updated_at")
    form_widget_args = {
        "required_fields_values": {"rows": 5},
    }
    form_args = {
        "fetch_type": {
            "validators": [
                validate_retailer_fetch_type,
            ]
        },
        "required_fields_values": {
            "description": "Optional configuration in YAML format",
            "validators": [
                validate_required_fields_values_yaml,
            ],
        },
    }
    column_formatters = {
        "required_fields_values": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.required_fields_values)
        + Markup("</pre>"),
    }

    @action(
        "deactivate-reward-type",
        "DEACTIVATE",
        "This action can only be carried out on one reward_config at a time and is not reversible."
        " Are you sure you wish to proceed?",
    )
    def deactivate_reward_type(self, reward_config_ids: list[str]) -> None:
        if len(reward_config_ids) != 1:
            flash("This action must be completed for reward_configs one at a time", category="error")
            return
        reward_config_id = int(reward_config_ids[0])
        reward_config: RewardConfig | None = self.session.get(
            RewardConfig,
            reward_config_id,
            options=[joinedload(RewardConfig.reward_rules).joinedload(RewardRule.campaign)],
        )
        if not reward_config:
            raise ValueError(f"No RewardConfig with id {reward_config_id}")

        if not reward_config.active:
            flash("RewardConfig already DEACTIVATED")
            return

        if any(reward_rule.campaign.status == CampaignStatuses.ACTIVE for reward_rule in reward_config.reward_rules):
            flash("This RewardConfig has ACTIVE campaigns associated with it", category="error")
            return

        reward_config.active = False
        self.session.commit()
        flash("RewardConfig DEACTIVATED")


class RewardAdmin(BaseModelView):
    can_create = False
    column_list = (
        "account_holder",
        "reward_uuid",
        "code",
        "issued_date",
        "expiry_date",
        "status",
        "redeemed_date",
        "cancelled_date",
        "retailer",
        "associated_url",
        "campaign",
        "reward_file_log",
    )
    column_labels = {"reward_file_log": "Reward filename"}
    column_filters = (
        "account_holder.id",
        "account_holder.email",
        "retailer.slug",
        "campaign.slug",
        "reward_config.slug",
        "reward_file_log.file_name",
        "issued_date",
        RewardStatusFilter(
            "status",
            "Status",
            options=[(opt.value, opt.name) for opt in Reward.RewardStatuses],
        ),
    )
    column_searchable_list = (
        "account_holder.account_holder_uuid",
        "code",
    )
    column_formatters = {"account_holder": account_holder_repr, "reward_file_log": reward_file_log_format}
    form_widget_args = {
        "reward_id": {"readonly": True},
        "code": {"readonly": True},
        "account_holder": {"disabled": True},
    }
    column_formatters_export = {"account_holder": account_holder_export_repr}
    column_export_exclude_list = ["code"]
    can_export = True

    def is_accessible(self) -> bool:
        return super().is_accessible() if self.is_read_write_user else False

    def inaccessible_callback(self, name: str, **kwargs: dict | None) -> "Response":
        if self.is_read_write_user:
            return redirect(url_for("rewards.index_view"))

        if self.is_read_only_user:
            return redirect(url_for("ro-rewards.index_view"))

        return super().inaccessible_callback(name, **kwargs)

    def is_action_allowed(self, name: str) -> bool:
        return self.is_read_write_user if name == "delete-rewards" else False

    def _get_rewards_from_ids(self, reward_ids: list[str]) -> list[Reward]:
        return self.session.execute(select(Reward).where(Reward.id.in_(reward_ids))).scalars().all()

    def _get_retailer_by_id(self, retailer_id: int) -> Retailer:
        return self.session.execute(select(Retailer).where(Retailer.id == retailer_id)).scalar_one()

    @action(
        "delete-rewards",
        "Delete",
        "This action can only be carried out on non allocated and non soft deleted rewards."
        "This action is unreversible. Proceed?",
    )
    def delete_rewards(self, reward_ids: list[str]) -> None:
        # Get rewards for all selected ids
        rewards = self._get_rewards_from_ids(reward_ids)

        selected_retailer_id: int = rewards[0].retailer_id
        for reward in rewards:
            # Fail if all rewards are not eligible for deletion
            if reward.retailer_id != selected_retailer_id:
                self.session.rollback()
                flash("Not all selected rewards are for the same retailer", category="error")
                return

            if reward.status == Reward.RewardStatuses.ISSUED or reward.deleted:
                self.session.rollback()
                flash("Not all selected rewards are eligible for deletion", category="error")
                return

            self.session.delete(reward)

        self.session.commit()

        # Synchronously send activity for rewards deleted if successfully deleted
        rewards_deleted_count = len(rewards)
        retailer = self._get_retailer_by_id(selected_retailer_id)
        activity_payload = ActivityType.get_reward_deleted_activity_data(
            activity_datetime=datetime.now(tz=UTC),
            retailer_name=retailer.name,
            retailer_slug=retailer.slug,
            sso_username=self.sso_username,
            rewards_deleted_count=rewards_deleted_count,
        )
        sync_send_activity(activity_payload, routing_key=ActivityType.REWARD_DELETED.value)
        flash("Successfully deleted selected rewards")


class ReadOnlyRewardAdmin(RewardAdmin):
    column_details_exclude_list = ["code", "associated_url"]
    column_exclude_list = ["code", "associated_url"]
    column_export_exclude_list = [*RewardAdmin.column_export_exclude_list, "associated_url"]

    def is_accessible(self) -> bool:
        if self.is_read_write_user:
            return False
        return super(RewardAdmin, self).is_accessible()


class FetchTypeAdmin(BaseModelView):
    can_create = False
    can_edit = False
    can_delete = False
    column_searchable_list = ("name",)
    column_formatters = {
        "required_fields": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.required_fields)
        + Markup("</pre>"),
    }


class RewardUpdateAdmin(BaseModelView):
    column_searchable_list = ("id", "reward.reward_uuid", "reward.code")
    column_filters = ("reward.retailer.slug",)


class RewardFileLogAdmin(BaseModelView):
    can_create = False
    can_edit = False
    can_delete = False
    column_searchable_list = ("id", "file_name")
    column_filters = ("file_name", "file_agent_type", "created_at")
