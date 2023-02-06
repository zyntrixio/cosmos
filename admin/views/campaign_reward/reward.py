from typing import TYPE_CHECKING

from flask import Markup, flash, redirect, url_for
from flask_admin.actions import action
from sqlalchemy.orm import joinedload

from admin.helpers.custom_formatters import account_holder_export_repr, account_holder_repr
from admin.views.campaign_reward.validators import validate_required_fields_values_yaml, validate_retailer_fetch_type
from admin.views.model_views import BaseModelView
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import RewardConfig, RewardRule

if TYPE_CHECKING:
    from werkzeug.wrappers import Response


class RewardConfigAdmin(BaseModelView):
    column_filters = ("retailer.slug",)
    form_excluded_columns = ("rewards", "reward_rules", "status", "created_at", "updated_at")
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
    column_searchable_list = (
        "account_holder.id",
        "account_holder.email",
        "account_holder.account_holder_uuid",
        "code",
    )
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
    )
    column_labels = {"account_holder": "Account Holder", "retailer": "Retailer Slug", "campaign": "Campagin slug"}
    column_filters = (
        "account_holder.retailer.slug",
        "campaign.slug",
        "issued_date",
        # "status", # FIXME: Need custom filter. Flask-admin can find it as a 'column', as its a model property
    )
    column_formatters = {"account_holder": account_holder_repr}
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


class ReadOnlyRewardAdmin(RewardAdmin):
    column_details_exclude_list = ["code", "associated_url"]
    column_exclude_list = ["code", "associated_url"]
    column_export_exclude_list = RewardAdmin.column_export_exclude_list + ["associated_url"]

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
