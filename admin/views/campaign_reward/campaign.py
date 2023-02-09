import logging

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import wtforms

from flask_admin.model import typefmt
from wtforms.validators import DataRequired

from admin.activity_utils.enums import ActivityType
from admin.views.campaign_reward.validators import (
    validate_campaign_end_date_change,
    validate_campaign_loyalty_type,
    validate_campaign_slug_update,
    validate_campaign_start_date_change,
    validate_campaign_status_change,
    validate_earn_rule_deletion,
    validate_earn_rule_increment,
    validate_earn_rule_max_amount,
    validate_increment_multiplier,
    validate_retailer_update,
    validate_reward_cap_for_loyalty_type,
    validate_reward_rule_allocation_window,
    validate_reward_rule_change,
    validate_reward_rule_deletion,
)
from admin.views.model_views import CanDeleteModelView
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.activity.tasks import sync_send_activity

if TYPE_CHECKING:
    from cosmos.db.models import Campaign, EarnRule, RewardRule


class CampaignAdmin(CanDeleteModelView):
    column_auto_select_related = True
    action_disallowed_list = ["delete"]
    column_filters = ("retailer.slug", "status")
    column_searchable_list = ("slug", "name")
    column_labels = {"retailer": "Retailer"}
    form_args = {
        "loyalty_type": {"validators": [DataRequired(), validate_campaign_loyalty_type]},
        "status": {"validators": [validate_campaign_status_change]},
    }
    form_create_rules = form_edit_rules = (
        "retailer",
        "name",
        "slug",
        "loyalty_type",
        "start_date",
        "end_date",
    )

    def is_action_allowed(self, name: str) -> bool:
        return False if name == "delete" else super().is_action_allowed(name)  # noqa: PLR2004

    def after_model_delete(self, model: "Campaign") -> None:
        # Synchronously send activity for a campaign deletion after successful deletion
        activity_data = {}
        try:
            activity_data = ActivityType.get_campaign_deleted_activity_data(
                retailer_slug=model.retailer.slug,
                campaign_name=model.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=timezone.utc),
                campaign_slug=model.slug,
                loyalty_type=model.loyalty_type,
                start_date=model.start_date,
                end_date=model.end_date,
            )
            sync_send_activity(
                activity_data,
                routing_key=ActivityType.CAMPAIGN.value,
            )
        except Exception as exc:  # noqa: BLE001
            logging.exception("Failed to publish CAMPAIGN (deleted) activity", exc_info=exc)

    def on_model_change(self, form: wtforms.Form, model: "Campaign", is_created: bool) -> None:
        if not is_created:
            validate_campaign_end_date_change(
                old_end_date=form.end_date.object_data,
                new_end_date=model.end_date,
                campaign_status=model.status,
                start_date=model.start_date,
            )
            validate_campaign_start_date_change(
                old_start_date=form.start_date.object_data,
                new_start_date=model.start_date,
                campaign_status=model.status,
            )
            validate_retailer_update(
                old_retailer=form.retailer.object_data,
                new_retailer=model.retailer,
                campaign_status=model.status,
            )
            validate_campaign_slug_update(
                old_campaign_slug=form.slug.object_data, new_campaign_slug=model.slug, campaign_status=model.status
            )

        return super().on_model_change(form, model, is_created)

    def after_model_change(self, form: wtforms.Form, model: "Campaign", is_created: bool) -> None:

        if is_created:
            # Synchronously send activity for campaign creation after successfull campaign creation
            sync_send_activity(
                ActivityType.get_campaign_created_activity_data(
                    retailer_slug=model.retailer.slug,
                    campaign_name=model.name,
                    sso_username=self.sso_username,
                    activity_datetime=datetime.now(tz=timezone.utc),
                    campaign_slug=model.slug,
                    loyalty_type=model.loyalty_type.name,
                    start_date=model.start_date,
                    end_date=model.end_date,
                ),
                routing_key=ActivityType.CAMPAIGN.value,
            )

        else:
            # Synchronously send activity for campaign update after successfull campaign update
            new_values: dict = {}
            original_values: dict = {}

            for field in form:
                if (new_val := getattr(model, field.name)) != field.object_data:
                    if isinstance(new_val, LoyaltyTypes):
                        new_values[field.name] = new_val.name
                        original_values[field.name] = field.object_data.name
                    else:
                        new_values[field.name] = new_val
                        original_values[field.name] = field.object_data

            if new_values:
                sync_send_activity(
                    ActivityType.get_campaign_updated_activity_data(
                        retailer_slug=model.retailer.slug,
                        campaign_name=model.name,
                        sso_username=self.sso_username,
                        activity_datetime=model.updated_at,
                        campaign_slug=model.slug,
                        new_values=new_values,
                        original_values=original_values,
                    ),
                    routing_key=ActivityType.CAMPAIGN.value,
                )


class EarnRuleAdmin(CanDeleteModelView):
    column_auto_select_related = True
    column_filters = ("campaign.name", "campaign.slug", "campaign.loyalty_type", "campaign.retailer.slug")
    column_searchable_list = ("campaign.name",)
    column_list = (
        "campaign.slug",
        "campaign.retailer",
        "threshold",
        "campaign.loyalty_type",
        "increment",
        "increment_multiplier",
        "max_amount",
        "created_at",
        "updated_at",
    )
    form_create_rules = form_edit_rules = (
        "campaign",
        "threshold",
        "increment",
        "increment_multiplier",
        "max_amount",
    )
    column_labels = {
        "campaign.slug": "Campaign",
        "campaign.retailer": "Retailer",
        "campaign.loyalty_type": "LoyaltyType",
    }
    form_args = {
        "increment": {
            "validators": [validate_earn_rule_increment, wtforms.validators.NumberRange(min=1)],
            "description": (
                "Leave blank for accumulator campaigns. For stamp, this is the number to be awarded per eligible "
                "transaction multiplied by 100. 100 = 1 stamp."
            ),
        },
        "threshold": {
            "validators": [wtforms.validators.NumberRange(min=0)],
            "description": ("Minimum transaction value for earn in pence. E.g. for £10.50, please enter '1050'."),
        },
        "increment_multiplier": {"validators": [validate_increment_multiplier, wtforms.validators.NumberRange(min=0)]},
        "max_amount": {
            "validators": [validate_earn_rule_max_amount, wtforms.validators.NumberRange(min=0)],
            "description": ("Upper limit for transaction earn in pence. 0 for stamp."),
        },
    }
    column_type_formatters = typefmt.BASE_FORMATTERS | {type(None): lambda _view, _value: "-"}

    def on_model_delete(self, model: "EarnRule") -> None:
        validate_earn_rule_deletion(model.campaign)

        # Synchronously send activity for an earn rule deletion after successful deletion
        sync_send_activity(
            ActivityType.get_earn_rule_deleted_activity_data(
                retailer_slug=model.campaign.retailer.slug,
                campaign_name=model.campaign.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=timezone.utc),
                campaign_slug=model.campaign.slug,
                threshold=model.threshold,
                increment=model.increment,
                increment_multiplier=model.increment_multiplier,
                max_amount=model.max_amount,
            ),
            routing_key=ActivityType.EARN_RULE.value,
        )

        return super().on_model_delete(model)

    def after_model_change(self, form: wtforms.Form, model: "EarnRule", is_created: bool) -> None:
        if is_created:
            # Synchronously send activity for earn rule creation after successful creation
            sync_send_activity(
                ActivityType.get_earn_rule_created_activity_data(
                    retailer_slug=model.campaign.retailer.slug,
                    campaign_name=model.campaign.name,
                    sso_username=self.sso_username,
                    activity_datetime=model.created_at,
                    campaign_slug=model.campaign.slug,
                    loyalty_type=model.campaign.loyalty_type.name,
                    threshold=model.threshold,
                    increment=model.increment,
                    increment_multiplier=model.increment_multiplier,
                    max_amount=model.max_amount,
                ),
                routing_key=ActivityType.EARN_RULE.value,
            )
        else:
            # Synchronously send activity for an earn rule update after successful update
            new_values: dict = {}
            original_values: dict = {}

            for field in form:
                if (new_val := getattr(model, field.name)) != field.object_data:
                    new_values[field.name] = new_val
                    original_values[field.name] = field.object_data

            if new_values:
                sync_send_activity(
                    ActivityType.get_earn_rule_updated_activity_data(
                        retailer_slug=model.campaign.retailer.slug,
                        campaign_name=model.campaign.name,
                        sso_username=self.sso_username,
                        activity_datetime=model.updated_at,
                        campaign_slug=model.campaign.slug,
                        new_values=new_values,
                        original_values=original_values,
                    ),
                    routing_key=ActivityType.EARN_RULE.value,
                )


class RewardRuleAdmin(CanDeleteModelView):
    column_auto_select_related = True
    column_filters = ("campaign.name", "campaign.slug", "campaign.retailer.slug")
    column_searchable_list = ("campaign.name",)
    column_list = (
        "campaign.name",
        "campaign.retailer",
        "reward_goal",
        "allocation_window",
        "reward_cap",
        "created_at",
        "updated_at",
    )
    column_labels = {
        "campaign.name": "Campaign",
        "campaign.retailer": "Retailer",
        "allocation_window": "Refund Window",
    }
    form_args = {
        "reward_goal": {
            "validators": [wtforms.validators.NumberRange(min=1)],
            "description": (
                "Balance goal used to calculate if a reward should be issued. "
                "This is a money value * 100, e.g. a reward goal of £10.50 should be 1050, "
                "and a reward goal of 8 stamps would be 800."
            ),
        },
        "allocation_window": {
            "default": 0,
            "validators": [validate_reward_rule_allocation_window, wtforms.validators.NumberRange(min=0)],
            "description": (
                "Period of time before a reward is allocated to an AccountHolder in days."
                " Accumulator campaigns only."
            ),
        },
        "reward_cap": {
            "default": None,
            "validators": [validate_reward_cap_for_loyalty_type],
            "blank_text": "None",
            "description": ("Transaction reward cap. Accumulator campaigns only."),
        },
    }
    column_type_formatters = typefmt.BASE_FORMATTERS | {type(None): lambda _view, _value: "-"}

    def on_model_delete(self, model: "RewardRule") -> None:
        validate_reward_rule_deletion(model.campaign)
        # Synchronously send activity for an earn rule deletion after successful deletion
        sync_send_activity(
            ActivityType.get_reward_rule_deleted_activity_data(
                retailer_slug=model.campaign.retailer.slug,
                campaign_name=model.campaign.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=timezone.utc),
                campaign_slug=model.campaign.slug,
                reward_goal=model.reward_goal,
                refund_window=model.allocation_window,
                reward_cap=model.reward_cap,
            ),
            routing_key=ActivityType.REWARD_RULE.value,
        )
        return super().on_model_delete(model)

    def on_model_change(self, form: wtforms.Form, model: "RewardRule", is_created: bool) -> None:
        validate_reward_rule_change(model.campaign, is_created)
        return super().on_model_change(form, model, is_created)

    def after_model_change(self, form: wtforms.Form, model: "RewardRule", is_created: bool) -> None:

        if is_created:
            # Synchronously send activity for reward rule creation after successful creation
            sync_send_activity(
                ActivityType.get_reward_rule_created_activity_data(
                    retailer_slug=model.campaign.retailer.slug,
                    campaign_name=model.campaign.name,
                    sso_username=self.sso_username,
                    activity_datetime=model.created_at,
                    campaign_slug=model.campaign.slug,
                    reward_goal=model.reward_goal,
                    refund_window=model.allocation_window,
                    reward_cap=model.reward_cap,
                ),
                routing_key=ActivityType.REWARD_RULE.value,
            )

        else:
            new_values: dict = {}
            original_values: dict = {}

            for field in form:
                if (new_val := getattr(model, field.name)) != field.object_data:
                    if field.name == "campaign":  # noqa: PLR2004
                        new_values["campaign_slug"] = new_val.slug
                        original_values["campaign_slug"] = field.object_data.slug
                    else:
                        new_values[field.name] = new_val
                        original_values[field.name] = field.object_data
            # Synchronously send activity for reward rule update after successful update
            sync_send_activity(
                ActivityType.get_reward_rule_updated_activity_data(
                    retailer_slug=model.campaign.retailer.slug,
                    campaign_name=model.campaign.name,
                    sso_username=self.sso_username,
                    activity_datetime=model.updated_at,
                    campaign_slug=model.campaign.slug,
                    new_values=new_values,
                    original_values=original_values,
                ),
                routing_key=ActivityType.REWARD_RULE.value,
            )
