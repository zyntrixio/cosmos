import logging

from dataclasses import dataclass
from datetime import UTC, datetime
from random import getrandbits
from typing import TYPE_CHECKING, TypeVar

import requests
import wtforms

from flask import flash, redirect, request, session, url_for
from flask_admin import expose
from flask_admin.actions import action
from flask_admin.contrib.sqla.fields import QuerySelectField
from flask_admin.form import BaseForm
from flask_admin.model import typefmt
from sqlalchemy import inspect, or_
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from admin.activity_utils.enums import ActivityType
from admin.views.campaign_reward.custom_actions import CampaignEndAction
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
from cosmos.campaigns.config import campaign_settings
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.db.models import Campaign, EarnRule, RewardRule
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.enums import PendingRewardMigrationActions

if TYPE_CHECKING:

    from werkzeug import Response

    from cosmos.db.base import Base

    T = TypeVar("T", bound=Base)


@dataclass
class EasterEgg:
    greet: str
    content: str


class CampaignAdmin(CanDeleteModelView):
    column_auto_select_related = True
    action_disallowed_list = ["delete"]
    column_filters = ("retailer.slug", "status")
    column_searchable_list = ("slug", "name")
    form_args = {
        "loyalty_type": {"validators": [wtforms.validators.DataRequired(), validate_campaign_loyalty_type]},
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
        return False if name == "delete" else super().is_action_allowed(name)

    def get_easter_egg(self) -> EasterEgg | None:
        try:
            first_name = self.sso_username.split(" ", 1)[0]
        except Exception:  # noqa: BLE001
            return None

        greet_msg = f"Hello there {first_name}"
        kitten_msg = "<img src='http://placekitten.com/200/300' alt='🐈 🐈‍⬛'>"
        profanities_msg = (
            "here's a list of profanities: "
            "<a href='https://en.wikipedia.org/wiki/Category:English_profanity'>profanities</a>"
        )

        match first_name.lower():
            case "francesco" | "susanne" | "rupal" | "lewis" | "haffi" | "bhasker":
                return EasterEgg(greet_msg, kitten_msg)
            case "jess" | "alyson" | "stewart":
                return EasterEgg(greet_msg, kitten_msg if getrandbits(1) else profanities_msg)

        return EasterEgg(
            greet_msg,
            "<p>This is an internal tool with very little input validation.</p>"
            "<p>¯\\_(ツ)_/¯</p>"
            "<p>Please do try not to destroy everything.</p>",
        )

    def after_model_delete(self, model: "Campaign") -> None:
        # Synchronously send activity for a campaign deletion after successful deletion
        activity_data = {}
        try:
            activity_data = ActivityType.get_campaign_deleted_activity_data(
                retailer_slug=model.retailer.slug,
                campaign_name=model.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=UTC),
                campaign_slug=model.slug,
                loyalty_type=model.loyalty_type.name,
                start_date=model.start_date,
                end_date=model.end_date,
            )
            sync_send_activity(
                activity_data,
                routing_key=ActivityType.CAMPAIGN.value,
            )
        except Exception as exc:
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
                    activity_datetime=datetime.now(tz=UTC),
                    campaign_slug=model.slug,
                    loyalty_type=model.loyalty_type,
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

    def _try_send_request_log_message(
        self,
        url: str,
        json_payload: dict,
        msg: str,
    ) -> bool:  # pragma: no cover
        try:
            logging.warning("\n\n %s \n\n", url)
            resp = requests.post(
                url,
                headers={"Authorization": f"token {campaign_settings.CAMPAIGN_API_AUTH_TOKEN}"},
                json=json_payload,
                timeout=campaign_settings.REQUEST_TIMEOUT,
            )
            if 200 <= resp.status_code <= 204:  # noqa: PLR2004
                flash(msg)
                return True

            self._flash_error_response(resp.json())

        except Exception as ex:
            msg = "Error: no response received."
            flash(msg, category="error")
            logging.exception(msg, exc_info=ex)

        return False

    def _send_campaign_status_change_request(
        self,
        campaign_slug: str,
        retailer_slug: str,
        requested_status: CampaignStatuses,
        pending_reward_action: PendingRewardMigrationActions,
    ) -> bool:

        return self._try_send_request_log_message(
            url=f"{campaign_settings.CAMPAIGN_BASE_URL}/{retailer_slug}/status-change",
            json_payload={
                "requested_status": requested_status.value,
                "campaign_slug": campaign_slug,
                "pending_rewards_action": pending_reward_action.value,
                "activity_metadata": {"sso_username": self.user_info["name"]},
            },
            msg=f"Selected campaign's status has been successfully changed to {requested_status}",
        )

    def _send_campaign_migration_request(
        self,
        *,
        to_campaign_slug: str,
        from_campaign_slug: str,
        retailer_slug: str,
        pending_reward_action: PendingRewardMigrationActions,
        transfer_balance: bool,
        conversion_rate: int,
        qualifying_threshold: int,
    ) -> bool:
        return self._try_send_request_log_message(
            url=f"{campaign_settings.CAMPAIGN_BASE_URL}/{retailer_slug}/migration",
            json_payload={
                "to_campaign": to_campaign_slug,
                "from_campaign": from_campaign_slug,
                "pending_rewards_action": pending_reward_action.value,
                "activity_metadata": {"sso_username": self.user_info["name"]},
                "balance_action": {
                    "transfer": transfer_balance,
                    "conversion_rate": conversion_rate,
                    "qualifying_threshold": qualifying_threshold,
                },
            },
            msg="Campaign migration completed successfully.",
        )

    @expose("/custom-actions/end-campaigns", methods=["GET", "POST"])
    def end_campaigns(self) -> "Response":

        if not self.user_info or self.user_session_expired:
            return redirect(url_for("auth_views.login"))

        campaigns_index_uri = url_for("campaigns.index_view")
        if not self.can_edit:
            return redirect(campaigns_index_uri)

        cmp_end_action = CampaignEndAction(self.session)

        if "form_dynamic_val" in session and request.method == "POST":
            form_dynamic_val = session["form_dynamic_val"]

        else:
            selected_campaigns_ids: list[str] = request.args.to_dict(flat=False).get("ids", [])
            if not selected_campaigns_ids:
                flash("no campaign selected.", category="error")
                return redirect(campaigns_index_uri)

            try:
                cmp_end_action.validate_selected_campaigns(selected_campaigns_ids)
            except ValueError:
                return redirect(campaigns_index_uri)

            form_dynamic_val = cmp_end_action.session_form_data.to_base64_str()
            session["form_dynamic_val"] = form_dynamic_val

        cmp_end_action.update_form(form_dynamic_val)

        if cmp_end_action.form.validate_on_submit():
            del session["form_dynamic_val"]
            cmp_end_action.end_campaigns(
                status_change_fn=self._send_campaign_status_change_request,
                migration_fn=self._send_campaign_migration_request,
            )
            return redirect(campaigns_index_uri)

        return self.render(
            "eh_end_campaign_action.html",
            active_campaign=cmp_end_action.session_form_data.active_campaign,
            draft_campaign=cmp_end_action.session_form_data.draft_campaign,
            form=cmp_end_action.form,
            easter_egg=self.get_easter_egg(),
        )

    @action(
        "end-campaigns",
        "End",
        "The selected campaign must be in an ACTIVE state.\n"
        "An optional DRAFT campaign from the same retailer can be selected, "
        "this will automatically activate it and enable the transfer configuration for balances and pending rewards.\n"
        "You will be redirected to an action configuration page.\n"
        "Are you sure you want to proceed?",
    )
    def end_campaigns_action(self, ids: list[str]) -> "Response":
        return redirect(url_for("campaigns.end_campaigns", ids=ids))

    def _send_cloned_campaign_activities(
        self, retailer_slug: str, campaign: Campaign, earn_rule: EarnRule, reward_rule: RewardRule
    ) -> None:
        sso_username = self.user_info["name"]
        campaign_slug = campaign.slug
        campaign_name = campaign.name
        loyalty_type = campaign.loyalty_type

        sync_send_activity(
            ActivityType.get_campaign_created_activity_data(
                retailer_slug=retailer_slug,
                campaign_name=campaign_name,
                sso_username=sso_username,
                activity_datetime=campaign.created_at,
                campaign_slug=campaign_slug,
                loyalty_type=loyalty_type,
                start_date=campaign.start_date,
                end_date=campaign.end_date,
            ),
            routing_key=ActivityType.CAMPAIGN.value,
        )
        sync_send_activity(
            ActivityType.get_earn_rule_created_activity_data(
                retailer_slug=retailer_slug,
                campaign_name=campaign_name,
                sso_username=sso_username,
                activity_datetime=earn_rule.created_at,
                campaign_slug=campaign_slug,
                loyalty_type=loyalty_type,
                threshold=earn_rule.threshold,
                increment=earn_rule.increment,
                increment_multiplier=earn_rule.increment_multiplier,
                max_amount=earn_rule.max_amount,
            ),
            routing_key=ActivityType.EARN_RULE.value,
        )
        sync_send_activity(
            ActivityType.get_reward_rule_created_activity_data(
                retailer_slug=retailer_slug,
                campaign_name=campaign_name,
                sso_username=sso_username,
                activity_datetime=reward_rule.created_at,
                campaign_slug=campaign_slug,
                reward_goal=reward_rule.reward_goal,
                reward_cap=reward_rule.reward_cap,
                refund_window=reward_rule.allocation_window,
            ),
            routing_key=ActivityType.REWARD_RULE.value,
        )

    def _clone_campaign_and_rules_instances(self, campaign: Campaign) -> Campaign | None:
        def clone_instance(old_model_instance: "T") -> "T":

            mapper = inspect(type(old_model_instance), raiseerr=True)
            new_model_instance = type(old_model_instance)()

            for name, col in mapper.columns.items():

                if not (col.primary_key or col.unique or name in ("created_at", "updated_at")):
                    setattr(new_model_instance, name, getattr(old_model_instance, name))

            return new_model_instance

        error_msg: str | None = None
        new_slug = f"CLONE_{campaign.slug}"
        new_campaign = clone_instance(campaign)
        new_campaign.slug = new_slug
        new_campaign.status = CampaignStatuses.DRAFT
        self.session.add(new_campaign)
        try:
            self.session.flush()
        except IntegrityError:
            error_msg = (
                f"Another campaign with slug '{new_slug}' already exists, "
                "please update it before trying to clone this campaign again."
            )

        except DataError:
            error_msg = f"Cloned campaign slug '{new_slug}' would exceed max slug length of 100 characters."

        if error_msg:
            self.session.rollback()
            flash(error_msg, category="error")
            return None

        if not (campaign.earn_rule and campaign.reward_rule):
            flash("Unable to clone, missing earn or reward rule.", category="error")
            return None

        earn_rule: EarnRule = clone_instance(campaign.earn_rule)
        earn_rule.campaign_id = new_campaign.id
        self.session.add(earn_rule)

        reward_rule: RewardRule = clone_instance(campaign.reward_rule)
        reward_rule.campaign_id = new_campaign.id
        self.session.add(reward_rule)

        self.session.commit()
        self._send_cloned_campaign_activities(campaign.retailer.slug, new_campaign, earn_rule, reward_rule)
        return new_campaign

    @action(
        "clone-campaign",
        "Clone",
        "Only one campaign allowed for this action, the selected campaign's retailer must be in a TEST state.",
    )
    def clone_campaign_action(self, ids: list[str]) -> None:
        if len(ids) > 1:
            flash("Only one campaign at a time is supported for this action.", category="error")
            return

        campaign = (
            self.session.execute(
                select(Campaign)
                .options(
                    joinedload(Campaign.reward_rule),
                    joinedload(Campaign.earn_rule),
                    joinedload(Campaign.retailer),
                )
                .where(Campaign.id == int(ids[0]))
            )
            .unique()
            .scalar_one()
        )

        if campaign.retailer.status != RetailerStatuses.TEST:
            flash("The campaign's retailer status must be TEST.", category="error")
            return

        if new_campaign := self._clone_campaign_and_rules_instances(campaign):
            flash(
                "Successfully cloned campaign, reward rules, and earn rules from campaign: "
                f"{campaign.slug} (id {campaign.id}) to campaign {new_campaign.slug} (id {new_campaign.id})."
            )

    def _campaigns_status_change(self, campaign_id: int, requested_status: CampaignStatuses) -> bool:
        if campaign := self.session.get(Campaign, campaign_id):
            return self._send_campaign_status_change_request(
                campaign.slug, campaign.retailer.slug, requested_status, PendingRewardMigrationActions.REMOVE
            )
        else:
            raise ValueError(f"No campaign found with id {campaign_id}")

    @action(
        "activate-campaigns",
        "Activate",
        "Selected campaigns must belong to the same Retailer, "
        "be in a DRAFT status and have one rewards rule and at least one earn rule.\n"
        "Are you sure you want to proceed?",
    )
    def action_activate_campaigns(self, ids: list[str]) -> None:
        if len(ids) > 1:
            flash("Cannot activate more than one campaign at once", category="error")
        else:
            self._campaigns_status_change(int(ids[0]), CampaignStatuses.ACTIVE)

    @action(
        "cancel-campaigns",
        "Cancel",
        "Selected campaigns must belong to the same Retailer and be in a ACTIVE status.\n"
        "Are you sure you want to proceed?",
    )
    def action_cancel_campaigns(self, ids: list[str]) -> None:
        if len(ids) > 1:
            flash("Cannot cancel more than one campaign at once", category="error")
        else:
            self._campaigns_status_change(int(ids[0]), CampaignStatuses.CANCELLED)

    def delete_model(self, model: Campaign) -> bool:
        if self.can_delete:
            if model.status == CampaignStatuses.DRAFT:
                return super().delete_model(model)
            flash("Cannot delete campaigns that are not DRAFT")
        else:
            flash("Only verified users can do this.", "error")

        return False


class EarnRuleAdmin(CanDeleteModelView):
    column_auto_select_related = True
    column_filters = ("campaign.name", "campaign.slug", "campaign.loyalty_type", "campaign.retailer.slug")
    column_searchable_list = ("campaign.slug", "campaign.name")
    column_list = (
        "campaign",
        "campaign.retailer",
        "threshold",
        "campaign.loyalty_type",
        "increment",
        "increment_multiplier",
        "max_amount",
        "created_at",
        "updated_at",
        "max_amount",
    )
    form_create_rules = form_edit_rules = (
        "campaign",
        "threshold",
        "increment",
        "increment_multiplier",
        "max_amount",
    )
    column_labels = {
        "campaign.retailer": "Retailer",
        "campaign.loyalty_type": "LoyaltyType",
    }
    column_type_formatters = typefmt.BASE_FORMATTERS | {type(None): lambda _view, _value: "-"}

    @property
    def form_args(self) -> dict:
        return {
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
            "increment_multiplier": {
                "validators": [validate_increment_multiplier, wtforms.validators.NumberRange(min=0)]
            },
            "max_amount": {
                "validators": [validate_earn_rule_max_amount, wtforms.validators.NumberRange(min=0)],
                "description": ("Upper limit for transaction earn in pence. 0 for stamp."),
            },
            "campaign": {
                "validators": [wtforms.validators.DataRequired()],
                "query_factory": lambda: self.session.query(Campaign).where(~Campaign.earn_rule.has()),
            },
        }

    def edit_form(self, obj: EarnRule) -> BaseForm:
        form = super().edit_form(obj)
        campaign_field: QuerySelectField = form.campaign
        campaign_field.query_factory = lambda: self.session.query(Campaign).where(
            or_(
                Campaign.id == obj.campaign_id,
                ~Campaign.earn_rule.has(),
            )
        )
        return form

    def on_model_delete(self, model: "EarnRule") -> None:
        validate_earn_rule_deletion(model.campaign)

        # Synchronously send activity for an earn rule deletion after successful deletion
        sync_send_activity(
            ActivityType.get_earn_rule_deleted_activity_data(
                retailer_slug=model.campaign.retailer.slug,
                campaign_name=model.campaign.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=UTC),
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
                    loyalty_type=model.campaign.loyalty_type,
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
    reward_cap_max = 10
    column_auto_select_related = True
    column_filters = ("campaign.slug", "campaign.name", "campaign.retailer.slug")
    column_searchable_list = ("campaign.slug", "campaign.name")
    column_list = (
        "campaign",
        "campaign.retailer",
        "reward_goal",
        "allocation_window",
        "reward_cap",
        "created_at",
        "updated_at",
    )
    column_labels = {
        "campaign.retailer": "Retailer",
        "allocation_window": "Refund Window",
    }
    column_type_formatters = typefmt.BASE_FORMATTERS | {type(None): lambda _view, _value: "-"}
    form_overrides = {"reward_cap": wtforms.SelectField}

    @property
    def form_args(self) -> dict:
        return {
            "reward_goal": {
                "validators": [wtforms.validators.NumberRange(min=1)],
                "description": (
                    "Balance goal used to calculate if a reward should be issued. "
                    "This is a money value * 100, e.g. a reward goal of £10.50 should be 1050, "
                    "and a reward goal of 8 stamps would be 800."
                ),
            },
            "allocation_window": {
                "default": None,
                "validators": [validate_reward_rule_allocation_window, wtforms.validators.NumberRange(min=1)],
                "description": (
                    "Period of time before a reward is allocated to an AccountHolder in days."
                    " Accumulator campaigns only."
                ),
            },
            "reward_cap": {
                "validators": [
                    validate_reward_cap_for_loyalty_type,
                    wtforms.validators.NumberRange(min=1, max=self.reward_cap_max),
                ],
                "description": ("Transaction reward cap. Accumulator campaigns only."),
                "choices": [("", "Not set")] + [(n, n) for n in range(1, self.reward_cap_max + 1)],
                "coerce": lambda x: int(x) if x else None,
            },
            "campaign": {
                "validators": [wtforms.validators.DataRequired()],
                "query_factory": lambda: self.session.query(Campaign).where(~Campaign.reward_rule.has()),
            },
        }

    def edit_form(self, obj: RewardRule) -> BaseForm:
        form = super().edit_form(obj)
        campaign_field: QuerySelectField = form.campaign
        campaign_field.query_factory = lambda: self.session.query(Campaign).where(
            or_(
                Campaign.id == obj.campaign_id,
                ~Campaign.reward_rule.has(),
            )
        )
        return form

    def on_model_delete(self, model: "RewardRule") -> None:
        validate_reward_rule_deletion(model.campaign)
        # Synchronously send activity for an earn rule deletion after successful deletion
        sync_send_activity(
            ActivityType.get_reward_rule_deleted_activity_data(
                retailer_slug=model.campaign.retailer.slug,
                campaign_name=model.campaign.name,
                sso_username=self.sso_username,
                activity_datetime=datetime.now(tz=UTC),
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
                    if field.name == "campaign":
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
