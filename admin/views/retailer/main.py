from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import wtforms
import yaml

from flask import Markup, flash, redirect, request, session, url_for
from flask_admin import expose
from flask_admin.actions import action
from sqlalchemy import update
from sqlalchemy.future import select
from wtforms.validators import DataRequired, Optional

from admin.activity_utils.enums import ActivityType
from admin.views.model_views import BaseModelView, CanDeleteModelView
from admin.views.retailer.custom_actions import DeleteRetailerAction
from admin.views.retailer.validators import (
    validate_account_number_prefix,
    validate_balance_lifespan_and_warning_days,
    validate_marketing_config,
    validate_optional_yaml,
    validate_retailer_config,
    validate_retailer_config_new_values,
)
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, Retailer
from cosmos.retailers.enums import RetailerStatuses

if TYPE_CHECKING:

    from werkzeug.wrappers import Response


class RetailerAdmin(BaseModelView):
    column_filters = ("created_at", "status")
    column_searchable_list = ("id", "slug", "name")
    column_labels = {"profile_config": "Enrolment Config"}
    column_exclude_list = ("profile_config", "marketing_preference_config")
    form_create_rules = (
        "name",
        "slug",
        "account_number_prefix",
        "profile_config",
        "marketing_preference_config",
        "loyalty_name",
        "balance_lifespan",
        "balance_reset_advanced_warning_days",
        "status",
    )
    column_details_list = ("created_at", "updated_at", *form_create_rules)
    form_excluded_columns = ("account_holder_collection",)
    form_widget_args = {
        "account_number_length": {"disabled": True},
        "profile_config": {"rows": 20},
        "marketing_preference_config": {"rows": 10},
        "status": {"disabled": True},
    }
    form_edit_rules = (
        "name",
        "profile_config",
        "marketing_preference_config",
        "loyalty_name",
        "balance_lifespan",
        "balance_reset_advanced_warning_days",
    )

    profile_config_placeholder = """
email:
  required: true
  label: Email address
first_name:
  required: true
  label: Forename
last_name:
  required: true
  label: Surname
""".strip()

    marketing_config_placeholder = """
marketing_pref:
  type: boolean
  label: Would you like to receive marketing?
""".strip()

    form_args = {
        "profile_config": {
            "label": "Enrolment Field Configuration",
            "validators": [
                validate_retailer_config,
            ],
            "render_kw": {"placeholder": profile_config_placeholder},
            "description": "Configuration in YAML format",
        },
        "marketing_preference_config": {
            "label": "Marketing Preferences Configuration",
            "validators": [validate_marketing_config],
            "render_kw": {"placeholder": marketing_config_placeholder},
            "description": "Optional configuration in YAML format",
        },
        "account_number_prefix": {
            "validators": [
                validate_account_number_prefix,
            ]
        },
        "status": {"default": "TEST", "validators": [Optional()]},
        "balance_lifespan": {
            "description": "Provide a value >0 (in days) if balances are to be periodically reset based on "
            "this value. Balances will not be reset if left empty.",
            "validators": [wtforms.validators.NumberRange(min=1)],
        },
        "balance_reset_advanced_warning_days": {
            "description": "Number of days ahead of account holder balance reset "
            "date that a balance reset nudge should be sent.",
            "validators": [wtforms.validators.NumberRange(min=1)],
        },
    }
    column_formatters = {
        "profile_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.profile_config)
        + Markup("</pre>"),
        "marketing_preference_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.marketing_preference_config)
        + Markup("</pre>"),
    }

    def after_model_change(self, form: wtforms.Form, model: Retailer, is_created: bool) -> None:
        if is_created:
            # Synchronously send activity for retailer creation
            sync_send_activity(
                ActivityType.get_retailer_created_activity_data(
                    sso_username=self.sso_username,
                    activity_datetime=model.created_at,
                    status=model.status.name,
                    retailer_name=model.name,
                    retailer_slug=model.slug,
                    account_number_prefix=model.account_number_prefix,
                    enrolment_config=yaml.safe_load(model.profile_config),
                    marketing_preferences=yaml.safe_load(model.marketing_preference_config),
                    loyalty_name=model.loyalty_name,
                    balance_lifespan=model.balance_lifespan,
                    balance_reset_advanced_warning_days=model.balance_reset_advanced_warning_days,
                ),
                routing_key=ActivityType.RETAILER_CREATED.value,
            )
        else:
            new_values, original_values = validate_retailer_config_new_values(form, model)
            if new_values:
                sync_send_activity(
                    ActivityType.get_retailer_update_activity_data(
                        sso_username=self.sso_username,
                        activity_datetime=model.updated_at,
                        retailer_name=model.name,
                        retailer_slug=model.slug,
                        new_values=new_values,
                        original_values=original_values,
                    ),
                    routing_key=ActivityType.RETAILER_CHANGED.value,
                )

    def on_model_change(self, form: wtforms.Form, model: "Retailer", is_created: bool) -> None:
        validate_balance_lifespan_and_warning_days(form, retailer_status=model.status)
        if not is_created and form.balance_lifespan.object_data is None and form.balance_lifespan.data is not None:
            reset_date = (datetime.now(tz=UTC) + timedelta(days=model.balance_lifespan)).date()
            stmt = (
                update(CampaignBalance)
                .where(
                    CampaignBalance.account_holder_id == AccountHolder.id,
                    AccountHolder.retailer_id == model.id,
                )
                .values(reset_date=reset_date)
                .execution_options(synchronize_session=False)
            )
            self.session.execute(stmt)

        return super().on_model_change(form, model, is_created)

    def _get_retailer_by_id(self, retailer_id: int) -> Retailer:
        return self.session.execute(select(Retailer).where(Retailer.id == retailer_id)).scalar_one()

    def _check_activate_campaign_for_retailer(self, retailer_id: int) -> list[int]:
        return (
            self.session.execute(
                select(Campaign.id).where(
                    Campaign.retailer_id == retailer_id, Campaign.status == CampaignStatuses.ACTIVE
                )
            )
            .scalars()
            .all()
        )

    @action(
        "activate retailer",
        "Activate",
        "Selected test retailer must have an activate campaign. Are you sure you want to proceed?",
    )
    def activate_retailer(self, ids: list[str]) -> None:
        if len(ids) > 1:
            flash("Cannot activate more than one retailer at once", category="error")
            return

        retailer = self._get_retailer_by_id(int(ids[0]))
        if (original_status := retailer.status) != RetailerStatuses.TEST:
            flash("Retailer in incorrect state for activation", category="error")
            return

        if self._check_activate_campaign_for_retailer(retailer.id):
            try:
                retailer.status = RetailerStatuses.ACTIVE
                self.session.commit()
                flash("Update retailer status successfully")
            except Exception:  # noqa: BLE001
                self.session.rollback()
                flash("Failed to update retailer", category="error")
            else:
                self.session.refresh(retailer)
                sync_send_activity(
                    ActivityType.get_retailer_status_update_activity_data(
                        sso_username=self.sso_username,
                        activity_datetime=retailer.updated_at,
                        new_status=retailer.status.name,
                        original_status=original_status.name,
                        retailer_name=retailer.name,
                        retailer_slug=retailer.slug,
                    ),
                    routing_key=ActivityType.RETAILER_STATUS.value,
                )
        else:
            flash("Retailer has no active campaign", category="error")

    @expose("/custom-actions/delete-retailer", methods=["GET", "POST"])
    def delete_retailer(self) -> "Response":
        if not self.user_info or self.user_session_expired:
            return redirect(url_for("auth_views.login"))

        retailers_index_uri = url_for("retailers.index_view")
        if not self.can_edit:
            return redirect(retailers_index_uri)

        del_ret_action = DeleteRetailerAction()

        if "action_context" in session and request.method == "POST":
            del_ret_action.session_data = session["action_context"]

        else:
            if error_msg := del_ret_action.validate_selected_ids(request.args.to_dict(flat=False).get("ids", [])):
                flash(error_msg, category="error")
                return redirect(retailers_index_uri)

            session["action_context"] = del_ret_action.session_data.to_base64_str()

        if del_ret_action.form.validate_on_submit():
            del session["action_context"]
            if del_ret_action.delete_retailer():
                sync_send_activity(
                    ActivityType.get_retailer_deletion_activity_data(
                        sso_username=self.sso_username,
                        activity_datetime=datetime.now(tz=UTC),
                        retailer_name=del_ret_action.session_data.retailer_name,
                        retailer_slug=del_ret_action.session_data.retailer_slug,
                        original_values={
                            "status": del_ret_action.session_data.retailer_status.name,
                            "name": del_ret_action.session_data.retailer_name,
                            "slug": del_ret_action.session_data.retailer_slug,
                            "loyalty_name": del_ret_action.session_data.loyalty_name,
                        },
                    ),
                    routing_key=ActivityType.RETAILER_DELETED.value,
                )

            return redirect(retailers_index_uri)

        return self.render(
            "eh_delete_retailer_action.html",
            retailer_name=del_ret_action.session_data.retailer_name,
            account_holders_count=del_ret_action.affected_account_holders_count(),
            transactions_count=del_ret_action.affected_transactions_count(),
            rewards_count=del_ret_action.affected_rewards_count(),
            campaign_slugs=del_ret_action.affected_campaigns_slugs(),
            form=del_ret_action.form,
        )

    @action(
        "delete-retailer",
        "Delete",
        "Only one non active retailer allowed for this action. This action is unreversible, Proceed?",
    )
    def delete_retailer_action(self, ids: list[str]) -> "Response":
        return redirect(url_for("retailers.delete_retailer", ids=ids))


class EmailTemplateAdmin(CanDeleteModelView):
    column_list = (
        "template_id",
        "type",
        "required_keys",
        "retailer",
        "created_at",
        "updated_at",
    )
    column_searchable_list = ("template_id",)
    column_filters = (
        "type",
        "required_keys.name",
        "retailer.slug",
        "retailer.name",
        "retailer.id",
    )
    column_details_list = ("template_id", "type", "required_keys", "retailer")
    form_excluded_columns = (
        "created_at",
        "updated_at",
    )
    column_labels = {"required_keys": "Template Key", "retailer": "Retailer"}


class EmailTemplateKeyAdmin(BaseModelView):
    can_view_details = True
    can_create = False
    can_edit = False
    can_delete = False
    column_searchable_list = ("name", "display_name", "description")
    form_excluded_columns = (
        "template",
        "created_at",
        "updated_at",
    )
    column_labels = {"display_name": "Display Name"}


class RetailerStoreAdmin(BaseModelView):
    column_labels = {"retailer": "Retailer"}
    column_filters = ("retailer.slug", "created_at")
    column_searchable_list = ("store_name", "mid")

    form_args = {
        "store_name": {
            "validators": [DataRequired(message="Store name is required")],
        },
        "mid": {
            "validators": [DataRequired(message="MID is required")],
        },
    }


class RetailerFetchTypeAdmin(CanDeleteModelView):
    column_list = (
        "retailer",
        "fetch_type",
        "agent_config",
        "created_at",
        "updated_at",
    )
    form_create_rules = form_edit_rules = (
        "retailer",
        "fetch_type",
        "agent_config",
    )
    column_searchable_list = ("retailer.slug", "fetch_type.name")
    form_widget_args = {
        "agent_config": {"rows": 5},
    }
    column_formatters = {
        "agent_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.agent_config)
        + Markup("</pre>"),
        "fetch_type": lambda _v, _c, model, _p: model.fetch_type.name,
    }
    column_labels = {"retailer": "Retailer"}
    form_args = {
        "agent_config": {
            "description": "Optional configuration in YAML format",
            "validators": [
                validate_optional_yaml,
            ],
        },
    }
