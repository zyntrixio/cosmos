from typing import TYPE_CHECKING

import wtforms
import yaml

from flask import Markup
from wtforms.validators import DataRequired, Optional

from admin.activity_utils.enums import ActivityType
from admin.views.model_views import BaseModelView, CanDeleteModelView
from admin.views.retailer.validators import (
    validate_account_number_prefix,
    validate_marketing_config,
    validate_optional_yaml,
    validate_retailer_config,
    validate_retailer_config_new_values,
)
from cosmos.core.activity.tasks import sync_send_activity

if TYPE_CHECKING:

    from cosmos.db.models import Retailer


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
        # "balance_reset_advanced_warning_days", #FIXME: Add back in once Retailer model has this column
        "status",
    )
    column_details_list = ("created_at", "updated_at") + form_create_rules
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
        # "balance_reset_advanced_warning_days", #FIXME: Add back in once Retailer model has this column
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
            "this value. 0 implies balances will not be reset.",
            "validators": [wtforms.validators.NumberRange(min=0)],
        },
        #  FIXME: Add back in once Retailer model has this column
        # "balance_reset_advanced_warning_days": {
        #     "description": "Number of days ahead of account holder balance reset "
        #     "date that a balance reset nudge should be sent.",
        #     "validators": [wtforms.validators.NumberRange(min=0)],
        # },
    }
    column_formatters = {
        "profile_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.profile_config)
        + Markup("</pre>"),
        "marketing_preference_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.marketing_preference_config)
        + Markup("</pre>"),
    }

    def after_model_change(self, form: wtforms.Form, model: "Retailer", is_created: bool) -> None:
        if is_created:
            # Synchronously send activity for retailer creation
            #  FIXME: Fix once Retailer model has balance_reset_advanced_warning_days column
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
                    # balance_reset_advanced_warning_days=model.balance_reset_advanced_warning_days,
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

    #  FIXME: Add back in once Retailer model has balance_reset_advanced_warning_days column
    # def on_model_change(self, form: wtforms.Form, model: "Retailer", is_created: bool) -> None:
    # validate_balance_reset_advanced_warning_days(form, retailer_status=model.status)
    # if not is_created and form.balance_lifespan.object_data == 0 and form.balance_lifespan.data > 0:
    #     reset_date = (datetime.now(tz=timezone.utc) + timedelta(days=model.balance_lifespan)).date()
    #     stmt = (
    #         update(CampaignBalance)
    #         .where(
    #             CampaignBalance.account_holder_id == AccountHolder.id,
    #             AccountHolder.retailer_id == model.id,
    #         )
    #         .values(reset_date=reset_date)
    #         .execution_options(synchronize_session=False)
    #     )
    #     self.session.execute(stmt)

    # return super().on_model_change(form, model, is_created)


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
        "fetch_type.retailer",
        "fetch_type",
        "agent_config",
        "created_at",
        "updated_at",
    )
    column_searchable_list = ("fetch_type.retailer.slug", "fetch_type.name")
    form_widget_args = {
        "agent_config": {"rows": 5},
    }
    column_formatters = {
        "agent_config": lambda _v, _c, model, _p: Markup("<pre>")
        + Markup.escape(model.agent_config)
        + Markup("</pre>"),
        "fetch_type": lambda _v, _c, model, _p: model.fetch_type.name,
    }
    column_labels = {"fetch_type.retailer": "Retailer"}
    form_args = {
        "agent_config": {
            "description": "Optional configuration in YAML format",
            "validators": [
                validate_optional_yaml,
            ],
        },
    }
