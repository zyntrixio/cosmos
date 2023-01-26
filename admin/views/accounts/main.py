from admin.helpers.custom_formatters import account_holder_repr, campaign_slug_repr
from admin.views.model_views import BaseModelView


class AccountHolderAdmin(BaseModelView):
    can_create = False
    column_filters = (
        "retailer.slug",
        "retailer.name",
        "retailer.id",
        "opt_out_token",
        "created_at",
    )
    form_excluded_columns = (
        "created_at",
        "updated_at",
        "current_balances",
        "profile",
        "rewards",
        "marketing_preferences",
    )
    column_labels = {"retailerconfig": "Retailer"}
    column_searchable_list = ("id", "email", "account_holder_uuid", "account_number")
    form_widget_args = {
        "opt_out_token": {"readonly": True},
    }


class AccountHolderProfileAdmin(BaseModelView):
    can_create = False
    column_searchable_list = ("account_holder_id", "account_holder.email", "account_holder.account_holder_uuid")
    column_labels = {"account_holder": "Account Holder"}
    column_formatters = {"account_holder": account_holder_repr}
    column_default_sort = ("account_holder.created_at", True)


class PendingRewardAdmin(BaseModelView):
    can_create = False
    can_export = True
    column_searchable_list = (
        "account_holder.id",
        "account_holder.email",
        "account_holder.account_holder_uuid",
    )
    column_labels = {"account_holder": "Account Holder", "id": "Pending Reward id"}
    column_filters = ("account_holder.retailer.slug", "campaign.slug", "created_date", "conversion_date")
    column_formatters = {"account_holder": account_holder_repr, "campaign": campaign_slug_repr}
    form_widget_args = {"account_holder": {"disabled": True}}
    column_export_list = [
        "account_holder.account_holder_uuid",
        "created_at",
        "updated_at",
        "id",
        "created_date",
        "conversion_date",
        "value",
        "campaign.slug",
        "account_holder.retailer.slug",
        "total_cost_to_user",
        "count",
    ]


class CampaignBalanceAdmin(BaseModelView):
    can_create = False
    column_searchable_list = ("account_holder.id", "account_holder.email", "account_holder.account_holder_uuid")
    column_labels = {"account_holder": "Account Holder"}
    column_filters = ("account_holder.retailer.slug", "campaign.slug")
    column_formatters = {"account_holder": account_holder_repr, "campaign": campaign_slug_repr}
    form_widget_args = {"account_holder": {"disabled": True}}


class MarketingPreferenceAdmin(BaseModelView):
    column_searchable_list = ("account_holder.id", "account_holder.email", "account_holder.account_holder_uuid")
    column_filters = ("key_name", "value_type")
    column_labels = {"account_holder": "Account Holder"}
    column_formatters = {"account_holder": account_holder_repr}
    column_default_sort = ("account_holder.created_at", True)
