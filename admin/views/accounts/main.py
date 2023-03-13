from collections.abc import Generator
from datetime import UTC, datetime
from typing import NamedTuple
from uuid import UUID

from flask import flash
from flask_admin.actions import action
from sqlalchemy import delete
from sqlalchemy.orm import joinedload

from admin.activity_utils.enums import ActivityType
from admin.helpers.custom_formatters import account_holder_repr, campaign_slug_repr
from admin.views.model_views import BaseModelView
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.db.models import AccountHolder, Retailer
from cosmos.retailers.enums import RetailerStatuses


class AccountDeletedData(NamedTuple):
    account_holder_uuid: UUID
    retailer_name: str
    retailer_status: RetailerStatuses
    retailer_slug: str


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
    column_labels = {"retailer": "Retailer"}
    column_searchable_list = ("id", "email", "account_holder_uuid", "account_number")
    form_widget_args = {
        "opt_out_token": {"readonly": True},
    }

    def _generate_payloads_for_delete_account_holder_activity(
        self,
        deleted_accounts_data: list[AccountDeletedData],
        sso_username: str,
    ) -> Generator[dict, None, None]:  # pragma: no cover

        return (
            ActivityType.get_account_holder_deleted_activity_data(
                activity_datetime=datetime.now(tz=UTC),
                account_holder_uuid=str(deleted_account.account_holder_uuid),
                retailer_name=deleted_account.retailer_name,
                retailer_status=deleted_account.retailer_status.name,
                retailer_slug=deleted_account.retailer_slug,
                sso_username=sso_username,
            )
            for deleted_account in deleted_accounts_data
        )

    @action(
        "delete-account-holder",
        "Delete",
        "The selected account holders' retailer must be in a TEST state. "
        "This action is not reversible, are you sure you wish to proceed?",
    )
    def delete_account_holder(self, ids: list[str]) -> None:
        activity_payloads: Generator[dict, None, None] | None = None
        account_holders_ids = [int(ah_id) for ah_id in ids]

        result: list[AccountDeletedData] = self.session.execute(
            delete(AccountHolder)
            .options(joinedload(Retailer))
            .where(
                AccountHolder.id.in_(account_holders_ids),
                AccountHolder.retailer_id == Retailer.id,
                Retailer.status == RetailerStatuses.TEST,
            )
            .returning(
                AccountHolder.account_holder_uuid,
                Retailer.name,
                Retailer.status,
                Retailer.slug,
            )
            .execution_options(synchronize_session=False)
        ).all()

        if not result:
            flash("This action is allowed only for account holders that belong to a TEST retailer.", category="error")
            return

        activity_payloads = self._generate_payloads_for_delete_account_holder_activity(result, self.sso_username)
        sync_send_activity(activity_payloads, routing_key=ActivityType.ACCOUNT_DELETED.value)
        self.session.commit()

        flash(f"Deleted {len(result)} Account Holders.")


class ProfileAdmin(BaseModelView):
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
    column_filters = ("account_holder.retailer.slug", "campaign.slug", "reset_date")
    column_formatters = {"account_holder": account_holder_repr, "campaign": campaign_slug_repr}
    form_widget_args = {"account_holder": {"disabled": True}}


class MarketingPreferenceAdmin(BaseModelView):
    column_searchable_list = ("account_holder.id", "account_holder.email", "account_holder.account_holder_uuid")
    column_filters = ("key_name", "value_type")
    column_labels = {"account_holder": "Account Holder"}
    column_formatters = {"account_holder": account_holder_repr}
    column_default_sort = ("account_holder.created_at", True)
