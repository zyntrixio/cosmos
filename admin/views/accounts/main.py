import hashlib

from collections.abc import Generator, Sequence
from datetime import UTC, datetime
from typing import NamedTuple, cast
from uuid import UUID

from flask import flash
from flask_admin.actions import action
from retry_tasks_lib.utils.synchronous import enqueue_retry_task, sync_create_task
from sqlalchemy import Row, Table
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import joinedload

from admin.activity_utils.enums import ActivityType
from admin.config import admin_settings
from admin.helpers.custom_formatters import account_holder_repr, campaign_slug_repr
from admin.hubble.db.session import activity_scoped_session
from admin.views.model_views import BaseModelView
from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.core.activity.tasks import sync_send_activity
from cosmos.core.config import redis_raw
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

        result: Sequence[Row[tuple[UUID, str, RetailerStatuses, str]]] = self.session.execute(
            cast(Table, AccountHolder.__table__)
            .delete()
            .options(joinedload(AccountHolder.retailer))
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

        del_datas = [
            AccountDeletedData(
                account_holder_uuid=res.account_holder_uuid,
                retailer_name=res.name,
                retailer_status=res.status,
                retailer_slug=res.slug,
            )
            for res in result
        ]
        activity_payloads = self._generate_payloads_for_delete_account_holder_activity(del_datas, self.sso_username)
        sync_send_activity(activity_payloads, routing_key=ActivityType.ACCOUNT_DELETED.value)
        self.session.commit()

        flash(f"Deleted {len(result)} Account Holders.")

    @action(
        "anonymise-account-holder",
        "Anonymise account holder (RTBF)",
        "This action is not reversible. Are you sure you wish to proceed?",
    )
    def anonymise_user(self, account_holder_ids: list[str]) -> None:
        if len(account_holder_ids) != 1:
            flash("This action must be completed for account holders one at a time", category="error")
            return

        if not (account_holder := self.session.get(AccountHolder, account_holder_ids[0])):
            flash("Account holder not found", category="error")
            return

        if account_holder.status == AccountHolderStatuses.INACTIVE:
            flash("Account holder is INACTIVE", category="error")
            return

        email = account_holder.email

        account_holder.status = AccountHolderStatuses.INACTIVE
        account_holder.email = hashlib.sha224(
            (account_holder.email + str(account_holder.account_holder_uuid)).encode("utf-8")
        ).hexdigest()

        account_holder.account_number = hashlib.sha224(
            ((account_holder.account_number or "N/A") + str(account_holder.account_holder_uuid)).encode("utf-8")
        ).hexdigest()

        if account_holder.profile:
            self.session.delete(account_holder.profile)

        try:
            self.session.flush()
            anonymise_activities_task = sync_create_task(
                activity_scoped_session,
                task_type_name=admin_settings.ANONYMISE_ACTIVITIES_TASK_NAME,
                params={
                    "retailer_slug": account_holder.retailer.slug,
                    "account_holder_uuid": account_holder.account_holder_uuid,
                    "account_holder_email": email,
                },
            )

        except DBAPIError:
            self.session.rollback()
            flash(f"Failed to anonymise Account Holder (id: {account_holder.id}), rolling back.", category="error")
            return

        self.session.commit()
        activity_scoped_session.commit()
        enqueue_retry_task(connection=redis_raw, retry_task=anonymise_activities_task)
        flash(f"Account Holder (id: {account_holder.id}) successfully anonymised.")


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
    column_list = (
        "account_holder",
        "account_holder.retailer",
        "key_name",
        "value",
        "value_type",
        "created_at",
        "updated_at",
    )
    column_searchable_list = ("account_holder.id", "account_holder.email", "account_holder.account_holder_uuid")
    column_filters = ("key_name", "value_type", "account_holder.retailer.slug", "value")
    column_labels = {"account_holder": "Account Holder", "account_holder.retailer": "Retailer"}
    column_formatters = {"account_holder": account_holder_repr}
    column_default_sort = ("account_holder.created_at", True)


class AccountHolderEmailAdmin(BaseModelView):
    can_view_details = True
    can_create = False
    can_edit = False
    can_delete = False
    column_searchable_list = ("retry_task_id", "message_uuid")
    column_filters = (
        "account_holder.id",
        "account_holder.email",
        "account_holder.account_holder_uuid",
        "campaign.name",
        "campaign.slug",
        "email_type.slug",
        "current_status",
        "allow_re_send",
    )
    column_formatters = {"account_holder": account_holder_repr}
