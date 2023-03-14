import logging

from dataclasses import dataclass

from flask import flash
from sqlalchemy import func
from sqlalchemy.exc import DBAPIError
from sqlalchemy.future import select

from admin.hubble.db.models import Activity
from admin.hubble.db.session import activity_scoped_session
from admin.views.retailer.forms import DeleteRetailerActionForm
from admin.views.utils import SessionDataMethodsMixin
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import AccountHolder, Campaign, Retailer, Reward, Transaction
from cosmos.db.session import scoped_db_session as db_session
from cosmos.retailers.enums import RetailerStatuses


@dataclass
class SessionData(SessionDataMethodsMixin):
    retailer_name: str
    retailer_slug: str
    retailer_id: int
    retailer_status: RetailerStatuses
    loyalty_name: str


class DeleteRetailerAction:
    logger = logging.getLogger("delete-retailer-action")

    def __init__(self) -> None:
        self.form = DeleteRetailerActionForm()
        self._session_data: SessionData | None = None

    @property
    def session_data(self) -> SessionData:
        if not self._session_data:
            raise ValueError("session_data is not set")

        return self._session_data

    @session_data.setter
    def session_data(self, value: str) -> None:
        self._session_data = SessionData.from_base64_str(value)

    def affected_account_holders_count(self) -> int:  # pragma: no cover
        return db_session.scalar(
            select(func.count(AccountHolder.id)).where(AccountHolder.retailer_id == self.session_data.retailer_id)
        )

    def affected_transactions_count(self) -> int:  # pragma: no cover
        return db_session.scalar(
            select(func.count(Transaction.id)).where(Transaction.retailer_id == self.session_data.retailer_id)
        )

    def affected_rewards_count(self) -> int:  # pragma: no cover
        return db_session.scalar(
            select(func.count(Reward.id)).where(
                Reward.account_holder_id == AccountHolder.id,
                AccountHolder.retailer_id == self.session_data.retailer_id,
            )
        )

    def affected_campaigns_slugs(self) -> list[str]:  # pragma: no cover
        return db_session.scalars(
            select(Campaign.slug).where(
                Campaign.status == CampaignStatuses.ACTIVE,
                Campaign.retailer_id == self.session_data.retailer_id,
            )
        ).all()

    @staticmethod
    def _get_retailer_by_id(retailer_id: int) -> Retailer:  # pragma: no cover
        return db_session.get(Retailer, retailer_id)

    def validate_selected_ids(self, ids: list[str]) -> str | None:
        if not ids:
            return "no retailer selected."

        if len(ids) > 1:
            return "Only one Retailer allowed for this action"

        retailer = self._get_retailer_by_id(int(ids[0]))

        if retailer.status == RetailerStatuses.ACTIVE:
            return "Only non active Retailers allowed for this action"

        self._session_data = SessionData(
            retailer_name=retailer.name,
            retailer_slug=retailer.slug,
            retailer_id=retailer.id,
            retailer_status=retailer.status,
            loyalty_name="ACCUMULATOR",
        )

        return None

    def _delete_retailer_data(self) -> None:  # pragma: no cover
        db_session.execute(Retailer.__table__.delete().where(Retailer.slug == self.session_data.retailer_slug))
        db_session.flush()

    def _delete_hubble_retailer_data(self) -> None:  # pragma: no cover
        activity_scoped_session.execute(
            Activity.__table__.delete().where(Activity.retailer == self.session_data.retailer_slug)
        )
        activity_scoped_session.flush()

    def delete_retailer(self) -> bool:
        """Tries to delete all retailer related data in cosmos and hubble, returns True if successful False if not."""

        if not self.form.acceptance.data:
            flash("User did not agree to proceed, action halted.")
            return False

        try:
            self._delete_retailer_data()
            self._delete_hubble_retailer_data()
        except DBAPIError:
            db_session.rollback()
            activity_scoped_session.rollback()

            self.logger.exception(
                "Exception while trying to delete retailer %s (%d)",
                self.session_data.retailer_slug,
                self.session_data.retailer_id,
            )
            flash("Something went wrong, database changes rolled back", category="error")
            return False

        db_session.commit()
        activity_scoped_session.commit()
        flash(
            f"All rows related to retailer {self.session_data.retailer_name} ({self.session_data.retailer_id}) "
            "have been deleted."
        )
        return True
