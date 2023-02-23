import logging

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flask import flash
from sqlalchemy.future import select

from admin.activity_utils.enums import ActivityType
from admin.views.campaign_reward.forms import EndCampaignActionForm, PendingRewardMigrationActions
from admin.views.utils import SessionDataMethodsMixin
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import Campaign, Retailer

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session


@dataclass
class CampaignRow:
    campaign_id: int
    campaign_slug: str
    campaign_type: CampaignStatuses


@dataclass
class SessionFormData(SessionDataMethodsMixin):
    retailer_slug: str
    active_campaign: CampaignRow
    draft_campaign: CampaignRow | None
    optional_fields_needed: bool


@dataclass
class ActivityData:
    activity_type: ActivityType
    payload: dict
    error_message: str


class CampaignEndAction:
    logger = logging.getLogger("campaign-end-action")
    form_optional_fields = ["transfer_balance", "convert_rate", "qualify_threshold"]

    def __init__(self, db_session: "Session") -> None:
        self.db_session = db_session
        self.form = EndCampaignActionForm()
        self._session_form_data: SessionFormData | None = None

    @property
    def session_form_data(self) -> SessionFormData:
        if not self._session_form_data:
            raise ValueError(
                "validate_selected_campaigns or update_form must be called before accessing session_form_data"
            )

        return self._session_form_data

    @staticmethod
    def _get_and_validate_campaigns(campaign_rows: list) -> tuple[CampaignRow | None, CampaignRow | None, list[str]]:

        errors: list[str] = []
        try:
            active_campaign, *extra_active = (
                CampaignRow(cmp.id, cmp.slug, cmp.loyalty_type)
                for cmp in campaign_rows
                if cmp.status == CampaignStatuses.ACTIVE
            )
        except ValueError:
            active_campaign = None
            extra_active = []

        try:
            draft_campaign, *extra_draft = (
                CampaignRow(cmp.id, cmp.slug, cmp.loyalty_type)
                for cmp in campaign_rows
                if cmp.status == CampaignStatuses.DRAFT
            )
        except ValueError:
            draft_campaign = None
            extra_draft = []

        if not active_campaign:
            errors.append("One ACTIVE campaign must be provided.")

        if extra_active or extra_draft:
            errors.append("Only up to one DRAFT and one ACTIVE campaign allowed.")

        return active_campaign, draft_campaign, errors

    @staticmethod
    def _check_retailer_and_status(campaign_rows: list["Row"]) -> list[str]:
        errors: list[str] = []

        if not campaign_rows:
            errors.append("No campaign found.")

        if any(cmp.status not in (CampaignStatuses.ACTIVE, CampaignStatuses.DRAFT) for cmp in campaign_rows):
            errors.append("Only ACTIVE or DRAFT campaigns allowed for this action.")

        if any(cmp.retailer_slug != campaign_rows[0].retailer_slug for cmp in campaign_rows):
            errors.append("Selected campaigns must belong to the same retailer.")

        if any(cmp.loyalty_type != campaign_rows[0].loyalty_type for cmp in campaign_rows):
            errors.append("Selected campaigns must have the same loyalty type.")

        return errors

    def _get_campaign_rows(self, selected_campaigns_ids: list[str]) -> list["Row"]:
        return self.db_session.execute(
            select(
                Campaign.id,
                Campaign.slug,
                Campaign.loyalty_type,
                Campaign.status,
                Retailer.slug.label("retailer_slug"),
            )
            .select_from(Campaign)
            .join(Retailer)
            .where(
                Campaign.id.in_([int(campaigns_id) for campaigns_id in selected_campaigns_ids]),
            )
        ).all()

    def validate_selected_campaigns(self, selected_campaigns_ids: list[str]) -> None:
        campaign_rows = self._get_campaign_rows(selected_campaigns_ids)
        errors = self._check_retailer_and_status(campaign_rows)
        active_campaign, draft_campaign, other_errors = self._get_and_validate_campaigns(campaign_rows)
        errors += other_errors

        if errors or active_campaign is None:
            for error in errors:
                flash(error, category="error")

            raise ValueError("failed validation")

        self._session_form_data = SessionFormData(
            retailer_slug=campaign_rows[0].retailer_slug,
            active_campaign=active_campaign,
            draft_campaign=draft_campaign,
            optional_fields_needed=draft_campaign is not None,
        )

    def update_form(self, form_dynamic_values: str) -> None:
        if not self._session_form_data:
            self._session_form_data = SessionFormData.from_base64_str(form_dynamic_values)

        self.form.handle_pending_rewards.choices = PendingRewardMigrationActions.get_choices(
            self._session_form_data.optional_fields_needed
        )
        if not self._session_form_data.optional_fields_needed:

            for field_name in self.form_optional_fields:
                delattr(self.form, field_name)

    def end_campaigns(self, status_change_fn: Callable, migration_fn: Callable) -> None:
        transfer_requested = (self.form.transfer_balance and self.form.transfer_balance.data) or (
            self.form.handle_pending_rewards.data == PendingRewardMigrationActions.TRANSFER
        )

        if not self.session_form_data.draft_campaign and transfer_requested:
            raise ValueError("unexpected: no draft campaign found")

        if self.session_form_data.draft_campaign:
            migration_fn(
                to_campaign_slug=self.session_form_data.draft_campaign.campaign_slug,
                from_campaign_slug=self.session_form_data.active_campaign.campaign_slug,
                retailer_slug=self.session_form_data.retailer_slug,
                pending_reward_action=self.form.handle_pending_rewards.data,
                transfer_balance=self.form.transfer_balance.data,
                conversion_rate=self.form.convert_rate.data,
                qualifying_threshold=self.form.qualify_threshold.data,
            )

        else:
            status_change_fn(
                campaign_slug=self.session_form_data.active_campaign.campaign_slug,
                retailer_slug=self.session_form_data.retailer_slug,
                requested_status=CampaignStatuses.ENDED,
                pending_reward_action=self.form.handle_pending_rewards.data,
            )
