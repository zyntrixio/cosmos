from datetime import date, datetime
from enum import Enum
from uuid import UUID

from cosmos.accounts.activity.schemas import (
    AccountEventSchema,
    AccountRequestSchema,
    BalanceChangeDataSchema,
    BalanceResetWholeDataSchema,
    MarketingPreferenceChangeSchema,
)
from cosmos.accounts.config import account_settings
from cosmos.core.activity.enums import ActivityTypeMixin


class ActivityType(ActivityTypeMixin, Enum):
    ACCOUNT_REQUEST = f"activity.{account_settings.core.PROJECT_NAME}.account.request"
    ACCOUNT_VIEW = f"activity.{account_settings.core.PROJECT_NAME}.account.view"
    ACCOUNT_AUTHENTICATION = f"activity.{account_settings.core.PROJECT_NAME}.account.authentication"
    ACCOUNT_ENROLMENT = f"activity.{account_settings.core.PROJECT_NAME}.account.enrolment"
    ACCOUNT_CHANGE = f"activity.{account_settings.core.PROJECT_NAME}.account.change"
    BALANCE_CHANGE = f"activity.{account_settings.core.PROJECT_NAME}.balance.change"

    @classmethod
    def get_account_request_activity_data(
        cls,
        *,
        activity_datetime: datetime,
        retailer_slug: str,
        channel: str,
        request_data: dict,
        retailer_profile_config: dict,
        result: str | None,
    ) -> dict:
        fields = [
            {"field_name": k, "value": str(v)}
            for k, v in request_data["credentials"].items()
            if k in retailer_profile_config
        ]
        if marketing_prefs := request_data.get("marketing_preferences", []):
            fields.extend([{"field_name": pref["key"], "value": pref["value"]} for pref in marketing_prefs])
        email = request_data["credentials"]["email"]
        return cls._assemble_payload(
            ActivityType.ACCOUNT_REQUEST.name,
            user_id=str(request_data["third_party_identifier"]),
            underlying_datetime=activity_datetime,
            summary=f"Enrolment Requested for {email}",
            associated_value=email,
            retailer_slug=retailer_slug,
            data=AccountRequestSchema(datetime=activity_datetime, channel=channel, fields=fields, result=result).dict(),
        )

    @classmethod
    def get_account_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        activity_datetime: datetime,
        retailer_slug: str,
        channel: str,
        campaign_slugs: list[str],
    ) -> dict:
        return cls._assemble_payload(
            ActivityType.ACCOUNT_VIEW.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            summary="Account viewed",
            reasons=["/accounts call made"],
            associated_value=channel,
            retailer_slug=retailer_slug,
            campaigns=campaign_slugs,
            data={},
        )

    @classmethod
    def get_account_auth_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        activity_datetime: datetime,
        retailer_slug: str,
        channel: str,
    ) -> dict:
        return cls._assemble_payload(
            ActivityType.ACCOUNT_AUTHENTICATION.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            summary=f"Account added to {channel}",
            associated_value=channel,
            retailer_slug=retailer_slug,
            data=AccountEventSchema(datetime=activity_datetime, channel=channel).dict(),
        )

    @classmethod
    def get_account_enrolment_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        activity_datetime: datetime,
        retailer_slug: str,
        channel: str,
        third_party_identifier: str,
    ) -> dict:
        return cls._assemble_payload(
            ActivityType.ACCOUNT_ENROLMENT.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            summary=f"Joined via {channel}; Account activated",
            reasons=[f"Third Party Identifier: {third_party_identifier}"],
            activity_identifier=third_party_identifier,
            associated_value=channel,
            retailer_slug=retailer_slug,
            data=AccountEventSchema(datetime=activity_datetime, channel=channel).dict(),
        )

    @classmethod
    def get_marketing_preference_change_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        retailer_slug: str,
        activity_datetime: datetime,
        summary: str,
        associated_value: str,
        field_name: str,
        original_value: str,
        new_value: str,
    ) -> dict:
        return cls._assemble_payload(
            ActivityType.ACCOUNT_CHANGE.name,
            underlying_datetime=activity_datetime,
            user_id=account_holder_uuid,
            retailer_slug=retailer_slug,
            summary=summary,
            associated_value=associated_value,
            data=MarketingPreferenceChangeSchema(
                field_name=field_name, original_value=original_value, new_value=new_value
            ).dict(),
        )

    @classmethod
    def get_balance_change_activity_data(
        cls,
        *,
        account_holder_uuid: UUID | str,
        retailer_slug: str,
        summary: str,
        new_balance: int,
        campaigns: list[str],
        reason: str,
        activity_datetime: datetime,
        original_balance: int,
    ) -> dict:

        return cls._assemble_payload(
            ActivityType.BALANCE_CHANGE.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            activity_identifier="N/A",
            summary=summary,
            reasons=[reason],
            associated_value=str(new_balance),
            retailer_slug=retailer_slug,
            campaigns=campaigns,
            data=BalanceChangeDataSchema(new_balance=new_balance, original_balance=original_balance).dict(
                exclude_unset=True
            ),
        )

    @classmethod
    def get_balance_reset_activity_data(
        cls,
        *,
        reset_date: date,
        activity_datetime: datetime,
        underlying_datetime: datetime,
        retailer_slug: str,
        balance_lifespan: int,
        campaign_slug: str,
        old_balance: int,
        account_holder_uuid: str,
    ) -> dict:
        return BalanceResetWholeDataSchema(
            type=ActivityType.BALANCE_CHANGE.name,
            datetime=activity_datetime,
            underlying_datetime=underlying_datetime,
            summary=f"{retailer_slug} {campaign_slug} Balance {old_balance}",
            reasons=[f"Balance Reset every {balance_lifespan} days"],
            activity_identifier="N/A",
            user_id=account_holder_uuid,
            associated_value=0,
            retailer=retailer_slug,
            campaigns=[campaign_slug],
            data={
                "reset_date": reset_date,
                "new_balance": 0,
                "original_balance": old_balance,
            },
        ).dict()
