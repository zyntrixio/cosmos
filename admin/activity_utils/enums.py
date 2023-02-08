from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Literal

from cosmos_message_lib.schemas import utc_datetime

from admin.activity_utils.schemas import (
    ActivitySchema,
    BalanceChangeWholeActivitySchema,
    CampaignCreatedActivitySchema,
    CampaignDeletedActivitySchema,
    CampaignMigrationActivitySchema,
    CampaignUpdatedActivitySchema,
    EarnRuleCreatedActivitySchema,
    EarnRuleDeletedActivitySchema,
    EarnRuleUpdatedActivitySchema,
    RetailerCreatedActivitySchema,
    RetailerDeletedActivitySchema,
    RetailerStatusUpdateActivitySchema,
    RetailerUpdateActivitySchema,
    RewardRuleCreatedActivitySchema,
    RewardRuleDeletedActivitySchema,
    RewardRuleUpdatedActivitySchema,
    RewardStatusWholeActivitySchema,
)
from admin.config import admin_settings
from cosmos.core.utils import pence_integer_to_currency_string


class ActivityType(Enum):
    CAMPAIGN = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.campaign.change"
    EARN_RULE = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.earn_rule.change"
    REWARD_RULE = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.reward_rule.change"
    BALANCE_CHANGE = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.balance.change"
    CAMPAIGN_MIGRATION = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.campaign.migration"
    RETAILER_CREATED = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.retailer.created"
    RETAILER_CHANGED = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.retailer.changed"
    RETAILER_DELETED = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.retailer.deleted"
    RETAILER_STATUS = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.retailer.status"
    REWARD_STATUS = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.reward.status"
    ACCOUNT_DELETED = f"activity.{admin_settings.ADMIN_PROJECT_NAME}.account.deleted"

    @classmethod
    def get_campaign_created_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        loyalty_type: str,
        start_date: utc_datetime | None = None,
        end_date: utc_datetime | None = None,
    ) -> dict:

        return {
            "type": cls.CAMPAIGN.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} created",
            "reasons": [],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": CampaignCreatedActivitySchema(
                campaign={
                    "new_values": {
                        "name": campaign_name,
                        "slug": campaign_slug,
                        "status": "draft",
                        "loyalty_type": loyalty_type.title(),
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_campaign_updated_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        new_values: dict,
        original_values: dict,
    ) -> dict:

        return {
            "type": cls.CAMPAIGN.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} changed",
            "reasons": ["Updated"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": CampaignUpdatedActivitySchema(
                campaign={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_campaign_deleted_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        loyalty_type: str,
        start_date: utc_datetime | None = None,
        end_date: utc_datetime | None = None,
    ) -> dict:

        return {
            "type": cls.CAMPAIGN.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} deleted",
            "reasons": ["Deleted"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": CampaignDeletedActivitySchema(
                campaign={
                    "original_values": {
                        "retailer": retailer_slug,
                        "name": campaign_name,
                        "slug": campaign_slug,
                        "loyalty_type": loyalty_type.title(),
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_earn_rule_created_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        loyalty_type: Literal["STAMPS", "ACCUMULATOR"],
        threshold: int,
        increment: int,
        increment_multiplier: Decimal,
        max_amount: int,
    ) -> dict:
        new_values = {"threshold": threshold, "increment_multiplier": increment_multiplier}
        if max_amount:
            new_values["max_amount"] = max_amount
        if loyalty_type == "STAMPS":
            new_values["increment"] = increment

        return {
            "type": cls.EARN_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Earn Rule created",
            "reasons": ["Created"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": EarnRuleCreatedActivitySchema(
                loyalty_type=loyalty_type,
                earn_rule={"new_values": new_values},
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_earn_rule_updated_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        new_values: dict,
        original_values: dict,
    ) -> dict:

        return {
            "type": cls.EARN_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Earn Rule changed",
            "reasons": ["Updated"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": EarnRuleUpdatedActivitySchema(
                earn_rule={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_earn_rule_deleted_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        threshold: int,
        increment: int,
        increment_multiplier: Decimal,
        max_amount: int,
    ) -> dict:

        return {
            "type": cls.EARN_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Earn Rule removed",
            "reasons": ["Deleted"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": EarnRuleDeletedActivitySchema(
                earn_rule={
                    "original_values": {
                        "threshold": threshold,
                        "increment": increment,
                        "increment_multiplier": increment_multiplier,
                        "max_amount": max_amount,
                    }
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_reward_rule_created_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        reward_goal: int,
        refund_window: int,
        reward_cap: int | None,
    ) -> dict:

        return {
            "type": cls.REWARD_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Reward Rule created",
            "reasons": ["Created"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": RewardRuleCreatedActivitySchema(
                reward_rule={
                    "new_values": {
                        "campaign_slug": campaign_slug,
                        "reward_goal": reward_goal,
                        "refund_window": refund_window,
                        "reward_cap": reward_cap,
                    }
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_balance_change_activity_data(
        cls,
        *,
        retailer_slug: str,
        from_campaign_slug: str,
        to_campaign_slug: str,
        account_holder_uuid: str,
        activity_datetime: datetime,
        new_balance: int,
        loyalty_type: str,
    ) -> dict:

        match loyalty_type:  # noqa: E999
            case "STAMPS":
                stamp_balance = new_balance // 100
                associated_value = f"{stamp_balance} stamp" + ("s" if stamp_balance != 1 else "")
            case "ACCUMULATOR":
                associated_value = pence_integer_to_currency_string(new_balance, "GBP")
            case _:
                raise ValueError(f"Unexpected value {loyalty_type} for loyalty_type.")

        return BalanceChangeWholeActivitySchema(
            type=cls.BALANCE_CHANGE.name,
            datetime=datetime.now(tz=timezone.utc),
            underlying_datetime=activity_datetime,
            summary=f"{retailer_slug} {to_campaign_slug} Balance {associated_value}",
            reasons=[f"Migrated from ended campaign {from_campaign_slug}"],
            activity_identifier="N/A",
            user_id=account_holder_uuid,
            associated_value=associated_value,
            retailer=retailer_slug,
            campaigns=[to_campaign_slug],
            data={
                "loyalty_type": loyalty_type,
                "new_balance": new_balance,
                "original_balance": 0,
            },
        ).dict()

    @classmethod
    def get_campaign_migration_activity_data(
        cls,
        *,
        retailer_slug: str,
        from_campaign_slug: str,
        to_campaign_slug: str,
        sso_username: str,
        activity_datetime: datetime,
        balance_conversion_rate: int,
        qualify_threshold: int,
        pending_rewards: str,
        transfer_balance_requested: bool,
    ) -> dict:

        return {
            "type": cls.CAMPAIGN_MIGRATION.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": (
                f"{retailer_slug} Campaign {from_campaign_slug} has ended"
                f" and account holders have been migrated to Campaign {to_campaign_slug}"
            ),
            "reasons": [f"Campaign {from_campaign_slug} was ended"],
            "activity_identifier": retailer_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [from_campaign_slug, to_campaign_slug],
            "data": CampaignMigrationActivitySchema(
                transfer_balance_requested=transfer_balance_requested,
                ended_campaign=from_campaign_slug,
                activated_campaign=to_campaign_slug,
                balance_conversion_rate=balance_conversion_rate,
                qualify_threshold=qualify_threshold,
                pending_rewards=pending_rewards,
            ).dict(),
        }

    @classmethod
    def get_reward_rule_updated_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        new_values: dict,
        original_values: dict,
    ) -> dict:

        return {
            "type": cls.REWARD_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Reward Rule changed",
            "reasons": ["Updated"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": RewardRuleUpdatedActivitySchema(
                reward_rule={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        }

    @classmethod
    def get_reward_rule_deleted_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        reward_goal: int,
        refund_window: int,
        reward_cap: int | None,
    ) -> dict:

        return {
            "type": cls.REWARD_RULE.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{campaign_name} Reward Rule deleted",
            "reasons": ["Deleted"],
            "activity_identifier": campaign_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [campaign_slug],
            "data": RewardRuleDeletedActivitySchema(
                reward_rule={
                    "original_values": {
                        "campaign_slug": campaign_slug,
                        "reward_goal": reward_goal,
                        "refund_window": refund_window,
                        "reward_cap": reward_cap,
                    },
                }
            ).dict(exclude_unset=True, exclude_none=True),
        }

    @classmethod
    def get_retailer_created_activity_data(
        cls,
        *,
        sso_username: str,
        activity_datetime: datetime,
        status: str,
        retailer_name: str,
        retailer_slug: str,
        account_number_prefix: str,
        enrolment_config: dict,
        marketing_preferences: dict | None,
        loyalty_name: str,
        balance_lifespan: int,
        # balance_reset_advanced_warning_days: int,
    ) -> dict:
        enrolment_config_data = [{"key": k, **v} for k, v in enrolment_config.items()]

        if marketing_preferences:
            marketing_pref_data = [{"key": k, **v} for k, v in marketing_preferences.items()]

        return {
            "type": cls.RETAILER_CREATED.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{retailer_name} retailer created",
            "reasons": ["Created"],
            "activity_identifier": retailer_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [],
            "data": {
                "retailer": RetailerCreatedActivitySchema(
                    new_values={
                        "status": status,
                        "name": retailer_name,
                        "slug": retailer_slug,
                        "account_number_prefix": account_number_prefix,
                        "enrolment_config": enrolment_config_data,
                        "marketing_preference_config": marketing_pref_data if marketing_preferences else None,
                        "loyalty_name": loyalty_name,
                        "balance_lifespan": balance_lifespan,
                        # "balance_reset_advanced_warning_days": balance_reset_advanced_warning_days,
                    }
                ).dict(exclude_unset=True, exclude_none=True),
            },
        }

    @classmethod
    def get_retailer_update_activity_data(
        cls,
        *,
        sso_username: str,
        activity_datetime: datetime,
        retailer_name: str,
        retailer_slug: str,
        new_values: dict,
        original_values: dict,
    ) -> dict:
        if "profile_config" in new_values:
            new_values["enrolment_config"] = new_values.pop("profile_config")
            original_values["enrolment_config"] = original_values.pop("profile_config")

        return {
            "type": cls.RETAILER_CHANGED.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{retailer_name} changed",
            "reasons": ["Updated"],
            "activity_identifier": retailer_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [],
            "data": {
                "retailer": RetailerUpdateActivitySchema(
                    new_values=new_values,
                    original_values=original_values,
                ).dict(exclude_unset=True, exclude_none=True),
            },
        }

    @classmethod
    def get_retailer_deletion_activity_data(
        cls,
        *,
        sso_username: str,
        activity_datetime: datetime,
        retailer_name: str,
        retailer_slug: str,
        original_values: dict,
    ) -> dict:

        return {
            "type": cls.RETAILER_DELETED.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{retailer_name} deleted",
            "reasons": ["Deleted"],
            "activity_identifier": retailer_slug,
            "user_id": sso_username,
            "associated_value": "N/A",
            "retailer": retailer_slug,
            "campaigns": [],
            "data": {
                "retailer": RetailerDeletedActivitySchema(
                    original_values=original_values,
                ).dict(exclude_unset=True, exclude_none=True),
            },
        }

    @classmethod
    def get_retailer_status_update_activity_data(
        cls,
        *,
        sso_username: str,
        activity_datetime: datetime,
        new_status: str,
        original_status: str,
        retailer_name: str,
        retailer_slug: str,
    ) -> dict:

        return {
            "type": cls.RETAILER_STATUS.name,
            "datetime": datetime.now(tz=timezone.utc),
            "underlying_datetime": activity_datetime,
            "summary": f"{retailer_name} status is {new_status}",
            "reasons": ["Updated"],
            "activity_identifier": retailer_slug,
            "user_id": sso_username,
            "associated_value": new_status,
            "retailer": retailer_slug,
            "campaigns": [],
            "data": {
                "retailer": RetailerStatusUpdateActivitySchema(
                    new_values={"status": new_status},
                    original_values={"status": original_status},
                ).dict(),
            },
        }

    @classmethod
    def get_reward_status_activity_data(
        cls,
        *,
        retailer_slug: str,
        from_campaign_slug: str,
        to_campaign_slug: str,
        account_holder_uuid: str,
        activity_datetime: datetime,
        pending_reward_uuid: str,
    ) -> dict:

        return RewardStatusWholeActivitySchema(
            type=cls.REWARD_STATUS.name,
            datetime=datetime.now(tz=timezone.utc),
            underlying_datetime=activity_datetime,
            summary=f"{retailer_slug} pending reward transferred from {from_campaign_slug} to {to_campaign_slug}",
            reasons=["Pending reward transferred at campaign end"],
            activity_identifier=pending_reward_uuid,
            user_id=account_holder_uuid,
            associated_value="N/A",
            retailer=retailer_slug,
            campaigns=[from_campaign_slug, to_campaign_slug],
            data={
                "new_campaign": to_campaign_slug,
                "old_campaign": from_campaign_slug,
            },
        ).dict()

    @classmethod
    def get_account_holder_deleted_activity_data(
        cls,
        *,
        activity_datetime: datetime,
        account_holder_uuid: str,
        retailer_name: str,
        retailer_status: str,
        retailer_slug: str,
        sso_username: str,
    ) -> dict:

        return ActivitySchema(
            type=cls.ACCOUNT_DELETED.name,
            datetime=datetime.now(tz=timezone.utc),
            underlying_datetime=activity_datetime,
            summary=f"Account holder deleted for {retailer_name}",
            reasons=["Deleted"],
            activity_identifier=account_holder_uuid,
            user_id=sso_username,
            associated_value=retailer_status,
            retailer=retailer_slug,
            campaigns=[],
            data={},
        ).dict()
