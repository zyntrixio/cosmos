from datetime import datetime
from decimal import Decimal
from enum import Enum

from cosmos_message_lib.schemas import utc_datetime

from admin.activity_utils.schemas import (
    CampaignCreatedActivitySchema,
    CampaignDeletedActivitySchema,
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
)
from admin.config import admin_settings
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.activity.enums import ActivityTypeMixin


class ActivityType(ActivityTypeMixin, Enum):
    CAMPAIGN = f"activity.{admin_settings.core.PROJECT_NAME}.campaign.change"
    EARN_RULE = f"activity.{admin_settings.core.PROJECT_NAME}.earn_rule.change"
    REWARD_RULE = f"activity.{admin_settings.core.PROJECT_NAME}.reward_rule.change"
    RETAILER_CREATED = f"activity.{admin_settings.core.PROJECT_NAME}.retailer.created"
    RETAILER_CHANGED = f"activity.{admin_settings.core.PROJECT_NAME}.retailer.changed"
    RETAILER_DELETED = f"activity.{admin_settings.core.PROJECT_NAME}.retailer.deleted"
    RETAILER_STATUS = f"activity.{admin_settings.core.PROJECT_NAME}.retailer.status"
    REWARD_DELETED = f"activity.{admin_settings.core.PROJECT_NAME}.reward.deleted"
    ACCOUNT_DELETED = f"activity.{admin_settings.core.PROJECT_NAME}.account.deleted"

    @classmethod
    def get_campaign_created_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        loyalty_type: LoyaltyTypes,
        start_date: utc_datetime | None = None,
        end_date: utc_datetime | None = None,
    ) -> dict:

        return cls._assemble_payload(
            cls.CAMPAIGN.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} created",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=[],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=CampaignCreatedActivitySchema(
                campaign={
                    "new_values": {
                        "name": campaign_name,
                        "slug": campaign_slug,
                        "status": "draft",
                        "loyalty_type": loyalty_type.name,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                }
            ).dict(exclude_unset=True),
        )

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

        return cls._assemble_payload(
            cls.CAMPAIGN.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} changed",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=["Updated"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=CampaignUpdatedActivitySchema(
                campaign={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        )

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

        return cls._assemble_payload(
            cls.CAMPAIGN.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} deleted",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=["Deleted"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=CampaignDeletedActivitySchema(
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
        )

    @classmethod
    def get_earn_rule_created_activity_data(
        cls,
        *,
        retailer_slug: str,
        campaign_name: str,
        sso_username: str,
        activity_datetime: datetime,
        campaign_slug: str,
        loyalty_type: LoyaltyTypes,
        threshold: int,
        increment: int,
        increment_multiplier: Decimal,
        max_amount: int,
    ) -> dict:
        new_values = {"threshold": threshold, "increment_multiplier": increment_multiplier}
        if max_amount:
            new_values["max_amount"] = max_amount
        if loyalty_type == LoyaltyTypes.STAMPS:
            new_values["increment"] = increment

        return cls._assemble_payload(
            cls.EARN_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Earn Rule created",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=["Created"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=EarnRuleCreatedActivitySchema(
                loyalty_type=loyalty_type.name,
                earn_rule={"new_values": new_values},
            ).dict(exclude_unset=True),
        )

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

        return cls._assemble_payload(
            cls.EARN_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Earn Rule changed",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=["Updated"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=EarnRuleUpdatedActivitySchema(
                earn_rule={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        )

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

        return cls._assemble_payload(
            cls.EARN_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Earn Rule removed",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            data=EarnRuleDeletedActivitySchema(
                earn_rule={
                    "original_values": {
                        "threshold": threshold,
                        "increment": increment,
                        "increment_multiplier": increment_multiplier,
                        "max_amount": max_amount,
                    }
                }
            ).dict(exclude_unset=True),
            activity_identifier=campaign_slug,
            reasons=["Deleted"],
            campaigns=[campaign_slug],
            user_id=sso_username,
        )

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

        return cls._assemble_payload(
            cls.REWARD_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Reward Rule created",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            data=RewardRuleCreatedActivitySchema(
                reward_rule={
                    "new_values": {
                        "campaign_slug": campaign_slug,
                        "reward_goal": reward_goal,
                        "refund_window": refund_window,
                        "reward_cap": reward_cap,
                    }
                }
            ).dict(exclude_unset=True),
            activity_identifier=campaign_slug,
            reasons=["Created"],
            campaigns=[campaign_slug],
            user_id=sso_username,
        )

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

        return cls._assemble_payload(
            cls.REWARD_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Reward Rule changed",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=campaign_slug,
            reasons=["Updated"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            data=RewardRuleUpdatedActivitySchema(
                reward_rule={
                    "new_values": new_values,
                    "original_values": original_values,
                }
            ).dict(exclude_unset=True),
        )

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
        refund_window: int | None,
        reward_cap: int | None,
    ) -> dict:

        return cls._assemble_payload(
            cls.REWARD_RULE.name,
            underlying_datetime=activity_datetime,
            summary=f"{campaign_name} Reward Rule deleted",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            reasons=["Deleted"],
            campaigns=[campaign_slug],
            user_id=sso_username,
            activity_identifier=campaign_slug,
            data=RewardRuleDeletedActivitySchema(
                reward_rule={
                    "original_values": {
                        "campaign_slug": campaign_slug,
                        "reward_goal": reward_goal,
                        "refund_window": refund_window,
                        "reward_cap": reward_cap,
                    },
                }
            ).dict(exclude_unset=True),
        )

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
        balance_lifespan: int | None,
        balance_reset_advanced_warning_days: int | None,
    ) -> dict:
        enrolment_config_data = [{"key": k, **v} for k, v in enrolment_config.items()]

        if marketing_preferences:
            marketing_pref_data = [{"key": k, **v} for k, v in marketing_preferences.items()]

        return cls._assemble_payload(
            cls.RETAILER_CREATED.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_name} retailer created",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=retailer_slug,
            reasons=["Created"],
            campaigns=[],
            user_id=sso_username,
            data={
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
                        "balance_reset_advanced_warning_days": balance_reset_advanced_warning_days,
                    }
                ).dict(exclude_unset=True, exclude_none=True),
            },
        )

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

        return cls._assemble_payload(
            cls.RETAILER_CHANGED.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_name} changed",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            activity_identifier=retailer_slug,
            reasons=["Updated"],
            campaigns=[],
            user_id=sso_username,
            data={
                "retailer": RetailerUpdateActivitySchema(
                    new_values=new_values,
                    original_values=original_values,
                ).dict(exclude_unset=True, exclude_none=True),
            },
        )

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

        return cls._assemble_payload(
            cls.RETAILER_DELETED.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_name} deleted",
            associated_value="N/A",
            retailer_slug=retailer_slug,
            data={
                "retailer": RetailerDeletedActivitySchema(
                    original_values=original_values,
                ).dict(exclude_unset=True, exclude_none=True),
            },
            activity_identifier=retailer_slug,
            reasons=["Deleted"],
            campaigns=[],
            user_id=sso_username,
        )

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

        return cls._assemble_payload(
            cls.RETAILER_STATUS.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_name} status is {new_status}",
            associated_value=new_status,
            retailer_slug=retailer_slug,
            activity_identifier=retailer_slug,
            reasons=["Updated"],
            campaigns=[],
            user_id=sso_username,
            data={
                "retailer": RetailerStatusUpdateActivitySchema(
                    new_values={"status": new_status},
                    original_values={"status": original_status},
                ).dict()
            },
        )

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

        return cls._assemble_payload(
            cls.ACCOUNT_DELETED.name,
            underlying_datetime=activity_datetime,
            summary=f"Account holder deleted for {retailer_name}",
            associated_value=retailer_status,
            retailer_slug=retailer_slug,
            data={},
            activity_identifier=account_holder_uuid,
            reasons=["Deleted"],
            campaigns=[],
            user_id=sso_username,
        )

    @classmethod
    def get_reward_deleted_activity_data(
        cls,
        *,
        activity_datetime: datetime,
        retailer_name: str,
        retailer_slug: str,
        sso_username: str,
        rewards_deleted_count: int,
    ) -> dict:

        return cls._assemble_payload(
            cls.REWARD_DELETED.name,
            underlying_datetime=activity_datetime,
            summary=f"{retailer_name} reward(s) deleted",
            associated_value="Deleted",
            retailer_slug=retailer_slug,
            data={"rewards_deleted": rewards_deleted_count},
            activity_identifier="N/A",
            reasons=["Reward(s) deleted"],
            campaigns=[],
            user_id=sso_username,
        )
