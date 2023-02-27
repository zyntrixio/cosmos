from datetime import datetime, timezone
from typing import TYPE_CHECKING

import yaml

from pydantic import UUID4, ValidationError
from retry_tasks_lib.utils.asynchronous import async_create_task

from cosmos.accounts.activity.enums import ActivityType as AccountsActivityType
from cosmos.accounts.api import crud
from cosmos.accounts.api.schemas import (
    AccountHolderEnrolment,
    AccountHolderStatuses,
    GetAccountHolderByCredentials,
    MarketingPreference,
)
from cosmos.accounts.config import account_settings
from cosmos.accounts.enums import MarketingPreferenceValueTypes
from cosmos.core.api.exceptions import RequestPayloadValidationError
from cosmos.core.api.service import Service, ServiceError, ServiceResult
from cosmos.core.api.tasks import enqueue_task
from cosmos.core.error_codes import ErrorCode
from cosmos.retailers.enums import EmailTemplateTypes
from cosmos.retailers.schemas import (
    retailer_marketing_info_validation_factory,
    retailer_profile_info_validation_factory,
)

if TYPE_CHECKING:
    from fastapi import Request

    from cosmos.db.models import AccountHolder, Retailer


class AccountService(Service):
    def _validate_profile_data(self, profile_data: dict, retailer_profile_config: dict) -> dict:
        ProfileConfigSchema = retailer_profile_info_validation_factory(retailer_profile_config)  # noqa N806
        return ProfileConfigSchema(**profile_data).dict(exclude_unset=True)

    def _process_and_validate_marketing_data(
        self, marketing_prefs: list[MarketingPreference], marketing_config_raw: str
    ) -> list[dict]:
        if not marketing_config_raw:
            return []

        marketing_config = yaml.safe_load(marketing_config_raw)
        MarketingConfigSchema = retailer_marketing_info_validation_factory(marketing_config)  # noqa N806
        validated_marketing_data = MarketingConfigSchema(**{mk.key: mk.value for mk in marketing_prefs}).dict(
            exclude_unset=False
        )

        marketing_preferences = []
        for k, v in validated_marketing_data.items():
            value_type = MarketingPreferenceValueTypes[marketing_config[k]["type"].upper()]
            value = ", ".join(v) if value_type == MarketingPreferenceValueTypes.STRING_LIST else str(v)
            marketing_preferences.append({"key_name": k, "value": value, "value_type": value_type})

        return marketing_preferences

    async def handle_account_enrolment(
        self, request_payload: AccountHolderEnrolment, *, channel: str
    ) -> ServiceResult[dict, Exception]:
        """Main handler for account holder enrolments"""
        result = "Error"  # default - assume unhandled Error until we reach Accepted after successful commit
        try:
            retailer_profile_config = yaml.safe_load(self.retailer.profile_config)
            profile_data = self._validate_profile_data(request_payload.credentials, retailer_profile_config)
            marketing_preferences_data = self._process_and_validate_marketing_data(
                request_payload.marketing_preferences, self.retailer.marketing_preference_config
            )

            email = profile_data.pop("email").lower()
            try:
                account_holder = await crud.create_account_holder(
                    self.db_session,
                    email=email,
                    retailer_id=self.retailer.id,
                    profile_data=profile_data,
                    marketing_preferences_data=marketing_preferences_data,
                )
            except crud.AccountExistsError:
                result = ErrorCode.ACCOUNT_EXISTS.name
                return ServiceResult(error=ServiceError(error_code=ErrorCode.ACCOUNT_EXISTS))

            callback_task = await async_create_task(
                self.db_session,
                task_type_name=account_settings.ENROLMENT_CALLBACK_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "callback_url": request_payload.callback_url,
                    "third_party_identifier": request_payload.third_party_identifier,
                },
            )
            welcome_email_task = await async_create_task(
                self.db_session,
                task_type_name=account_settings.SEND_EMAIL_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "template_type": EmailTemplateTypes.WELCOME_EMAIL.name,
                    "retailer_id": self.retailer.id,
                },
            )
            activation_task = await async_create_task(
                self.db_session,
                task_type_name=account_settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "callback_retry_task_id": callback_task.retry_task_id,
                    "welcome_email_retry_task_id": welcome_email_task.retry_task_id,
                    "third_party_identifier": request_payload.third_party_identifier,
                    "channel": channel,
                },
            )
        except ValidationError as exc:
            result = "FIELD_VALIDATION_ERROR"
            return ServiceResult(error=RequestPayloadValidationError(validation_error=exc))
        else:
            await self.commit_db_changes()
            result = "Accepted"
            await self.trigger_asyncio_task(enqueue_task(retry_task_id=activation_task.retry_task_id))
            return ServiceResult({})
        finally:
            account_request_activity_payload = {
                "activity_datetime": datetime.now(tz=timezone.utc),
                "retailer_slug": self.retailer.slug,
                "channel": channel,
                "result": result,
                "request_data": request_payload.dict(exclude_unset=True),
                "retailer_profile_config": retailer_profile_config,
            }
            await self.store_activity(
                activity_type=AccountsActivityType.ACCOUNT_REQUEST,
                payload_formatter_fn=AccountsActivityType.get_account_request_activity_data,
                formatter_kwargs=account_request_activity_payload,
            )
            await self.format_and_send_stored_activities()

    async def handle_account_auth(
        self, request_payload: GetAccountHolderByCredentials, *, tx_qty: int, channel: str
    ) -> ServiceResult["AccountHolder", ServiceError]:
        """Main handler for account auth"""
        account_holder = await crud.get_account_holder(
            self.db_session,
            retailer_id=self.retailer.id,
            fetch_rewards=True,
            fetch_balances=True,
            tx_qty=tx_qty,
            email=request_payload.email,
            account_number=request_payload.account_number,
        )
        if not account_holder or account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND))

        await self.store_activity(
            activity_type=AccountsActivityType.ACCOUNT_AUTHENTICATION,
            payload_formatter_fn=AccountsActivityType.get_account_auth_activity_data,
            formatter_kwargs={
                "account_holder_uuid": str(account_holder.account_holder_uuid),
                "activity_datetime": datetime.now(tz=timezone.utc),
                "retailer_slug": self.retailer.slug,
                "channel": channel,
            },
        )
        await self.format_and_send_stored_activities()
        return ServiceResult(account_holder)

    async def handle_get_account(
        self,
        *,
        account_holder_uuid: str | UUID4,
        retailer: "Retailer",
        request: "Request",
        tx_qty: int,
    ) -> ServiceResult["AccountHolder", ServiceError]:
        """Main handler for account data"""

        account_holder = await crud.get_account_holder(
            self.db_session,
            retailer_id=self.retailer.id,
            fetch_rewards=True,
            fetch_balances=True,
            tx_qty=tx_qty,
            account_holder_uuid=account_holder_uuid,
        )
        if not account_holder or account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND))

        await self.store_activity(
            activity_type=AccountsActivityType.ACCOUNT_VIEW,
            payload_formatter_fn=AccountsActivityType.get_account_activity_data,
            formatter_kwargs={
                "account_holder_uuid": str(account_holder_uuid),
                "activity_datetime": datetime.now(tz=timezone.utc),
                "retailer_slug": retailer.slug,
                "channel": request.headers.get("bpl-user-channel"),
                "campaign_slugs": {balance.campaign.slug for balance in account_holder.current_balances},
            },
        )
        await self.format_and_send_stored_activities()
        return ServiceResult(account_holder)
