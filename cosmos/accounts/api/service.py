import asyncio

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pydantic
import yaml

from cosmos.accounts.api import crud
from cosmos.accounts.enums import MarketingPreferenceValueTypes
from cosmos.accounts.schemas import AccountHolderEnrolment, GetAccountHolderByCredentials, MarketingPreference
from cosmos.core.activity.enums import ActivityType
from cosmos.core.activity.tasks import async_send_activity
from cosmos.core.api.crud import commit, create_retry_task
from cosmos.core.api.exceptions import RequestPayloadValidationError
from cosmos.core.api.http_error import HttpErrors
from cosmos.core.api.service_result import ServiceResult
from cosmos.core.config import settings
from cosmos.core.exception_handlers import FIELD_VALIDATION_ERROR
from cosmos.db.models import Retailer
from cosmos.retailers.enums import EmailTemplateTypes
from cosmos.retailers.schemas import (
    retailer_marketing_info_validation_factory,
    retailer_profile_info_validation_factory,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class AccountService:
    def __init__(self, db_session: "AsyncSession", retailer: Retailer):
        self.db_session = db_session
        self.retailer = retailer

    def _validate_profile_data(self, profile_data: dict, retailer_profile_config: dict) -> dict:
        ProfileConfigSchema = retailer_profile_info_validation_factory(  # pylint: disable=invalid-name
            retailer_profile_config
        )
        return ProfileConfigSchema(**profile_data).dict(exclude_unset=True)

    def _process_and_validate_marketing_data(
        self, marketing_prefs: list[MarketingPreference], marketing_config_raw: str
    ) -> list[dict]:
        if marketing_config_raw == "":
            return []

        marketing_config = yaml.safe_load(marketing_config_raw)
        MarketingConfigSchema = retailer_marketing_info_validation_factory(  # pylint: disable=invalid-name
            marketing_config
        )
        validated_marketing_data = MarketingConfigSchema(**{mk.key: mk.value for mk in marketing_prefs}).dict(
            exclude_unset=False
        )

        marketing_preferences = []
        for k, v in validated_marketing_data.items():
            value_type = MarketingPreferenceValueTypes[marketing_config[k]["type"].upper()]
            if value_type == MarketingPreferenceValueTypes.STRING_LIST:
                value = ", ".join(v)
            else:
                value = str(v)

            marketing_preferences.append({"key_name": k, "value": value, "value_type": value_type})

        return marketing_preferences

    async def handle_account_enrolment(self, request_payload: AccountHolderEnrolment, *, channel: str) -> ServiceResult:
        "Main handler for account holder enrolments"
        result = "Error"  # default - assume unhandled Error until we reach Accepted after successful commit
        try:
            retailer_profile_config = yaml.safe_load(self.retailer.profile_config)
            profile_data = self._validate_profile_data(request_payload.credentials, retailer_profile_config)
            marketing_preferences_data = self._process_and_validate_marketing_data(
                request_payload.marketing_preferences, self.retailer.marketing_preference_config
            )

            email = profile_data.pop("email").lower()
            account_holder = await crud.create_account_holder(
                self.db_session,
                email=email,
                retailer_id=self.retailer.id,
                profile_data=profile_data,
                marketing_preferences_data=marketing_preferences_data,
            )

            callback_task = await create_retry_task(
                self.db_session,
                task_type_name=settings.ENROLMENT_CALLBACK_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "callback_url": request_payload.callback_url,
                    "third_party_identifier": request_payload.third_party_identifier,
                },
            )
            welcome_email_task = await create_retry_task(
                self.db_session,
                task_type_name=settings.SEND_EMAIL_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "template_type": EmailTemplateTypes.WELCOME_EMAIL.name,
                    "retailer_id": self.retailer.id,
                },
            )
            welcome_email_task = await create_retry_task(
                self.db_session,
                task_type_name=settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
                params={
                    "account_holder_id": account_holder.id,
                    "callback_retry_task_id": callback_task.retry_task_id,
                    "welcome_email_retry_task_id": welcome_email_task.retry_task_id,
                    "third_party_identifier": request_payload.third_party_identifier,
                    "channel": channel,
                },
            )
        except pydantic.ValidationError as exc:
            result = FIELD_VALIDATION_ERROR
            return ServiceResult(RequestPayloadValidationError(validation_error=exc))
        except crud.AccountExists:
            result = "ACCOUNT_EXISTS"
            return ServiceResult(HttpErrors.ACCOUNT_EXISTS.value)
        else:
            await commit(self.db_session)
            result = "Accepted"
            return ServiceResult({})
        finally:
            activity_payload = ActivityType.get_account_request_activity_data(
                activity_datetime=datetime.now(tz=timezone.utc),
                retailer_slug=self.retailer.slug,
                channel=channel,
                result=result,
                request_data=request_payload.dict(exclude_unset=True),
                retailer_profile_config=retailer_profile_config,
            )
            asyncio.create_task(async_send_activity(activity_payload, routing_key=ActivityType.ACCOUNT_REQUEST.value))

    async def handle_account_auth(
        self, request_payload: GetAccountHolderByCredentials, *, tx_qty: int | None, channel: str
    ) -> ServiceResult:
        "Main handler for account auth"
        account_holder = await crud.get_account_holder(
            self.db_session,
            email=request_payload.email,
            retailer_id=self.retailer.id,
            account_number=request_payload.account_number,
            fetch_rewards=True,
            tx_qty=tx_qty,
            raise_404_if_inactive=True,
        )
        activity_payload = ActivityType.get_account_authentication_activity_data(
            account_holder_uuid=account_holder.account_holder_uuid,
            activity_datetime=datetime.now(tz=timezone.utc),
            retailer_slug=self.retailer.slug,
            channel=channel,
        )
        asyncio.create_task(
            async_send_activity(activity_payload, routing_key=ActivityType.ACCOUNT_AUTHENTICATION.value)
        )
        setattr(account_holder, "retailer_status", self.retailer.status)
        return account_holder
