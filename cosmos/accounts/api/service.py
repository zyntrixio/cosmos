import asyncio

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import yaml

from pydantic import UUID4, ValidationError

from cosmos.accounts.api import crud
from cosmos.accounts.api.schemas import (
    AccountHolderEnrolment,
    AccountHolderStatuses,
    AccountHolderUpdateStatusSchema,
    GetAccountHolderByCredentials,
    MarketingPreference,
)
from cosmos.accounts.enums import MarketingPreferenceValueTypes
from cosmos.core.activity.enums import ActivityType
from cosmos.core.activity.tasks import async_send_activity

# from cosmos.core.api.exception_handlers import FIELD_VALIDATION_ERROR
from cosmos.core.api.exceptions import RequestPayloadValidationError
from cosmos.core.api.service import Service, ServiceError, ServiceResult

# from cosmos.core.config import settings
from cosmos.core.error_codes import ErrorCode

# from cosmos.retailers.enums import EmailTemplateTypes
from cosmos.retailers.schemas import (
    retailer_marketing_info_validation_factory,
    retailer_profile_info_validation_factory,
)

if TYPE_CHECKING:
    from cosmos.db.models import AccountHolder


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
                account_holder = await crud.create_account_holder(  # noqa: F841 # remove me
                    self.db_session,
                    email=email,
                    retailer_id=self.retailer.id,
                    profile_data=profile_data,
                    marketing_preferences_data=marketing_preferences_data,
                )
            except crud.AccountExistsError:
                result = ErrorCode.ACCOUNT_EXISTS.name
                return ServiceResult(error=ServiceError(error_code=ErrorCode.ACCOUNT_EXISTS))

            # callback_task = await create_retry_task(
            #     self.db_session,
            #     task_type_name=settings.ENROLMENT_CALLBACK_TASK_NAME,
            #     params={
            #         "account_holder_id": account_holder.id,
            #         "callback_url": request_payload.callback_url,
            #         "third_party_identifier": request_payload.third_party_identifier,
            #     },
            # )
            # welcome_email_task = await create_retry_task(
            #     self.db_session,
            #     task_type_name=settings.SEND_EMAIL_TASK_NAME,
            #     params={
            #         "account_holder_id": account_holder.id,
            #         "template_type": EmailTemplateTypes.WELCOME_EMAIL.name,
            #         "retailer_id": self.retailer.id,
            #     },
            # )
            # welcome_email_task = await create_retry_task(
            #     self.db_session,
            #     task_type_name=settings.ACCOUNT_HOLDER_ACTIVATION_TASK_NAME,
            #     params={
            #         "account_holder_id": account_holder.id,
            #         "callback_retry_task_id": callback_task.retry_task_id,
            #         "welcome_email_retry_task_id": welcome_email_task.retry_task_id,
            #         "third_party_identifier": request_payload.third_party_identifier,
            #         "channel": channel,
            #     },
            # )
        except ValidationError as exc:
            result = "FIELD_VALIDATION_ERROR"
            return ServiceResult(error=RequestPayloadValidationError(validation_error=exc))
        else:
            await self.commit_db_changes()
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
        self, request_payload: GetAccountHolderByCredentials, *, tx_qty: int | None = 10, channel: str
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
        if not account_holder:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND))
        if account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.USER_NOT_ACTIVE))

        activity_payload = ActivityType.get_account_authentication_activity_data(
            account_holder_uuid=account_holder.account_holder_uuid,
            activity_datetime=datetime.now(tz=timezone.utc),
            retailer_slug=self.retailer.slug,
            channel=channel,
        )
        asyncio.create_task(
            async_send_activity(activity_payload, routing_key=ActivityType.ACCOUNT_AUTHENTICATION.value)
        )
        setattr(account_holder, "retailer_status", self.retailer.status)  # noqa: B010
        return ServiceResult(account_holder)

    async def handle_get_account(
        self, *, account_holder_uuid: str | UUID4, tx_qty: int | None = 10, is_status_request: bool
    ) -> ServiceResult["AccountHolder", ServiceError]:
        """Main handler for account data"""

        fetch_extras = not is_status_request
        account_holder = await crud.get_account_holder(
            self.db_session,
            retailer_id=self.retailer.id,
            fetch_rewards=fetch_extras,
            fetch_balances=fetch_extras,
            tx_qty=tx_qty if fetch_extras else None,
            account_holder_uuid=account_holder_uuid,
        )
        if not account_holder:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND))
        if account_holder.status != AccountHolderStatuses.ACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.USER_NOT_ACTIVE))

        return ServiceResult(account_holder)

    async def handle_update_account_holder_status(
        self,
        *,
        account_holder_uuid: str | UUID4,
        request_payload: AccountHolderUpdateStatusSchema,  # noqa ARG002
    ) -> ServiceResult[dict, ServiceError]:
        """Handler for account holder status update"""
        account_holder = await crud.get_account_holder(
            self.db_session,
            retailer_id=self.retailer.id,
            account_holder_uuid=account_holder_uuid,
        )

        if not account_holder:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.NO_ACCOUNT_FOUND))
        if account_holder.status == AccountHolderStatuses.INACTIVE:
            return ServiceResult(error=ServiceError(error_code=ErrorCode.USER_NOT_ACTIVE))

        # TODO/FIXME: Needs implementation
        # account_anonymisation_retry_task = await crud.update_account_holder_status(
        #     self.db_session,
        #     account_holder=account_holder,
        #     retailer_id=self.retailer.id,
        #     status=request_payload.status,
        # )
        # asyncio.create_task(enqueue_task(account_anonymisation_retry_task.retry_task_id))

        return ServiceResult({})
