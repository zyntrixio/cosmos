import asyncio

from datetime import datetime, timezone
from typing import Any, cast

import yaml

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.accounts.api.enums.http_error import HttpErrors
from cosmos.accounts.api.operations import enrol_account_holder
from cosmos.accounts.enums import MarketingPreferenceValueTypes
from cosmos.accounts.schemas import AccountHolderEnrolment
from cosmos.core.activity.enums import ActivityType
from cosmos.core.activity.tasks import async_send_activity
from cosmos.core.api.deps import RetailerDependency, get_session
from cosmos.core.api.tasks import enqueue_task
from cosmos.core.exception_handlers import FIELD_VALIDATION_ERROR
from cosmos.db.models import Retailer
from cosmos.retailers.schemas import (
    retailer_marketing_info_validation_factory,
    retailer_profile_info_validation_factory,
)

get_retailer = RetailerDependency(no_retailer_found_exc=HttpErrors.INVALID_RETAILER.value)

router = APIRouter()


def _validate_profile_data(profile_data: dict, retailer_profile_config: dict) -> dict:
    ProfileConfigSchema = retailer_profile_info_validation_factory(  # pylint: disable=invalid-name
        retailer_profile_config
    )
    try:
        requested_by_retailer = ProfileConfigSchema(**profile_data).dict(exclude_unset=True)
    except ValidationError as exc:
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        )

    return requested_by_retailer


def _process_and_validate_marketing_data(marketing_data: dict, marketing_config_raw: str) -> list[dict]:
    if marketing_config_raw == "":
        return []

    marketing_config = yaml.safe_load(marketing_config_raw)
    MarketingConfigSchema = retailer_marketing_info_validation_factory(marketing_config)  # pylint: disable=invalid-name
    try:
        validated_marketing_data = MarketingConfigSchema(**{mk["key"]: mk["value"] for mk in marketing_data}).dict(
            exclude_unset=False
        )
    except ValidationError as exc:
        raise HTTPException(  # pylint: disable=raise-missing-from
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
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


@router.post(path="/{retailer_slug}/accounts/enrolment", status_code=status.HTTP_202_ACCEPTED)
async def endpoint_accounts_enrolment(
    payload: AccountHolderEnrolment,
    bpl_user_channel: str = Header(None),
    retailer: Retailer = Depends(get_retailer),
    db_session: AsyncSession = Depends(get_session),
) -> Any:
    data = payload.dict(exclude_unset=True)
    retailer_profile_config = yaml.safe_load(retailer.profile_config)

    result = None
    try:
        profile_data = _validate_profile_data(data["credentials"], retailer_profile_config)
        marketing_preferences_data = _process_and_validate_marketing_data(
            data["marketing_preferences"], retailer.marketing_preference_config
        )
        email = profile_data.pop("email").lower()

        _, activation_retry_task = await enrol_account_holder(
            db_session=db_session,
            retailer_id=retailer.id,
            email=email,
            callback_url=data["callback_url"],
            third_party_identifier=data["third_party_identifier"],
            profile_data=profile_data,
            marketing_preferences_data=marketing_preferences_data,
            channel=bpl_user_channel,
        )
        asyncio.create_task(enqueue_task(activation_retry_task.retry_task_id))
        result = "Accepted"
    except HTTPException as exc:
        result = FIELD_VALIDATION_ERROR if exc.status_code == 422 else cast(dict[str, str], exc.detail)["code"]
        raise
    finally:
        activity_payload = ActivityType.get_account_request_activity_data(
            activity_datetime=datetime.now(tz=timezone.utc),
            retailer_slug=retailer.slug,
            channel=bpl_user_channel,
            result=result,
            request_data=data,
            retailer_profile_config=retailer_profile_config,
        )
        asyncio.create_task(async_send_activity(activity_payload, routing_key=ActivityType.ACCOUNT_REQUEST.value))
    return {}
