import asyncio

from typing import TYPE_CHECKING
from uuid import UUID

from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession

from cosmos.core.activity.enums import ActivityType
from cosmos.core.activity.tasks import async_send_activity
from cosmos.core.api.crud import commit, get_reward
from cosmos.core.api.service import ServiceException, ServiceResult
from cosmos.core.error_codes import ErrorCode
from cosmos.public_api.api import crud
from cosmos.public_api.api.metrics import invalid_marketing_opt_out, microsite_reward_requests
from cosmos.retailers.crud import get_retailer_by_slug

if TYPE_CHECKING:
    from cosmos.db.models import Reward  # pragma: no cover

RESPONSE_TEMPLATE = """
<!DOCTYPE HTML>
<html lang="en">
    <head>
        <title>Marketing opt out</title>
    </head>
    <body>
        <p>{msg}</p>
    </body>
</html>
"""


class PublicService:
    def __init__(self, db_session: "AsyncSession", retailer_slug: str) -> None:
        self.db_session = db_session
        self.retailer_slug = retailer_slug

    async def handle_marketing_unsubscribe(self, u: str | None) -> ServiceResult[str, ServiceException]:
        msg = "You have opted out of any further marketing"
        if u:
            try:
                opt_out_uuid = UUID(u)
            except ValueError:
                invalid_marketing_opt_out.labels(unknown_retailer=False, invalid_token=True).inc()
                html_resp = RESPONSE_TEMPLATE.format(msg=msg)
                return ServiceResult(value=html_resp)

            data = await crud.get_account_holder_and_retailer_data_by_opt_out_token(
                self.db_session, opt_out_uuid=opt_out_uuid
            )
            if data is None:
                invalid_marketing_opt_out.labels(unknown_retailer=False, invalid_token=True).inc()
            elif data.retailer_slug != self.retailer_slug:
                invalid_marketing_opt_out.labels(unknown_retailer=True, invalid_token=False).inc()
            else:
                updates = await crud.update_boolean_marketing_preferences(
                    self.db_session, account_holder_id=data.account_holder_id
                )
                await commit(self.db_session)
                msg += f" for {data.retailer_name}"

                for (pref_name, updated_at) in updates:
                    activity_payload = ActivityType.get_marketing_preference_change_activity_data(
                        account_holder_uuid=data.account_holder_uuid,
                        retailer_slug=data.retailer_slug,
                        field_name=pref_name,
                        activity_datetime=updated_at,
                        summary="Unsubscribed via marketing opt-out",
                        associated_value="Marketing Preferences unsubscribed",
                        original_value="True",
                        new_value="False",
                    )
                    asyncio.create_task(
                        async_send_activity(activity_payload, routing_key=ActivityType.ACCOUNT_CHANGE.value)
                    )

        else:
            invalid_marketing_opt_out.labels(unknown_retailer=False, invalid_token=True).inc()

        html_resp = RESPONSE_TEMPLATE.format(msg=msg)
        return ServiceResult(value=html_resp)

    async def handle_get_reward_for_microsite(self, reward_uuid: str) -> ServiceResult["Reward", ServiceException]:
        try:
            valid_reward_uuid = UUID(reward_uuid)
        except ValueError:
            microsite_reward_requests.labels(
                response_status=status.HTTP_404_NOT_FOUND,
                unknown_retailer=False,
                invalid_reward_uuid=True,
            ).inc()
            return ServiceResult(error=ServiceException(error_code=ErrorCode.INVALID_REQUEST))

        if retailer := await get_retailer_by_slug(self.db_session, retailer_slug=self.retailer_slug):
            if reward := await get_reward(self.db_session, reward_uuid=valid_reward_uuid, retailer_id=retailer.id):
                microsite_reward_requests.labels(
                    response_status=status.HTTP_200_OK, unknown_retailer=False, invalid_reward_uuid=False
                ).inc()
                return ServiceResult(reward)
            return ServiceResult(error=ServiceException(error_code=ErrorCode.NO_REWARD_FOUND))

        microsite_reward_requests.labels(
            response_status=status.HTTP_404_NOT_FOUND,
            unknown_retailer=retailer is None,
            invalid_reward_uuid=retailer is not None,
        ).inc()
        return ServiceResult(error=ServiceException(error_code=ErrorCode.INVALID_REQUEST))
