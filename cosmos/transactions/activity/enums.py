from enum import Enum
from typing import TYPE_CHECKING

from cosmos.core.activity.enums import ActivityTypeMixin
from cosmos.core.error_codes import ErrorCode
from cosmos.core.utils import pence_integer_to_currency_string
from cosmos.transactions.activity.schemas import (
    ProcessedTXEventSchema,
    RefundNotRecoupedDataSchema,
    TxImportEventSchema,
)
from cosmos.transactions.activity.utils import build_tx_history_earns, build_tx_history_reasons
from cosmos.transactions.config import tx_settings

if TYPE_CHECKING:
    from datetime import datetime
    from uuid import UUID

    from pydantic import NonNegativeInt

    from cosmos.db.models import Retailer, Transaction
    from cosmos.transactions.api.service import AdjustmentAmount


class TxImportReasons(Enum):
    REFUNDS_NOT_SUPPORTED = "Refunds not supported"
    NO_ACTIVE_CAMPAIGNS = "No active campaigns"
    NO_ACTIVE_USER = "No active user"
    DUPLICATE_TRANSACTION = "Transaction ID not unique"
    GENERIC_HANDLED_ERROR = "Internal server error"


class ActivityType(ActivityTypeMixin, Enum):
    TX_HISTORY = f"activity.{tx_settings.core.PROJECT_NAME}.tx.processed"
    TX_IMPORT = f"activity.{tx_settings.core.PROJECT_NAME}.tx.import"
    REFUND_NOT_RECOUPED = f"activity.{tx_settings.core.PROJECT_NAME}.refund.not.recouped"

    @classmethod
    def get_processed_tx_activity_data(
        cls,
        *,
        account_holder_uuid: "str | UUID",
        processed_tx: "Transaction",
        retailer: "Retailer",
        adjustment_amounts: dict[str, "AdjustmentAmount"],
        store_name: str,
        currency: str = "GBP",
    ) -> dict:
        # NOTE: retailer and processed_tx are not bound to this db_session
        # so we can't use the relationships on those objects
        # ie: retailer.stores and processed_tx.retailer
        return cls._assemble_payload(
            cls.TX_HISTORY.name,
            underlying_datetime=processed_tx.datetime,
            summary=f"{retailer.slug} Transaction Processed for {store_name} (MID: {processed_tx.mid})",
            reasons=build_tx_history_reasons(processed_tx.amount, adjustment_amounts, currency),
            activity_identifier=processed_tx.transaction_id,
            user_id=str(account_holder_uuid),
            associated_value=pence_integer_to_currency_string(processed_tx.amount, currency),
            retailer_slug=retailer.slug,
            campaigns=list(adjustment_amounts.keys()),
            data=ProcessedTXEventSchema(
                transaction_id=processed_tx.transaction_id,
                datetime=processed_tx.datetime,
                amount=pence_integer_to_currency_string(processed_tx.amount, currency, currency_sign=False),
                amount_currency=currency,
                store_name=store_name,
                mid=processed_tx.mid,
                earned=build_tx_history_earns(adjustment_amounts, currency),
            ).dict(),
        )

    @staticmethod
    def _get_http_error_reason(error: str) -> str:
        match error:

            case ErrorCode.NO_ACTIVE_CAMPAIGNS.name:
                reason = TxImportReasons.NO_ACTIVE_CAMPAIGNS.value
            case ErrorCode.USER_NOT_ACTIVE.name | ErrorCode.USER_NOT_FOUND.name:
                reason = TxImportReasons.NO_ACTIVE_USER.value
            case ErrorCode.DUPLICATE_TRANSACTION.name:
                reason = TxImportReasons.DUPLICATE_TRANSACTION.value
            case _:
                reason = TxImportReasons.GENERIC_HANDLED_ERROR.value

        return reason

    @classmethod
    def get_tx_import_activity_data(
        cls,
        *,
        retailer: "Retailer",
        campaign_slugs: list[str],
        request_payload: dict,
        currency: str = "GBP",
        error: str | None,
        invalid_refund: bool = False,
    ) -> dict:

        reason = []
        summary = f"{retailer.name} Transaction Imported"
        if error or invalid_refund:
            summary = f"{retailer.name} Transaction Import Failed"
            reason = (
                [cls._get_http_error_reason(error=error)] if error else [TxImportReasons.REFUNDS_NOT_SUPPORTED.value]
            )
        return cls._assemble_payload(
            activity_type=ActivityType.TX_IMPORT.name,
            underlying_datetime=request_payload["transaction_datetime"],
            summary=summary,
            reasons=reason,
            activity_identifier=request_payload["transaction_id"],
            user_id=request_payload["account_holder_uuid"],
            associated_value=pence_integer_to_currency_string(request_payload["amount"], currency),
            retailer_slug=retailer.slug,
            campaigns=campaign_slugs,
            data=TxImportEventSchema(
                transaction_id=request_payload["transaction_id"],
                datetime=request_payload["transaction_datetime"],
                amount=pence_integer_to_currency_string(request_payload["amount"], currency, currency_sign=False),
                mid=request_payload["mid"],
            ).dict(),
        )

    @classmethod
    def get_refund_not_recouped_activity_data(
        cls,
        *,
        account_holder_uuid: "UUID | str",
        activity_datetime: "datetime",
        retailer: "Retailer",
        campaigns: list[str],
        adjustment: int,
        amount_recouped: int,
        amount_not_recouped: "NonNegativeInt",
        transaction_id: str | None,
    ) -> dict:
        return cls._assemble_payload(
            ActivityType.REFUND_NOT_RECOUPED.name,
            user_id=account_holder_uuid,
            underlying_datetime=activity_datetime,
            summary=f"{retailer.name} Refund transaction caused an account shortfall",
            reasons=["Account Holder Balance and/or Pending Rewards did not cover the refund"],
            activity_identifier=transaction_id,
            associated_value=pence_integer_to_currency_string(adjustment, "GBP"),
            retailer_slug=retailer.slug,
            campaigns=campaigns,
            data=RefundNotRecoupedDataSchema(
                datetime=activity_datetime,
                transaction_id=transaction_id,
                amount=adjustment,
                amount_recouped=amount_recouped,
                amount_not_recouped=amount_not_recouped,
            ).dict(),
        )
