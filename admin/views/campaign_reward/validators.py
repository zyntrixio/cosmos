import json

from datetime import datetime

import pydantic as pd
import wtforms
import yaml

from sqlalchemy import func
from sqlalchemy.future import select
from wtforms.validators import StopValidation

from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.db.models import Campaign, EarnRule
from cosmos.db.session import scoped_db_session
from cosmos.retailers.enums import RetailerStatuses

FIELD_TYPES = {
    "integer": int,
    "float": float,
    "string": str,
}
INVALID_YAML_ERROR = StopValidation("The submitted YAML is not valid.")


def _count_earn_rules(campaign_id: int, *, has_inc_value: bool) -> int:
    stmt = select(func.count()).select_from(EarnRule).join(Campaign).where(Campaign.id == campaign_id)
    stmt = stmt.where(EarnRule.increment.isnot(None)) if has_inc_value else stmt.where(EarnRule.increment.is_(None))
    return scoped_db_session.execute(stmt).scalar()


def validate_campaign_loyalty_type(form: wtforms.Form, field: wtforms.Field) -> None:
    if form._obj:
        if field.data == LoyaltyTypes.ACCUMULATOR and _count_earn_rules(form._obj.id, has_inc_value=True):
            raise wtforms.ValidationError("This field cannot be changed as there are earn rules with increment values")

        if field.data == LoyaltyTypes.STAMPS and _count_earn_rules(form._obj.id, has_inc_value=False):
            raise wtforms.ValidationError("This field cannot be changed as there are earn rules with null increments")


def validate_campaign_status_change(form: wtforms.Form, field: wtforms.Field) -> None:
    original_campaign = form._obj

    if (original_campaign.status != CampaignStatuses.ACTIVE and field.data == CampaignStatuses.ACTIVE.name) and (
        original_campaign.earn_rule is None or original_campaign.reward_rule is None
    ):
        raise wtforms.ValidationError("To activate a campaign one reward rule and at least one earn rule are required.")


def validate_earn_rule_increment(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.campaign.data.loyalty_type == LoyaltyTypes.STAMPS:
        if field.data is None:
            raise wtforms.validators.StopValidation(
                "The campaign requires that this field is populated due to campaign.loyalty_type setting"
            )
        if field.data % 100:
            raise wtforms.validators.StopValidation("This field must be a multiple of 100")

    if form.campaign.data.loyalty_type == LoyaltyTypes.ACCUMULATOR and field.data is not None:
        raise wtforms.ValidationError(
            "The campaign requires that this field is not populated due to campaign.loyalty_type setting"
        )


def validate_increment_multiplier(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.campaign.data.loyalty_type == LoyaltyTypes.STAMPS and not str(field.data).isnumeric():
        raise wtforms.ValidationError("All stamp campaigns must have an integer for this field.")


def validate_earn_rule_max_amount(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.campaign.data.loyalty_type != LoyaltyTypes.ACCUMULATOR and field.data != 0:
        raise wtforms.ValidationError(
            "The campaign requires that this field is set to 0 due to campaign.loyalty_type setting"
        )


def validate_reward_rule_allocation_window(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.campaign.data.loyalty_type == LoyaltyTypes.STAMPS and field.data != 0:
        raise wtforms.ValidationError(
            "The campaign requires that this field is set to 0 due to campaign.loyalty_type setting"
        )


def validate_reward_cap_for_loyalty_type(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.campaign.data.loyalty_type != LoyaltyTypes.ACCUMULATOR and field.data is not None:
        raise wtforms.ValidationError("Reward cap can only be set for accumulator campaigns")


def validate_retailer_fetch_type(form: wtforms.Form, field: wtforms.Field) -> None:

    if field.data not in form.retailer.data.fetch_types:
        raise wtforms.ValidationError("Fetch Type not allowed for this retailer")


def _validate_required_fields_values(required_fields: dict, fields_to_check: dict) -> pd.BaseModel:
    class Config(pd.BaseConfig):
        extra = pd.Extra.forbid
        anystr_lower = True
        anystr_strip_whitespace = True
        min_anystr_length = 2

    required_fields_value_model = pd.create_model(  # type: ignore
        "RequiredFieldsValuesModel",
        __config__=Config,
        **{k: (FIELD_TYPES[v], ...) for k, v in required_fields.items()},
    )

    try:
        return required_fields_value_model(**fields_to_check)
    except pd.ValidationError as ex:
        raise StopValidation(  # noqa: B904
            ", ".join([f"{' -> '.join(err.get('loc'))}: {err.get('msg')}" for err in json.loads(ex.json())])
        )


def validate_required_fields_values_yaml(form: wtforms.Form, field: wtforms.Field) -> None:
    if form.fetch_type.data.required_fields in (None, ""):
        required_fields = None
    else:
        required_fields = yaml.safe_load(form.fetch_type.data.required_fields)

    try:
        field_data = None if field.data is None else yaml.safe_load(field.data)
    except (yaml.YAMLError, AttributeError):  # pragma: no cover
        raise INVALID_YAML_ERROR  # noqa: B904

    if required_fields is None:
        if field_data == required_fields:
            field.data = ""
            return

        raise StopValidation("'required_fields_values' must be empty for this fetch type.")

    if not isinstance(field_data, dict):
        raise INVALID_YAML_ERROR

    field.data = yaml.dump(_validate_required_fields_values(required_fields, field_data).dict(), indent=2)


def validate_campaign_end_date_change(
    old_end_date: datetime | None,
    new_end_date: datetime | None,
    start_date: datetime | None,
    campaign_status: CampaignStatuses,
) -> None:
    if old_end_date:
        old_end_date = old_end_date.replace(microsecond=0)
    if campaign_status not in (CampaignStatuses.DRAFT, CampaignStatuses.ACTIVE) and new_end_date != old_end_date:
        raise wtforms.ValidationError(
            "Can not amend the end date field of anything other than a draft or active campaign."
        )

    if new_end_date and start_date:
        if new_end_date < start_date:
            raise wtforms.ValidationError("Can not set end date to be earlier than start date.")
        if old_end_date and campaign_status == CampaignStatuses.ACTIVE and old_end_date > new_end_date:
            raise wtforms.ValidationError(
                "Active campaign end dates cannot be brought forward, they can only be extended."
            )


def validate_campaign_start_date_change(
    old_start_date: datetime | None, new_start_date: datetime | None, campaign_status: CampaignStatuses
) -> None:
    if old_start_date:
        old_start_date = old_start_date.replace(microsecond=0)
    if campaign_status != CampaignStatuses.DRAFT and new_start_date != old_start_date:
        raise wtforms.ValidationError("Can not amend the start date field of anything other than a draft campaign.")


def validate_retailer_update(old_retailer: str, new_retailer: str, campaign_status: CampaignStatuses) -> None:
    if old_retailer != new_retailer and campaign_status != CampaignStatuses.DRAFT:
        raise wtforms.ValidationError("Can only change retailer for a draft campaign")


def validate_campaign_slug_update(
    old_campaign_slug: str, new_campaign_slug: str, campaign_status: CampaignStatuses
) -> None:
    if old_campaign_slug != new_campaign_slug and campaign_status != CampaignStatuses.DRAFT:
        raise wtforms.ValidationError("Can only change campaign slug for a draft campaign")


def validate_earn_rule_deletion(campaign: "Campaign") -> None:
    if campaign.status == CampaignStatuses.ACTIVE:
        raise wtforms.ValidationError("Can not delete earn rule of an active campaign.")


def validate_reward_rule_deletion(campaign: "Campaign") -> None:
    if campaign.status == CampaignStatuses.ACTIVE:
        raise wtforms.ValidationError("Can not delete the reward rule of an active campaign.")


def validate_reward_rule_change(campaign: Campaign, is_created: bool) -> None:
    if (
        campaign.status == CampaignStatuses.ACTIVE
        and campaign.retailer.status != RetailerStatuses.TEST
        and not is_created
    ):
        raise wtforms.ValidationError("Can not edit the reward rule of an active campaign.")
