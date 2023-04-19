import json
import re

from typing import TYPE_CHECKING, Literal

import pydantic as pd
import wtforms
import yaml

from pydantic import BaseConfig, BaseModel, ConstrainedStr, validator
from sqlalchemy import select
from wtforms.validators import StopValidation

from cosmos.db.base_class import Base
from cosmos.db.models import EmailTemplate, EmailType
from cosmos.db.session import scoped_db_session
from cosmos.retailers.enums import EmailTypeSlugs, RetailerStatuses

if TYPE_CHECKING:
    from cosmos.db.models import Retailer

REQUIRED_ACCOUNTS_JOIN_FIELDS = ["first_name", "last_name", "email"]
FIELD_TYPES = {
    "integer": int,
    "float": float,
    "string": str,
}
INVALID_YAML_ERROR = StopValidation("The submitted YAML is not valid.")


def _get_optional_profile_field_names() -> list[str]:  # pragma: no cover
    return [
        str(col.name)
        for col in Base.metadata.tables["account_holder_profile"].c
        if not col.primary_key and not col.foreign_keys and str(col.name) not in REQUIRED_ACCOUNTS_JOIN_FIELDS
    ]


def validate_retailer_config(_: wtforms.Form, field: wtforms.Field) -> None:
    class FieldOptionsConfig(BaseConfig):
        extra = pd.Extra.forbid

    class FieldOptions(BaseModel):
        required: bool
        label: str | None = None

        Config = FieldOptionsConfig  # type: type[BaseConfig]

    def ensure_required_true(options: FieldOptions) -> FieldOptions:
        if not options.required:
            raise ValueError("'required' must be true")
        return options

    try:
        form_data = yaml.safe_load(field.data)
    except yaml.YAMLError:  # pragma: no cover
        form_data = None

    if not isinstance(form_data, dict):
        raise wtforms.ValidationError("The submitted YAML is not valid")

    required_fields = REQUIRED_ACCOUNTS_JOIN_FIELDS + [
        field for field in _get_optional_profile_field_names() if field in form_data
    ]

    retailer_config_model = pd.create_model(  # type: ignore [call-overload]
        "RetailerConfigModel",
        __config__=FieldOptionsConfig,
        __validators__={
            f"{field}_validator": validator(field, allow_reuse=True)(ensure_required_true)
            for field in REQUIRED_ACCOUNTS_JOIN_FIELDS
        },
        **{field: (FieldOptions, ...) for field in required_fields},
    )

    try:
        retailer_config_model(**form_data)
    except pd.ValidationError as ex:
        raise wtforms.ValidationError(  # noqa: B904
            ", ".join([f"{' -> '.join(err.get('loc'))}: {err.get('msg')}" for err in json.loads(ex.json())])
        )


def validate_marketing_config(_: wtforms.Form, field: wtforms.Field) -> None:
    if field.data == "":
        return

    class LabelVal(ConstrainedStr):
        strict = True
        strip_whitespace = True
        min_length = 2

    class KeyNameVal(LabelVal):
        to_lower = True

    class FieldOptions(BaseModel):
        type: Literal["boolean", "integer", "float", "string", "string_list", "date", "datetime"]  # noqa: A003
        label: LabelVal

        extra = pd.Extra.forbid  # type: pd.Extra

    class MarketingPreferenceConfigVal(BaseModel):
        __root__: dict[KeyNameVal, FieldOptions]

    try:
        form_data = yaml.safe_load(field.data)
    except yaml.YAMLError:  # pragma: no cover
        form_data = None

    if not isinstance(form_data, dict):
        raise wtforms.ValidationError("The submitted YAML is not valid")

    try:
        validated_data = MarketingPreferenceConfigVal(__root__=form_data)
    except pd.ValidationError as ex:
        formatted_errors = []
        for err in json.loads(ex.json()):
            loc = err.get("loc")[1:]
            if loc[0] == "__key__":
                loc[0] = "'key'"

            formatted_errors.append(f"{' -> '.join(loc)}: {err.get('msg')}")

        raise wtforms.ValidationError(", ".join(formatted_errors)) from None
    else:
        field.data = yaml.dump(validated_data.dict(exclude_unset=True)["__root__"], sort_keys=True)


def validate_account_number_prefix(_: wtforms.Form, field: wtforms.Field) -> None:
    required = re.compile(r"^[a-zA-Z]{2,4}$")
    if not bool(required.match(field.data)):
        raise wtforms.ValidationError("Account number prefix needs to be 2-4 alpha characters")

    field.data = field.data.upper()


def _validate_active_retailer_update(old_warning_days: int | None, updated_warning_days: int | None) -> None:
    if old_warning_days is not None and updated_warning_days != old_warning_days:
        raise wtforms.ValidationError(
            "You cannot update the balance_reset_advanced_warning_days for an active retailer"
        )


def validate_balance_lifespan_and_warning_days(
    form: wtforms.Form,
    retailer_status: RetailerStatuses,
) -> None:
    old_warning_days = form.balance_reset_advanced_warning_days.object_data
    updated_warning_days = form.balance_reset_advanced_warning_days.data
    balance_lifespan = form.balance_lifespan.data
    if retailer_status == RetailerStatuses.ACTIVE:
        _validate_active_retailer_update(old_warning_days, updated_warning_days)
    if balance_lifespan and updated_warning_days and balance_lifespan <= updated_warning_days:
        raise wtforms.ValidationError("The balance_reset_advanced_warning_days must be less than the balance_lifespan")
    if balance_lifespan and not updated_warning_days or updated_warning_days and not balance_lifespan:
        raise wtforms.ValidationError(
            "You must set both the balance_lifespan with the balance_reset_advanced_warning_days"
        )


def validate_retailer_config_new_values(form: wtforms.Form, model: "Retailer") -> tuple[dict, dict]:
    new_values: dict = {}
    original_values: dict = {}

    for field in form:
        if (new_val := getattr(model, field.name)) != field.object_data:
            new_values[field.name] = new_val
            original_values[field.name] = field.object_data

    def format_from_yaml(key_name: str) -> None:
        if key_name in new_values:
            if loaded_data := yaml.safe_load(new_values[key_name]):
                new_values[key_name] = [{"key": k, **v} for k, v in loaded_data.items()]
            else:
                new_values[key_name] = [{key_name: ""}]
            if original_values[key_name]:
                original_values[key_name] = [
                    {"key": k, **v} for k, v in yaml.safe_load(original_values[key_name]).items()
                ]
            else:
                original_values[key_name] = [{key_name: ""}]

    format_from_yaml("marketing_preference_config")
    format_from_yaml("profile_config")

    return new_values, original_values


def validate_optional_yaml(_: wtforms.Form, field: wtforms.Field) -> None:
    try:
        if field.data in (None, ""):
            field.data = ""
            return

        field_data = yaml.safe_load(field.data)

    except (yaml.YAMLError, AttributeError):  # pragma: no cover
        raise INVALID_YAML_ERROR from None

    if not isinstance(field_data, dict):
        raise INVALID_YAML_ERROR

    field.data = yaml.dump(field_data, indent=2)


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
        raise StopValidation(
            ", ".join([f"{' -> '.join(err.get('loc'))}: {err.get('msg')}" for err in json.loads(ex.json())])
        ) from None


def validate_required_fields_values_yaml(form: wtforms.Form, field: wtforms.Field) -> None:
    if (required_fields_raw := form.email_type.data.required_fields) in (None, ""):
        required_fields = None
    else:
        required_fields = yaml.safe_load(required_fields_raw)

    try:
        field_data = yaml.safe_load(field.data) if field.data else None
    except (yaml.YAMLError, AttributeError):  # pragma: no cover
        raise INVALID_YAML_ERROR from None

    if not required_fields:
        if field_data == required_fields:
            field.data = None
            return

        raise StopValidation("'required_fields_values' must be empty for this email type.")

    if not isinstance(field_data, dict):
        raise INVALID_YAML_ERROR

    field.data = yaml.dump(_validate_required_fields_values(required_fields, field_data).dict(), indent=2)


def validate_balance_reset_email_template(form: wtforms.Form, field: wtforms.Field) -> None:
    if field.data and not (
        form._obj
        and scoped_db_session.scalar(
            select(EmailTemplate.id).where(
                EmailTemplate.retailer_id == form._obj.id,
                EmailTemplate.email_type_id == EmailType.id,
                EmailType.slug == EmailTypeSlugs.BALANCE_RESET.name,
            )
        )
    ):
        raise StopValidation("Balance nudge email must be configured before these values can be set")
