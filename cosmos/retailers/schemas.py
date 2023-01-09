from datetime import date, datetime
from typing import TYPE_CHECKING

from pydantic import EmailStr, Field, StrictBool, StrictFloat, StrictInt, StrictStr, constr, create_model

if TYPE_CHECKING:  # pragma: no cover
    from pydantic import BaseModel  # pragma: no cover


PROFILE_FIELD_TYPES = {
    "email": EmailStr,
    "first_name": constr(regex=r"^(((?=.{1,100}$)[A-Za-z\-\u00C0-\u00FF'])+\s*){1,4}$"),
    "last_name": constr(regex=r"^(((?=.{1,100}$)[A-Za-z\-\u00C0-\u00FF'])+\s*){1,4}$"),
    "date_of_birth": date,
    "phone": constr(regex=r"^(?:0|\+?44)(?:\d-?\s?){9,15}$"),
    "address_line1": constr(regex=r"^([a-zA-Z0-9#.,;:+\-&' ]){1,100}$", strip_whitespace=True),
    "address_line2": constr(regex=r"^([a-zA-Z0-9#.,;:+\-&' ]){1,100}$", strip_whitespace=True),
    "postcode": constr(regex=r"^([A-Za-z][A-Ha-hJ-Yj-y]?[0-9][A-Za-z0-9]? ?[0-9][A-Za-z]{2}|[Gg][Ii][Rr] ?0[Aa]{2})$"),
    "city": constr(regex=r"^([a-zA-Z0-9#.,;:+\-&' ]){1,100}$", strip_whitespace=True),
    "custom": constr(min_length=1, strip_whitespace=True),
}

MARKETING_FIELD_TYPES = {
    "boolean": StrictBool,
    "string": StrictStr,
    "integer": StrictInt,
    "float": StrictFloat,
    "string_list": list[StrictStr],
    "date": date,
    "datetime": datetime,
    "timestamp": StrictInt,
}


def retailer_profile_info_validation_factory(profile_config: dict) -> type["BaseModel"]:
    return create_model(
        "ProfileConfigSchema",
        **{
            field_name: (
                PROFILE_FIELD_TYPES.get(field_name, str),
                Field(... if options and options.get("required") else None),
            )
            for field_name, options in profile_config.items()
        }  # type: ignore [call-overload]
    )


def retailer_marketing_info_validation_factory(marketing_config: dict) -> type["BaseModel"]:
    return create_model(
        "MarketingConfigSchema",
        **{
            field_name: (MARKETING_FIELD_TYPES[options["type"]], Field(...))
            for field_name, options in marketing_config.items()
        }  # type: ignore [call-overload]
    )
