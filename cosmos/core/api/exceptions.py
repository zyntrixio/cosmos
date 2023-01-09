import pydantic


class RequestPayloadValidationError(Exception):
    def __init__(self, *args: str, validation_error: pydantic.ValidationError, **kwargs: str) -> None:  # noqa ARG002
        self.validation_error = validation_error
