import pydantic


class RequestPayloadValidationError(Exception):
    def __init__(
        self, *args: str, validation_error: pydantic.ValidationError, **kwargs: str  # pylint: disable=unused-argument
    ) -> None:
        self.validation_error = validation_error
