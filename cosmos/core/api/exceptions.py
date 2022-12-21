import pydantic


class ServiceError(Exception):
    pass


class RequestPayloadValidationError(Exception):
    def __init__(self, *args: str, validation_error: pydantic.ValidationError, **kwargs: str) -> None:
        self.validation_error = validation_error
