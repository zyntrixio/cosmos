from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi_prometheus_metrics.endpoints import router as metrics_router
from fastapi_prometheus_metrics.manager import PrometheusManager
from fastapi_prometheus_metrics.middleware import MetricsSecurityMiddleware, PrometheusMiddleware
from starlette.exceptions import HTTPException

from cosmos.core.api.exception_handlers import (
    http_exception_handler,
    payload_request_validation_error,
    request_validation_handler,
    service_exception_handler,
    unexpected_exception_handler,
)
from cosmos.core.api.exceptions import RequestPayloadValidationError
from cosmos.core.api.healthz import healthz_router
from cosmos.core.api.service import ServiceError
from cosmos.public_api.api.endpoints import public_router


def create_app() -> FastAPI:
    api = FastAPI(title="Public Facing API")
    api.include_router(healthz_router)
    api.include_router(public_router)
    api.include_router(metrics_router)
    api.add_exception_handler(RequestValidationError, request_validation_handler)
    api.add_exception_handler(RequestPayloadValidationError, payload_request_validation_error)
    api.add_exception_handler(HTTPException, http_exception_handler)
    api.add_exception_handler(ServiceError, service_exception_handler)
    api.add_exception_handler(status.HTTP_500_INTERNAL_SERVER_ERROR, unexpected_exception_handler)

    api.add_middleware(MetricsSecurityMiddleware)
    api.add_middleware(PrometheusMiddleware)

    PrometheusManager("public", metric_name_prefix="bpl")

    return api


app = create_app()
