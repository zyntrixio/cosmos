from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi_prometheus_metrics.endpoints import router as metrics_router
from fastapi_prometheus_metrics.manager import PrometheusManager
from fastapi_prometheus_metrics.middleware import MetricsSecurityMiddleware, PrometheusMiddleware
from starlette.exceptions import HTTPException

from cosmos.accounts.api import api_router
from cosmos.core.exception_handlers import (
    http_exception_handler,
    request_validation_handler,
    unexpected_exception_handler,
)


def create_app() -> FastAPI:
    fapi = FastAPI(title="Transactions API")
    fapi.include_router(api_router)
    fapi.include_router(metrics_router)
    fapi.add_exception_handler(RequestValidationError, request_validation_handler)
    fapi.add_exception_handler(HTTPException, http_exception_handler)
    fapi.add_exception_handler(status.HTTP_500_INTERNAL_SERVER_ERROR, unexpected_exception_handler)

    fapi.add_middleware(MetricsSecurityMiddleware)
    fapi.add_middleware(PrometheusMiddleware)

    PrometheusManager("transactions", metric_name_prefix="bpl")  # initialise signals

    # Prevent 307 temporary redirects if URLs have slashes on the end
    fapi.router.redirect_slashes = False

    return fapi


app = create_app()
