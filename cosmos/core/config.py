import logging
import sys

from logging.config import dictConfig
from typing import TYPE_CHECKING, Any, Literal

import sentry_sdk

from pydantic import AnyHttpUrl, BaseSettings, Field, HttpUrl, validator
from pydantic.validators import str_validator
from redis import Redis
from retry_tasks_lib.settings import load_settings
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.rq import RqIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from cosmos.core.key_vault import key_vault
from cosmos.db.config import DatabaseSettings, db_settings
from cosmos.version import __version__

if TYPE_CHECKING:  # pragma: no cover
    from pydantic.typing import CallableGenerator


class LogLevel(str):
    @classmethod
    def __modify_schema__(cls, field_schema: dict[str, Any]) -> None:
        field_schema.update(type="string", format="log_level")

    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        yield str_validator
        yield cls.validate

    @classmethod
    def validate(cls, value: str) -> str:
        v = value.upper()
        if v not in ("CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"):
            raise ValueError(f"{value} is not a valid LOG_LEVEL value")
        return v


class CoreSettings(BaseSettings):
    API_PREFIX: str = "/api"

    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    PUBLIC_URL: AnyHttpUrl

    PROJECT_NAME: str = "cosmos"
    ROOT_LOG_LEVEL: LogLevel | None = None
    QUERY_LOG_LEVEL: LogLevel | None = None
    PROMETHEUS_LOG_LEVEL: LogLevel | None = None
    LOG_FORMATTER: Literal["json", "brief", "console"] = "json"
    SENTRY_ENV: str | None = None
    SENTRY_DSN: HttpUrl | None = None

    @validator("SENTRY_DSN", pre=True)
    @classmethod
    def sentry_dsn_can_be_blank(cls, v: str | None) -> str | None:
        return v or None

    SENTRY_TRACES_SAMPLE_RATE: float = Field(0.0, ge=0.0, le=1.0)
    # The prefix used on every Redis key.
    REDIS_KEY_PREFIX = "cosmos:"

    db: DatabaseSettings = db_settings

    HTTP_REQUEST_RETRY_TIMES: int = 3

    CALLBACK_OAUTH2_RESOURCE: str = "api://midas-nonprod"

    AZURE_OAUTH2_TOKEN_URL: str = "http://169.254.169.254"

    TASK_QUEUE_PREFIX: str = "cosmos:"
    TASK_QUEUES: list[str] | None = None

    @validator("TASK_QUEUES")
    @classmethod
    def task_queues(cls, v: list[str] | None, values: dict[str, Any]) -> list[str]:
        if v and isinstance(v, list):
            return v
        return [values["TASK_QUEUE_PREFIX"] + name for name in ("high", "default", "low")]

    PENDING_REWARDS_SCHEDULE: str = "0 2 * * *"
    REPORT_ANOMALOUS_TASKS_SCHEDULE: str = "*/10 * * * *"
    REPORT_TASKS_SUMMARY_SCHEDULE: str = "5,20,35,50 */1 * * *"
    REPORT_JOB_QUEUE_LENGTH_SCHEDULE: str = "*/10 * * * *"
    TASK_CLEANUP_SCHEDULE: str = "0 1 * * *"

    TASK_MAX_RETRIES: int = 6
    TASK_RETRY_BACKOFF_BASE: float = 3
    PROMETHEUS_HTTP_SERVER_PORT: int = 9100
    SEND_EMAIL: bool = False
    SEND_EMAIL_TASK_NAME: str = "send-email"
    SEND_EMAIL_TASK_RETRY_BACKOFF_BASE: float = 1
    ACTIVATE_TASKS_METRICS: bool = True

    RABBITMQ_DSN: str = "amqp://guest:guest@localhost:5672//"
    MESSAGE_EXCHANGE_NAME: str = "hubble-activities"

    MAILJET_API_URL: str | None = "https://api.mailjet.com/v3.1/send"  # Set in the env
    MAILJET_API_PUBLIC_KEY: str = ""

    @validator("MAILJET_API_PUBLIC_KEY")
    @classmethod
    def fetch_mailjet_api_public_key(cls, v: str) -> str:
        return v or key_vault.get_secret("bpl-mailjet-api-public-key")

    MAILJET_API_SECRET_KEY: str = ""

    @validator("MAILJET_API_SECRET_KEY")
    @classmethod
    def fetch_mailjet_api_secret_key(cls, v: str) -> str:
        return v or key_vault.get_secret("bpl-mailjet-api-secret-key")

    class Config:
        case_sensitive = True
        # env var settings priority ie priority 1 will override priority 2:
        # 1 - env vars already loaded (ie the one passed in by kubernetes)
        # 2 - env vars read from *local.env file
        # 3 - values assigned directly in the Settings class
        env_file = "local.env"
        env_file_encoding = "utf-8"

    CONFLUENCE_API_BASE_URL: str = "https://hellobink.atlassian.net/wiki"
    CONFLUENCE_PAGE_ID: str = ""
    CONFLUENCE_USER: str = ""
    CONFLUENCE_API_TOKEN: str = ""
    CONFLUENCE_ATTACHMENT_NAME: str = "cosmos_schema"


core_settings = CoreSettings()
load_settings(core_settings)

dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "brief": {"format": "%(levelname)s:     %(asctime)s - %(message)s"},
            "console": {"()": "cosmos.core.reporting.ConsoleFormatter"},
            "detailed": {"()": "cosmos.core.reporting.ConsoleFormatter"},
            "json": {"()": "cosmos.core.reporting.JSONFormatter"},
        },
        "handlers": {
            "stderr": {
                "level": logging.NOTSET,
                "class": "logging.StreamHandler",
                "stream": sys.stderr,
                "formatter": core_settings.LOG_FORMATTER,
            },
            "stdout": {
                "level": logging.NOTSET,
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": core_settings.LOG_FORMATTER,
            },
        },
        "loggers": {
            "root": {
                "level": core_settings.ROOT_LOG_LEVEL or logging.INFO,
                "handlers": ["stdout"],
            },
            "prometheus": {
                "propagate": False,
                "level": core_settings.PROMETHEUS_LOG_LEVEL or logging.INFO,
                "handlers": ["stderr"],
            },
            "uvicorn": {
                "propagate": False,
                "handlers": ["stdout"],
            },
            "enrol-callback": {
                "propagate": False,
                "handlers": ["stdout"],
            },
            "sqlalchemy.engine": {
                "level": core_settings.QUERY_LOG_LEVEL or logging.WARN,
            },
            "alembic": {
                "level": logging.INFO,
                "handlers": ["stderr"],
                "propagate": False,
            },
        },
    }
)

# this will decode responses:
# >>> redis.set('test', 'hello')
# True
# >>> redis.get('test')
# 'hello'
redis = Redis.from_url(
    core_settings.db.REDIS_URL,
    socket_connect_timeout=3,
    socket_keepalive=True,
    retry_on_timeout=False,
    decode_responses=True,
)

# used for RQ:
# this will not decode responses:
# >>> redis.set('test', 'hello')
# True
# >>> redis.get('test')
# b'hello'
redis_raw = Redis.from_url(
    core_settings.db.REDIS_URL,
    socket_connect_timeout=3,
    socket_keepalive=True,
    retry_on_timeout=False,
)

if core_settings.SENTRY_DSN:  # pragma: no cover
    sentry_sdk.init(
        dsn=core_settings.SENTRY_DSN,
        environment=core_settings.SENTRY_ENV,
        integrations=[
            RedisIntegration(),
            RqIntegration(),
            SqlalchemyIntegration(),
        ],
        release=__version__,
        traces_sample_rate=core_settings.SENTRY_TRACES_SAMPLE_RATE,
    )
