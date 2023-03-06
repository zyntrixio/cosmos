import sys

from urllib.parse import urlparse

from pydantic import BaseSettings, PostgresDsn, validator


class DatabaseSettings(BaseSettings):
    TESTING: bool = False

    @validator("TESTING")
    @classmethod
    def is_test(cls, v: bool) -> bool:
        command = sys.argv[0]
        args = sys.argv[1:] if len(sys.argv) > 1 else []

        return True if "pytest" in command or any("test" in arg for arg in args) else v

    REDIS_URL: str

    @validator("REDIS_URL")
    @classmethod
    def assemble_redis_url(cls, v: str, values: dict) -> str:
        if values["TESTING"]:
            base_url, db_n = v.rsplit("/", 1)
            return f"{base_url}/{int(db_n) + 1}"
        return v

    SQL_DEBUG: bool = False
    USE_NULL_POOL: bool = False
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "cosmos"
    SQLALCHEMY_DATABASE_URI: str = ""
    DB_CONNECTION_RETRY_TIMES: int = 3

    @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    @classmethod
    def assemble_db_connection(cls, v: str, values: dict) -> str:

        parsed_uri = urlparse(
            v.format(values["POSTGRES_DB"])
            if v
            else PostgresDsn.build(
                scheme="postgresql+psycopg",
                user=values.get("POSTGRES_USER"),
                password=values.get("POSTGRES_PASSWORD"),
                host=values.get("POSTGRES_HOST"),
                port=values.get("POSTGRES_PORT"),
                path="/" + values.get("POSTGRES_DB", ""),
            )
        )

        if "+psycopg" not in parsed_uri.scheme:
            parsed_uri = parsed_uri._replace(scheme=f"{parsed_uri.scheme}+psycopg")

        if values["TESTING"]:
            parsed_uri = parsed_uri._replace(path=f"{parsed_uri.path}_test")

        return parsed_uri.geturl()

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


db_settings = DatabaseSettings()
