import sys

from pydantic import BaseSettings, PostgresDsn, validator


class DatabaseSettings(BaseSettings):
    TESTING: bool = False

    @validator("TESTING")
    @classmethod
    def is_test(cls, v: bool) -> bool:
        command = sys.argv[0]
        args = sys.argv[1:] if len(sys.argv) > 1 else []

        return True if "pytest" in command or any("test" in arg for arg in args) else v

    SQL_DEBUG: bool = False
    USE_NULL_POOL: bool = False
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = "cosmos"
    SQLALCHEMY_DATABASE_URI: str = ""
    SQLALCHEMY_DATABASE_URI_ASYNC: str = ""
    DB_CONNECTION_RETRY_TIMES: int = 3

    @validator("SQLALCHEMY_DATABASE_URI", pre=True)
    @classmethod
    def assemble_db_connection(cls, v: str, values: dict) -> str:
        db_uri = (
            v.format(values["POSTGRES_DB"])
            if v
            else PostgresDsn.build(
                scheme="postgresql",
                user=values.get("POSTGRES_USER"),
                password=values.get("POSTGRES_PASSWORD"),
                host=values.get("POSTGRES_HOST"),
                port=values.get("POSTGRES_PORT"),
                path="/" + values.get("POSTGRES_DB", ""),
            )
        )
        if values["TESTING"]:
            db_uri += "_test"

        return db_uri

    @validator("SQLALCHEMY_DATABASE_URI_ASYNC", pre=True)
    @classmethod
    def adapt_db_connection_to_async(cls, v: str, values: dict) -> str:
        return (
            v.format(values["POSTGRES_DB"])
            if v
            else (
                values["SQLALCHEMY_DATABASE_URI"]
                .replace("postgresql://", "postgresql+asyncpg://")
                .replace("sslmode=", "ssl=")
            )
        )

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


db_settings = DatabaseSettings()
