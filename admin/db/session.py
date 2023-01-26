from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from cosmos.core.config import settings

# application_name
CONNECT_ARGS = {"application_name": "cosmos"}

sync_engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    connect_args=CONNECT_ARGS,
    pool_pre_ping=True,
    echo=settings.SQL_DEBUG,
    future=True,
    poolclass=NullPool,
)
db_session = scoped_session(sessionmaker(bind=sync_engine))
