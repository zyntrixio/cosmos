from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from cosmos.core.config import settings

db_uri = settings.SQLALCHEMY_DATABASE_URI.rsplit("/", 1)[0] + f"/{settings.ACTIVITY_DB}"

engine = create_engine(
    db_uri,
    poolclass=NullPool,
)
SyncSessionMaker = sessionmaker(bind=engine, future=True, expire_on_commit=False)
db_session = scoped_session(SyncSessionMaker)
