from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from admin.config import admin_settings

db_uri = admin_settings.core.db.SQLALCHEMY_DATABASE_URI.rsplit("/", 1)[0] + f"/{admin_settings.ACTIVITY_DB}"

engine = create_engine(
    db_uri,
    poolclass=NullPool,
)
SyncSessionMaker = sessionmaker(bind=engine, future=True, expire_on_commit=False)
db_session = scoped_session(SyncSessionMaker)
