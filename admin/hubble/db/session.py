from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from admin.config import admin_settings

engine = create_engine(admin_settings.ACTIVITY_SQLALCHEMY_URI, poolclass=NullPool)
SyncSessionMaker = sessionmaker(bind=engine, future=True, expire_on_commit=False)
db_session = scoped_session(SyncSessionMaker)
