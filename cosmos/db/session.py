from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from cosmos.db.config import db_settings

engine_kwargs = {
    "connect_args": {"application_name": "cosmos"},
    "pool_pre_ping": True,
    "echo": db_settings.SQL_DEBUG,
} | ({"poolclass": NullPool} if db_settings.USE_NULL_POOL or db_settings.TESTING else {})

async_engine = create_async_engine(db_settings.SQLALCHEMY_DATABASE_URI, **engine_kwargs)
sync_engine = create_engine(db_settings.SQLALCHEMY_DATABASE_URI, **engine_kwargs)
AsyncSessionMaker = async_sessionmaker(async_engine, expire_on_commit=False)
SyncSessionMaker = sessionmaker(sync_engine, expire_on_commit=False)
scoped_db_session = scoped_session(sessionmaker(bind=sync_engine))  # For Flask-Admin
