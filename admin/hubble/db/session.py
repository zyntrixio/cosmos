from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from admin.config import admin_settings

if TYPE_CHECKING:

    from sqlalchemy.orm import Session


activity_engine = create_engine(admin_settings.ACTIVITY_SQLALCHEMY_URI, poolclass=NullPool)
SessionMaker = sessionmaker(bind=activity_engine, future=True, expire_on_commit=False)
activity_scoped_session: scoped_session["Session"] = scoped_session(SessionMaker)
