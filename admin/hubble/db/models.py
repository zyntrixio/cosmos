from typing import Any

from retry_tasks_lib.db.models import load_models_to_metadata
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Mapped, mapped_column

Base: Any = automap_base()
load_models_to_metadata(Base.metadata)


class Activity(Base):
    __tablename__ = "activity"

    reasons: Mapped[list[str]] = mapped_column(ARRAY(String), index=True, nullable=False)
    campaigns: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False, index=True)
    retailer: Mapped[str] = mapped_column(index=True, nullable=False)

    def __str__(self) -> str:
        return f"{self.type} {self.summary}"
