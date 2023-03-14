from retry_tasks_lib.db.models import load_models_to_metadata
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.automap import AutomapBase, automap_base

Base: AutomapBase = automap_base()
load_models_to_metadata(Base.metadata)


class Activity(Base):
    __tablename__ = "activity"

    reasons = Column(ARRAY(String), index=True, nullable=False)
    campaigns = Column(ARRAY(String), nullable=False, index=True)
    retailer = Column(String, index=True, nullable=False)

    def __str__(self) -> str:
        return f"{self.type} {self.summary}"
