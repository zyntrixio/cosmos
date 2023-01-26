from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.automap import AutomapBase, automap_base
from sqlalchemy.sql.schema import MetaData

metadata = MetaData()
Base: AutomapBase = automap_base(metadata=metadata)


class Activity(Base):
    __tablename__ = "activity"

    reasons = Column(ARRAY(String), index=True, nullable=False)
    campaigns = Column(ARRAY(String), nullable=False, index=True)

    def __str__(self) -> str:
        return f"{self.type} {self.summary}"
