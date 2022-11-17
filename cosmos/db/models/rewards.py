import uuid

import yaml

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin

from ...rewards.enums import FileAgentType, RewardTypeStatuses, RewardUpdateStatuses


class FetchType(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "fetch_type"

    name = Column(String, nullable=False, unique=True)
    required_fields = Column(Text, nullable=True)
    path = Column(String, nullable=False)

    retailers = relationship(
        "Retailer", back_populates="fetch_types", secondary="retailer_fetch_type", overlaps="retailer_fetch_types"
    )
    retailer_fetch_types = relationship(
        "RetailerFetchType", back_populates="fetch_type", overlaps="fetch_types,retailers"
    )
    reward_configs = relationship("RewardConfig", back_populates="fetch_type")

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: ({self.id}) {self.name}"


class RetailerFetchType(Base, TimestampMixin):
    __tablename__ = "retailer_fetch_type"

    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    fetch_type_id = Column(BigInteger, ForeignKey("fetch_type.id", ondelete="CASCADE"), nullable=False)
    agent_config = Column(Text, nullable=True)

    retailer = relationship("Retailer", back_populates="retailer_fetch_types", overlaps="fetch_types,retailers")
    fetch_type = relationship("FetchType", back_populates="retailer_fetch_types", overlaps="fetch_types,retailers")
    __table_args__ = (PrimaryKeyConstraint("retailer_id", "fetch_type_id"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: {self.retailer_id} - {self.fetch_type}"

    def load_agent_config(self) -> dict:
        if self.agent_config in ["", None]:
            return {}

        return yaml.safe_load(self.agent_config)


class Reward(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward"

    reward_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4)
    code = Column(String, nullable=False, index=True)
    # allocated = Column(Boolean, default=False, nullable=False)
    deleted = Column(Boolean, default=False, nullable=False)

    issued_date = Column(DateTime, nullable=False)
    expiry_date = Column(DateTime, nullable=False)
    # status = Column(Enum(AccountHolderRewardStatuses), nullable=False, default=AccountHolderRewardStatuses.ISSUED)
    redeemed_date = Column(DateTime, nullable=True)
    cancelled_date = Column(DateTime, nullable=True)
    associated_url = Column(String, nullable=False, server_default="")

    account_holder_id = Column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True, nullable=True
    )
    account_holder = relationship("AccountHolder", back_populates="rewards")

    # reward_config_id = Column(BigInteger, ForeignKey("reward_config.id"), nullable=False)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), nullable=False)

    reward_config = relationship("RewardConfig", back_populates="rewards")
    retailer = relationship("Retailer", back_populates="rewards")
    campaign = relationship("Campaign", back_populates="rewards")
    reward_updates = relationship("RewardUpdate", back_populates="reward")

    __table_args__ = (
        UniqueConstraint(
            "code",
            "retailer_id",
            # "reward_config_id",
            # name="code_retailer_reward_config_unq",
            name="code_retailer_unq",
        ),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.code}, {self.allocated})"


class RewardConfig(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_config"

    reward_slug = Column(String(32), index=True, nullable=False)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    fetch_type_id = Column(BigInteger, ForeignKey("fetch_type.id", ondelete="CASCADE"), nullable=False)
    status = Column(Enum(RewardTypeStatuses), nullable=False, default=RewardTypeStatuses.ACTIVE)
    required_fields_values = Column(Text, nullable=True)

    campaigns = relationship("Campaign", back_populates="reward_config")
    rewards = relationship("Reward", back_populates="reward_config")
    retailer = relationship("Retailer", back_populates="reward_configs")
    fetch_type = relationship("FetchType", back_populates="reward_configs")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("reward_slug", "retailer_id", name="reward_slug_retailer_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.reward_slug})"

    def load_required_fields_values(self) -> dict:
        if self.required_fields_values in ["", None]:
            return {}

        return yaml.safe_load(self.required_fields_values)


class RewardUpdate(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_update"

    reward_id = Column(BigInteger, ForeignKey("reward.id", ondelete="CASCADE"), nullable=False)
    date = Column(Date, nullable=False)
    status = Column(Enum(RewardUpdateStatuses), nullable=False)

    reward = relationship("Reward", back_populates="reward_updates")

    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"


class RewardFileLog(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_file_log"

    file_name = Column(String(500), index=True, nullable=False)
    file_agent_type = Column(Enum(FileAgentType), index=True, nullable=False)

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"
