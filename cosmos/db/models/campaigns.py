# from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, Column, DateTime, Enum, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import relationship

from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin

from ...campaigns.enums import CampaignStatuses, LoyaltyTypes, RewardCap


class Campaign(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "campaign"

    status = Column(Enum(CampaignStatuses), nullable=False, server_default="DRAFT")
    name = Column(String(128), nullable=False)
    slug = Column(String(32), index=True, unique=True, nullable=False)
    reward_config_id = Column(BigInteger, ForeignKey("reward_config.id", ondelete="CASCADE"), nullable=False)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    loyalty_type = Column(Enum(LoyaltyTypes), nullable=False, server_default="STAMPS")
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    reward_config = relationship("RewardConfig", back_populates="campaigns")
    retailer = relationship("Retailer", back_populates="campaigns")
    earn_rules = relationship("EarnRule", cascade="all,delete", back_populates="campaign")
    reward_rule = relationship("RewardRule", cascade="all,delete", back_populates="campaign", uselist=False)

    def __str__(self) -> str:  # pragma: no cover
        return str(self.name)

    def is_activable(self) -> bool:
        return self.status == CampaignStatuses.DRAFT and self.reward_rule is not None and len(self.earn_rules) >= 1


class EarnRule(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "earn_rule"

    threshold = Column(Integer, nullable=False)
    increment = Column(Integer, nullable=True)
    increment_multiplier = Column(Numeric(scale=2), default=1, nullable=False)
    max_amount = Column(Integer, nullable=False, server_default="0")

    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), nullable=False)
    campaign = relationship("Campaign", back_populates="earn_rules")


class RewardRule(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_rule"

    reward_goal = Column(Integer, nullable=False)
    reward_slug = Column(String(32), index=True, unique=False, nullable=False)
    allocation_window = Column(Integer, nullable=False, server_default="0")
    reward_cap = Column(
        Enum(RewardCap, values_callable=lambda x: [str(e.value) for e in RewardCap]),
        nullable=True,
    )

    campaign_id = Column(Integer, ForeignKey("campaign.id", ondelete="CASCADE"), nullable=False)
    campaign = relationship("Campaign", back_populates="reward_rule")
