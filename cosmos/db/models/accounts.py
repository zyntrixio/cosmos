from typing import TYPE_CHECKING
from uuid import uuid4

# from cosmos.core.config import settings
from pydantic import PositiveInt
from sqlalchemy import BigInteger, Column, Date, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from ...accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from ..base_class import Base, IdPkMixin, TimestampMixin

if TYPE_CHECKING:
    from cosmos.db.models.retailers import Retailer


class AccountHolder(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder"

    email = Column(String, index=True, nullable=False)
    status = Column(Enum(AccountHolderStatuses), nullable=False, default=AccountHolderStatuses.PENDING)
    account_number = Column(String, nullable=True, index=True, unique=True)
    account_holder_uuid = Column(UUID(as_uuid=True), nullable=False, default=uuid4, unique=True)
    opt_out_token = Column(UUID(as_uuid=True), nullable=False, default=uuid4, unique=True)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id"), index=True)

    retailer = relationship("Retailer", back_populates="account_holders")
    profile = relationship("AccountHolderProfile", uselist=False, back_populates="account_holder")
    balance_adjustments = relationship("BalanceAdjustment", back_populates="account_holder")
    pending_rewards = relationship("AccountHolderPendingReward", back_populates="account_holder")
    rewards = relationship("Reward", back_populates="account_holder")
    current_balances = relationship("AccountHolderCampaignBalance", back_populates="account_holder")
    marketing_preferences = relationship("AccountHolderMarketingPreference", back_populates="account_holder")
    transactions = relationship("AccountHolderTransactionHistory", back_populates="account_holder")

    __table_args__ = (UniqueConstraint("email", "retailer_id", name="email_retailer_unq"),)
    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.id}: {self.email}"  # pragma: no cover

    # @property
    # def marketing_opt_out_link(self) -> str:
    #     return (
    #         f"{settings.POLARIS_PUBLIC_URL}{settings.API_PREFIX}/{self.retailer.slug}/marketing/unsubscribe"
    #         f"?u={self.opt_out_token}"
    #     )


class AccountHolderProfile(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_profile"

    account_holder_id = Column(BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    date_of_birth = Column(Date, nullable=True)
    phone = Column(String, nullable=True)
    address_line1 = Column(String, nullable=True)
    address_line2 = Column(String, nullable=True)
    postcode = Column(String, nullable=True)
    city = Column(String, nullable=True)
    custom = Column(String, nullable=True)

    account_holder = relationship("AccountHolder", back_populates="profile")

    __mapper_args__ = {"eager_defaults": True}


class AccountHolderCampaignBalance(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_campaign_balance"

    account_holder_id = Column(BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True)
    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), index=True)
    balance = Column(Integer, nullable=False)

    account_holder = relationship("AccountHolder", back_populates="current_balances")
    campaign = relationship("Campaign", back_populates="pending_rewards")

    __table_args__ = (UniqueConstraint("account_holder_id", "campaign_id", name="account_holder_campaign_unq"),)


class AccountHolderMarketingPreference(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_marketing_preference"

    account_holder_id = Column(BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True)
    key_name = Column(String, nullable=False)
    value = Column(String, nullable=False)
    value_type = Column(Enum(MarketingPreferenceValueTypes), nullable=False)

    account_holder = relationship("AccountHolder", back_populates="marketing_preferences")


# # IDEMPOTENCY_TOKEN_PENDING_REWARD_UNQ_CONSTRAINT_NAME = "idempotency_token_account_holder_pending_reward_unq"


class AccountHolderPendingReward(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_pending_reward"

    pending_reward_uuid = Column(UUID(as_uuid=True), nullable=False)
    account_holder_id = Column(BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True)
    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), index=True)
    reward_config_id = Column(BigInteger, ForeignKey("reward_config.id", ondelete="CASCADE"), index=True)
    created_date = Column(DateTime, nullable=False)
    conversion_date = Column(DateTime, nullable=False)
    value = Column(Integer, nullable=False)
    # enqueued = Column(Boolean, server_default="false", nullable=False)
    count = Column(Integer, nullable=False)
    total_cost_to_user = Column(Integer, nullable=False)

    account_holder = relationship("AccountHolder", back_populates="pending_rewards")
    campaign = relationship("Campaign", back_populates="pending_rewards")
    reward_config = relationship("RewardConfig", back_populates="pending_rewards")

    @property
    def total_value(self) -> int:
        return self.count * self.value

    @property
    def slush(self) -> int:
        return self.total_cost_to_user - self.total_value

    @slush.setter
    def slush(self, value: PositiveInt) -> None:
        self.total_cost_to_user = self.total_value + value


class AccountHolderTransactionHistory(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_transaction_history"

    account_holder_id = Column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True, nullable=False
    )
    transaction_id = Column(String, nullable=False, unique=True)
    datetime = Column(DateTime, nullable=False)
    amount = Column(String, nullable=False)
    amount_currency = Column(String, nullable=False)
    location_name = Column(String, nullable=False)
    earned = Column(JSONB, nullable=False)

    account_holder = relationship("AccountHolder", back_populates="transactions")
