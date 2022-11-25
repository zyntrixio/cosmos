# from typing import TYPE_CHECKING
import uuid

from uuid import uuid4

import yaml

# from cosmos.core.config import settings
from pydantic import PositiveInt
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from cosmos.accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes, RewardCap, TransactionProcessingStatuses
from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin
from cosmos.retailers.enums import EmailTemplateTypes, RetailerStatuses
from cosmos.rewards.enums import FileAgentType, RewardTypeStatuses, RewardUpdateStatuses


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
    # balance_adjustments = relationship("BalanceAdjustment", back_populates="account_holder")
    pending_rewards = relationship("AccountHolderPendingReward", back_populates="account_holder")
    rewards = relationship("Reward", back_populates="account_holder")
    current_balances = relationship("AccountHolderCampaignBalance", back_populates="account_holder")
    marketing_preferences = relationship("AccountHolderMarketingPreference", back_populates="account_holder")
    transaction_history = relationship("AccountHolderTransactionHistory", back_populates="account_holder")
    transactions = relationship("Transaction", back_populates="account_holder")

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
    campaign = relationship("Campaign", back_populates="current_balances")

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

    account_holder = relationship("AccountHolder", back_populates="transaction_history")


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
    pending_rewards = relationship("AccountHolderPendingReward", back_populates="campaign")
    current_balances = relationship("AccountHolderCampaignBalance", back_populates="campaign")
    rewards = relationship("Reward", back_populates="campaign")

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


class EmailTemplate(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "email_template"

    template_id = Column(String, nullable=False)
    type = Column(Enum(EmailTemplateTypes), nullable=False)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id"), index=True)

    retailer = relationship("Retailer", back_populates="email_templates")

    required_keys = relationship(
        "EmailTemplateKey",
        back_populates="email_templates",
        secondary="email_template_required_key",
    )

    __table_args__ = (UniqueConstraint("type", "retailer_id", name="type_retailer_unq"),)

    def __repr__(self) -> str:
        return f"{self.retailer.slug}: {self.type}"


class EmailTemplateKey(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "email_template_key"

    name = Column(String, nullable=False, unique=True)
    display_name = Column(String, nullable=False, server_default="")
    description = Column(String, nullable=False, server_default="")

    email_templates = relationship(
        "EmailTemplate",
        back_populates="required_keys",
        secondary="email_template_required_key",
    )

    def __repr__(self) -> str:
        return self.name


class EmailTemplateRequiredKey(Base, TimestampMixin):
    __tablename__ = "email_template_required_key"

    email_template_id = Column(Integer, ForeignKey("email_template.id", ondelete="CASCADE"), nullable=False)
    email_template_key_id = Column(Integer, ForeignKey("email_template_key.id", ondelete="CASCADE"), nullable=False)

    __table_args__ = (PrimaryKeyConstraint("email_template_id", "email_template_key_id"),)


class RetailerStore(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "retailer_store"

    store_name = Column(String, nullable=False)
    mid = Column(String, nullable=False, unique=True)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)

    retailer = relationship("Retailer", back_populates="stores")


class FetchType(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "fetch_type"

    name = Column(String, nullable=False, unique=True)
    required_fields = Column(Text, nullable=True)
    path = Column(String, nullable=False)

    retailer = relationship("Retailer", back_populates="fetch_types", secondary="retailer_fetch_type")
    reward_configs = relationship("RewardConfig", back_populates="fetch_type")

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: ({self.id}) {self.name}"


class RetailerFetchType(Base, TimestampMixin):
    __tablename__ = "retailer_fetch_type"

    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    fetch_type_id = Column(BigInteger, ForeignKey("fetch_type.id", ondelete="CASCADE"), nullable=False)
    agent_config = Column(Text, nullable=True)

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
    reward_config_id = Column(BigInteger, ForeignKey("reward_config.id"), nullable=False)
    account_holder_id = Column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True, nullable=True
    )
    code = Column(String, nullable=False, index=True)
    deleted = Column(Boolean, default=False, nullable=False)

    issued_date = Column(DateTime, nullable=False)
    expiry_date = Column(DateTime, nullable=False)
    # status = Column(Enum(AccountHolderRewardStatuses), nullable=False, default=AccountHolderRewardStatuses.ISSUED)
    redeemed_date = Column(DateTime, nullable=True)
    cancelled_date = Column(DateTime, nullable=True)
    associated_url = Column(String, nullable=False, server_default="")

    account_holder = relationship("AccountHolder", back_populates="rewards")

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
    pending_rewards = relationship("AccountHolderPendingReward", back_populates="reward_config")
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


class Transaction(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "transaction"

    account_holder_id = Column(BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), nullable=False)
    retailer_id = Column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), nullable=False)
    transaction_id = Column(String(128), nullable=False, index=True)
    amount = Column(Integer, nullable=False)
    mid = Column(String(128), nullable=False)
    datetime = Column(DateTime, nullable=False)
    payment_transaction_id = Column(String(128), nullable=True, index=True)
    status = Column(Enum(TransactionProcessingStatuses), nullable=True, index=True)

    account_holder = relationship("AccountHolder", back_populates="transactions")
    retailer = relationship("Retailer", back_populates="transactions")

    __table_args__ = (UniqueConstraint("transaction_id", "retailer_id", name="transaction_retailer_unq"),)
    __mapper_args__ = {"eager_defaults": True}


class TransactionCampaign(Base, TimestampMixin):
    __tablename__ = "transaction_campaign"

    transaction_id = Column(
        BigInteger, ForeignKey("transaction.id", ondelete="CASCADE"), nullable=False, primary_key=True
    )
    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), nullable=False, primary_key=True)


class Retailer(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "retailer"

    name = Column(String(128), nullable=False)
    slug = Column(String(32), index=True, unique=True, nullable=False)
    account_number_prefix = Column(String(6), nullable=False)
    account_number_length = Column(Integer, server_default=text("10"), nullable=False)
    profile_config = Column(Text, nullable=False)
    marketing_preference_config = Column(Text, nullable=False)
    loyalty_name = Column(String(64), nullable=False)
    status = Column(Enum(RetailerStatuses), nullable=False)

    account_holders = relationship("AccountHolder", back_populates="retailer")
    campaigns = relationship(Campaign, back_populates="retailer")
    reward_configs = relationship("RewardConfig", back_populates="retailer")
    transactions = relationship("Transaction", back_populates="retailer")
    stores = relationship("RetailerStore", back_populates="retailer")
    email_templates = relationship("EmailTemplate", back_populates="retailer")
    fetch_types = relationship("FetchType", secondary="retailer_fetch_type", back_populates="retailer")
    rewards = relationship("Reward", back_populates="retailer")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return str(self.name)  # pragma: no cover
