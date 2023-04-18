import enum
import re
import uuid

from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlencode, urlsplit
from uuid import UUID, uuid4

import yaml

from pydantic import PositiveInt
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy import types as sqla_types
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.schema import Index

from cosmos.accounts.enums import AccountHolderStatuses, MarketingPreferenceValueTypes
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.core.utils import pence_integer_to_currency_string, raw_stamp_value_to_string
from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin
from cosmos.public.config import public_settings
from cosmos.retailers.enums import RetailerStatuses
from cosmos.rewards.enums import FileAgentType, RewardUpdateStatuses


class AccountHolder(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder"

    email: Mapped[str] = mapped_column(index=True)
    status: Mapped[AccountHolderStatuses] = mapped_column(default=AccountHolderStatuses.PENDING)
    account_number: Mapped[str | None] = mapped_column(index=True, unique=True)
    account_holder_uuid: Mapped[UUID] = mapped_column(sqla_types.UUID, default=uuid4, unique=True)
    opt_out_token: Mapped[UUID] = mapped_column(sqla_types.UUID, default=uuid4, unique=True)
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), index=True)

    retailer: Mapped["Retailer"] = relationship(back_populates="account_holders")
    profile: Mapped["AccountHolderProfile"] = relationship(uselist=False, back_populates="account_holder")
    pending_rewards: Mapped[list["PendingReward"]] = relationship(back_populates="account_holder")
    rewards: Mapped[list["Reward"]] = relationship(back_populates="account_holder")
    current_balances: Mapped[list["CampaignBalance"]] = relationship(back_populates="account_holder")
    marketing_preferences: Mapped[list["MarketingPreference"]] = relationship(back_populates="account_holder")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="account_holder")
    sent_emails: Mapped[list["AccountHolderEmail"]] = relationship(back_populates="account_holder")

    __table_args__ = (
        UniqueConstraint("email", "retailer_id", name="email_retailer_unq"),
        Index("ix_retailer_id_email_account_number", "retailer_id", "email", "account_number"),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.id}: {self.email}"  # pragma: no cover

    @property
    def marketing_opt_out_link(self) -> str:
        base_url = urlsplit(public_settings.core.PUBLIC_URL)
        relative_path = re.sub(
            "/{2,}",
            "/",
            f"/{base_url.path}/{public_settings.PUBLIC_API_PREFIX}/{self.retailer.slug}/marketing/unsubscribe",
        )

        return base_url._replace(
            path=relative_path,
            query=urlencode({"u": self.opt_out_token}),
        ).geturl()


class AccountHolderProfile(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_profile"

    account_holder_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    first_name: Mapped[str]
    last_name: Mapped[str]
    date_of_birth: Mapped[date | None]
    phone: Mapped[str | None]
    address_line1: Mapped[str | None]
    address_line2: Mapped[str | None]
    postcode: Mapped[str | None]
    city: Mapped[str | None]
    custom: Mapped[str | None]

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="profile")

    __mapper_args__ = {"eager_defaults": True}


class CampaignBalance(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "campaign_balance"

    account_holder_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    campaign_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), index=True)
    balance: Mapped[int]
    reset_date: Mapped[date | None]

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="current_balances")
    campaign: Mapped["Campaign"] = relationship(back_populates="current_balances")

    __table_args__ = (UniqueConstraint("account_holder_id", "campaign_id", name="account_holder_campaign_unq"),)


class MarketingPreference(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "marketing_preference"

    account_holder_id: Mapped[str] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    key_name: Mapped[str]
    value: Mapped[str]
    value_type: Mapped[MarketingPreferenceValueTypes]

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="marketing_preferences")


class PendingReward(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "pending_reward"

    pending_reward_uuid: Mapped[UUID] = mapped_column(sqla_types.UUID, default=uuid.uuid4)
    account_holder_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    campaign_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), index=True)
    created_date: Mapped[datetime]
    conversion_date: Mapped[datetime]
    value: Mapped[int]
    count: Mapped[int]
    total_cost_to_user: Mapped[int]

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="pending_rewards")
    campaign: Mapped["Campaign"] = relationship(back_populates="pending_rewards")

    @property
    def total_value(self) -> int:
        return self.count * self.value

    @property
    def slush(self) -> int:
        return self.total_cost_to_user - self.total_value

    @slush.setter
    def slush(self, value: PositiveInt) -> None:
        self.total_cost_to_user = self.total_value + value


class Campaign(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "campaign"

    status: Mapped[CampaignStatuses] = mapped_column(server_default="DRAFT")
    name: Mapped[str] = mapped_column(String(128))
    slug: Mapped[str] = mapped_column(String(100), index=True, unique=True)
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), index=True)
    loyalty_type: Mapped[LoyaltyTypes] = mapped_column(server_default="STAMPS")
    start_date: Mapped[datetime | None]
    end_date: Mapped[datetime | None]

    retailer: Mapped["Retailer"] = relationship(back_populates="campaigns")
    earn_rule: Mapped["EarnRule"] = relationship(cascade="all,delete", back_populates="campaign", uselist=False)
    reward_rule: Mapped["RewardRule"] = relationship(cascade="all,delete", back_populates="campaign", uselist=False)
    pending_rewards: Mapped[list["PendingReward"]] = relationship(back_populates="campaign")
    current_balances: Mapped[list["CampaignBalance"]] = relationship(back_populates="campaign")
    rewards: Mapped[list["Reward"]] = relationship(back_populates="campaign")
    sent_emails: Mapped[list["AccountHolderEmail"]] = relationship(back_populates="campaign")

    def __str__(self) -> str:  # pragma: no cover
        return f"{self.name} ({self.slug})"

    def is_activable(self) -> bool:
        return self.status == CampaignStatuses.DRAFT and self.reward_rule is not None and self.earn_rule is not None


class EarnRule(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "earn_rule"

    campaign_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"))
    threshold: Mapped[int]
    increment: Mapped[int | None]
    increment_multiplier = mapped_column(Numeric(scale=2), default=1)
    max_amount: Mapped[int] = mapped_column(server_default="0")

    campaign: Mapped["Campaign"] = relationship(back_populates="earn_rule")


class RewardRule(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_rule"

    campaign_id: Mapped[int] = mapped_column(Integer, ForeignKey("campaign.id", ondelete="CASCADE"), unique=True)
    reward_config_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("reward_config.id", ondelete="CASCADE"))
    reward_goal: Mapped[int]
    allocation_window: Mapped[int | None] = mapped_column(
        CheckConstraint("allocation_window > 0 OR allocation_window IS NULL", name="allocation_window_check"),
    )
    reward_cap: Mapped[int | None] = mapped_column(
        CheckConstraint("(reward_cap >= 1 and reward_cap <= 10) OR reward_cap IS NULL", name="reward_cap_check"),
    )

    campaign: Mapped["Campaign"] = relationship(back_populates="reward_rule")
    reward_config: Mapped["RewardConfig"] = relationship(back_populates="reward_rules")


class EmailType(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "email_type"

    slug: Mapped[str] = mapped_column(unique=True, index=True)
    send_email_params_fn: Mapped[str | None]
    required_fields: Mapped[str | None] = mapped_column(Text)

    email_templates: Mapped[list["EmailTemplate"]] = relationship(back_populates="email_type")
    sent_emails: Mapped[list["AccountHolderEmail"]] = relationship(back_populates="email_type")

    def __repr__(self) -> str:
        return self.slug


class EmailTemplate(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "email_template"

    template_id: Mapped[str]
    required_fields_values: Mapped[str | None] = mapped_column(Text)
    email_type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("email_type.id", ondelete="CASCADE"))
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"), index=True)

    email_type: Mapped["EmailType"] = relationship(back_populates="email_templates")
    retailer: Mapped["Retailer"] = relationship(back_populates="email_templates")

    required_keys: Mapped[list["EmailTemplateKey"]] = relationship(
        back_populates="email_templates", secondary="email_template_required_key"
    )

    __table_args__ = (UniqueConstraint("email_type_id", "retailer_id", name="type_retailer_unq"),)

    def __repr__(self) -> str:
        return f"{self.retailer.slug}: {self.email_type.slug}"

    def load_required_fields_values(self) -> dict:
        return yaml.safe_load(self.required_fields_values) if self.required_fields_values else {}


class EmailTemplateKey(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "email_template_key"

    name: Mapped[str] = mapped_column(unique=True)
    display_name: Mapped[str] = mapped_column(server_default="")
    description: Mapped[str] = mapped_column(server_default="")

    email_templates: Mapped[list["EmailTemplate"]] = relationship(
        back_populates="required_keys", secondary="email_template_required_key"
    )

    def __repr__(self) -> str:
        return self.name


class EmailTemplateRequiredKey(Base, TimestampMixin):
    __tablename__ = "email_template_required_key"

    email_template_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("email_template.id", ondelete="CASCADE"))
    email_template_key_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("email_template_key.id", ondelete="CASCADE")
    )

    __table_args__ = (PrimaryKeyConstraint("email_template_id", "email_template_key_id"),)


class AccountHolderEmail(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "account_holder_email"

    account_holder_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    email_type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("email_type.id", ondelete="CASCADE"), index=True)
    campaign_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), index=True
    )
    retry_task_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("retry_task.retry_task_id", ondelete="CASCADE"), index=True, unique=True
    )
    message_uuid: Mapped[UUID | None] = mapped_column(sqla_types.UUID, index=True, unique=True)
    current_status: Mapped[str | None] = mapped_column(
        String,
    )
    allow_re_send: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="sent_emails")
    email_type: Mapped["EmailType"] = relationship(back_populates="sent_emails")
    campaign: Mapped["Campaign | None"] = relationship(back_populates="sent_emails")


class RetailerStore(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "retailer_store"

    store_name: Mapped[str]
    mid: Mapped[str] = mapped_column(unique=True)
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"))

    retailer: Mapped["Retailer"] = relationship(back_populates="stores")


class FetchType(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "fetch_type"

    name: Mapped[str] = mapped_column(unique=True)
    required_fields: Mapped[str | None] = mapped_column(Text)
    path: Mapped[str]

    retailer: Mapped["Retailer | None"] = relationship(
        back_populates="fetch_types", secondary="retailer_fetch_type", uselist=False
    )
    reward_configs: Mapped[list["RewardConfig"]] = relationship(back_populates="fetch_type")
    retailer_fetch_type: Mapped["RetailerFetchType | None"] = relationship(
        back_populates="fetch_type", viewonly=True, uselist=False
    )

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: ({self.id}) {self.name}"


class RetailerFetchType(Base, TimestampMixin):
    __tablename__ = "retailer_fetch_type"

    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"))
    fetch_type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fetch_type.id", ondelete="CASCADE"))
    agent_config: Mapped[str | None] = mapped_column(Text)

    fetch_type: Mapped["FetchType"] = relationship(back_populates="retailer_fetch_type", overlaps="retailer")
    retailer: Mapped["Retailer"] = relationship(back_populates="retailer_fetch_type", overlaps="retailer")

    __table_args__ = (PrimaryKeyConstraint("retailer_id", "fetch_type_id"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}: {self.retailer_id} - {self.fetch_type}"

    def load_agent_config(self) -> dict:
        return yaml.safe_load(self.agent_config) if self.agent_config else {}


class Reward(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward"

    reward_uuid: Mapped[UUID] = mapped_column(sqla_types.UUID, default=uuid.uuid4)
    reward_config_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("reward_config.id"))
    account_holder_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    code: Mapped[str] = mapped_column(index=True)
    deleted: Mapped[bool] = mapped_column(default=False)
    issued_date: Mapped[datetime | None]
    expiry_date: Mapped[datetime | None]
    redeemed_date: Mapped[datetime | None]
    cancelled_date: Mapped[datetime | None]
    associated_url: Mapped[str] = mapped_column(server_default="")

    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"))
    campaign_id: Mapped[int | None] = mapped_column(  # Set when issued
        BigInteger, ForeignKey("campaign.id", ondelete="SET NULL")
    )
    reward_file_log_id: Mapped[int | None] = mapped_column(  # nullable - backwards compat
        BigInteger, ForeignKey("reward_file_log.id", ondelete="SET NULL")
    )

    account_holder: Mapped["AccountHolder | None"] = relationship(back_populates="rewards")
    reward_config: Mapped["RewardConfig"] = relationship(back_populates="rewards")
    retailer: Mapped["Retailer"] = relationship(back_populates="rewards")
    campaign: Mapped["Campaign | None"] = relationship(back_populates="rewards")
    reward_updates: Mapped[list["RewardUpdate"]] = relationship(back_populates="reward")
    reward_file_log: Mapped["RewardFileLog | None"] = relationship(back_populates="rewards")

    class RewardStatuses(enum.Enum):
        UNALLOCATED = "unallocated"
        ISSUED = "issued"
        CANCELLED = "cancelled"
        REDEEMED = "redeemed"
        EXPIRED = "expired"

    @property
    def status(self) -> RewardStatuses:
        if self.account_holder_id:
            if self.redeemed_date:
                return self.RewardStatuses.REDEEMED
            if self.cancelled_date:
                return self.RewardStatuses.CANCELLED
            if self.expiry_date and datetime.now(tz=UTC) >= self.expiry_date.replace(tzinfo=UTC):
                return self.RewardStatuses.EXPIRED
            return self.RewardStatuses.ISSUED
        return self.RewardStatuses.UNALLOCATED

    __table_args__ = (
        UniqueConstraint(
            "code",
            "retailer_id",
            "reward_config_id",  # https://hellobink.atlassian.net/browse/BPL-244 - check this requirement again
            name="code_retailer_reward_config_unq",
        ),
    )
    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.code}, {self.status.value})"


class RewardConfig(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_config"

    slug: Mapped[str] = mapped_column(index=True)
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"))
    fetch_type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fetch_type.id", ondelete="CASCADE"))
    active: Mapped[bool] = mapped_column(default=True)
    required_fields_values: Mapped[str | None] = mapped_column(Text)

    rewards: Mapped[list["Reward"]] = relationship(back_populates="reward_config")
    retailer: Mapped["Retailer"] = relationship(back_populates="reward_configs")
    fetch_type: Mapped["FetchType"] = relationship(back_populates="reward_configs")
    reward_rules: Mapped[list["RewardRule"]] = relationship(back_populates="reward_config")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("slug", "retailer_id", name="slug_retailer_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.retailer.slug}, " f"{self.id})"

    def load_required_fields_values(self) -> dict:
        return yaml.safe_load(self.required_fields_values) if self.required_fields_values else {}


class RewardUpdate(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_update"

    reward_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("reward.id", ondelete="CASCADE"))
    date: Mapped[date]
    status: Mapped[RewardUpdateStatuses]

    reward: Mapped["Reward"] = relationship(back_populates="reward_updates")

    __mapper_args__ = {"eager_defaults": True}

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"


class RewardFileLog(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "reward_file_log"

    file_name: Mapped[str] = mapped_column(String(500), index=True)
    file_agent_type: Mapped[FileAgentType] = mapped_column(index=True)

    rewards: Mapped[list["Reward"]] = relationship(back_populates="reward_file_log")

    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (UniqueConstraint("file_name", "file_agent_type", name="file_name_file_agent_type_unq"),)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({self.id})"


class Transaction(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "transaction"

    account_holder_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("account_holder.id", ondelete="CASCADE"), index=True
    )
    retailer_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("retailer.id", ondelete="CASCADE"))
    transaction_id: Mapped[str] = mapped_column(String(128), index=True)
    amount: Mapped[int]
    mid: Mapped[str] = mapped_column(String(128), index=True)
    datetime: Mapped[datetime]
    payment_transaction_id: Mapped[str | None] = mapped_column(String(128), index=True)
    processed: Mapped[bool | None] = mapped_column(
        Boolean,
        CheckConstraint("processed IS NULL OR processed IS TRUE", name="processed_null_or_true_check"),
        index=True,
    )

    account_holder: Mapped["AccountHolder"] = relationship(back_populates="transactions")
    retailer: Mapped["Retailer"] = relationship(back_populates="transactions")
    store: Mapped["RetailerStore | None"] = relationship(
        uselist=False, primaryjoin="Transaction.mid==RetailerStore.mid", foreign_keys=mid
    )
    transaction_earn: Mapped["TransactionEarn | None"] = relationship(uselist=False, back_populates="transaction")

    __table_args__ = (
        UniqueConstraint("transaction_id", "retailer_id", "processed", name="transaction_retailer_processed_unq"),
    )
    __mapper_args__ = {"eager_defaults": True}

    def humanized_transaction_amount(self, currency_sign: bool = False) -> str:
        return pence_integer_to_currency_string(self.amount, "GBP", currency_sign=currency_sign)


class TransactionEarn(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "transaction_earn"

    transaction_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("transaction.id", ondelete="CASCADE"))
    loyalty_type: Mapped[LoyaltyTypes]
    earn_amount: Mapped[int]

    transaction: Mapped["Transaction"] = relationship(uselist=False, back_populates="transaction_earn")

    def humanized_earn_amount(self, currency_sign: bool = False) -> str:
        return (
            pence_integer_to_currency_string(self.earn_amount, "GBP", currency_sign=currency_sign)
            if self.loyalty_type == LoyaltyTypes.ACCUMULATOR
            else raw_stamp_value_to_string(self.earn_amount, stamp_suffix=currency_sign)
        )


RETAILER_BALANCE_RESET_ADVANCED_WARNING_DAYS_CHECK = """(
            (
                (balance_reset_advanced_warning_days > 0)
                AND (
                balance_reset_advanced_warning_days < balance_lifespan
                AND balance_lifespan IS NOT NULL
                OR balance_reset_advanced_warning_days = NULL
                )
            ) AND
            (
                (balance_lifespan > 0 AND balance_reset_advanced_warning_days > 0)
                OR (balance_lifespan = NULL AND balance_reset_advanced_warning_days = NULL)
            )
        )
        """


class Retailer(IdPkMixin, Base, TimestampMixin):
    __tablename__ = "retailer"

    name: Mapped[str] = mapped_column(String(128))
    slug: Mapped[str] = mapped_column(String(32), index=True, unique=True)
    account_number_prefix: Mapped[str] = mapped_column(String(6))
    account_number_length: Mapped[int] = mapped_column(server_default=text("10"))
    profile_config: Mapped[str] = mapped_column(Text)
    marketing_preference_config: Mapped[str] = mapped_column(Text)
    loyalty_name: Mapped[str] = mapped_column(String(64))
    status: Mapped[RetailerStatuses]
    balance_lifespan: Mapped[int | None] = mapped_column(
        CheckConstraint(
            "balance_lifespan IS NULL OR balance_lifespan > 0", name="balance_lifespan_positive_int_or_null_check"
        ),
    )
    balance_reset_advanced_warning_days: Mapped[int | None] = mapped_column(
        CheckConstraint(
            RETAILER_BALANCE_RESET_ADVANCED_WARNING_DAYS_CHECK,
            name="balance_reset_check",
        ),
    )

    account_holders: Mapped[list["AccountHolder"]] = relationship(back_populates="retailer")
    campaigns: Mapped[list["Campaign"]] = relationship(back_populates="retailer")
    reward_configs: Mapped[list["RewardConfig"]] = relationship(back_populates="retailer")
    transactions: Mapped[list["Transaction"]] = relationship(back_populates="retailer")
    stores: Mapped[list["RetailerStore"]] = relationship(back_populates="retailer")
    email_templates: Mapped[list["EmailTemplate"]] = relationship(back_populates="retailer")
    fetch_types: Mapped[list["FetchType"]] = relationship(
        secondary="retailer_fetch_type", back_populates="retailer", viewonly=True
    )
    rewards: Mapped[list["Reward"]] = relationship(back_populates="retailer")
    retailer_fetch_type: Mapped["RetailerFetchType"] = relationship(back_populates="retailer", overlaps="retailer")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return f"{self.name} ({self.slug})"

    @property
    def current_balance_reset_date(self) -> date | None:
        if self.balance_lifespan:
            return datetime.now(tz=UTC).date() + timedelta(days=self.balance_lifespan)
        return None
