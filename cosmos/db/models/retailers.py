from sqlalchemy import (
    BigInteger,
    Column,
    Enum,
    ForeignKey,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import relationship

from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin
from cosmos.retailers.enums import EmailTemplateTypes, RetailerStatuses


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
    campaigns = relationship("Campaign", back_populates="retailer")
    reward_configs = relationship("RewardConfig", back_populates="retailer")
    transactions = relationship("Transaction", back_populates="retailer")
    processed_transactions = relationship("ProcessedTransaction", back_populates="retailer")
    stores = relationship("RetailerStore", back_populates="retailer")
    email_templates = relationship("EmailTemplate", back_populates="retailer")

    __mapper_args__ = {"eager_defaults": True}

    def __str__(self) -> str:
        return str(self.name)  # pragma: no cover


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
