from sqlalchemy import BigInteger, Column, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from cosmos.campaigns.enums import TransactionProcessingStatuses
from cosmos.db.base_class import Base, IdPkMixin, TimestampMixin


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

    __table_args__ = (UniqueConstraint("transaction_id", "retailer_id", name="transaction_retailer_unq"),)
    __mapper_args__ = {"eager_defaults": True}


class TransactionCampaign(Base, TimestampMixin):
    __tablename__ = "transaction_campaign"

    transaction_id = Column(
        BigInteger, ForeignKey("transaction.id", ondelete="CASCADE"), nullable=False, primary_key=True
    )
    campaign_id = Column(BigInteger, ForeignKey("campaign.id", ondelete="CASCADE"), nullable=False, primary_key=True)
