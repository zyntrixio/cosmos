from sqlalchemy import Column, DateTime, text

utc_timestamp_sql = text("TIMEZONE('utc', CURRENT_TIMESTAMP)")


class UpdatedAtMixin:
    """
    Required for any model that has an updated_at field.
    """

    updated_at = Column(
        DateTime,
        server_default=utc_timestamp_sql,
        onupdate=utc_timestamp_sql,
        nullable=False,
    )
