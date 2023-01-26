from flask_admin import Admin

from admin.db.session import db_session
from admin.views.transactions.main import TransactionAdmin
from cosmos.core.config import settings
from cosmos.db.models import Transaction


def register_transactions_admin(admin: "Admin") -> None:
    transaction_menu_title = "Transactions"
    admin.add_view(
        TransactionAdmin(
            Transaction,
            db_session,
            "Transactions",
            endpoint=f"{settings.TRANSACTIONS_MENU_PREFIX}/transactions",
            category=transaction_menu_title,
        )
    )
