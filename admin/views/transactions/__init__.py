from flask_admin import Admin

from admin.views.transactions.main import TransactionAdmin, TransactionEarnAdmin
from cosmos.db.models import Transaction, TransactionEarn
from cosmos.db.session import scoped_db_session


def register_transactions_admin(admin: "Admin") -> None:
    transaction_menu_title = "Transactions"
    admin.add_view(
        TransactionAdmin(
            Transaction,
            scoped_db_session,
            "Transactions",
            endpoint="transactions",
            category=transaction_menu_title,
        )
    )
    admin.add_view(
        TransactionEarnAdmin(
            TransactionEarn,
            scoped_db_session,
            "Transaction Earn",
            endpoint="transaction-earns",
            category=transaction_menu_title,
        )
    )
