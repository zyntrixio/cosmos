from admin.helpers.custom_formatters import account_holder_repr_transaction_earn, transaction_repr
from admin.views.model_views import BaseModelView


class TransactionAdmin(BaseModelView):
    column_list = (
        "retailer",
        "transaction_id",
        "processed",
        "amount",
        "mid",
        "datetime",
        "account_holder.account_holder_uuid",
        "payment_transaction_id",
        "created_at",
        "updated_at",
    )
    column_filters = ("retailer.slug", "created_at", "datetime", "processed")
    column_searchable_list = ("transaction_id", "payment_transaction_id", "account_holder.account_holder_uuid")
    column_labels = {
        "retailer.slug": "Retailer",
        "account_holder.account_holder_uuid": "Account Holder UUID",
    }
    column_formatters = {"store": lambda _v, _c, m, _p: m.store.store_name}


class TransactionEarnAdmin(BaseModelView):
    column_list = column_details_list = (
        "transaction.account_holder",
        "transaction",
        "transaction.amount",
        "transaction.datetime",
        "transaction.mid",
        "transaction.store.store_name",
        "earn_amount",
        "loyalty_type",
        "transaction.payment_transaction_id",
        "created_at",
    )
    column_filters = (
        "transaction.retailer.slug",
        "transaction.datetime",
        "transaction.store.store_name",
        "loyalty_type",
        "created_at",
    )

    column_formatters = {
        "transaction": transaction_repr,
        "transaction.account_holder": account_holder_repr_transaction_earn,
    }
    column_searchable_list = (
        "transaction.transaction_id",
        "transaction.account_holder.account_holder_uuid",
        "transaction.payment_transaction_id",
    )
    column_labels = {
        "transaction.datetime": "Transaction date",
        "transaction.transaction_id": "Transaction ID",
        "transaction.account_holder": "Account Holder",
        "transaction.payment_transaction_id": "Payment Transaction ID",
        "transaction.amount": "Transaction amount",
        "transaction.mid": "MID",
        "transaction.store.store_name": "Store name",
    }
