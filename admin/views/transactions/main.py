from admin.views.model_views import BaseModelView


class TransactionAdmin(BaseModelView):
    column_list = (
        "retailer",
        "transaction_id",
        "amount",
        "mid",
        "datetime",
        "account_holder.account_holder_uuid",
        "payment_transaction_id",
        "created_at",
        "updated_at",
    )
    column_filters = ("retailer.slug", "created_at", "datetime")
    column_searchable_list = ("transaction_id", "payment_transaction_id", "account_holder.account_holder_uuid")
    column_labels = {
        "retailer.slug": "Retailer",
        "account_holder.account_holder_uuid": "Account Holder UUID",
    }


class TransactionEarnAdmin(BaseModelView):
    column_list = (
        "transaction_id",
        "transaction.account_holder.account_holder_uuid",
        "earn_amount",
        "loyalty_type",
        "transaction.payment_transaction_id",
        "created_at",
    )
    column_filters = ("loyalty_type", "created_at")
    column_searchable_list = (
        "transaction_id",
        "transaction.account_holder.account_holder_uuid",
        "transaction.payment_transaction_id",
    )
    column_labels = {
        "transaction.account_holder.account_holder_uuid": "Account Holder UUID",
        "transaction.payment_transaction_id": "Payment Transaction ID",
    }
