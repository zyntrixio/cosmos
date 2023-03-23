from flask_admin import Admin

from admin.views.retailer.main import (
    EmailTemplateAdmin,
    EmailTemplateKeyAdmin,
    EmailTypeAdmin,
    RetailerAdmin,
    RetailerFetchTypeAdmin,
    RetailerStoreAdmin,
)
from cosmos.db.models import EmailTemplate, EmailTemplateKey, EmailType, Retailer, RetailerFetchType, RetailerStore
from cosmos.db.session import scoped_db_session


def register_retailer_admin(admin: "Admin") -> None:
    retailer_management = "Retailer"
    admin.add_view(
        RetailerAdmin(
            Retailer,
            scoped_db_session,
            "Retailers",
            endpoint="retailers",
            category=retailer_management,
        )
    )
    admin.add_view(
        RetailerStoreAdmin(
            RetailerStore,
            scoped_db_session,
            "Retailer's Stores",
            endpoint="retailer-stores",
            category=retailer_management,
        )
    )
    admin.add_view(
        RetailerFetchTypeAdmin(
            RetailerFetchType,
            scoped_db_session,
            "Retailer's Fetch Types",
            endpoint="retailer-fetch-types",
            category=retailer_management,
        )
    )
    admin.add_view(
        EmailTypeAdmin(
            EmailType,
            scoped_db_session,
            "Email Types",
            endpoint="email-types",
            category=retailer_management,
        )
    )
    admin.add_view(
        EmailTemplateAdmin(
            EmailTemplate,
            scoped_db_session,
            "Email Templates",
            endpoint="email-templates",
            category=retailer_management,
        )
    )
    admin.add_view(
        EmailTemplateKeyAdmin(
            EmailTemplateKey,
            scoped_db_session,
            "Email Template Keys",
            endpoint="email-template-keys",
            category=retailer_management,
        )
    )
