from flask_admin import Admin

from admin.views.main import CosmosAdminPanelIndexView
from cosmos.core.config import settings

main_admin = Admin(
    name="Event Horizon",
    template_mode="bootstrap3",  # Note: checkbox validation errors (invalid-feedback) are not displayed with bootstrap4
    index_view=CosmosAdminPanelIndexView(url=f"{settings.ADMIN_ROUTE_BASE}/"),
    base_template="eh_master.html",
)
