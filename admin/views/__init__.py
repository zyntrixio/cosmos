from flask_admin import Admin

from admin.config import admin_settings
from admin.views.main import CosmosAdminPanelIndexView

main_admin = Admin(
    name="Cosmos Admin",
    template_mode="bootstrap4",
    index_view=CosmosAdminPanelIndexView(url=f"{admin_settings.ADMIN_ROUTE_BASE}/"),
    base_template="eh_master.html",
)
