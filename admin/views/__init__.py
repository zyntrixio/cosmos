from flask_admin import Admin

from admin.config import admin_settings
from admin.views.main import CosmosAdminPanelIndexView

main_admin = Admin(
    name="Event Horizon",
    template_mode=admin_settings.ADMIN_TEMPLATE_MODE,
    index_view=CosmosAdminPanelIndexView(
        url=f"{admin_settings.ADMIN_ROUTE_BASE}/", menu_class_name=f"bg-{admin_settings.ADMIN_NAV_STYLE}"
    ),
    base_template="eh_master.html",
)
