from flask_admin import Admin

from admin.views.main import CosmosAdminPanelIndexView
from cosmos.core.config import settings

main_admin = Admin(
    name="Event Horizon",
    template_mode=settings.ADMIN_TEMPLATE_MODE,
    index_view=CosmosAdminPanelIndexView(
        url=f"{settings.ADMIN_ROUTE_BASE}/", menu_class_name=f"bg-{settings.ADMIN_NAV_STYLE}"
    ),
    base_template="eh_master.html",
)
