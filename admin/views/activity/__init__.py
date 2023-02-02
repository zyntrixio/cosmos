from typing import TYPE_CHECKING

from admin.hubble.db.models import Activity
from admin.views.activity.main import ActivityAdmin
from cosmos.core.config import settings

if TYPE_CHECKING:
    from flask_admin import Admin


def register_hubble_admin(admin: "Admin") -> None:
    from admin.hubble.db.session import db_session

    hubble_menu_title = "Activity"
    admin.add_view(
        ActivityAdmin(
            Activity,
            db_session,
            "Activity",
            endpoint=f"{settings.ACTIVITY_MENU_PREFIX}/activity",
            category=hubble_menu_title,
        )
    )
