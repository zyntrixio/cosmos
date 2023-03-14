from typing import TYPE_CHECKING

from admin.hubble.db.models import Activity
from admin.views.activity.main import ActivityAdmin

if TYPE_CHECKING:
    from flask_admin import Admin


def register_hubble_admin(admin: "Admin") -> None:
    from admin.hubble.db.models import Base
    from admin.hubble.db.session import activity_scoped_session

    menu_title = "Activity"

    admin.add_view(
        ActivityAdmin(
            Activity,
            activity_scoped_session,
            "Activity",
            endpoint="activities",
            category=menu_title,
        )
    )
