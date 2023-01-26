from typing import TYPE_CHECKING

from flask import redirect, url_for
from flask_admin import AdminIndexView, expose

from admin.views.model_views import UserSessionMixin

if TYPE_CHECKING:
    from werkzeug.wrappers import Response


class CosmosAdminPanelIndexView(AdminIndexView, UserSessionMixin):
    @expose("/")
    def index(self) -> "Response":
        if not self.user_info or self.user_session_expired:
            return redirect(url_for("auth_views.login"))
        return super().index()
