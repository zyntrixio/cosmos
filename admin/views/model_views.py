import logging

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from flask import abort, flash, redirect, session, url_for
from flask_admin.contrib.sqla import ModelView

from cosmos.core.config import settings

if TYPE_CHECKING:
    from werkzeug.wrappers import Response  # pragma: no cover


class UserSessionMixin:
    RO_AZURE_ROLES = {"Viewer"}
    RW_AZURE_ROLES = {"Admin", "Editor"}
    ALL_AZURE_ROLES = RW_AZURE_ROLES | RO_AZURE_ROLES

    @property
    def user_info(self) -> dict:
        return session.get("user", {})

    @property
    def user_session_expired(self) -> bool:
        session_exp: int | None = self.user_info.get("exp")
        return session_exp < datetime.now(tz=timezone.utc).timestamp() if session_exp else True

    @property
    def user_roles(self) -> set[str]:
        return set(self.user_info.get("roles", []))

    @property
    def user_is_authorized(self) -> bool:
        return bool(self.user_roles.intersection(self.ALL_AZURE_ROLES))

    @property
    def sso_username(self) -> str:
        return self.user_info["name"]

    @property
    def is_read_only_user(self) -> bool:
        return bool(self.user_roles.intersection(self.RO_AZURE_ROLES))

    @property
    def is_read_write_user(self) -> bool:
        return bool(self.user_roles.intersection(self.RW_AZURE_ROLES))


# custom admin classes needed for authorisation
class AuthorisedModelView(ModelView, UserSessionMixin):
    can_view_details = True

    @property
    def can_delete(self) -> bool:
        return False

    @property
    def can_create(self) -> bool:
        return self.is_read_write_user

    @property
    def can_edit(self) -> bool:
        return self.is_read_write_user

    def is_accessible(self) -> bool:
        if settings.TESTING:
            return True
        return not self.user_session_expired and self.user_is_authorized if self.user_info else False

    def inaccessible_callback(self, name: str, **kwargs: dict | None) -> "Response":  # noqa: ARG002
        if self.user_info and not self.user_is_authorized:
            return abort(403)
        session.pop("user", None)
        return redirect(url_for("auth_views.login"))

    def is_action_allowed(self, name: str) -> bool:
        if settings.TESTING:
            return True
        return self.can_delete if name == "delete" else self.can_edit  # noqa: PLR2004


class BaseModelView(AuthorisedModelView):
    """
    Set some baseline behaviour for all ModelViews
    """

    list_template = "eh_list.html"
    edit_template = "eh_edit.html"
    create_template = "eh_create.html"
    column_default_sort: None | str | tuple[str, bool] = ("created_at", True)
    form_excluded_columns: tuple[str, ...] = ("created_at", "updated_at")

    def get_list_columns(self) -> list[str]:
        # Shunt created_at and updated_at to the end of the table
        list_columns = super().get_list_columns()
        for name in ("created_at", "updated_at"):
            for i, (col_name, _disp_name) in enumerate(list_columns):
                if col_name == name:
                    list_columns += [list_columns.pop(i)]
                    break
        return list_columns

    def _flash_error_response(self, resp_json: list | dict) -> None:
        try:
            if isinstance(resp_json, list):
                for error in resp_json:
                    flash(
                        f"{error['display_message']} ::: {', '.join(error['campaigns'])}",
                        category="error",
                    )

            else:
                flash(resp_json["display_message"], category="error")
        except Exception as ex:  # noqa: BLE001
            msg = f"Unexpected response received: {resp_json}" if resp_json else "Unexpected response received"
            flash(msg, category="error")
            logging.exception(msg, exc_info=ex)


class CanDeleteModelView(BaseModelView):
    """
    Add delete permissions
    """

    fast_mass_delete = False

    @property
    def can_delete(self) -> bool:
        return self.can_edit
