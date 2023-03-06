from typing import TYPE_CHECKING

from authlib.integrations.base_client.errors import MismatchingStateError
from flask import Blueprint, redirect, session, url_for

from admin.app import oauth
from admin.config import admin_settings

if TYPE_CHECKING:
    from werkzeug.wrappers import Response

auth_bp = Blueprint("auth_views", __name__, url_prefix=admin_settings.ADMIN_ROUTE_BASE)


@auth_bp.route("/login/")
def login() -> "Response":
    redirect_uri = admin_settings.OAUTH_REDIRECT_URI or url_for("auth_views.authorize", _external=True)
    return oauth.cosmos_admin.authorize_redirect(redirect_uri)


@auth_bp.route("/logout/")
def logout() -> "Response":
    # session['user'] will always be set again as long
    # as your AAD session is still alive.
    session.pop("user", None)
    return redirect(url_for("admin.index"))


@auth_bp.route("/authorize/")
def authorize() -> "Response":
    try:
        token = oauth.cosmos_admin.authorize_access_token()
    except MismatchingStateError:
        return redirect(url_for("auth_views.login"))

    if userinfo := token.get("userinfo"):
        session["user"] = userinfo

    return redirect(url_for("admin.index"))
