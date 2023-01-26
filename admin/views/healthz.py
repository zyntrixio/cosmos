from typing import Any

from flask.blueprints import Blueprint
from sqlalchemy.sql import text

from admin.db.session import db_session

healthz_bp = Blueprint("healthz", __name__)


@healthz_bp.route("/livez", methods=["GET"])
def livez() -> Any:  # noqa: ANN401
    return {}


@healthz_bp.route("/readyz", methods=["GET"])
def readyz() -> Any:  # noqa: ANN401
    payload = {}
    status_code = 200
    db_errors = []

    try:
        db_session.execute(text("SELECT 1"))
    except Exception as ex:  # noqa: BLE001
        db_errors.append(f"failed to connect to cosmos database due to error: {ex!r}")

    if db_errors:
        payload = {"postgres": db_errors}
        status_code = 500

    return payload, status_code
