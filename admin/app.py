import logging

from typing import Any

import sentry_sdk

from authlib.integrations.flask_client import OAuth
from flask import Blueprint, Flask, Response
from flask_wtf.csrf import CSRFProtect
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from admin.db.session import db_session
from admin.hubble.db import db_session as hubble_db_session
from admin.hubble.db.models import Base as HubbleModelBase
from admin.hubble.db.session import engine as hubble_engine
from admin.version import __version__
from admin.views import main_admin
from cosmos.core.config import settings

oauth = OAuth()
oauth.register(
    "event_horizon",  # FIXME: This is the registered app name on Azure AD
    server_metadata_url=settings.OAUTH_SERVER_METADATA_URL,
    client_kwargs={"scope": "openid profile email"},
)


class RelativeLocationHeaderResponse(Response):
    # Below setting allows relative location headers, allowing us to redirect
    # without having to hardcode the Azure Front Door host to all redirects
    autocorrect_location_header = False


def create_app() -> Flask:
    HubbleModelBase.prepare(hubble_engine, reflect=True)

    from admin.views.accounts import register_customer_admin
    from admin.views.activity import register_hubble_admin
    from admin.views.auth import auth_bp
    from admin.views.campaign_reward import register_campaign_and_reward_management_admin
    from admin.views.healthz import healthz_bp
    from admin.views.retailer import register_retailer_admin
    from admin.views.transactions import register_transactions_admin

    sqla_logger = logging.getLogger("sqlalchemy.engine")
    sqla_logger.setLevel(settings.ADMIN_QUERY_LOG_LEVEL)
    sqla_handler = logging.StreamHandler()
    sqla_handler.setLevel(level=settings.ADMIN_QUERY_LOG_LEVEL)
    sqla_logger.addHandler(sqla_handler)

    if settings.SENTRY_DSN is not None:
        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            integrations=[FlaskIntegration(), SqlalchemyIntegration()],
            environment=settings.SENTRY_ENV,
            release=__version__,
            traces_sample_rate=0.0,
        )

    app = Flask(__name__)
    app.config.from_object(settings)
    app.response_class = RelativeLocationHeaderResponse

    register_customer_admin(main_admin)
    register_retailer_admin(main_admin)
    register_campaign_and_reward_management_admin(main_admin)
    register_transactions_admin(main_admin)
    register_hubble_admin(main_admin)

    main_admin.init_app(app)
    oauth.init_app(app)
    CSRFProtect(app)

    app.register_blueprint(auth_bp)
    app.register_blueprint(healthz_bp)

    eh_bp = Blueprint("eh", __name__, static_url_path=f"{settings.ADMIN_ROUTE_BASE}/eh/static", static_folder="static")
    app.register_blueprint(eh_bp)

    @app.teardown_appcontext
    def remove_session(exception: BaseException | None = None) -> Any:  # noqa: ARG001, ANN401
        db_session.remove()
        hubble_db_session.remove()

    return app
