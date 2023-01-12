import requests

from cosmos.core.config import settings
from cosmos.db.models import AccountHolder

from . import logger, send_request_with_metrics


class SendEmailFalseError(Exception):
    pass


def send_email_to_mailjet(account_holder: AccountHolder, template_id: str, email_variables: dict) -> requests.Response:
    logger.info(f"Sending email to mailjet for {account_holder.account_holder_uuid}, template id: {template_id}")
    if settings.SEND_EMAIL:
        resp = send_request_with_metrics(
            method="POST",
            url_template="{url}",
            url_kwargs={"url": settings.MAILJET_API_URL},
            exclude_from_label_url=[],
            auth=(settings.MAILJET_API_PUBLIC_KEY, settings.MAILJET_API_SECRET_KEY),
            json={
                "Messages": [
                    {
                        "To": [
                            {
                                "Email": account_holder.email,
                                "Name": account_holder.profile.first_name + " " + account_holder.profile.last_name,
                            }
                        ],
                        "TemplateID": int(template_id),
                        "TemplateLanguage": True,
                        "Variables": email_variables,
                    }
                ]
            },
        )

        return resp
    raise SendEmailFalseError(f"SEND_EMAIL = {settings.SEND_EMAIL}")
