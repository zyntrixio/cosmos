"""Script to generate entity-relational diagram from sqlalchemy models"""

import json
import logging

import requests
import typer

from eralchemy2 import render_er
from pydantic import BaseModel, Field, parse_obj_as
from requests import Response
from requests.auth import HTTPBasicAuth

from cosmos.core.config import core_settings
from cosmos.db.base import Base

script = typer.Typer()

auth = HTTPBasicAuth(core_settings.CONFLUENCE_USER, core_settings.CONFLUENCE_API_TOKEN)
headers = {"Accept": "application/json"}


class ConfluencePageVersionSchema(BaseModel):
    author_id: str | None = Field(None, alias="authorId")
    created_at: str | None = Field(None, alias="createdAt")
    message: str
    minor_edit: bool | None = Field(None, alias="minorEdit")
    number: int


class ConfluenceStorageObjectSchema(BaseModel):
    value: str
    representation: str


class ConfluencePageBodySchema(BaseModel):
    storage: ConfluenceStorageObjectSchema


class GetPageResponseSchema(BaseModel):
    author_id: str = Field(alias="authorId")
    body: ConfluencePageBodySchema
    created_at: str = Field(alias="createdAt")
    id: int  # noqa: A003
    parent_id: int = Field(alias="parentId")
    space_id: int = Field(alias="spaceId")
    status: str
    title: str
    version: ConfluencePageVersionSchema


class UpdatePageRequestSchema(BaseModel):
    id: int  # noqa: A003
    status: str
    title: str
    spaceId: int = Field(alias="space_id")  # noqa: N815
    body: ConfluenceStorageObjectSchema
    version: ConfluencePageVersionSchema


def generate_erd(fmt: str) -> None:
    """Uses eralchemy package to generate a schema diagram from SQLAlchemy models"""
    try:
        render_er(Base, f"{core_settings.CONFLUENCE_ATTACHMENT_NAME}.{fmt}")
    except Exception as ex:
        logging.exception("Failed to generate db schema diagrams", exc_info=ex)
        raise SystemExit(1) from ex


def upload_erd() -> None:
    """Confluence API request to upload a png file to a confluence page as an attachment. If one already exists with
    the same name, it will replace it with a new version of the attachment. Version history is handled by confluence.
    """
    api_url = (
        f"{core_settings.CONFLUENCE_API_BASE_URL}/rest/api/content/{core_settings.CONFLUENCE_PAGE_ID}/child/attachment"
    )

    with open(f"{core_settings.CONFLUENCE_ATTACHMENT_NAME}.png", "rb") as png_file:
        response = requests.request(
            "PUT",
            api_url,
            headers=headers,
            auth=auth,
            files={"file": png_file},
        )

        if response.status_code == 200:  # noqa: PLR2004
            logging.info("Successfully upload diagram to confluence")
        else:
            logging.error(f"Failed to upload diagram to confluence API. Response status: {response.status_code}")
            logging.error(f"Response body: {response.text}")
            raise SystemExit(1)


def _get_page_by_id(page_url: str) -> Response:
    """Confluence API request to retrieve a page and it's contents"""
    page_storage_url = f"{page_url}?body-format=storage"
    response = requests.request("GET", page_storage_url, headers=headers, auth=auth)

    if response.status_code != 200:  # noqa: PLR2004
        logging.error(f"Failed to fetch to confluence page. Response status: {response.status_code}")
        logging.error(f"Response body: {response.text}")
        raise SystemExit(1)

    return response


def _update_page_by_id(
    page_url: str,
    page_resp: GetPageResponseSchema,
    schema_env: str,
    version: str,
    message: str,
) -> None:
    """Confluence API request to update a confluence page title with updated app version"""
    headers["Content-Type"] = "application/json"

    request_payload = UpdatePageRequestSchema(
        id=page_resp.id,
        status="current",
        title=f"Cosmos {schema_env} Schema: v{version}",
        space_id=page_resp.space_id,
        body=ConfluenceStorageObjectSchema(value=page_resp.body.storage.value, representation="storage"),
        version=ConfluencePageVersionSchema(number=int(page_resp.version.number) + 1, message=message),
    )

    response = requests.request(
        "PUT",
        page_url,
        data=json.dumps(request_payload.dict(exclude_unset=True)),
        headers=headers,
        auth=auth,
    )

    if response.status_code == 200:  # noqa: PLR2004
        logging.info("Successfully update confluence page title with new version")
    else:
        logging.error(f"Failed to update confluence page version. Response status: {response.status_code}")
        logging.error(f"Response body: {response.text}")
        raise SystemExit(1)


def update_page_version(schema_env: str, version: str, message: str) -> None:
    page_url = f"{core_settings.CONFLUENCE_API_BASE_URL}/api/v2/pages/{core_settings.CONFLUENCE_PAGE_ID}"

    # Get page contents
    resp = _get_page_by_id(page_url=page_url)
    page_resp = parse_obj_as(GetPageResponseSchema, resp.json())

    # Update page with new contents
    _update_page_by_id(page_url=page_url, page_resp=page_resp, schema_env=schema_env, version=version, message=message)


@script.command()
def cli(
    update: bool = typer.Option(
        False,
        "--update",
        help="Update remote confluence page. Omit to only generate graph locally",
    ),
    fmt: str = typer.Option("png", "--fmt", "-o", help="Output format. .dot|.png|.jpg|.er|.md"),
    schema_env: str = typer.Option("", "--schema-env", help="Schema env to use for page title"),
    version: str = typer.Option("", "--version", help="Version to use for diagram title"),
    message: str = typer.Option("", "--message", help="Optional Message to store with the update version"),
) -> None:
    generate_erd(fmt=fmt)
    # The confluence page is setup to only show the png attachment
    if update:
        if fmt != "png":
            logging.error("Confluence page not updated. Can only update with .png format")
            return

        upload_erd()
        update_page_version(schema_env=schema_env, version=version, message=message)


if __name__ == "__main__":
    script()
