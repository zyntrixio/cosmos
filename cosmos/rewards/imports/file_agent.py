import csv
import logging
import string

from collections import defaultdict
from datetime import date, datetime, timezone
from functools import lru_cache
from io import StringIO
from typing import TYPE_CHECKING, Callable, DefaultDict, NamedTuple

import sentry_sdk

from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobClient, BlobLeaseClient, BlobServiceClient  # pylint: disable=unused-import
from pydantic import ValidationError
from sqlalchemy import bindparam, update
from sqlalchemy.future import select
from sqlalchemy.sql import and_, not_, or_

from cosmos.core.config import settings
from cosmos.core.scheduled_tasks.scheduler import acquire_lock, cron_scheduler
from cosmos.db.base_class import sync_run_query
from cosmos.db.models import Retailer, Reward, RewardConfig, RewardFileLog, RewardUpdate
from cosmos.db.session import SyncSessionMaker
from cosmos.rewards.enums import FileAgentType, RewardTypeStatuses, RewardUpdateStatuses
from cosmos.rewards.schemas import RewardUpdateSchema

logger = logging.getLogger("reward-import")

if TYPE_CHECKING:  # pragma: no cover
    from azure.storage.blob import BlobProperties
    from sqlalchemy.orm import Session


class RewardUpdateRow(NamedTuple):
    data: RewardUpdateSchema
    row_num: int


class BlobProcessingError(Exception):
    pass


class RewardConfigNotActiveError(Exception):
    def __init__(self, slug: str, *args: object) -> None:
        self.slug = slug
        super().__init__(*args)


class BlobFileAgent:
    blob_path_template = string.Template("")  # Override in subclass
    scheduler_name = "carina-blob-file-agent"

    def __init__(self) -> None:
        self.file_agent_type: FileAgentType
        self.container_name = settings.BLOB_IMPORT_CONTAINER
        self.schedule = settings.BLOB_IMPORT_SCHEDULE
        blob_client_logger = logging.getLogger("blob-client")
        blob_client_logger.setLevel(settings.BLOB_IMPORT_LOGGING_LEVEL)
        self.blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
            settings.BLOB_STORAGE_DSN, logger=blob_client_logger
        )
        # type hints for blob storage still not working properly, remove ignores if it gets fixed.
        try:
            self.blob_service_client.create_container(self.container_name)  # type: ignore
        except ResourceExistsError:
            pass  # this is fine
        self.container_client = self.blob_service_client.get_container_client(self.container_name)  # type: ignore

    def _blob_name_is_duplicate(self, db_session: "Session", file_name: str) -> bool:
        file_name = sync_run_query(
            lambda: db_session.execute(
                select(RewardFileLog.file_name).where(
                    RewardFileLog.file_agent_type == self.file_agent_type,
                    RewardFileLog.file_name == file_name,
                )
            ).scalar_one_or_none(),
            db_session,
        )

        return file_name is not None

    @staticmethod
    def _log_warn_and_alert(msg: str) -> None:
        logger.warning(msg)
        sentry_sdk.capture_message(msg)

    @staticmethod
    def get_retailers(db_session: "Session") -> list[Retailer]:
        return sync_run_query(lambda: db_session.execute(select(Retailer)).scalars().all(), db_session)

    def process_csv(
        self, retailer: Retailer, blob_name: str, blob_content: str, db_session: "Session"
    ) -> None:  # pragma: no cover
        raise NotImplementedError

    def move_blob(
        self,
        destination_container: str,
        src_blob_client: "BlobClient",
        src_blob_lease: "BlobLeaseClient",
        *,
        dst_blob_name: str | None = None,
    ) -> None:

        try:
            self.blob_service_client.create_container(destination_container)
        except ResourceExistsError:
            pass  # this is fine

        dst_blob_client = self.blob_service_client.get_blob_client(
            destination_container,
            dst_blob_name
            if dst_blob_name is not None
            else f"{datetime.now(tz=timezone.utc).strftime('%Y/%m/%d/%H%M')}/{src_blob_client.blob_name}",
        )
        dst_blob_client.start_copy_from_url(src_blob_client.url)  # Synchronous within the same storage account
        src_blob_client.delete_blob(lease=src_blob_lease)

    def _do_import(self) -> None:  # pragma: no cover
        with SyncSessionMaker() as db_session:
            for retailer in self.get_retailers(db_session):
                self.process_blobs(retailer, db_session)

    def _process_blob(
        self,
        db_session: "Session",
        *,
        retailer: Retailer,
        blob: "BlobProperties",
        blob_client: BlobClient,
        lease: BlobLeaseClient,
        byte_content: bytes,
    ) -> None:
        logger.debug(f"Processing blob {blob.name}.")
        try:
            self.process_csv(
                retailer=retailer,
                blob_name=blob.name,
                blob_content=byte_content.decode("utf-8", "strict"),
                db_session=db_session,
            )
        except BlobProcessingError as ex:
            logger.error(f"Problem processing blob {blob.name} - {ex}. Moving to {settings.BLOB_ERROR_CONTAINER}")
            self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
            db_session.rollback()
        except UnicodeDecodeError as ex:
            logger.error(
                f"Problem decoding blob {blob.name} (files should be utf-8 encoded) - {ex}. "
                f"Moving to {settings.BLOB_ERROR_CONTAINER}"
            )
            self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
            db_session.rollback()
        except RewardConfigNotActiveError as ex:
            logger.error(
                (
                    f"Received invalid set of {retailer.slug} reward codes to import due to non-active reward "
                    f"type: {ex.slug}, moving to errors blob container for manual fix"
                )
            )
            self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
            db_session.rollback()
        else:
            logger.debug(f"Archiving blob {blob.name}.")
            self.move_blob(settings.BLOB_ARCHIVE_CONTAINER, blob_client, lease)
            db_session.add(RewardFileLog(file_name=blob.name, file_agent_type=self.file_agent_type))
            # commit all or nothing
            db_session.commit()

    def process_blobs(self, retailer: Retailer, db_session: "Session") -> None:
        for blob in self.container_client.list_blobs(
            name_starts_with=self.blob_path_template.substitute(retailer_slug=retailer.slug)
        ):
            blob_client = self.blob_service_client.get_blob_client(self.container_name, blob.name)

            try:
                lease = blob_client.acquire_lease(lease_duration=settings.BLOB_CLIENT_LEASE_SECONDS)
            except HttpResponseError:
                msg = f"Skipping blob {blob.name} as we could not acquire a lease."
                self._log_warn_and_alert(msg)
                continue

            if self._blob_name_is_duplicate(db_session, file_name=blob.name):
                logger.error(f"{blob.name} is a duplicate. Moving to {settings.BLOB_ERROR_CONTAINER} for checking")
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                continue

            if not blob.name.endswith(".csv"):
                logger.error(
                    f"{blob.name} does not have .csv ext. Moving to {settings.BLOB_ERROR_CONTAINER} for checking"
                )
                self.move_blob(settings.BLOB_ERROR_CONTAINER, blob_client, lease)
                continue

            byte_content = blob_client.download_blob(lease=lease).readall()
            self._process_blob(
                db_session,
                retailer=retailer,
                blob=blob,
                blob_client=blob_client,
                lease=lease,
                byte_content=byte_content,
            )


class RewardImportAgent(BlobFileAgent):
    """
    File name format (expiry date optional):

        `rewards.import.<reward slug>[.expires.yyyy-mm-dd].<any suffix>.csv`

    Examples:

        - ``rewards.import.viator.batch1.csv`` (rewards do not expire)
        - ``rewards.import.viator.expires.2023-12-31.batch1.csv`` (rewards expire 2023-12-31)

    File content example (one code per line):

    ```
    code1
    code2
    code3
    ...
    ```
    """

    blob_path_template = string.Template("$retailer_slug/rewards.import.")
    scheduler_name = "carina-reward-import-scheduler"

    def __init__(self) -> None:
        super().__init__()
        self.file_agent_type = FileAgentType.IMPORT

    @acquire_lock(runner=cron_scheduler)
    def do_import(self) -> None:  # pragma: no cover
        super()._do_import()

    @lru_cache()
    def reward_configs_by_slug(self, retailer_id: int, db_session: "Session") -> dict[str, RewardConfig]:
        reward_configs = (
            db_session.execute(select(RewardConfig).where(RewardConfig.retailer_id == retailer_id)).scalars().all()
        )
        return {reward_config.slug: reward_config for reward_config in reward_configs}

    @staticmethod
    def _get_expiry_date(sub_blob_name: str, blob_name: str) -> date | None:
        if ".expires." in sub_blob_name:
            try:
                extracted_date = sub_blob_name.split(".expires.")[1].split(".")[0]
                expiry_date = datetime.strptime(extracted_date, "%Y-%m-%d").date()
            except ValueError as ex:
                raise BlobProcessingError(f"Invalid filename, expiry date is invalid: {blob_name}") from ex
        else:
            expiry_date = None
        return expiry_date

    def _report_pre_existing_codes(
        self, pre_existing_reward_codes: list[str], row_nums_by_code: dict[str, list[int]], blob_name: str
    ) -> None:
        msg = f"Pre-existing reward codes found in {blob_name}:\n" + "\n".join(
            [f"rows: {', '.join(map(str, row_nums_by_code[code]))}" for code in pre_existing_reward_codes]
        )
        self._log_warn_and_alert(msg)

    def _report_invalid_rows(self, invalid_rows: list[int], blob_name: str) -> None:
        if invalid_rows:
            self._log_warn_and_alert(
                f"Invalid rows found in {blob_name}:\nrows: {', '.join(map(str, sorted(invalid_rows)))}",
            )

    def _get_reward_codes_and_report_invalid(
        self,
        db_session: "Session",
        *,
        retailer: Retailer,
        reward_config: RewardConfig,
        blob_name: str,
        blob_content: str,
    ) -> tuple[list[str], defaultdict[str, list[int]]]:
        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")
        invalid_rows: list[int] = []

        row_nums_by_code: defaultdict[str, list[int]] = defaultdict(list)
        for row_num, row in enumerate(content_reader, start=1):
            if not len(row) == 1:
                invalid_rows.append(row_num)
            elif code := row[0].strip():
                row_nums_by_code[code].append(row_num)

        db_reward_codes = (
            db_session.execute(
                select(Reward.code).where(
                    or_(
                        and_(
                            Reward.code.in_(row_nums_by_code.keys()),
                            Reward.retailer_id == retailer.id,
                            Reward.reward_config_id == reward_config.id,
                        ),
                        and_(Reward.reward_config_id != reward_config.id, not_(Reward.deleted)),
                        and_(Reward.reward_config_id == reward_config.id, Reward.deleted),
                    )
                )
            )
            .scalars()
            .all()
        )

        self._report_invalid_rows(invalid_rows, blob_name)
        return db_reward_codes, row_nums_by_code

    def process_csv(self, retailer: Retailer, blob_name: str, blob_content: str, db_session: "Session") -> None:
        try:
            _, sub_blob_name = blob_name.split(self.blob_path_template.substitute(retailer_slug=retailer.slug))
        except ValueError as ex:
            raise BlobProcessingError(f"Invalid filename, path does not match blob path template: {blob_name}") from ex
        try:
            slug = sub_blob_name.split(".", 1)[0]
            reward_config = self.reward_configs_by_slug(retailer.id, db_session)[slug]
        except KeyError:
            raise BlobProcessingError(f"No RewardConfig found for slug {slug}")  # pylint: disable=raise-missing-from

        if reward_config.status != RewardTypeStatuses.ACTIVE:
            raise RewardConfigNotActiveError(slug=slug)

        expiry_date = self._get_expiry_date(sub_blob_name, blob_name)

        db_reward_codes, row_nums_by_code = self._get_reward_codes_and_report_invalid(
            db_session, retailer=retailer, reward_config=reward_config, blob_name=blob_name, blob_content=blob_content
        )

        pre_existing_reward_codes = list(set(db_reward_codes) & set(row_nums_by_code.keys()))
        if pre_existing_reward_codes:
            self._report_pre_existing_codes(pre_existing_reward_codes, row_nums_by_code, blob_name)
            for pre_existing_code in pre_existing_reward_codes:
                row_nums_by_code.pop(pre_existing_code)

        new_rewards: list[Reward] = [
            Reward(
                code=code,
                reward_config_id=reward_config.id,
                retailer_id=retailer.id,
                expiry_date=expiry_date,
            )
            for code in set(row_nums_by_code)
            if code  # caters for blank lines
        ]

        db_session.add_all(new_rewards)


class RewardUpdatesAgent(BlobFileAgent):
    """
    File name format (expiry date optional):

        `rewards.update.<any suffix>.csv`

    Example:

        ``rewards.update.batch1.csv``

    File content example (one code per line):

    ```
    code1,2022-09-08,cancelled
    code2,2022-09-08,cancelled
    code3,2022-09-08,redeemed
    ```
    """

    blob_path_template = string.Template("$retailer_slug/rewards.update.")
    scheduler_name = "carina-reward-update-scheduler"

    def __init__(self) -> None:
        super().__init__()
        self.file_agent_type = FileAgentType.UPDATE

    @acquire_lock(runner=cron_scheduler)
    def do_import(self) -> None:  # pragma: no cover
        super()._do_import()

    def process_csv(self, retailer: Retailer, blob_name: str, blob_content: str, db_session: "Session") -> None:
        content_reader = csv.reader(StringIO(blob_content), delimiter=",", quotechar="|")

        # This is a defaultdict(list) incase we encounter the reward code twice in one file
        reward_update_rows_by_code: defaultdict = defaultdict(list[RewardUpdateRow])
        invalid_rows: list[tuple[int, Exception]] = []
        for row_num, row in enumerate(content_reader, start=1):
            try:
                data = RewardUpdateSchema(
                    code=row[0].strip(),
                    date=row[1].strip(),
                    status=RewardUpdateStatuses(row[2].strip()),
                )
            except (ValidationError, IndexError, ValueError) as ex:
                invalid_rows.append((row_num, ex))
            else:
                reward_update_rows_by_code[data.dict()["code"]].append(RewardUpdateRow(data, row_num=row_num))

        if invalid_rows:
            msg = f"Error validating RewardUpdate from CSV file {blob_name}:\n" + "\n".join(
                [f"row {row_num}: {repr(e)}" for row_num, e in invalid_rows]
            )
            self._log_warn_and_alert(msg)

        if not reward_update_rows_by_code:
            logger.warning(f"No relevant reward updates found in blob: {blob_name}")

        self._process_updates(
            db_session=db_session,
            retailer=retailer,
            reward_update_rows_by_code=reward_update_rows_by_code,
            blob_name=blob_name,
        )

    def _report_duplicates(
        self,
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        blob_name: str,
    ) -> None:
        reward_update_row_datas: list[RewardUpdateRow]
        if duplicates := dict(filter(lambda x: len(x[1]) > 1, reward_update_rows_by_code.items())):
            row_nums = []
            for duplicate in duplicates:
                reward_update_row_datas = reward_update_rows_by_code.pop(duplicate, [])
                row_nums.extend([update_row.row_num for update_row in reward_update_row_datas])

            msg = f"Duplicate reward codes found while processing {blob_name}, rows: {', '.join(map(str, row_nums))}"
            self._log_warn_and_alert(msg)

    def _report_unknown_codes(
        self,
        reward_codes_in_file: list[str],
        db_reward_data_by_code: dict[str, dict[str, str | bool]],
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        blob_name: str,
    ) -> None:
        reward_update_row_datas: list[RewardUpdateRow]
        if unknown_reward_codes := list(set(reward_codes_in_file) - set(db_reward_data_by_code.keys())):
            row_nums = []
            for unknown_reward_code in unknown_reward_codes:
                reward_update_row_datas = reward_update_rows_by_code.pop(unknown_reward_code, [])
                row_nums.extend([update_row.row_num for update_row in reward_update_row_datas])

            msg = f"Unknown reward codes found while processing {blob_name}, rows: {', '.join(map(str, row_nums))}"
            self._log_warn_and_alert(msg)

    def _process_unallocated_codes(
        self,
        db_session: "Session",
        *,
        retailer: Retailer,
        blob_name: str,
        reward_codes_in_file: list[str],
        db_reward_data_by_code: dict[str, dict[str, str | bool]],
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
    ) -> None:
        if unallocated_reward_codes := list(
            set(reward_codes_in_file)
            & {code for code, reward_data in db_reward_data_by_code.items() if reward_data["allocated"] is False}
        ):
            # Soft delete unallocated reward codes
            update_rows: list[RewardUpdateRow] = []
            for unallocated_reward_code in unallocated_reward_codes:
                rows = reward_update_rows_by_code.pop(unallocated_reward_code, [])
                update_rows.extend(rows)

            db_session.execute(
                update(Reward)
                .where(Reward.code.in_(unallocated_reward_codes), Reward.retailer_id == retailer.id)
                .values(deleted=True)
            )
            msg = f"Unallocated reward codes found while processing {blob_name}:\n" + "\n".join(
                [
                    f"Reward id: {db_reward_data_by_code[row_data.data.code]['id']}"
                    f" row: {row_data.row_num}, status change: {row_data.data.status.value}"
                    for row_data in update_rows
                ]
            )
            self._log_warn_and_alert(msg)

    def _process_updates(
        self,
        db_session: "Session",
        retailer: Retailer,
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        blob_name: str,
    ) -> None:

        reward_codes_in_file = list(reward_update_rows_by_code.keys())

        reward_datas = (
            db_session.execute(
                select(Reward.id, Reward.code, Reward.account_holder_id).with_for_update()
                # FIXME: should we filter on deleted=False?
                .where(Reward.code.in_(reward_codes_in_file), Reward.retailer_id == retailer.id)
            )
            .mappings()
            .all()
        )
        # Provides a dict in the following format:
        # {'<code>': {'id': 'f2c44cf7-9d0f-45d0-b199-44a3c8b72db3', 'allocated': True}}
        db_reward_data_by_code: dict[str, dict[str, str | bool]] = {
            reward_data["code"]: {
                "id": str(reward_data["id"]),
                "allocated": reward_data["account_holder_id"] is not None,
            }
            for reward_data in reward_datas
        }

        self._report_unknown_codes(reward_codes_in_file, db_reward_data_by_code, reward_update_rows_by_code, blob_name)

        self._process_unallocated_codes(
            db_session,
            retailer=retailer,
            blob_name=blob_name,
            reward_codes_in_file=reward_codes_in_file,
            db_reward_data_by_code=db_reward_data_by_code,
            reward_update_rows_by_code=reward_update_rows_by_code,
        )
        self._report_duplicates(reward_update_rows_by_code, blob_name)
        self._finalise_data(db_session, reward_update_rows_by_code, db_reward_data_by_code)

    def _finalise_data(
        self,
        db_session: "Session",
        reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]],
        db_reward_data_by_code: dict[str, dict[str, str | bool]],
    ) -> None:
        reward_update_objs = []
        update_rows_by_status: defaultdict = defaultdict(list)
        for code, reward_update_rows in reward_update_rows_by_code.items():
            for row in reward_update_rows:  # after _report_duplicates, all of these should be a single item lists
                reward_update_objs.append(
                    RewardUpdate(
                        reward_id=db_reward_data_by_code[code]["id"],
                        date=row.data.date_,
                        status=row.data.status,
                    )
                )
                update_rows_by_status[row.data.status].append(row)

        # Persist RewardUpdate object rows
        db_session.add_all(reward_update_objs)

        # Persist updates to the Reward objects themselves
        # This code produces two "executemany" contexts for each status with the following rules:
        #  * Unallocated rewards (i.e. account_holder_id == NULL) will be ignored
        #  * CANCELLED rewards will be ignored if cancelled_date is not NULL (i.e. previously updated)
        #  * REDEEMED rewards will be ignored if redeemed_date is not NULL (i.e. previously updated)
        reward_table = Reward.__table__

        for status, update_rows in update_rows_by_status.items():
            params = [{"reward_code": update.data.code, "date": update.data.date_} for update in update_rows]
            if status == RewardUpdateStatuses.CANCELLED:
                values = {"cancelled_date": bindparam("date")}
                date_where = reward_table.c.cancelled_date.is_(None)

            elif status == RewardUpdateStatuses.REDEEMED:
                values = {"redeemed_date": bindparam("date")}
                date_where = reward_table.c.redeemed_date.is_(None)

            else:
                raise ValueError(f"Unknown status: {status}")

            wheres = [
                reward_table.c.account_holder_id.is_not(None),
                reward_table.c.code == bindparam("reward_code"),
            ]
            db_session.execute(reward_table.update().where(date_where, *wheres).values(**values), params)
