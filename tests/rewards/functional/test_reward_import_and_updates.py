import logging
import uuid

from collections import defaultdict
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, DefaultDict

import pytest

from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from pytest_mock import MockerFixture
from sqlalchemy.future import select
from testfixtures import LogCapture

from cosmos.db.models import AccountHolder, Retailer, Reward, RewardConfig, RewardUpdate
from cosmos.rewards.enums import FileAgentType, RewardUpdateStatuses
from cosmos.rewards.imports.file_agent import (
    BlobProcessingError,
    RewardFileLog,
    RewardImportAgent,
    RewardUpdateRow,
    RewardUpdatesAgent,
)
from cosmos.rewards.schemas import RewardUpdateSchema
from tests.rewards.conftest import RewardsSetupType

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def _get_reward_update_rows(db_session: "Session", reward_codes: list[str]) -> list[RewardUpdate]:
    return db_session.execute(select(RewardUpdate).join(Reward).where(Reward.code.in_(reward_codes))).scalars().all()


def _get_reward_rows(db_session: "Session") -> list[Reward]:
    return db_session.execute(select(Reward)).scalars().all()


def test_import_agent__process_csv(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    mocker.patch("cosmos.rewards.imports.file_agent.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup_rewards
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 1
    assert rewards[0] == pre_existing_reward

    blob_content = (
        "\n".join([*eligible_reward_codes, pre_existing_reward.code])
        + "\nthis,is,a,bad,line"  # this should be reported to sentry (line 5)
        + "\nanother,bad,line"  # this should be reported to sentry (line 6)
    )

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name="re-test/rewards.import.test-reward-slug.new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 2  # Errors should all be rolled up in to one call per error category
    assert (
        capture_message_spy.call_args_list[0][0][0]
        == "Invalid rows found in re-test/rewards.import.test-reward-slug.new-reward.csv:\nrows: 5, 6"
    )
    assert (
        capture_message_spy.call_args_list[1][0][0]
        == "Pre-existing reward codes found in re-test/rewards.import.test-reward-slug.new-reward.csv:\nrows: 4"
    )


def test_import_agent__process_csv_with_expiry_date(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name="re-test/rewards.import.test-reward-slug.expires.2023-01-16.new-reward.csv",
        blob_content="reward1\nreward2\nreward3",
        db_session=db_session,
    )

    expiry_date = datetime.strptime("2023-01-16", "%Y-%m-%d").replace(tzinfo=timezone.utc).date()
    rewards = _get_reward_rows(db_session)
    for reward in rewards[1:4]:
        assert reward.expiry_date.date() == expiry_date


def test_import_agent__process_csv_with_bad_expiry_date(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()

    with pytest.raises(BlobProcessingError) as exc_info:
        reward_agent.process_csv(
            retailer=reward_config.retailer,
            blob_name="re-test/rewards.import.test-reward-slug.expires.BAD-DATE.new-reward.csv",
            blob_content="reward1\nreward2\nreward3",
            db_session=db_session,
        )

    assert exc_info.value.args == (
        "Invalid filename, expiry date is invalid: re-test/rewards"
        + ".import.test-reward-slug.expires.BAD-DATE.new-reward.csv",
    )


def test_import_agent__process_csv_soft_deleted(
    setup_rewards: RewardsSetupType, create_reward_config: Callable, mocker: MockerFixture
) -> None:
    """
    Test that a reward code will be imported OK when the code exists in the DB but for a different reward slug,
    and it has been soft deleted
    """
    mocker.patch("cosmos.rewards.imports.file_agent.sentry_sdk")
    db_session, reward_config, pre_existing_reward = setup_rewards
    second_reward_config = create_reward_config(**{"slug": "second-test-reward"})
    # Associate the existing reward with a different reward config i.e. a different reward slug.
    # This means the same reward code should import OK for the 'test-reward' reward slug
    pre_existing_reward.reward_config_id = second_reward_config.id
    pre_existing_reward.deleted = True
    db_session.commit()
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join([*eligible_reward_codes, pre_existing_reward.code])

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name="re-test/rewards.import.test-reward-slug.new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 5
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    assert capture_message_spy.call_count == 0  # All rewards should import OK


def test_import_agent__process_csv_not_soft_deleted(
    setup_rewards: RewardsSetupType, create_reward_config: Callable, mocker: MockerFixture
) -> None:
    """
    Test that a reward code imported for a different reward slug, but where that existing reward code has
    NOT been soft-deleted, will cause an error to be reported and will not be imported
    """
    db_session, reward_config, pre_existing_reward = setup_rewards
    second_reward_config = create_reward_config(**{"slug": "second-test-reward"})
    # Associate the existing reward with a different reward config i.e. a different reward slug.
    # This means the same reward code should import OK for the 'test-reward' reward type slug
    pre_existing_reward.reward_config_id = second_reward_config.id
    db_session.commit()
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join([*eligible_reward_codes, pre_existing_reward.code])

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name="re-test/rewards.import.test-reward-slug.new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 1
    assert (
        capture_message_spy.call_args_list[0][0][0]
        == "Pre-existing reward codes found in re-test/rewards.import.test-reward-slug.new-reward.csv:\nrows: 4"
    )


def test_import_agent__process_csv_same_reward_slug_not_soft_deleted(
    setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    """
    Test that a reward code imported for the same reward slug, where that existing reward code HAS
    been soft-deleted, will cause an error to be reported and will not be imported
    """
    db_session, reward_config, pre_existing_reward = setup_rewards
    pre_existing_reward.deleted = True
    db_session.commit()
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.SENTRY_DSN = "SENTRY_DSN"
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    reward_agent = RewardImportAgent()
    eligible_reward_codes = ["reward1", "reward2", "reward3"]

    blob_content = "\n".join([*eligible_reward_codes, pre_existing_reward.code])

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name="re-test/rewards.import.test-reward-slug.new-reward.csv",
        blob_content=blob_content,
        db_session=db_session,
    )

    rewards = _get_reward_rows(db_session)
    assert len(rewards) == 4
    assert all(v in [reward.code for reward in rewards] for v in eligible_reward_codes)
    # We should be sentry warned about the existing token
    assert capture_message_spy.call_count == 1
    assert (
        capture_message_spy.call_args_list[0][0][0]
        == "Pre-existing reward codes found in re-test/rewards.import.test-reward-slug.new-reward.csv:\nrows: 4"
    )


def test_import_agent__reward_config_non_active_status_error(
    log_capture: LogCapture, setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    db_session, reward_config, _ = setup_rewards
    reward_config.active = False
    db_session.commit()
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardImportAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    retailer: Retailer = reward_config.retailer
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    blob_filename = "re-test/rewards.import.test-reward-slug.new-reward.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(blob_filename),
        ]
    )
    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)
    message = (
        f"Received invalid set of {retailer.slug} reward codes to import due to non-active reward "
        f"type: {reward_config.slug}, moving to errors blob container for manual fix"
    )
    assert any(message in record.msg for record in log_capture.records)
    mock_move_blob.assert_called_once()


def test_import_agent__process_csv_no_reward_config(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards

    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        reward_agent.process_csv(
            retailer=reward_config.retailer,
            blob_name="re-test/rewards.import.incorrect-reward-type.new-rewards.csv",
            blob_content="reward1\nreward2\nreward3",
            db_session=db_session,
        )
    assert exc_info.value.args == ("No RewardConfig found for slug incorrect-reward-type",)


def test_import_agent__process_csv_blob_path_does_not_template(
    setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    db_session, reward_config, _ = setup_rewards

    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardImportAgent()
    with pytest.raises(BlobProcessingError) as exc_info:
        reward_agent.process_csv(
            retailer=reward_config.retailer,
            blob_name="re-test/rewards.reward-slug.whatever.csv",
            blob_content="reward1\nreward2\nreward3",
            db_session=db_session,
        )
    assert exc_info.value.args == (
        "Invalid filename, path does not match blob path template: re-test/rewards.reward-slug.whatever.csv",
    )


def test_updates_agent__process_csv(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards

    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardUpdatesAgent()
    mock__process_updates = mocker.patch.object(reward_agent, "_process_updates")
    blob_name = "/re-test/rewards.update.test.csv"
    content = """
TEST12345678,2021-07-30,cancelled
TEST87654321,2021-07-21,redeemed
TEST87654322,2021-07-30,CANCELLED
TEST87654322,2021-07-30,redeemed
""".strip()

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )
    expected_reward_update_rows_by_code = defaultdict(
        list[RewardUpdateRow],
        {
            "TEST12345678": [
                RewardUpdateRow(
                    row_num=1,
                    data=RewardUpdateSchema(
                        code="TEST12345678", date="2021-07-30", status=RewardUpdateStatuses.CANCELLED
                    ),
                )
            ],
            "TEST87654321": [
                RewardUpdateRow(
                    row_num=2,
                    data=RewardUpdateSchema(
                        code="TEST87654321", date="2021-07-21", status=RewardUpdateStatuses.REDEEMED
                    ),
                )
            ],
            "TEST87654322": [
                RewardUpdateRow(
                    row_num=3,
                    data=RewardUpdateSchema(
                        code="TEST87654322", date="2021-07-30", status=RewardUpdateStatuses.CANCELLED
                    ),
                ),
                RewardUpdateRow(
                    row_num=4,
                    data=RewardUpdateSchema(
                        code="TEST87654322", date="2021-07-30", status=RewardUpdateStatuses.REDEEMED
                    ),
                ),
            ],
        },
    )
    mock__process_updates.assert_called_once_with(
        retailer=reward_config.retailer,
        blob_name=blob_name,
        db_session=db_session,
        reward_update_rows_by_code=expected_reward_update_rows_by_code,
    )


def test_updates_agent__process_csv_reward_code_fails_non_validating_rows(
    setup_rewards: RewardsSetupType, mocker: MockerFixture, account_holder: AccountHolder
) -> None:
    """If non-validating values are encountered, sentry should log a msg"""
    db_session, reward_config, reward = setup_rewards
    reward.account_holder_id = account_holder.id  # allocated/issued
    db_session.commit()

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    blob_name = "/re-test/rewards.update.test.csv"
    bad_date = "20210830"
    bad_status = "nosuchstatus"
    content = f"""
TSTCD1234,2021-07-30,redeemed
TEST12345678,{bad_date},redeemed
TEST666666,2021-07-30,{bad_status}
""".strip()

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )

    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert f"time data '{bad_date}' does not match format '%Y-%m-%d'" in expected_error_msg
    assert f"'{bad_status}' is not a valid RewardUpdateStatuses" in expected_error_msg


def test_updates_agent__process_csv_reward_code_fails_malformed_csv_rows(
    setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    """If a bad CSV row in encountered, sentry should log a msg"""
    db_session, reward_config, _ = setup_rewards

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    blob_name = "/re-test/rewards.update.test.csv"
    content = "TEST87654321,2021-07-30\nTEST12345678,redeemed\n"

    reward_agent.process_csv(
        retailer=reward_config.retailer,
        blob_name=blob_name,
        blob_content=content,
        db_session=db_session,
    )

    assert capture_message_spy.call_count == 1  # Both index errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "IndexError('list index out of range')" in expected_error_msg


def test_updates_agent__process_updates(
    setup_rewards: RewardsSetupType, mocker: MockerFixture, account_holder: AccountHolder
) -> None:
    db_session, reward_config, reward = setup_rewards
    reward.account_holder_id = account_holder.id  # allocated/issued
    reward.issued_date = datetime(2021, 7, 25, tzinfo=timezone.utc)  # not required but for the sake of completeness
    other_reward = Reward(
        code="other_code",
        retailer_id=account_holder.retailer_id,
        reward_uuid=uuid.uuid4(),
        reward_config_id=reward_config.id,
        account_holder_id=account_holder.id,
        issued_date=datetime(2021, 7, 20, tzinfo=timezone.utc),
    )
    db_session.add(other_reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.ISSUED
    assert other_reward.status == Reward.RewardStatuses.ISSUED

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardUpdatesAgent()
    blob_name = "/re-test/rewards.update.test.csv"
    reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[reward.code] = [
        RewardUpdateRow(
            data=RewardUpdateSchema(
                code=reward.code,
                date="2021-07-30",
                status=RewardUpdateStatuses("redeemed"),
            ),
            row_num=1,
        ),
    ]
    reward_update_rows_by_code[other_reward.code] = [
        RewardUpdateRow(
            data=RewardUpdateSchema(
                code=other_reward.code,
                date="2021-07-29",
                status=RewardUpdateStatuses("cancelled"),
            ),
            row_num=2,
        ),
    ]

    reward_agent._process_updates(
        retailer=reward_config.retailer,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [reward.code, other_reward.code])

    assert len(reward_update_rows) == 2
    assert capture_message_spy.call_count == 0  # Should be no errors

    assert reward_update_rows[0].reward_id == reward.id
    assert reward_update_rows[0].status == RewardUpdateStatuses.REDEEMED
    assert isinstance(reward_update_rows[0].date, date)
    assert str(reward_update_rows[0].date) == "2021-07-30"

    assert reward_update_rows[1].reward_id == other_reward.id
    assert reward_update_rows[1].status == RewardUpdateStatuses.CANCELLED
    assert isinstance(reward_update_rows[1].date, date)
    assert str(reward_update_rows[1].date) == "2021-07-29"

    db_session.refresh(reward)
    db_session.refresh(other_reward)

    assert reward.redeemed_date == datetime(2021, 7, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    assert reward.cancelled_date is None
    assert reward.status == Reward.RewardStatuses.REDEEMED

    assert other_reward.cancelled_date == datetime(2021, 7, 29, tzinfo=timezone.utc).replace(tzinfo=None)
    assert other_reward.redeemed_date is None
    assert other_reward.status == Reward.RewardStatuses.CANCELLED


def test_updates_agent__process_updates_duplicates(
    setup_rewards: RewardsSetupType, mocker: MockerFixture, account_holder: AccountHolder, reward_config: RewardConfig
) -> None:
    db_session, reward_config, reward = setup_rewards
    reward.account_holder_id = account_holder.id  # allocated/issued
    reward.issued_date = datetime(2021, 7, 25, tzinfo=timezone.utc)  # not required but for the sake of completeness

    other_reward = Reward(
        code="code2",
        retailer_id=account_holder.retailer_id,
        reward_uuid=uuid.uuid4(),
        reward_config_id=reward_config.id,
        account_holder_id=account_holder.id,
        issued_date=datetime(2021, 7, 29, tzinfo=timezone.utc),
    )
    db_session.add(other_reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.ISSUED
    assert other_reward.status == Reward.RewardStatuses.ISSUED

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    reward_agent = RewardUpdatesAgent()
    blob_name = "/re-test/rewards.update.test.csv"
    reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[reward.code] = [
        RewardUpdateRow(
            data=RewardUpdateSchema(
                code=reward.code,
                date="2021-07-30",
                status=RewardUpdateStatuses("redeemed"),
            ),
            row_num=1,
        ),
        RewardUpdateRow(
            data=RewardUpdateSchema(
                code=reward.code,
                date="2021-07-30",
                status=RewardUpdateStatuses("cancelled"),  # Duplicate/conflicting
            ),
            row_num=2,
        ),
    ]
    reward_update_rows_by_code[other_reward.code] = [
        RewardUpdateRow(
            data=RewardUpdateSchema(
                code=other_reward.code,
                date="2021-07-30",
                status=RewardUpdateStatuses("cancelled"),  # Duplicate/conflicting
            ),
            row_num=3,
        ),
    ]

    reward_agent._process_updates(
        retailer=reward_config.retailer,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [reward.code, other_reward.code])

    assert len(reward_update_rows) == 1
    assert reward_update_rows[0].reward_id == other_reward.id
    assert reward_update_rows[0].status == RewardUpdateStatuses.CANCELLED
    assert isinstance(reward_update_rows[0].date, date)
    assert str(reward_update_rows[0].date) == "2021-07-30"
    assert capture_message_spy.call_count == 1  # Should be no errors
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert (
        expected_error_msg
        == "Duplicate reward codes found while processing /re-test/rewards.update.test.csv, rows: 1, 2"
    )
    db_session.refresh(reward)
    db_session.refresh(other_reward)

    assert reward.redeemed_date is None
    assert reward.cancelled_date is None
    assert reward.status == Reward.RewardStatuses.ISSUED  # This one is ignored due to duplicate/conflicting lines

    assert other_reward.cancelled_date == datetime(2021, 7, 30, tzinfo=timezone.utc).replace(tzinfo=None)
    assert other_reward.redeemed_date is None
    assert other_reward.status == Reward.RewardStatuses.CANCELLED


def test_updates_agent__process_updates_reward_code_not_allocated(
    setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    """If the reward is not allocated, it should be soft-deleted"""
    db_session, reward_config, reward = setup_rewards
    assert reward.account_holder_id is None
    db_session.commit()

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    mocker.patch.object(reward_agent, "_report_unknown_codes", autospec=True)
    blob_name = "/re-test/rewards-update.test.csv"
    data = RewardUpdateSchema(
        code=reward.code,
        date="2021-07-30",
        status=RewardUpdateStatuses("redeemed"),
    )
    reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[reward.code].append(RewardUpdateRow(data=data, row_num=1))

    reward_agent._process_updates(
        db_session=db_session,
        retailer=reward_config.retailer,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [reward.code])

    db_session.refresh(reward)

    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "Unallocated reward codes found" in expected_error_msg
    assert not reward_update_rows
    assert reward.status == Reward.RewardStatuses.UNALLOCATED
    assert reward.expiry_date is None
    assert reward.cancelled_date is None
    assert reward.redeemed_date is None
    assert reward.issued_date is None


def test_updates_agent__process_updates_reward_code_does_not_exist(
    setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    """The reward does not exist"""
    db_session, reward_config, _ = setup_rewards

    from cosmos.rewards.imports.file_agent import sentry_sdk as file_agent_sentry_sdk

    capture_message_spy = mocker.spy(file_agent_sentry_sdk, "capture_message")
    mocker.patch("cosmos.rewards.imports.file_agent.BlobServiceClient")
    # mocker.patch.object(RewardUpdatesAgent, "enqueue_reward_updates")
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_IMPORT_LOGGING_LEVEL = logging.INFO
    reward_agent = RewardUpdatesAgent()
    mocker.patch.object(reward_agent, "_process_unallocated_codes", autospec=True)
    blob_name = "/re-test/rewards.update.test.csv"
    bad_reward_code = "IDONOTEXIST"
    data = RewardUpdateSchema(
        code=bad_reward_code,
        date="2021-07-30",
        status=RewardUpdateStatuses("cancelled"),
    )
    reward_update_rows_by_code: DefaultDict[str, list[RewardUpdateRow]] = defaultdict(list[RewardUpdateRow])
    reward_update_rows_by_code[bad_reward_code].append(RewardUpdateRow(data=data, row_num=1))

    reward_agent._process_updates(
        retailer=reward_config.retailer,
        reward_update_rows_by_code=reward_update_rows_by_code,
        blob_name=blob_name,
        db_session=db_session,
    )
    reward_update_rows = _get_reward_update_rows(db_session, [bad_reward_code])

    assert capture_message_spy.call_count == 1  # Errors should all be rolled up in to a single call
    expected_error_msg: str = capture_message_spy.call_args.args[0]
    assert "Unknown reward codes found while processing /re-test/rewards.update.test.csv, rows: 1" in expected_error_msg
    assert not reward_update_rows


class Blob:
    def __init__(self, name: str) -> None:
        self.name = name


def test_process_blobs(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    file_name = "re-test/rewards.update.update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)

    reward_file_log_row = db_session.execute(
        select(RewardFileLog).where(
            RewardFileLog.file_name == file_name, RewardFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert reward_file_log_row
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_process_blobs_unicodedecodeerror(
    log_capture: LogCapture, setup_rewards: RewardsSetupType, mocker: MockerFixture
) -> None:
    db_session, reward_config, _ = setup_rewards
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    blob_filename = "re-test/rewards.update.update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(blob_filename),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value.download_blob.return_value.readall.return_value = (
        b"\xca,2021,09,13,cancelled"
    )

    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)

    assert not mock_process_csv.called
    message = f"Problem decoding blob {blob_filename} (files should be utf-8 encoded)"
    assert any(message in record.msg for record in log_capture.records)
    mock_move_blob.assert_called_once()


def test_process_blobs_not_csv(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)
    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    from cosmos.rewards.imports.file_agent import logger

    logger_error_spy = mocker.spy(logger, "error")

    reward_agent = RewardUpdatesAgent()
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob("re-test/rewards.update.update.docx"),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)
    assert logger_error_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        logger_error_spy.call_args.args[0]
        == "re-test/rewards.update.update.docx does not have .csv ext. Moving to ERROR-CONTAINER for checking"
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_duplicate(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    db_session, reward_config, _ = setup_rewards
    file_name = "re-test/rewards.update.update.csv"
    db_session.add(
        RewardFileLog(
            file_name=file_name,
            file_agent_type=FileAgentType.UPDATE,
        )
    )
    db_session.commit()
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)
    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    from cosmos.rewards.imports.file_agent import logger

    logger_error_spy = mocker.spy(logger, "error")

    reward_agent = RewardUpdatesAgent()
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)
    mock_settings = mocker.patch("cosmos.rewards.imports.file_agent.reward_settings")
    mock_settings.BLOB_ERROR_CONTAINER = "ERROR-CONTAINER"

    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)
    assert logger_error_spy.call_count == 1  # Errors should all be rolled up in to a single call
    assert (
        logger_error_spy.call_args.args[0]
        == "re-test/rewards.update.update.csv is a duplicate. Moving to ERROR-CONTAINER for checking"
    )
    mock_move_blob.assert_called_once()
    assert mock_move_blob.call_args[0][0] == "ERROR-CONTAINER"
    mock_process_csv.assert_not_called()


def test_process_blobs_filename_is_not_duplicate(setup_rewards: RewardsSetupType, mocker: MockerFixture) -> None:
    """A filename exists in the log, but the file agent type is different"""
    db_session, reward_config, _ = setup_rewards
    file_name = "re-test/rewards.update.update.csv"
    db_session.add(
        RewardFileLog(
            file_name=file_name,
            file_agent_type=FileAgentType.IMPORT,
        )
    )
    db_session.commit()
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    mock_blob_service_client = mocker.MagicMock(spec=BlobServiceClient)

    MockBlobServiceClient.from_connection_string.return_value = mock_blob_service_client
    reward_agent = RewardUpdatesAgent()
    container_client = mocker.patch.object(reward_agent, "container_client", spec=ContainerClient)
    mock_process_csv = mocker.patch.object(reward_agent, "process_csv")
    mock_move_blob = mocker.patch.object(reward_agent, "move_blob")
    file_name = "re-test/rewards.update.update.csv"
    container_client.list_blobs = mocker.MagicMock(
        return_value=[
            Blob(file_name),
        ]
    )
    mock_blob_service_client.get_blob_client.return_value = mocker.MagicMock(spec=BlobClient)

    reward_agent.process_blobs(reward_config.retailer, db_session=db_session)

    reward_file_log_row = db_session.execute(
        select(RewardFileLog).where(
            RewardFileLog.file_name == file_name, RewardFileLog.file_agent_type == FileAgentType.UPDATE
        )
    ).scalar_one_or_none()

    assert reward_file_log_row  # The new record
    mock_process_csv.assert_called_once()
    mock_move_blob.assert_called_once()


def test_move_blob(mocker: MockerFixture) -> None:
    MockBlobServiceClient = mocker.patch(  # noqa: N806
        "cosmos.rewards.imports.file_agent.BlobServiceClient", autospec=True
    )
    MockBlobServiceClient.from_connection_string.return_value = mocker.MagicMock(spec=BlobServiceClient)
    mock_src_blob_client = mocker.patch("cosmos.rewards.imports.file_agent.BlobClient", autospec=True)
    mock_src_blob_client.url = "https://a-blob-url"
    src_blob_lease_client = mocker.patch("cosmos.rewards.imports.file_agent.BlobLeaseClient", autospec=True)

    reward_agent = RewardUpdatesAgent()
    mock_src_blob_client.blob_name = blob_name = "/re-test/rewards.update.test.csv"

    blob_service_client = mocker.patch.object(reward_agent, "blob_service_client")
    mock_dst_blob_client = mocker.MagicMock()
    blob_service_client.delete_blob = mocker.MagicMock()
    blob_service_client.get_blob_client.return_value = mock_dst_blob_client

    reward_agent.move_blob(
        "DESTINATION-CONTAINER",
        mock_src_blob_client,
        src_blob_lease_client,
    )
    blob = f"{datetime.now(tz=timezone.utc).strftime('%Y/%m/%d/%H%M')}/{blob_name}"

    blob_service_client.get_blob_client.assert_called_once_with("DESTINATION-CONTAINER", blob)
    mock_dst_blob_client.start_copy_from_url.assert_called_once_with("https://a-blob-url")
    mock_src_blob_client.delete_blob.assert_called_once()
