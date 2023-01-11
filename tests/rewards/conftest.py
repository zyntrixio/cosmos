from collections import namedtuple
from typing import Generator

import pytest

from sqlalchemy.orm import Session

from cosmos.db.models import Reward, RewardConfig

SetupType = namedtuple("SetupType", ["db_session", "reward_config", "reward"])


@pytest.fixture(scope="function")
def setup(db_session: "Session", reward_config: RewardConfig, reward: Reward) -> Generator[SetupType, None, None]:
    yield SetupType(db_session, reward_config, reward)
