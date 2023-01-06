import logging

from prometheus_client import Counter

logger = logging.getLogger(__name__)

METRIC_NAME_PREFIX = "bpl_"


invalid_marketing_opt_out = Counter(
    name=f"{METRIC_NAME_PREFIX}erroneous_marketing_opt_out_get_requests",
    documentation="The current number of bad requests",
    labelnames=("unknown_retailer", "invalid_token"),
)

microsite_reward_requests = Counter(
    name=f"{METRIC_NAME_PREFIX}erroneous_rewards_for_microsite_get_requests",
    documentation="The current number of requests",
    labelnames=("response_status", "unknown_retailer", "invalid_reward_uuid"),
)
