from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import Table, case, func, select

from cosmos.db.models import Reward
from cosmos.rewards.config import reward_settings

from .base import BaseAgent


class PreLoaded(BaseAgent):
    def issue_reward(self) -> str | None:
        """
        Issue pre-loaded reward

        issued_date and expiry_date are set at the time of allocation

        returns Reward.associated_url | None
        """

        validity_days: int = self.reward_config.load_required_fields_values()["validity_days"]

        now = datetime.now(UTC).replace(tzinfo=None)
        expiry_date = now + timedelta(days=validity_days) if validity_days else None

        associated_url_template = (
            f"{reward_settings.PRE_LOADED_REWARD_BASE_URL}/reward?retailer={self.reward_config.retailer.slug}&reward=%s"
        )

        available_reward = (
            select(Reward.id)
            .with_for_update(skip_locked=True)
            .where(
                Reward.reward_config_id == self.reward_config.id,
                Reward.account_holder_id.is_(None),
                Reward.deleted.is_(False),
            )
            .limit(1)
        ).cte("available_reward")

        res = self.db_session.execute(
            cast(Table, Reward.__table__)
            .update()
            .values(
                account_holder_id=self.account_holder.id,
                campaign_id=self.campaign.id,
                issued_date=now,
                expiry_date=case(
                    (Reward.expiry_date.is_(None), expiry_date),
                    else_=Reward.expiry_date,
                ),
                associated_url=func.format(associated_url_template, Reward.reward_uuid),
            )
            .where(Reward.id == available_reward.c.id)
            .returning(Reward.reward_uuid, Reward.issued_date, Reward.expiry_date, Reward.associated_url)
        ).all()

        if len(res) > 1:  # pragma: no cover
            # this should not be possbile but it's here as safeguard in case we modify db contraints
            self.db_session.rollback()
            raise ValueError("Something went wrong, more than one Reward picked up, rolling back")

        reward_data = res[0] if res else None

        if reward_data:

            if not reward_data.expiry_date:  # pragma: no cover
                # this should not be possbile but it's here as safeguard in case we modify db contraints
                self.db_session.rollback()
                raise ValueError("Both validity_days and expiry_date are None")

            self.db_session.commit()
            self._send_issued_reward_activity(reward_uuid=reward_data.reward_uuid, issued_date=reward_data.issued_date)
        else:
            self.db_session.rollback()

        return reward_data.associated_url if reward_data else None

    def fetch_balance(self) -> int:  # pragma: no cover
        raise NotImplementedError
