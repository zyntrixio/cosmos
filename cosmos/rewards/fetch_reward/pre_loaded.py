from datetime import datetime, timedelta, timezone

from sqlalchemy import case, func
from sqlalchemy.future import select

from cosmos.db.models import Reward
from cosmos.rewards.config import reward_settings

from .base import BaseAgent


class PreLoaded(BaseAgent):
    def issue_reward(self) -> bool:
        """
        Issue pre-loaded reward

        issued_date and expiry_date are set at the time of allocation

        returns Success True | False
        """

        validity_days: int = self.reward_config.load_required_fields_values()["validity_days"]

        now = datetime.now(timezone.utc).replace(tzinfo=None)
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
            Reward.__table__.update()
            .values(
                account_holder_id=self.account_holder.id,
                campaign_id=self.campaign.id,
                issued_date=now,
                expiry_date=case(
                    [(Reward.expiry_date.is_(None), expiry_date)],
                    else_=Reward.expiry_date,
                ),
                associated_url=func.format(associated_url_template, Reward.reward_uuid),
            )
            .where(Reward.id == available_reward.c.id)
            .returning(Reward.reward_uuid, Reward.issued_date, Reward.expiry_date)
        )

        if res.rowcount > 1:  # pragma: no cover
            # this should not be possbile but it's here as safeguard in case we modify db contraints
            self.db_session.rollback()
            raise ValueError("Something went wrong, more than one Reward picked up, rolling back")

        success = bool(res.rowcount)
        data_for_activity = res.first()

        if success and not data_for_activity.expiry_date:  # pragma: no cover
            # this should not be possbile but it's here as safeguard in case we modify db contraints
            self.db_session.rollback()
            raise ValueError("Both validity_days and expiry_date are None")

        if success:
            self.db_session.commit()
            self._send_issued_reward_activity(
                reward_uuid=data_for_activity.reward_uuid, issued_date=data_for_activity.issued_date
            )
        else:
            self.db_session.rollback()

        return success

    def fetch_balance(self) -> int:  # pragma: no cover
        raise NotImplementedError
