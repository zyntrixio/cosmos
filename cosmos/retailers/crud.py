from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import contains_eager, joinedload

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import Campaign, Retailer


async def get_retailer_by_slug(
    db_session: "AsyncSession", retailer_slug: str, with_campaign_data: bool = False
) -> Retailer | None:
    stmt = select(Retailer).where(Retailer.slug == retailer_slug)
    if with_campaign_data:
        stmt = (
            stmt.outerjoin(Retailer.campaigns)
            .outerjoin(Retailer.stores)
            .options(
                contains_eager(Retailer.stores),
                contains_eager(Retailer.campaigns).options(
                    joinedload(Campaign.reward_rule),
                    joinedload(Campaign.earn_rule),
                ),
            )
            .where(Campaign.status == CampaignStatuses.ACTIVE)
        )
    return (await db_session.execute(stmt)).unique().scalar_one_or_none()
