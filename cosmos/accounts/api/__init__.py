from fastapi import APIRouter

# from cosmos.accounts.api.endpoints import enrolment

api_router = APIRouter()

# version 1
# api_router.include_router(
#     enrolment.router,
#     prefix=ROUTER_PREFIX,
#     dependencies=[Depends(user_is_authorised), Depends(bpl_channel_header_is_populated)],
# )
# api_router.include_router(
#     account_holder.bpl_operations_router,
#     prefix=ROUTER_PREFIX,
#     dependencies=[Depends(user_is_authorised)],
# )
# api_router.include_router(
#     reward.reward_router,
#     prefix=ROUTER_PREFIX,
#     dependencies=[Depends(user_is_authorised)],
# )

# api_router.include_router(
#     public_facing.public_facing_router,
#     prefix=ROUTER_PREFIX,
# )
