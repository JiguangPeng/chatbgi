from datetime import datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select

import api.globals as g
from api.database import get_async_session_context
from api.enums import ChatStatus
from api.models import User
from api.schema import ServerStatusSchema
from api.users import current_active_user

router = APIRouter()


server_status_cache = None
server_status_cache_last_update_time: datetime | None = None


@router.get("/status", tags=["status"], response_model=ServerStatusSchema)
async def get_status(_user: User = Depends(current_active_user)):
    global server_status_cache
    global server_status_cache_last_update_time
    if server_status_cache is not None and server_status_cache_last_update_time is not None:
        if server_status_cache_last_update_time > datetime.utcnow() - timedelta(seconds=5):
            return server_status_cache
    async with get_async_session_context() as session:
        users = await session.execute(select(User))
        users = users.scalars().all()
    # 根据 active_time, 统计 5m/1h/1d 内的在线人数
    active_user_in_5m = 0
    active_user_in_1h = 0
    active_user_in_1d = 0
    current_time = datetime.utcnow()
    queueing_count = 0
    for user in users:
        if not user.active_time or user.is_superuser:
            continue
        if user.chat_status == ChatStatus.queueing:
            queueing_count += 1
        if user.active_time > current_time - timedelta(minutes=5):
            active_user_in_5m += 1
        if user.active_time > current_time - timedelta(hours=1):
            active_user_in_1h += 1
        if user.active_time > current_time - timedelta(days=1):
            active_user_in_1d += 1

    server_status_cache = ServerStatusSchema(
        active_user_in_5m=active_user_in_5m,
        active_user_in_1h=active_user_in_1h,
        active_user_in_1d=active_user_in_1d,
        is_chatbot_busy=any(_.locked() for _ in g.chatgpt_manager.semaphore_dict.values()),
        chatbot_waiting_count=queueing_count
    )
    server_status_cache_last_update_time = datetime.utcnow()
    return server_status_cache

