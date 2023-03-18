import asyncio
import uvicorn

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from sqlalchemy import select
from starlette.exceptions import HTTPException as StarletteHTTPException

# from os import environ

# environ["CHATGPT_BASE_URL"] = environ.get("CHATGPT_BASE_URL", "https://chat.openai.com/backend-api/")

import api.globals as g
from api.enums import ChatStatus
from api.models import Conversation, User
from api.response import (
    CustomJSONResponse,
    PrettyJSONResponse,
    handle_exception_response,
)
from api.config import config
from api.database import create_db_and_tables, get_async_session_context
from api.exceptions import SelfDefinedException
from api.routers import users, chat, status
from fastapi.middleware.cors import CORSMiddleware

from utils.create_user import create_user
import dateutil.parser

app = FastAPI(default_response_class=CustomJSONResponse)

app.include_router(users.router)
app.include_router(chat.router)
app.include_router(status.router)

origins = [
    "http://localhost",
    "http://localhost:4000",
]

# 解决跨站问题
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 定义若干异常处理器


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return handle_exception_response(exc)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return handle_exception_response(exc)


@app.exception_handler(SelfDefinedException)
async def validation_exception_handler(request, exc):
    return handle_exception_response(exc)


@app.on_event("startup")
async def on_startup():
    await create_db_and_tables()

    if config.get("create_initial_admin_user", False):
        await create_user(
            config.get("initial_admin_username"),
            "admin",
            "admin@admin.com",
            config.get("initial_admin_password"),
            is_superuser=True,
        )

    if config.get("create_initial_user", False):
        await create_user(
            config.get("initial_user_username"),
            "user",
            "user@user.com",
            config.get("initial_user_password"),
            is_superuser=False,
        )

    if not config.get("sync_conversations_on_startup", True):
        return

    # 重置所有用户chat_status
    async with get_async_session_context() as session:
        r = await session.execute(select(User))
        results = r.scalars().all()
        for user in results:
            user.chat_status = ChatStatus.idling
            session.add(user)
        await session.commit()


if __name__ == "__main__":
    uvicorn.run(app, host=config.get("host"), port=config.get("port"), log_level="info")
