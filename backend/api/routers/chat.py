import asyncio
import time
from datetime import datetime
from typing import List
import json
import requests
from fastapi import APIRouter, Depends, WebSocket
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select, or_, and_, delete, func
import api.globals as g
from api.config import config
from api.database import get_async_session_context
from api.enums import ChatStatus
from api.exceptions import InvalidParamsException, AuthorityDenyException
from api.models import User, Conversation
from api.schema import ConversationSchema
from api.users import current_active_user, websocket_auth, current_super_user
from utils.common import async_wrap_iter
from api.response import response

router = APIRouter()


async def get_conversation_by_id(
    conversation_id: str, user: User = Depends(current_active_user)
):
    async with get_async_session_context() as session:
        r = await session.execute(
            select(Conversation).where(Conversation.conversation_id == conversation_id)
        )
        conversation = r.scalars().one_or_none()
        if conversation is None:
            raise InvalidParamsException("errors.conversationNotFound")
        if not user.is_superuser and conversation.user_id != user.id:
            raise AuthorityDenyException
        return conversation


@router.get("/conv", tags=["conversation"], response_model=List[ConversationSchema])
async def get_all_conversations(
    user: User = Depends(current_active_user), valid_only: bool = True
):
    """
    对于普通用户，返回其自己的有效会话
    对于管理员，返回所有对话，并可以指定是否只返回有效会话
    """
    #g.chatgpt_manager.clean_api()
    #print(g.chatgpt_manager.api_dict)
    if not valid_only and not user.is_superuser:
        raise AuthorityDenyException()
    stat = None
    if not user.is_superuser:
        stat = and_(Conversation.user_id == user.id, Conversation.is_valid)
    else:
        if valid_only:
            stat = Conversation.is_valid
    async with get_async_session_context() as session:
        if stat is not None:
            r = await session.execute(select(Conversation).where(stat))
        else:
            r = await session.execute(select(Conversation))
        results = r.scalars().all()
        results = jsonable_encoder(results)
        return results


@router.get("/conv/{conversation_id}", tags=["conversation"])
async def get_conversation_history(
    conversation: Conversation = Depends(get_conversation_by_id),
):
    result = jsonable_encoder(json.loads(conversation.record))
    return result


@router.delete("/conv/{conversation_id}", tags=["conversation"])
async def delete_conversation(
    conversation: Conversation = Depends(get_conversation_by_id),
):
    if not conversation.is_valid:
        raise InvalidParamsException("errors.conversationAlreadyDeleted")
    async with get_async_session_context() as session:
        conversation.is_valid = False
        session.add(conversation)
        await session.commit()
    return response(200)


@router.delete("/conv/{conversation_id}/vanish", tags=["conversation"])
async def vanish_conversation(
    conversation: Conversation = Depends(get_conversation_by_id),
):
    async with get_async_session_context() as session:
        await session.execute(
            delete(Conversation).where(
                Conversation.conversation_id == conversation.conversation_id
            )
        )
        await session.commit()
    return response(200)


@router.patch(
    "/conv/{conversation_id}", tags=["conversation"], response_model=ConversationSchema
)
async def change_conversation_title(
    title: str, conversation: Conversation = Depends(get_conversation_by_id)
):
    async with get_async_session_context() as session:
        conversation.title = title
        session.add(conversation)
        await session.commit()
        await session.refresh(conversation)
    result = jsonable_encoder(conversation)
    return result


@router.patch("/conv/{conversation_id}/assign/{username}", tags=["conversation"])
async def assign_conversation(
    username: str, conversation_id: str, _user: User = Depends(current_super_user)
):
    async with get_async_session_context() as session:
        user = await session.execute(select(User).where(User.username == username))
        user = user.scalars().one_or_none()
        if user is None:
            raise InvalidParamsException("errors.userNotFound")
        conversation = await session.execute(
            select(Conversation).where(Conversation.conversation_id == conversation_id)
        )
        conversation = conversation.scalars().one_or_none()
        if conversation is None:
            raise InvalidParamsException("errors.conversationNotFound")
        conversation.user_id = user.id
        session.add(conversation)
        await session.commit()
    return response(200)


async def change_user_chat_status(user_id: int, status: ChatStatus):
    async with get_async_session_context() as session:
        user = await session.get(User, user_id)
        user.chat_status = status
        session.add(user)
        await session.commit()
        await session.refresh(user)
    return user


@router.patch(
    "/conv/{conversation_id}/gen_title",
    tags=["conversation"],
    response_model=ConversationSchema,
)
async def generate_conversation_title(
    message_id: str, conversation: Conversation = Depends(get_conversation_by_id)
):
    if conversation.title is not None:
        raise InvalidParamsException("errors.conversationTitleAlreadyGenerated")
    result = jsonable_encoder(conversation)
    return result


@router.websocket("/conv")
async def ask(websocket: WebSocket):
    """
    利用 WebSocket 实时更新 ChatGPT 回复.

    客户端第一次连接：发送 { message, conversation_id?, parent_id?, use_paid?, timeout? }
        conversation_id 为空则新建会话，否则回复 parent_id 指定的消息
    服务端返回格式：{ type, tip, message, conversation_id, parent_id, use_paid, title }
    其中：type 可以为 "waiting" / "message" / "title"
    """

    await websocket.accept()
    user = await websocket_auth(websocket)
    print(f"{user.username} connected to websocket")

    if user is None:
        await websocket.close(1008, "errors.unauthorized")
        return

    if user.chat_status != ChatStatus.idling:
        await websocket.close(1008, "errors.cannotConnectMoreThanOneClient")
        return

    # 读取用户输入
    params = await websocket.receive_json()
    message = params.get("message", None)
    conversation_id = params.get("conversation_id", None)
    parent_id = params.get("parent_id", None)
    use_paid = params.get("use_paid", None)
    timeout = params.get("timeout", 360)  # default 360s
    new_title = params.get("new_title", None)

    if message is None:
        await websocket.close(1007, "errors.missingMessage")
        return
    if parent_id is not None and conversation_id is None:
        await websocket.close(1007, "errors.missingConversationId")
        return
    if use_paid:
        if not user.can_use_paid:
            await websocket.close(1007, "errors.userNotAllowToUsePaidModel")
            return
        if not config.get("chatgpt_paid", False):
            await websocket.close(1007, "errors.paidModelNotAvailable")
            return

    new_conv = conversation_id is None
    conversation_history = ""
    if not new_conv:
        conversation = await get_conversation_by_id(conversation_id, user)
        conversation_history = conversation.record
        if not use_paid:
            use_paid = conversation.use_paid or False

    # 判断是否能新建对话，以及是否能继续提问
    async with get_async_session_context() as session:
        user_conversations_count = await session.execute(
            select(func.count(Conversation.id)).filter(Conversation.user_id == user.id)
        )
        user_conversations_count = user_conversations_count.scalar()
        if (
            new_conv
            and user.max_conv_count != -1
            and user_conversations_count >= user.max_conv_count
        ):
            await websocket.close(1008, "errors.maxConversationCountReached")
            return
        if user.available_ask_count != -1 and user.available_ask_count <= 0:
            await websocket.close(1008, "errors.noAvailableAskCount")
            return

    if g.chatgpt_manager.is_busy(conversation_id):
        await websocket.send_json({"type": "waiting", "tip": "tips.queueing"})

    websocket_code = 1001
    websocket_reason = "tips.terminated"
    try:
        # 标记用户为 queueing
        await change_user_chat_status(user.id, ChatStatus.queueing)
        async with g.chatgpt_manager.semaphore_dict[conversation_id]:
            await change_user_chat_status(user.id, ChatStatus.asking)
            await websocket.send_json({"type": "waiting", "tip": "tips.waiting"})
            request_start_time = time.time()
            (
                ask_gen,
                new_conversation_id,
                new_parent_id,
            ) = g.chatgpt_manager.get_ask_generator(
                message,
                use_paid,
                conversation_id,
                parent_id,
                conversation_history,
                timeout,
            )
            message = ""
            async for data in async_wrap_iter(ask_gen):
                message += data
                reply = {
                    "type": "message",
                    "message": message,
                    "conversation_id": conversation_id,
                    "parent_id": new_parent_id,
                    "use_paid": use_paid,
                }
                await websocket.send_json(reply)
                if conversation_id is None:
                    conversation_id = new_conversation_id
            g.chatgpt_manager.api_dict[conversation_id].active_time = datetime.utcnow()
            print(
                f"finish ask {conversation_id}, using time: {time.time() - request_start_time}s"
            )


            async with get_async_session_context() as session:
                # 若新建了对话，则添加到数据库
                if new_conv and conversation_id is not None:
                    current_time = datetime.utcnow()
                    conversation = Conversation(
                        conversation_id=conversation_id,
                        title=new_title,
                        user_id=user.id,
                        use_paid=use_paid,
                        create_time=current_time,
                        active_time=current_time,
                        record=json.dumps(
                            g.chatgpt_manager.get_conversation_messages(conversation_id)
                        ),
                    )
                    session.add(conversation)
                # 若更改了 paid 类型，则更新 conversation
                if not new_conv:
                    conversation = await session.get(
                        Conversation, conversation.id
                    )  # 此前的 conversation 属于另一个session
                    conversation.active_time = datetime.utcnow()
                    if conversation.use_paid != use_paid:
                        conversation.use_paid = use_paid
                    conversation.record = json.dumps(
                        g.chatgpt_manager.get_conversation_messages(conversation_id)
                    )
                    session.add(conversation)
                # 扣除一次对话次数
                assert user.available_ask_count != 0
                if user.available_ask_count > 0:
                    user = await session.get(User, user.id)
                    user.available_ask_count -= 1
                    session.add(user)
                await session.commit()
            websocket_code = 1000
            websocket_reason = "tips.finished"
    except requests.exceptions.Timeout:
        await websocket.send_json({"type": "error", "tip": "errors.timeout"})
        websocket_code = 1001
        websocket_reason = "tips.timout"
    except Exception as e:
        print(e)
        await websocket.send_json({"type": "error", "message": e})
        websocket_code = 1011
        websocket_reason = "errors.unknownError"
    finally:
        await change_user_chat_status(user.id, ChatStatus.idling)
        await websocket.close(websocket_code, websocket_reason)
