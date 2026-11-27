"""
* 文件作用
  * 这是知识问答会话与消息管理的 FastAPI 接口层，负责创建会话、分页查询会话、读取详情、修改标题、删除会话，以及对单条消息执行新增、修改、删除和反馈操作。接口通过 Depends 注入 current_user 与 AsyncSession，把 user_id 和 conversation_id 交给 ConversationService 做数据库查询和所有权校验，再将 ORM 对象转换为响应 Schema。该文件体现 RESTful 路由设计、异步接口、依赖注入、分页参数和 HTTP 异常映射，是理解前端聊天记录如何落库与恢复的入口。
* 类说明
  * 本文件没有定义业务类，而是以模块级函数组织接口或工具能力；这样可以直接被 FastAPI 路由注册或被其他模块按函数导入调用。
* 函数与方法说明
  * `get_conversations()`：当前用户会话列表接口，通过 JWT 鉴权取得 current_user.id，按 skip/limit 分页调用 ConversationService.get_user_conversations() 查询 conversations 表，只返回属于该用户且未被排除的会话，并按最近更新时间排列供前端恢复历史对话。
  * `create_conversation()`：会话创建接口，把请求体中的标题、knowledge_base_id 和其他会话信息与 current_user.id 组合后交给 ConversationService.create_conversation()，在 conversations 表生成新的 UUID 会话记录并提交事务，返回创建后的会话对象。
  * `get_conversation()`：会话详情接口，根据 conversation_id 调用 get_conversation_with_permission_check()；该服务同时检查记录存在性和 user_id 所有权，只有会话所属用户才能取得详情，不存在返回 404、非本人访问返回 403。
  * `update_conversation()`：会话更新接口，先按 conversation_id 校验会话属于当前用户，再把 ConversationUpdate 中实际提供的标题、状态或知识库关联字段交给 ConversationService 更新并提交，返回刷新后的会话记录。
  * `delete_conversation()`：会话删除接口，先校验 conversation_id 对应会话归当前用户所有，再由 ConversationService 删除会话及关联消息或执行模型定义的删除行为并提交事务；失败转换为 HTTP 400，防止用户删除他人的聊天记录。
  * `get_conversation_messages()`：会话消息分页接口，先验证当前用户拥有 conversation_id，再按 skip/limit 查询 messages 表中属于该会话的记录，并按消息时间顺序返回，用于前端重新展示完整问答历史。
  * `add_conversation_message()`：会话消息新增接口，先验证会话所有权，再把 MessageCreate 中的角色、正文、来源和 token 信息交给 ConversationService.add_message() 写入 messages 表，同时更新会话 message_count 并返回新消息。
  * `update_conversation_message()`：消息更新接口，先验证当前用户拥有外层 conversation_id，再按 message_id 查找该会话中的消息并修改允许更新的字段；消息不存在返回 404，成功后提交事务并返回最新消息对象。
  * `delete_conversation_message()`：消息删除接口，先校验会话所有权，再确认 message_id 确实属于该会话，随后删除 messages 记录并同步维护会话的 message_count；消息不存在返回 404，避免跨会话删除。
"""

"""
负责会话和消息管理

    调用 ConversationService。
    会话接口负责创建、查询、更新、删除会话。
    消息接口负责列出对话消息、追加消息、更新消息、删除消息。
    所有接口都依赖 get_current_user，所以会按用户权限限制访问自己的会话。
"""
# 从 `typing` 导入当前模块需要的 `Any, List`。
from typing import Any, List
# 从 `fastapi` 导入当前模块需要的 `APIRouter, Depends, HTTPException`。
from fastapi import APIRouter, Depends, HTTPException
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession

# 从 `app.core.database` 导入当前模块需要的 `get_db`。
from app.core.database import get_db
# 从 `app.schemas.conversation` 导入当前模块需要的 `(`。
from app.schemas.conversation import (
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    Conversation as ConversationSchema,
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    ConversationCreate,
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    MessageCreate,
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    MessageUpdate,
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    ConversationUpdate
# 结束上方多行参数、集合或对象的定义。
)
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema
# 从 `app.services.conversation_service` 导入当前模块需要的 `ConversationService`。
from app.services.conversation_service import ConversationService
# 从 `app.api.deps` 导入当前模块需要的 `get_current_user`。
from app.api.deps import get_current_user


# 计算或设置 `router`，用于保存后续流程需要的中间结果。
router = APIRouter()


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/", response_model=List[ConversationSchema])
# 定义 `get_conversations()`，执行该函数对应的业务流程。
async def get_conversations(
    # 声明参数或字段 `skip`，表示分页跳过记录数。
    skip: int = 0,
    # 声明参数或字段 `limit`，表示本次最大返回数量。
    limit: int = 100,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取用户的对话列表
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        conversations = await conversation_service.get_user_conversations(
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=current_user.id,
            # 计算或设置 `skip`，用于保存分页跳过记录数。
            skip=skip,
            # 计算或设置 `limit`，用于保存本次最大返回数量。
            limit=limit
        # 结束上方多行参数、集合或对象的定义。
        )
        # 计算或设置 `result`，用于保存后续流程需要的中间结果。
        result = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for conv in conversations:
            # 开始组装多行调用参数、集合或对象内容。
            result.append({
                # 补充当前字符串、提示词或响应正文内容。
                "id": conv.id,
                # 补充当前字符串、提示词或响应正文内容。
                "user_id": conv.user_id,
                # 补充当前字符串、提示词或响应正文内容。
                "title": conv.title,
                # 补充当前字符串、提示词或响应正文内容。
                "description": conv.description,
                # 补充当前字符串、提示词或响应正文内容。
                "status": conv.status,
                # 补充当前字符串、提示词或响应正文内容。
                "message_count": conv.total_messages,  # Map total_messages to message_count for schema
                # 补充当前字符串、提示词或响应正文内容。
                "meta_data": conv.meta_data,
                # 补充当前字符串、提示词或响应正文内容。
                "created_at": conv.created_at,
                # 补充当前字符串、提示词或响应正文内容。
                "updated_at": conv.updated_at
            # 结束上方多行参数、集合或对象的定义。
            })
        
        # 结束当前函数，并把处理结果返回给调用方。
        return result
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "获取对话列表")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/", response_model=ConversationSchema)
# 定义 `create_conversation()`，执行该函数对应的业务流程。
async def create_conversation(
    # 声明参数或字段 `conversation_data`，表示当前业务需要的数据。
    conversation_data: ConversationCreate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    创建新的对话
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        conversation = await conversation_service.create_conversation(
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=current_user.id,
            # 计算或设置 `conversation_data`，用于保存后续流程需要的中间结果。
            conversation_data=conversation_data
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 补充当前字符串、提示词或响应正文内容。
            "id": conversation.id,
            # 补充当前字符串、提示词或响应正文内容。
            "user_id": conversation.user_id,
            # 补充当前字符串、提示词或响应正文内容。
            "title": conversation.title,
            # 补充当前字符串、提示词或响应正文内容。
            "description": conversation.description,
            # 补充当前字符串、提示词或响应正文内容。
            "status": conversation.status,
            # 补充当前字符串、提示词或响应正文内容。
            "message_count": conversation.total_messages,
            # 补充当前字符串、提示词或响应正文内容。
            "meta_data": conversation.meta_data,
            # 补充当前字符串、提示词或响应正文内容。
            "created_at": conversation.created_at,
            # 补充当前字符串、提示词或响应正文内容。
            "updated_at": conversation.updated_at
        # 结束上方多行参数、集合或对象的定义。
        }

    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "创建对话")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/{conversation_id}", response_model=ConversationSchema)
# 定义 `get_conversation()`，执行该函数对应的业务流程。
async def get_conversation(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    根据ID获取对话
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        conversation = await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 补充当前字符串、提示词或响应正文内容。
            "id": conversation.id,
            # 补充当前字符串、提示词或响应正文内容。
            "user_id": conversation.user_id,
            # 补充当前字符串、提示词或响应正文内容。
            "title": conversation.title,
            # 补充当前字符串、提示词或响应正文内容。
            "description": conversation.description,
            # 补充当前字符串、提示词或响应正文内容。
            "status": conversation.status,
            # 补充当前字符串、提示词或响应正文内容。
            "message_count": conversation.total_messages,
            # 补充当前字符串、提示词或响应正文内容。
            "meta_data": conversation.meta_data,
            # 补充当前字符串、提示词或响应正文内容。
            "created_at": conversation.created_at,
            # 补充当前字符串、提示词或响应正文内容。
            "updated_at": conversation.updated_at
        # 结束上方多行参数、集合或对象的定义。
        }
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "获取对话")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.put("/{conversation_id}", response_model=ConversationSchema)
# 定义 `update_conversation()`，执行该函数对应的业务流程。
async def update_conversation(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `conversation_update`，表示当前业务需要的数据。
    conversation_update: ConversationUpdate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    更新对话信息
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)

    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 权限检查
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        
        # 开始组装多行调用参数、集合或对象内容。
        updated_conversation = await conversation_service.update_conversation(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user.id, conversation_update
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return updated_conversation
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "更新对话")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.delete("/{conversation_id}")
# 定义 `delete_conversation()`，执行该函数对应的业务流程。
async def delete_conversation(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    删除对话
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 权限检查
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        
        # 开始组装多行调用参数、集合或对象内容。
        success = await conversation_service.delete_conversation(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user.id
        # 结束上方多行参数、集合或对象的定义。
        )
        # 判断 `not success` 是否成立，以选择正确的处理分支。
        if not success:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=400,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="删除对话失败"
            # 结束上方多行参数、集合或对象的定义。
            )
        # 结束当前函数，并把处理结果返回给调用方。
        return {"message": "对话删除成功"}
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "删除对话")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/{conversation_id}/messages")
# 定义 `get_conversation_messages()`，执行该函数对应的业务流程。
async def get_conversation_messages(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `skip`，表示分页跳过记录数。
    skip: int = 0,
    # 声明参数或字段 `limit`，表示本次最大返回数量。
    limit: int = 100,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取对话中的消息列表
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 权限检查
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        
        # 开始组装多行调用参数、集合或对象内容。
        messages = await conversation_service.get_conversation_messages(
            # 计算或设置 `conversation_id`，用于保存会话唯一标识。
            conversation_id=conversation_id,
            # 计算或设置 `skip`，用于保存分页跳过记录数。
            skip=skip,
            # 计算或设置 `limit`，用于保存本次最大返回数量。
            limit=limit
        # 结束上方多行参数、集合或对象的定义。
        )
        # Convert messages to dict to avoid PydanticSerializationError
        # 计算或设置 `result`，用于保存后续流程需要的中间结果。
        result = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for message in messages:
            # 开始组装多行调用参数、集合或对象内容。
            result.append({
                # 补充当前字符串、提示词或响应正文内容。
                "id": message.id,
                # 补充当前字符串、提示词或响应正文内容。
                "conversation_id": message.conversation_id,
                # 补充当前字符串、提示词或响应正文内容。
                "content": message.content,
                # 补充当前字符串、提示词或响应正文内容。
                "role": message.role,
                # 补充当前字符串、提示词或响应正文内容。
                "model_name": message.model_name,
                # 补充当前字符串、提示词或响应正文内容。
                "context": message.context,
                # 补充当前字符串、提示词或响应正文内容。
                "meta_data": message.meta_data,
                # 补充当前字符串、提示词或响应正文内容。
                "rating": message.rating,
                # 补充当前字符串、提示词或响应正文内容。
                "feedback": message.feedback,
                # 补充当前字符串、提示词或响应正文内容。
                "parent_message_id": message.parent_message_id,
                # 补充当前字符串、提示词或响应正文内容。
                "created_at": message.created_at,
                # 补充当前字符串、提示词或响应正文内容。
                "updated_at": message.updated_at
            # 结束上方多行参数、集合或对象的定义。
            })
        
        # 结束当前函数，并把处理结果返回给调用方。
        return result
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "获取对话消息")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/{conversation_id}/messages")
# 定义 `add_conversation_message()`，执行该函数对应的业务流程。
async def add_conversation_message(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `message_data`，表示当前业务需要的数据。
    message_data: MessageCreate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    向对话追加单条消息
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)

    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )

        # 开始组装多行调用参数、集合或对象内容。
        message = await conversation_service.add_message(
            # 计算或设置 `conversation_id`，用于保存会话唯一标识。
            conversation_id=conversation_id,
            # 计算或设置 `content`，用于保存当前处理的正文内容。
            content=message_data.content,
            # 计算或设置 `role`，用于保存消息发送者角色。
            role=message_data.role,
            # 计算或设置 `model_name`，用于保存后续流程需要的中间结果。
            model_name=message_data.model_name,
            # 计算或设置 `context`，用于保存提供给模型的上下文。
            context=message_data.context,
            # 计算或设置 `parent_id`，用于保存后续流程需要的中间结果。
            parent_id=message_data.parent_id
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 补充当前字符串、提示词或响应正文内容。
            "id": message.id,
            # 补充当前字符串、提示词或响应正文内容。
            "conversation_id": message.conversation_id,
            # 补充当前字符串、提示词或响应正文内容。
            "content": message.content,
            # 补充当前字符串、提示词或响应正文内容。
            "role": message.role,
            # 补充当前字符串、提示词或响应正文内容。
            "model_name": message.model_name,
            # 补充当前字符串、提示词或响应正文内容。
            "context": message.context,
            # 补充当前字符串、提示词或响应正文内容。
            "meta_data": message.meta_data,
            # 补充当前字符串、提示词或响应正文内容。
            "created_at": message.created_at,
            # 补充当前字符串、提示词或响应正文内容。
            "updated_at": message.updated_at
        # 结束上方多行参数、集合或对象的定义。
        }
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "保存对话消息")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.put("/{conversation_id}/messages/{message_id}")
# 定义 `update_conversation_message()`，执行该函数对应的业务流程。
async def update_conversation_message(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `message_id`，表示消息唯一标识。
    message_id: str,
    # 声明参数或字段 `message_update`，表示当前业务需要的数据。
    message_update: MessageUpdate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    更新对话中的单条消息
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)

    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )

        # 开始组装多行调用参数、集合或对象内容。
        message = await conversation_service.update_message(
            # 计算或设置 `conversation_id`，用于保存会话唯一标识。
            conversation_id=conversation_id,
            # 计算或设置 `message_id`，用于保存消息唯一标识。
            message_id=message_id,
            # 计算或设置 `message_update`，用于保存后续流程需要的中间结果。
            message_update=message_update
        # 结束上方多行参数、集合或对象的定义。
        )
        # 判断 `not message` 是否成立，以选择正确的处理分支。
        if not message:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(status_code=404, detail="消息未找到")
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 补充当前字符串、提示词或响应正文内容。
            "id": message.id,
            # 补充当前字符串、提示词或响应正文内容。
            "conversation_id": message.conversation_id,
            # 补充当前字符串、提示词或响应正文内容。
            "content": message.content,
            # 补充当前字符串、提示词或响应正文内容。
            "role": message.role,
            # 补充当前字符串、提示词或响应正文内容。
            "model_name": message.model_name,
            # 补充当前字符串、提示词或响应正文内容。
            "context": message.context,
            # 补充当前字符串、提示词或响应正文内容。
            "meta_data": message.meta_data,
            # 补充当前字符串、提示词或响应正文内容。
            "created_at": message.created_at,
            # 补充当前字符串、提示词或响应正文内容。
            "updated_at": message.updated_at
        # 结束上方多行参数、集合或对象的定义。
        }
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "更新对话消息")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.delete("/{conversation_id}/messages/{message_id}")
# 定义 `delete_conversation_message()`，执行该函数对应的业务流程。
async def delete_conversation_message(
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str,
    # 声明参数或字段 `message_id`，表示消息唯一标识。
    message_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    删除对话中的单条消息
    """
    # 计算或设置 `conversation_service`，用于保存后续流程需要的中间结果。
    conversation_service = ConversationService(db)

    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 异步等待 `conversation_service.get_conversation_with_permission_check` 完成，避免阻塞其他并发请求。
        await conversation_service.get_conversation_with_permission_check(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            conversation_id, current_user
        # 结束上方多行参数、集合或对象的定义。
        )

        # 开始组装多行调用参数、集合或对象内容。
        success = await conversation_service.delete_message(
            # 计算或设置 `conversation_id`，用于保存会话唯一标识。
            conversation_id=conversation_id,
            # 计算或设置 `message_id`，用于保存消息唯一标识。
            message_id=message_id
        # 结束上方多行参数、集合或对象的定义。
        )
        # 判断 `not success` 是否成立，以选择正确的处理分支。
        if not success:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(status_code=404, detail="消息未找到")
        # 结束当前函数，并把处理结果返回给调用方。
        return {"message": "消息删除成功"}
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        raise
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise conversation_service.handle_conversation_error(e, "删除对话消息")
