"""
* 文件作用
  * 这是知识问答功能的 FastAPI 接口入口，负责接收用户问题、知识库 ID、会话 ID和上下文限制，并以 Server-Sent Events 形式持续返回模型生成内容。接口先校验用户、会话和知识库访问权，再创建或恢复会话，调用 KnowledgeAssistantService 启动 RAG 流程，并在嵌套异步生成器中逐块发送答案、来源和结束事件，同时保存用户消息与助手消息。该文件应重点学习 StreamingResponse、async generator、SSE 数据格式、依赖注入、会话落库和异常事件处理。
* 类说明
  * 本文件没有定义业务类，而是以模块级函数组织接口或工具能力；这样可以直接被 FastAPI 路由注册或被其他模块按函数导入调用。
* 函数与方法说明
  * `get_knowledge_assistant_config()`：知识助手配置接口，通过 KnowledgeAssistantService.get_config() 返回当前可公开给前端的问答配置，例如上下文数量、文件限制或功能开关；数据库会话由 Depends 注入，但接口不会返回 API Key 等敏感配置。
  * `ask_knowledge_assistant()`：知识问答主接口，接收 question、knowledge_base_id、conversation_id 和历史消息，验证当前用户的会话与知识库访问权限后调用 KnowledgeAssistantService.ask_question_stream()；结果通过 StreamingResponse 以 SSE 分块发送，结束时把用户问题、助手完整回答和 sources 写入 messages 表。
  * `ask_knowledge_assistant.generate_stream()`：作为 SSE 异步生成器消费 RAGService 返回的事件：累计回答正文与 sources，逐条序列化为 SSE 数据发送给前端；流结束后把助手完整回答和引用来源写入 messages 表，异常时发送 error 事件。
  * `get_knowledge_documents()`：知识问答页面的文档列表接口，按 knowledge_base_id、当前用户和分页参数调用 KnowledgeAssistantService.get_documents()，只返回用户有权查看且属于目标知识库的文档元数据，供前端展示当前问答的数据来源范围。
  * `auto_select_kb()`：知识库自动选择接口，接收用户问题和最大候选数，先取得当前用户可访问的知识库，再由 KBSelectionService 使用文档信息与 LLM 相关性判断选择最匹配的知识库，返回其 knowledge_base_id；没有合适候选或模型失败时返回明确的失败结果。
"""

"""
知识助手端点，用于文档处理和问答
"""
# 从 `typing` 导入当前模块需要的 `Any, List, Dict`。
from typing import Any, List, Dict
# 从 `fastapi` 导入当前模块需要的 `APIRouter, Depends, HTTPException, status, UploadFile, File, Form`。
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
# 从 `fastapi.responses` 导入当前模块需要的 `StreamingResponse`。
from fastapi.responses import StreamingResponse
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 导入 `json`，供后续代码调用其功能。
import json
# 导入 `logging`，供后续代码调用其功能。
import logging

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)

# 从 `app.core.database` 导入当前模块需要的 `get_db`。
from app.core.database import get_db
# 从 `app.schemas.document` 导入当前模块需要的 `Document as DocumentSchema`。
from app.schemas.document import Document as DocumentSchema
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema
# 从 `app.services.knowledge_assistant_service` 导入当前模块需要的 `KnowledgeAssistantService`。
from app.services.knowledge_assistant_service import KnowledgeAssistantService
# 从 `app.services.conversation_service` 导入当前模块需要的 `ConversationService`。
from app.services.conversation_service import ConversationService
# 从 `app.models.conversation` 导入当前模块需要的 `MessageRole`。
from app.models.conversation import MessageRole
# 从 `app.api.deps` 导入当前模块需要的 `get_current_user`。
from app.api.deps import get_current_user
# 从 `app.services.kb_selection_service` 导入当前模块需要的 `KBSelectionService`。
from app.services.kb_selection_service import KBSelectionService

# 计算或设置 `router`，用于保存后续流程需要的中间结果。
router = APIRouter()


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/config")
# 定义 `get_knowledge_assistant_config()`，执行该函数对应的业务流程。
async def get_knowledge_assistant_config(
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
):
    """
    获取知识助手配置
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeAssistantService(db)
    # 结束当前函数，并把处理结果返回给调用方。
    return await service.get_config()

# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/ask")
# 定义 `ask_knowledge_assistant()`，执行该函数对应的业务流程。
async def ask_knowledge_assistant(
    # 声明参数或字段 `question`，表示用户当前问题。
    question: str = Form(...),
    # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
    knowledge_base_id: str = Form(None),
    # 声明参数或字段 `conversation_history`，表示此前的多轮对话历史。
    conversation_history: str = Form("[]"),  # 对话历史的JSON字符串
    # 声明参数或字段 `conversation_id`，表示会话唯一标识。
    conversation_id: str = Form(...),
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    使用RAG工作流向知识助手提问，返回流式响应
        接收问题、知识库ID、历史消息等，返回流式RAG回答。
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeAssistantService(db)
    # 计算或设置 `conv_service`，用于保存后续流程需要的中间结果。
    conv_service = ConversationService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 解析对话历史
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `conv_history`，用于保存后续流程需要的中间结果。
            conv_history = json.loads(conversation_history) if conversation_history else []
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except json.JSONDecodeError:
            # 计算或设置 `conv_history`，用于保存后续流程需要的中间结果。
            conv_history = []
        
        # 验证会话id（必须存在且属于当前用户）
        # 从 `uuid` 导入当前模块需要的 `UUID`。
        from uuid import UUID
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `conv_uuid`，用于保存后续流程需要的中间结果。
            conv_uuid = UUID(conversation_id)
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid conversation_id")

        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `conversation`。
        conversation = await conv_service.get_conversation(conv_uuid, current_user.id)
        # 判断 `not conversation` 是否成立，以选择正确的处理分支。
        if not conversation:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Conversation not found or no permission")
        # 计算或设置 `conv_id`，用于保存后续流程需要的中间结果。
        conv_id = str(conversation.id)
        
        # 生成流式响应
        # 定义 `generate_stream()`，执行该函数对应的业务流程。
        async def generate_stream():
            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            try:
                # 计算或设置 `assistant_buffer`，用于保存后续流程需要的中间结果。
                assistant_buffer = ""
                # 计算或设置 `start_sources`，用于保存后续流程需要的中间结果。
                start_sources = []
                # 开始组装多行调用参数、集合或对象内容。
                async for chunk in service.ask_question_stream(
                    # 计算或设置 `question`，用于保存用户当前问题。
                    question=question,
                    # 计算或设置 `user_id`，用于保存用户唯一标识。
                    user_id=current_user.id,
                    # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
                    knowledge_base_id=knowledge_base_id,
                    # 计算或设置 `conversation_history`，用于保存此前的多轮对话历史。
                    conversation_history=conv_history
                # 结束上方多行参数、集合或对象的定义。
                ):
                    # 判断 `chunk.get("type") == "start"` 是否成立，以选择正确的处理分支。
                    if chunk.get("type") == "start":
                        # 计算或设置 `start_sources`，用于保存后续流程需要的中间结果。
                        start_sources = chunk.get("sources") or []
                        # 计算或设置 `payload`，用于保存后续流程需要的中间结果。
                        payload = {**chunk, "conversation_id": conv_id}
                        # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    # 判断 `chunk.get("type") == "chunk"` 是否成立，以选择正确的处理分支。
                    elif chunk.get("type") == "chunk":
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        assistant_buffer += chunk.get("content", "")
                        # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                    # 判断 `chunk.get("type") == "end"` 是否成立，以选择正确的处理分支。
                    elif chunk.get("type") == "end":
                        # 将消息保存到对话中
                        payload = {**chunk, "conversation_id": conv_id}
                        # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    # 前面的判断条件均不成立时，执行当前备用分支。
                    else:
                        # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 从 `uuid` 导入当前模块需要的 `UUID`。
                    from uuid import UUID
                    # 异步等待 `conv_service.add_message` 完成，避免阻塞其他并发请求。
                    await conv_service.add_message(
                        # 计算或设置 `conversation_id`，用于保存会话唯一标识。
                        conversation_id=UUID(conv_id),
                        # 计算或设置 `content`，用于保存当前处理的正文内容。
                        content=question,
                        # 计算或设置 `role`，用于保存消息发送者角色。
                        role=MessageRole.USER,
                        # 计算或设置 `context`，用于保存提供给模型的上下文。
                        context={"knowledge_base_id": str(knowledge_base_id) if knowledge_base_id else None}
                    # 结束上方多行参数、集合或对象的定义。
                    )
                    # 异步等待 `conv_service.add_message` 完成，避免阻塞其他并发请求。
                    await conv_service.add_message(
                        # 计算或设置 `conversation_id`，用于保存会话唯一标识。
                        conversation_id=UUID(conv_id),
                        # 计算或设置 `content`，用于保存当前处理的正文内容。
                        content=assistant_buffer,
                        # 计算或设置 `role`，用于保存消息发送者角色。
                        role=MessageRole.ASSISTANT,
                        # 计算或设置 `context`，用于保存提供给模型的上下文。
                        context={"sources": start_sources}
                    # 结束上方多行参数、集合或对象的定义。
                    )
            
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except GeneratorExit:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.info("客户端断开连接，停止生成流式响应")
                    # 结束当前函数，并把处理结果返回给调用方。
                    return
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except Exception as e:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.error(f"流式响应生成错误: {str(e)}")
                # 开始组装多行调用参数、集合或对象内容。
                error_data = {
                    # 补充当前字符串、提示词或响应正文内容。
                    "type": "error",
                    # 补充当前字符串、提示词或响应正文内容。
                    "error": str(e)
                # 结束上方多行参数、集合或对象的定义。
                }
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
        
        # 结束当前函数，并把处理结果返回给调用方。
        return StreamingResponse(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            generate_stream(),
            # 计算或设置 `media_type`，用于保存后续流程需要的中间结果。
            media_type="text/event-stream",
            # 开始组装多行调用参数、集合或对象内容。
            headers={
                # 补充当前字符串、提示词或响应正文内容。
                "Cache-Control": "no-cache",
                # 补充当前字符串、提示词或响应正文内容。
                "Connection": "keep-alive",
                # 补充当前字符串、提示词或响应正文内容。
                "Access-Control-Allow-Origin": "*",
                # 补充当前字符串、提示词或响应正文内容。
                "Access-Control-Allow-Headers": "Cache-Control"
            # 结束上方多行参数、集合或对象的定义。
            }
        # 结束上方多行参数、集合或对象的定义。
        )
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"生成答案错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/documents")
# 定义 `get_knowledge_documents()`，执行该函数对应的业务流程。
async def get_knowledge_documents(
    # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
    knowledge_base_id: str = None,
    # 声明参数或字段 `skip`，表示分页跳过记录数。
    skip: int = 0,
    # 声明参数或字段 `limit`，表示本次最大返回数量。
    limit: int = 20,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取知识库中的文档列表（性能优化版本）
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeAssistantService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        result = await service.get_documents(
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=current_user.id,
            # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
            knowledge_base_id=knowledge_base_id,
            # 计算或设置 `skip`，用于保存分页跳过记录数。
            skip=skip,
            # 计算或设置 `limit`，用于保存本次最大返回数量。
            limit=limit
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return result
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"获取文档列表错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )

# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/auto-select-kb")
# 定义 `auto_select_kb()`，执行该函数对应的业务流程。
async def auto_select_kb(
    # 声明参数或字段 `question`，表示用户当前问题。
    question: str = Form(...),
    # 声明参数或字段 `max_candidates`，表示当前业务需要的数据。
    max_candidates: int = Form(100),
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    通过对文档进行 LLM 排序自动选择知识库并返回知识库 ID。
        根据用户问题自动选择最合适的知识库。
    """
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 计算或设置 `kb_selector`，用于保存后续流程需要的中间结果。
        kb_selector = KBSelectionService(db)
        # 开始组装多行调用参数、集合或对象内容。
        result = await kb_selector.select_kb_for_question(
            # 计算或设置 `question`，用于保存用户当前问题。
            question=question,
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=current_user.id,
            # 计算或设置 `max_candidates`，用于保存后续流程需要的中间结果。
            max_candidates=max_candidates,
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return result
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"选择知识库时出错: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )
