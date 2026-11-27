"""
* 文件作用
  * 这是 HR Agent 的 FastAPI 接口层，提供普通 JSON 对话、SSE 流式对话，以及简历筛选、面试方案和试卷生成等专用流式入口。接口通过 Depends 注入当前用户和异步数据库会话，创建 AgentService 后转发请求；业务判断不在本文件完成，本文件只负责鉴权、参数接收、SSE 事件编码和 HTTP 异常转换。
* 类说明
  * 本文件没有定义业务类，使用模块级函数实现 FastAPI 接口或 Skill 阶段。
* 主要函数与方法说明
  * `chat_with_agent()`：非流式 Agent 接口：把消息、附件、会话 ID 和确认参数交给 AgentService.chat()，一次性返回完整 AgentChatResponse。
  * `stream_chat_with_agent()`：主 SSE 接口：调用 AgentService.stream_chat_agent()，逐条发送 thinking、plan、delta、artifact 和 final 事件。
  * `stream_agent_progress()`：招聘综合流式入口：转发 JD 与评分标准等 Agent 工作流的步骤和生成内容。
  * `stream_resume_screening()`：简历筛选流式入口：接收附件与 JD，逐份返回评分进度、指标和最终结果。
  * `stream_interview_plan()`：面试方案流式入口：根据已评分候选人和 JD 生成方案并返回持久化产物。
  * `stream_exam_generation()`：基于已有文档信息生成试卷的流式入口。
  * `stream_exam_generation_with_documents()`：接收上传文档，先保存/解析为考试来源，再流式生成并保存试卷。
"""

# 导入 json，用于编码和解析 ReAct JSON、SSE 事件及模型结构化结果。
import json
# 导入 Any, List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, List, Optional
# 导入 UUID，用于使用 UUID 标识用户、会话和业务资源。
from uuid import UUID

# 导入 APIRouter, Depends, File, Form, HTTPException, UploadFile, status，用于FastAPI 路由、依赖注入和 HTTP 异常能力。
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
# 导入 StreamingResponse，用于SSE StreamingResponse 响应。
from fastapi.responses import StreamingResponse
# 导入 AsyncSession，用于异步数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession

# 导入 get_current_user，用于解析 JWT 并取得当前登录用户。
from app.api.deps import get_current_user
# 导入 get_db，用于取得 FastAPI 请求范围内的异步数据库会话。
from app.core.database import get_db
# 导入 AgentChatRequest，用于Agent 请求、步骤、产物和统一响应模型。
from app.schemas.agent import AgentChatRequest
# 导入 User as UserSchema，用于当前登录用户响应模型。
from app.schemas.user import User as UserSchema
# 导入 AgentService，用于HR Agent 总编排服务。
from app.services.agent_service import AgentService

# 调用 `APIRouter()` 完成当前解析、查询或对象构造，并把返回值保存到 `router`；它表示`router` 在当前函数中承载的中间业务值。
router = APIRouter()


# 将 POST /chat 注册到下方处理函数。
@router.post("/chat")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `chat_with_agent()`。非流式 Agent 接口：把消息、附件、会话 ID 和确认参数交给 AgentService.chat()，一次性返回完整 AgentChatResponse。
async def chat_with_agent(
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `AgentChatRequest`。
    request: AgentChatRequest,
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
        通过HR Agent处理自然语言任务
            chat_with_agent 是非流式 Agent 对话。
    """
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
        agent_service = AgentService(db)
        # 进入非流式 Agent 主流程，完成规划或工具执行后一次性取得统一响应。
        response = await agent_service.chat(
            # 使用关键字参数语法，把 `request.message.strip()` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=request.message.strip(),
            # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=current_user.id,
            # 使用关键字参数语法，把 `request.conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=request.conversation_id,
            # 使用关键字参数语法，把 `request.auto_execute` 传入 `auto_execute` 参数；它表示是否允许信息完整后自动执行低风险生成工具。
            auto_execute=request.auto_execute,
            # 使用关键字参数语法，把 `request.confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
            confirmed_requirements=request.confirmed_requirements,
            # 使用关键字参数语法，把 `[item.model_dump() for item in request.attachments]` 传入 `attachments` 参数；它表示用户随消息上传的附件列表。
            attachments=[item.model_dump() for item in request.attachments],
        # 结束 `chat_with_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 把 Pydantic 响应模型转换为可 JSON 序列化的字典。
        return response.model_dump()
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except Exception as exc:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_500_INTERNAL_SERVER_ERROR` 传入 `status_code` 参数；它表示`status_code` 在当前函数中承载的中间业务值。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 使用关键字参数语法，把 `f"HR Agent 执行失败: {str(exc)}"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail=f"HR Agent 执行失败: {str(exc)}",
        # 结束异常对象构造，并使用 `raise ... from ...` 保留原始异常链，便于排查根因。
        ) from exc


# 自然语言总入口
# 将 POST /chat/stream 注册到下方处理函数。
@router.post("/chat/stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_chat_with_agent()`。主 SSE 接口：调用 AgentService.stream_chat_agent()，逐条发送 thinking、plan、delta、artifact 和 final 事件。
async def stream_chat_with_agent(
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `AgentChatRequest`。
    request: AgentChatRequest,
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
        流式处理 HR Agent 自然语言任务
            stream_chat_with_agent 返回流式执行过程。
    """

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_chat_agent(
                # 使用关键字参数语法，把 `request.message.strip()` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=request.message.strip(),
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `request.conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=request.conversation_id,
                # 使用关键字参数语法，把 `request.auto_execute` 传入 `auto_execute` 参数；它表示是否允许信息完整后自动执行低风险生成工具。
                auto_execute=request.auto_execute,
                # 使用关键字参数语法，把 `request.confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
                confirmed_requirements=request.confirmed_requirements,
                # 使用关键字参数语法，把 `[item.model_dump() for item in request.attachments]` 传入 `attachments` 参数；它表示用户随消息上传的附件列表。
                attachments=[item.model_dump() for item in request.attachments],
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"HR Agent 执行失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"HR Agent 执行失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_chat_with_agent()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_chat_with_agent` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用字典字面量开始构造 `headers`；该字典用于保存`headers` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        headers={
            # 在字典中设置 `Cache-Control` 字段为 `"no-cache"`，用于表示`Cache-Control` 在当前函数中承载的中间业务值。
            "Cache-Control": "no-cache",
            # 在字典中设置 `Connection` 字段为 `"keep-alive"`，用于表示`Connection` 在当前函数中承载的中间业务值。
            "Connection": "keep-alive",
        # 结束 `stream_chat_with_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        },
    # 结束 `stream_chat_with_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 将 POST /stream 注册到下方处理函数。
@router.post("/stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_agent_progress()`。招聘综合流式入口：转发 JD 与评分标准等 Agent 工作流的步骤和生成内容。
async def stream_agent_progress(
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `AgentChatRequest`。
    request: AgentChatRequest,
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
        流式返回 HR Agent 执行进度
             stream_agent_progress 返回流式执行过程。
    """
    # 使用 `if` 判断 `not request.confirmed_requirements`；条件成立时执行下面分支。
    if not request.confirmed_requirements:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_400_BAD_REQUEST` 传入 `status_code` 参数；它表示`status_code` 在当前函数中承载的中间业务值。
            status_code=status.HTTP_400_BAD_REQUEST,
            # 使用关键字参数语法，把 `"stream 接口需要 confirmed_requirements"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="stream 接口需要 confirmed_requirements",
        # 结束 `stream_agent_progress` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_recruitment_agent(
                # 使用关键字参数语法，把 `request.message.strip()` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=request.message.strip(),
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `request.conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=request.conversation_id,
                # 使用关键字参数语法，把 `request.confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
                confirmed_requirements=request.confirmed_requirements,
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"HR Agent 执行失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"HR Agent 执行失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_agent_progress()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_agent_progress` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用字典字面量开始构造 `headers`；该字典用于保存`headers` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        headers={
            # 在字典中设置 `Cache-Control` 字段为 `"no-cache"`，用于表示`Cache-Control` 在当前函数中承载的中间业务值。
            "Cache-Control": "no-cache",
            # 在字典中设置 `Connection` 字段为 `"keep-alive"`，用于表示`Connection` 在当前函数中承载的中间业务值。
            "Connection": "keep-alive",
        # 结束 `stream_agent_progress` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        },
    # 结束 `stream_agent_progress` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 将 POST /resume-screen/stream 注册到下方处理函数。
@router.post("/resume-screen/stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_resume_screening()`。简历筛选流式入口：接收附件与 JD，逐份返回评分进度、指标和最终结果。
async def stream_resume_screening(
    # 声明参数 `job_description_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `UUID`，默认值为 `Form(...)`。
    job_description_id: UUID = Form(...),
    # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `Form(None)`。
    conversation_id: Optional[str] = Form(None),
    # 声明参数 `files`，表示`files` 在当前函数中承载的中间业务值，类型标注为 `List[UploadFile]`，默认值为 `File(...)`。
    files: List[UploadFile] = File(...),
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
        批量上传简历并流式返回筛选进度
            resume-screen/stream 支持上传多个简历文件，流式返回筛选进度。
    """
    # 使用 `if` 判断 `not files`；条件成立时执行下面分支。
    if not files:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请至少上传一份简历")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请至少上传一份简历")
    # 使用 `if` 判断 `len(files) > 20`；条件成立时执行下面分支。
    if len(files) > 20:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="单次最多上传 20 份简历")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="单次最多上传 20 份简历")

    # 把 `file_payloads` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`file_payloads` 在当前函数中承载的中间业务值。
    file_payloads = []
    # 使用 `for` 循环 `upload_file in files`，逐项处理集合数据。
    for upload_file in files:
        # 使用 `await` 异步调用 `upload_file.read()`，等待数据库、Dify 或模型处理完成后把结果保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
        content = await upload_file.read()
        # 使用 `if` 判断 `not content`；条件成立时执行下面分支。
        if not content:
            # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{upload_file.filename} 文件内容为空")`，立即终止当前失败流程并交给上层处理。
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{upload_file.filename} 文件内容为空")
        # 调用列表的 `append()`，把 `file_payloads.append({"filename": upload_file.filename, "content": content})` 对应的新元素追加到现有结果末尾。
        file_payloads.append({"filename": upload_file.filename, "content": content})

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_resume_screening(
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `job_description_id` 传入 `job_description_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
                job_description_id=job_description_id,
                # 使用关键字参数语法，把 `file_payloads` 传入 `files` 参数；它表示`files` 在当前函数中承载的中间业务值。
                files=file_payloads,
                # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=conversation_id,
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"简历批量筛选失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"简历批量筛选失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_resume_screening()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_resume_screening` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用关键字参数语法，把 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 传入 `headers` 参数；它表示`headers` 在当前函数中承载的中间业务值。
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    # 结束 `stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 将 POST /interview-plan/stream 注册到下方处理函数。
@router.post("/interview-plan/stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_interview_plan()`。面试方案流式入口：根据已评分候选人和 JD 生成方案并返回持久化产物。
async def stream_interview_plan(
    # 声明参数 `resume_evaluation_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `UUID`，默认值为 `Form(...)`。
    resume_evaluation_id: UUID = Form(...),
    # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `Form(None)`。
    conversation_id: Optional[str] = Form(None),
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    根据已评分简历流式生成面试计划
        interview-plan/stream 根据简历评价生成面试计划。
    """

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_interview_plan(
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `resume_evaluation_id` 传入 `resume_evaluation_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
                resume_evaluation_id=resume_evaluation_id,
                # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=conversation_id,
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"面试计划生成失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"面试计划生成失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_interview_plan()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_interview_plan` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用关键字参数语法，把 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 传入 `headers` 参数；它表示`headers` 在当前函数中承载的中间业务值。
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    # 结束 `stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 将 POST /exam/stream 注册到下方处理函数。
@router.post("/exam/stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_exam_generation()`。基于已有文档信息生成试卷的流式入口。
async def stream_exam_generation(
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `AgentChatRequest`。
    request: AgentChatRequest,
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
    流式生成并保存考试试卷
        exam/stream 根据结构化需求生成试卷。
    """
    # 使用 `if` 判断 `not request.confirmed_requirements`；条件成立时执行下面分支。
    if not request.confirmed_requirements:
        # 使用 `raise` 抛出 `HTTPException(`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(
            # 使用关键字参数语法，把 `status.HTTP_400_BAD_REQUEST` 传入 `status_code` 参数；它表示`status_code` 在当前函数中承载的中间业务值。
            status_code=status.HTTP_400_BAD_REQUEST,
            # 使用关键字参数语法，把 `"exam stream 接口需要 confirmed_requirements"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail="exam stream 接口需要 confirmed_requirements",
        # 结束 `stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_exam_generation(
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `request.confirmed_requirements` 传入 `exam_requirements` 参数；它表示`exam_requirements` 在当前函数中承载的中间业务值。
                exam_requirements=request.confirmed_requirements,
                # 使用关键字参数语法，把 `request.conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=request.conversation_id,
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"考试生成失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"考试生成失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_exam_generation()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_exam_generation` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用关键字参数语法，把 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 传入 `headers` 参数；它表示`headers` 在当前函数中承载的中间业务值。
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    # 结束 `stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 将 POST /exam/document-stream 注册到下方处理函数。
@router.post("/exam/document-stream")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_exam_generation_with_documents()`。接收上传文档，先保存/解析为考试来源，再流式生成并保存试卷。
async def stream_exam_generation_with_documents(
    # 声明参数 `exam_requirements`，表示`exam_requirements` 在当前函数中承载的中间业务值，类型标注为 `str`，默认值为 `Form(...)`。
    exam_requirements: str = Form(...),
    # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `Form(None)`。
    conversation_id: Optional[str] = Form(None),
    # 声明参数 `files`，表示`files` 在当前函数中承载的中间业务值，类型标注为 `List[UploadFile]`，默认值为 `File(...)`。
    files: List[UploadFile] = File(...),
    # 由 FastAPI 注入 JWT 已认证用户，后续所有资源操作都以该用户身份执行。
    current_user: UserSchema = Depends(get_current_user),
    # 由 FastAPI 注入本次请求的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    """
        上传参考文档后，基于文档流式生成并保存考试试卷
            exam/document-stream 支持上传参考文档后生成试卷。
    """
    # 使用 `if` 判断 `not files`；条件成立时执行下面分支。
    if not files:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请至少上传一个参考文档")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="请至少上传一个参考文档")
    # 使用 `if` 判断 `len(files) > 5`；条件成立时执行下面分支。
    if len(files) > 5:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="单次最多上传 5 个参考文档")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="单次最多上传 5 个参考文档")

    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `json.loads(exam_requirements)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed_requirements`；它表示从用户自然语言或模型输出中解析并校验后的结构化结果。
        parsed_requirements = json.loads(exam_requirements)
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except Exception as exc:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="考试配置 JSON 格式不正确") from exc`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="考试配置 JSON 格式不正确") from exc

    # 把 `file_payloads` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`file_payloads` 在当前函数中承载的中间业务值。
    file_payloads = []
    # 使用 `for` 循环 `upload_file in files`，逐项处理集合数据。
    for upload_file in files:
        # 使用 `await` 异步调用 `upload_file.read()`，等待数据库、Dify 或模型处理完成后把结果保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
        content = await upload_file.read()
        # 使用 `if` 判断 `not content`；条件成立时执行下面分支。
        if not content:
            # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{upload_file.filename} 文件内容为空")`，立即终止当前失败流程并交给上层处理。
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"{upload_file.filename} 文件内容为空")
        # 调用列表的 `append()`，把 `file_payloads.append({"filename": upload_file.filename, "content": content})` 对应的新元素追加到现有结果末尾。
        file_payloads.append({"filename": upload_file.filename, "content": content})

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `generate()`。
    async def generate():
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用当前请求的数据库会话创建 Agent 编排服务，工具执行与记忆查询共享该会话。
            agent_service = AgentService(db)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for event in agent_service.stream_exam_generation_with_documents(
                # 使用关键字参数语法，把 `current_user.id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=current_user.id,
                # 使用关键字参数语法，把 `parsed_requirements` 传入 `exam_requirements` 参数；它表示`exam_requirements` 在当前函数中承载的中间业务值。
                exam_requirements=parsed_requirements,
                # 使用关键字参数语法，把 `file_payloads` 传入 `files` 参数；它表示`files` 在当前函数中承载的中间业务值。
                files=file_payloads,
                # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=conversation_id,
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield "data: [DONE]\n\n"
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `{"type": "error", "error": f"基于文档生成考试失败: {str(exc)}"}` 并把结果保存到 `error_event`；该结果在后续流程中用作`error_event` 在当前函数中承载的中间业务值。
            error_event = {"type": "error", "error": f"基于文档生成考试失败: {str(exc)}"}
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield f"data: {json.dumps(error_event, ensure_ascii=False)}\n\n"

    # 从 `stream_exam_generation_with_documents()` 返回 `StreamingResponse(`，并结束当前函数。
    return StreamingResponse(
        # 调用 `generate()`，执行 `stream_exam_generation_with_documents` 当前步骤所需的构造、查询或业务处理。
        generate(),
        # 使用关键字参数语法，把 `"text/event-stream"` 传入 `media_type` 参数；它表示`media_type` 在当前函数中承载的中间业务值。
        media_type="text/event-stream",
        # 使用关键字参数语法，把 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 传入 `headers` 参数；它表示`headers` 在当前函数中承载的中间业务值。
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    # 结束 `stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )
