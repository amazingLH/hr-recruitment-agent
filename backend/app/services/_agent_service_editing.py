"""
* 文件作用
  * 这是 Agent 的编辑能力 Mixin，专门处理“修改刚才的 JD”或“调整评分标准”一类上下文请求。它从用户文字和会话记忆提取目标与修改要求，查询最近生成或保存的候选资源，处理序号选择和同名消歧，调用 LLM 重写正文，再构造更新 Schema 交给对应业务 Service 保存；规则解析在模型失败时提供兜底。
* 类说明
  * `_AgentServiceEditingMixin`：实现 JD 与评分标准的目标定位、修改要求解析、LLM 重写和持久化更新。
* 主要函数与方法说明
  * `_AgentServiceEditingMixin._handle_jd_edit()`：定位用户要修改的 JD，缺少目标时返回候选列表，目标明确后重写并保存更新。
  * `_AgentServiceEditingMixin._handle_criteria_edit()`：定位评分标准和修改要求，调用 LLM 重写正文，再通过 ScoringCriteriaService 持久化（保存到 .env 配置的远程 HR 服务所管理的存储中）。
"""

# 导入 annotations，用于启用延迟解析类型注解，避免前向类型引用在导入阶段报错。
from __future__ import annotations

# 导入 json，用于编码和解析 ReAct JSON、SSE 事件及模型结构化结果。
import json
# 导入 logging，用于记录 Agent 规划、工具执行和降级异常。
import logging
# 导入 hashlib，用于生成内容指纹，用于资源去重或流式状态识别。
import hashlib
# 导入 os，用于读取环境或处理文件系统路径。
import os
# 导入 re，用于通过正则识别意图、字段、候选人和邮件地址。
import re
# 导入 asyncio，用于组织异步流式任务和并发操作。
import asyncio
# 导入 dataclass，用于定义只保存数据、不承担业务执行的内部结构。
from dataclasses import dataclass
# 导入 Any, AsyncGenerator, Dict, List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, AsyncGenerator, Dict, List, Optional
# 导入 UUID，用于使用 UUID 标识用户、会话和业务资源。
from uuid import UUID
# 导入 select，用于SQLAlchemy 查询表达式。
from sqlalchemy import select
# 导入 AsyncSession，用于异步数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession
# 导入 settings，用于读取 LLM、Dify、Agent 记忆和运行环境配置。
from app.core.config import settings
# 导入 Conversation, Message, MessageRole，用于会话和消息 ORM 模型。
from app.models.conversation import Conversation, Message, MessageRole
# 导入 Document，用于查询或保存 Document 对应的 ORM 数据。
from app.models.document import Document
# 导入 ResumeEvaluation, ResumeStatus，用于查询或保存 ResumeEvaluation, ResumeStatus 对应的 ORM 数据。
from app.models.resume_evaluation import ResumeEvaluation, ResumeStatus
# 导入 AgentArtifact, AgentChatResponse, AgentStep，用于Agent 请求、步骤、产物和统一响应模型。
from app.schemas.agent import AgentArtifact, AgentChatResponse, AgentStep
# 导入 InterviewPlanCreate，用于校验 InterviewPlanCreate 对应的请求或响应数据。
from app.schemas.interview_plan import InterviewPlanCreate
# 导入 JobDescriptionCreate, JobDescriptionUpdate，用于校验 JobDescriptionCreate, JobDescriptionUpdate 对应的请求或响应数据。
from app.schemas.job_description import JobDescriptionCreate, JobDescriptionUpdate
# 导入 ScoringCriteriaCreate, ScoringCriteriaUpdate，用于校验 ScoringCriteriaCreate, ScoringCriteriaUpdate 对应的请求或响应数据。
from app.schemas.scoring_criteria import ScoringCriteriaCreate, ScoringCriteriaUpdate
# 导入 EmailSendService，用于通过 SMTP 发送人工确认后的邮件。
from app.services.email_service import EmailSendService
# 导入 DifyService，用于Dify 生成工作流客户端。
from app.services.dify_service import DifyService
# 导入 AgentSkillBundle, AgentSkillDispatcher, build_default_skill_dispatcher，用于可插拔 Skill 扫描与阶段调度能力。
from app.services.agent_skills import AgentSkillBundle, AgentSkillDispatcher, build_default_skill_dispatcher
# 导入 ExamService，用于生成、保存和管理考试试卷。
from app.services.exam_service import ExamService
# 导入 InterviewPlanService，用于保存、查询和更新候选人面试方案。
from app.services.interview_plan_service import InterviewPlanService
# 导入 IntentService，用于规则优先的意图识别服务。
from app.services.intent_service import IntentService
# 导入 JobDescriptionService，用于保存、查询和更新岗位 JD。
from app.services.job_description_service import JobDescriptionService
# 导入 KBSelectionService，用于从用户可访问知识库中选择相关文档。
from app.services.kb_selection_service import KBSelectionService
# 导入 LLMService，用于普通模型调用、ReAct 和文本改写服务。
from app.services.llm_service import LLMService
# 导入 ResumeEvaluationService，用于解析并评分简历以及查询候选人结果。
from app.services.resume_evaluation_service import ResumeEvaluationService
# 导入 ScoringCriteriaService，用于保存、查询和修改简历评分标准。
from app.services.scoring_criteria_service import ScoringCriteriaService
# 导入 extract_text_content，用于使用 extract_text_content 提供的类型或函数。
from app.utils.text_utils import extract_text_content

# 导入 AgentToolSpec, ReActDecision，用于构造受控工具规格和 ReAct 决策数据。
from app.services._agent_service_types import AgentToolSpec, ReActDecision

# 调用 `logging.getLogger(__name__)` 完成当前解析、查询或对象构造，并把返回值保存到 `logger`；它表示当前模块的日志记录器，用于记录执行状态和异常。
logger = logging.getLogger(__name__)

# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
    JD、评分标准编辑及候选人解析.
"""

# 使用 `class` 定义 `_AgentServiceEditingMixin`；该类的业务作用是：实现 JD 与评分标准的目标定位、修改要求解析、LLM 重写和持久化更新。
class _AgentServiceEditingMixin:
    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_handle_jd_edit()`。定位用户要修改的 JD，缺少目标时返回候选列表，目标明确后重写并保存更新。
    async def _handle_jd_edit(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 使用 `await` 异步调用 `self._parse_jd_edit_request(message, memory_context)`，等待数据库、Dify 或模型处理完成后把结果保存到 `edit_request`；它表示准备提交给下游服务的结构化请求参数。
        edit_request = await self._parse_jd_edit_request(message, memory_context)
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 调用 `self._planning_step()`，执行 `_AgentServiceEditingMixin._handle_jd_edit` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(selected_tool, "识别到 JD 修改请求，先定位目标 JD。"),
            # 计算表达式 `"locate_jd", title="定位要修改的 JD", status="running", detail="优先查找当前对话最近生成的 JD。", tool="edit_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="locate_jd", title="定位要修改的 JD", status="running", detail="优先查找当前对话最近生成的 JD。", tool="edit_jd"),
            # 计算表达式 `"collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),
            # 计算表达式 `"rewrite_jd", title="改写 JD 内容", status="pending", detail="基于原 JD 和修改要求生成新版。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="rewrite_jd", title="改写 JD 内容", status="pending", detail="基于原 JD 和修改要求生成新版。"),
            # 计算表达式 `"save_jd", title="保存修改", status="pending", detail="更新原 JD 记录。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_jd", title="保存修改", status="pending", detail="更新原 JD 记录。"),
        # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

        # 使用 `await` 异步调用 `self._resolve_recent_jd_candidates(user_id, conversation_id, edit_request.get("keyword") or "")`，等待数据库、Dify 或模型处理完成后把结果保存到 `candidates`；它表示根据用户描述找到、等待进一步选择的候选资源列表。
        candidates = await self._resolve_recent_jd_candidates(user_id, conversation_id, edit_request.get("keyword") or "")
        # 使用 `if` 判断 `not candidates`；条件成立时执行下面分支。
        if not candidates:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "failed", "detail": "没有找到可修改的 JD。"})
            # 从 `_AgentServiceEditingMixin._handle_jd_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我还没有找到可以修改的 JD。请先生成或保存一个 JD，或者明确告诉我要修改哪个岗位的 JD。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我还没有找到可以修改的 JD。请先生成或保存一个 JD，或者明确告诉我要修改哪个岗位的 JD。",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `[]` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=[],
                # 使用关键字参数语法，把 `["生成一个新的 JD", "说明要修改的岗位名称", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["生成一个新的 JD", "说明要修改的岗位名称", "去 JD 管理查看职位"],
            # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `len(candidates) > 1 and edit_request.get("keyword")`；条件成立时执行下面分支。
        if len(candidates) > 1 and edit_request.get("keyword"):
            # 调用 `self._match_delete_resources(candidates, edit_request.get("keyword") or "")` 完成当前解析、查询或对象构造，并把返回值保存到 `matches`；它表示`matches` 在当前函数中承载的中间业务值。
            matches = self._match_delete_resources(candidates, edit_request.get("keyword") or "")
            # 使用 `if` 判断 `matches`；条件成立时执行下面分支。
            if matches:
                # 计算表达式 `matches` 并把结果保存到 `candidates`；该结果在后续流程中用作根据用户描述找到、等待进一步选择的候选资源列表。
                candidates = matches

        # 使用 `if` 判断 `len(candidates) > 1`；条件成立时执行下面分支。
        if len(candidates) > 1:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "running", "detail": f"找到 {len(candidates)} 个可能的 JD，需要确认。"})
            # 从 `_AgentServiceEditingMixin._handle_jd_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                message=(
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    "我找到了多个可能要修改的 JD。为避免改错，请告诉我具体是哪一个：\n"
                    # 使用 `+` 将 `"\n".join(f"- {self._format_jd_candidate_label(item)}" for item in candidates[:5])` 拼接到前面的文本或集合，组成完整业务内容。
                    + "\n".join(f"- {self._format_jd_candidate_label(item)}" for item in candidates[:5])
                # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceEditingMixin._handle_jd_edit` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"jd_edit_candidates"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="jd_edit_candidates",
                        # 使用关键字参数语法，把 `"待确认 JD"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="待确认 JD",
                        # 使用关键字参数语法，把 `{"candidates": candidates[:5]}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"candidates": candidates[:5]},
                    # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `["修改最新的那个 JD", "说明岗位名称", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["修改最新的那个 JD", "说明岗位名称", "去 JD 管理查看职位"],
            # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 计算表达式 `candidates[0]` 并把结果保存到 `target`；该结果在后续流程中用作经会话记忆和候选列表消歧后确定的目标业务资源。
        target = candidates[0]
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": f"已定位：{target.get('name') or '最近的 JD'}。"})

        # 调用 `self._clean_optional_value(edit_request.get("changes"))` 完成当前解析、查询或对象构造，并把返回值保存到 `edit_instructions`；它表示用户要求对原内容执行的具体修改说明。
        edit_instructions = self._clean_optional_value(edit_request.get("changes"))
        # 使用 `if` 判断 `not edit_instructions`；条件成立时执行下面分支。
        if not edit_instructions:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
            steps[2] = steps[2].model_copy(update={"status": "pending", "detail": "等待用户说明要修改哪些内容。"})
            # 从 `_AgentServiceEditingMixin._handle_jd_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `f"可以，我找到了「{target.get('name') or '最近的 JD'}」。你想具体改哪些内容？比如薪资、地点、职责、技能要求或福利。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"可以，我找到了「{target.get('name') or '最近的 JD'}」。你想具体改哪些内容？比如薪资、地点、职责、技能要求或福利。",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceEditingMixin._handle_jd_edit` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"jd_edit_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="jd_edit_request",
                        # 使用关键字参数语法，把 `"等待修改要求"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="等待修改要求",
                        # 使用关键字参数语法，把 `{"job_description_id": target.get("id"), "title": target.get("name")}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"job_description_id": target.get("id"), "title": target.get("name")},
                    # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `["薪资改成 20-30K", "增加 React 和性能优化要求", "把工作地点改成长沙"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["薪资改成 20-30K", "增加 React 和性能优化要求", "把工作地点改成长沙"],
            # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": edit_instructions})
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `JobDescriptionService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `service`；它表示`service` 在当前函数中承载的中间业务值。
            service = JobDescriptionService(self.db)
            # 使用 `await` 异步调用 `service.get_job_description(str(target["id"]), user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `original_jd`；它表示`original_jd` 在当前函数中承载的中间业务值。
            original_jd = await service.get_job_description(str(target["id"]), user_id)
            # 调用 `original_jd.model_dump(mode="json")` 完成当前解析、查询或对象构造，并把返回值保存到 `original_data`；它表示`original_data` 在当前函数中承载的中间业务值。
            original_data = original_jd.model_dump(mode="json")
            # 调用 `original_data.get("content") or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `original_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            original_content = original_data.get("content") or ""
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
            steps[3] = steps[3].model_copy(update={"status": "running", "detail": "正在生成修改后的 JD。"})
            # 使用 `await` 异步调用 `self._rewrite_jd_content(original_content, edit_instructions)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            updated_content = await self._rewrite_jd_content(original_content, edit_instructions)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
            steps[3] = steps[3].model_copy(update={"status": "completed", "detail": "已生成修改后的 JD。"})

            # 调用 `self._build_jd_update_payload(original_data, updated_content, edit_instructions, edit_request)` 完成当前解析、查询或对象构造，并把返回值保存到 `update_data`；它表示`update_data` 在当前函数中承载的中间业务值。
            update_data = self._build_jd_update_payload(original_data, updated_content, edit_instructions, edit_request)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "running", "detail": "正在保存到原 JD。"})
            # 使用 `await` 异步调用 `service.update_job_description(str(target["id"]), update_data, user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated_jd`；它表示`updated_jd` 在当前函数中承载的中间业务值。
            updated_jd = await service.update_job_description(str(target["id"]), update_data, user_id)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "completed", "detail": f"已更新 JD：{updated_jd.id}"})
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `True)` 并把结果保存到 `logger.warning("Agent 修改 JD 失败: %s", exc, exc_info`；该结果在后续流程中用作`warning("Agent 修改 JD 失败: %s", exc, exc_info` 在当前函数中承载的中间业务值。
            logger.warning("Agent 修改 JD 失败: %s", exc, exc_info=True)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "failed", "detail": str(exc)})
            # 从 `_AgentServiceEditingMixin._handle_jd_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `f"我找到了 JD，但保存修改时失败了：{str(exc)}"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"我找到了 JD，但保存修改时失败了：{str(exc)}",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `[]` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=[],
                # 使用关键字参数语法，把 `["稍后重试", "去 JD 管理手动编辑", "重新生成 JD"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["稍后重试", "去 JD 管理手动编辑", "重新生成 JD"],
            # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 从 `_AgentServiceEditingMixin._handle_jd_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `f"已按你的要求更新「{updated_jd.title}」。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"已按你的要求更新「{updated_jd.title}」。",
            # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="jd_edit",
            # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/jd-generator",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
            artifacts=[
                # 调用 `AgentArtifact()`，执行 `_AgentServiceEditingMixin._handle_jd_edit` 当前步骤所需的构造、查询或业务处理。
                AgentArtifact(
                    # 使用关键字参数语法，把 `"job_description"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    type="job_description",
                    # 使用关键字参数语法，把 `f"{updated_jd.title} JD"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title=f"{updated_jd.title} JD",
                    # 使用关键字参数语法，把 `updated_jd.content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                    content=updated_jd.content,
                    # 使用字典字面量开始构造 `metadata`；该字典用于保存产物附加信息，例如数据库 ID、候选人和保存状态，下面各行会继续写入具体业务字段。
                    metadata={
                        # 在字典中设置 `job_title` 字段为 `updated_jd.title`，用于表示`job_title` 在当前函数中承载的中间业务值。
                        "job_title": updated_jd.title,
                        # 在字典中设置 `saved_jd_id` 字段为 `str(updated_jd.id)`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                        "saved_jd_id": str(updated_jd.id),
                        # 在字典中设置 `edit_instructions` 字段为 `edit_instructions`，用于表示用户要求对原内容执行的具体修改说明。
                        "edit_instructions": edit_instructions,
                        # 在字典中设置 `route` 字段为 `"/recruitment/jd-generator"`，用于表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                        "route": "/recruitment/jd-generator",
                    # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    },
                # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `["继续修改 JD", "基于这个 JD 筛选简历", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["继续修改 JD", "基于这个 JD 筛选简历", "去 JD 管理查看职位"],
        # 结束 `_AgentServiceEditingMixin._handle_jd_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_handle_criteria_edit()`。定位评分标准和修改要求，调用 LLM 重写正文，再通过 ScoringCriteriaService 持久化。
    async def _handle_criteria_edit(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 把 `final_response` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入下游服务返回、等待解析或转发的响应。
        final_response = None
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for event in self._stream_criteria_edit_response(message, user_id, conversation_id, memory_context):
            # 使用 `if` 判断 `event.get("type") == "final" and event.get("response")`；条件成立时执行下面分支。
            if event.get("type") == "final" and event.get("response"):
                # 调用 `AgentChatResponse.model_validate(event["response"])` 完成当前解析、查询或对象构造，并把返回值保存到 `final_response`；它表示下游服务返回、等待解析或转发的响应。
                final_response = AgentChatResponse.model_validate(event["response"])
        # 使用 `if` 判断 `final_response`；条件成立时执行下面分支。
        if final_response:
            # 从 `_AgentServiceEditingMixin._handle_criteria_edit()` 返回 `final_response`，并结束当前函数。
            return final_response
        # 从 `_AgentServiceEditingMixin._handle_criteria_edit()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `"评分标准修改没有返回结果，请稍后重试。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message="评分标准修改没有返回结果，请稍后重试。",
            # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="criteria_edit",
            # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/resume-screening",
            # 使用关键字参数语法，把 `[self._planning_step(selected_tool, "评分标准修改流程未返回最终结果。")]` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=[self._planning_step(selected_tool, "评分标准修改流程未返回最终结果。")],
            # 使用关键字参数语法，把 `["稍后重试", "重新生成评分标准"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["稍后重试", "重新生成评分标准"],
        # 结束 `_AgentServiceEditingMixin._handle_criteria_edit` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_parse_jd_edit_request()`。
    async def _parse_jd_edit_request(self, message: str, memory_context: str = "") -> Dict[str, Any]:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是 HR Agent 的 JD 修改请求解析器。请严格返回 JSON，不要解释。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "字段：keyword, changes, title, department, location, salary_range, experience_level, education, job_type, skills。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "keyword 填用户用于定位 JD 的岗位名、地点、薪资、ID短码或序号，例如“前端”“AI产品经理”“长沙”“15K”“第一个”；没有则返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "changes 填用户明确提出的修改要求；如果用户只是说“帮我改改上次那个JD”但没有说明改什么，返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果用户明确要求修改薪资、地点、经验、学历、职位名称、部门、工作类型或技能，请同步填入对应结构化字段；未提到返回 null，skills 未提到返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果上一轮助手已经询问“你想具体改哪些内容”，当前用户消息通常就是 changes。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"当前用户消息：{message}\n"
            # 在字典中设置 `返回格式：{\"keyword\` 字段为 `null,\"changes\":null,\"title\":null,\"department\":null,\"location\":null,\"salary_range\":null,...`，用于表示`返回格式：{\"keyword\` 在当前函数中承载的中间业务值。
            "返回格式：{\"keyword\":null,\"changes\":null,\"title\":null,\"department\":null,\"location\":null,\"salary_range\":null,\"experience_level\":null,\"education\":null,\"job_type\":null,\"skills\":null}"
        # 结束 `_AgentServiceEditingMixin._parse_jd_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
            if self.llm_service is None:
                # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
                self.llm_service = LLMService()
            # 使用 `await` 异步调用 `self.llm_service.generate_response(prompt)`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = await self.llm_service.generate_response(prompt)
            # 调用 `self._safe_json_loads(response)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed`；它表示从用户自然语言中解析出的结构化招聘参数。
            parsed = self._safe_json_loads(response)
            # 调用 `self._clean_optional_value(parsed.get("changes"))` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
            changes = self._clean_optional_value(parsed.get("changes"))
            # 使用 `if` 判断 `changes and self._is_generic_jd_edit_request(changes)`；条件成立时执行下面分支。
            if changes and self._is_generic_jd_edit_request(changes):
                # 把 `changes` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`changes` 在当前函数中承载的中间业务值。
                changes = None
            # 使用 `if` 判断 `not changes and self._is_jd_edit_followup(message, memory_context)`；条件成立时执行下面分支。
            if not changes and self._is_jd_edit_followup(message, memory_context):
                # 调用 `message.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
                changes = message.strip()
            # 从 `_AgentServiceEditingMixin._parse_jd_edit_request()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `keyword` 字段为 `self._clean_optional_value(parsed.get("keyword"))`，用于表示`keyword` 在当前函数中承载的中间业务值。
                "keyword": self._clean_optional_value(parsed.get("keyword")),
                # 在字典中设置 `changes` 字段为 `changes`，用于表示`changes` 在当前函数中承载的中间业务值。
                "changes": changes,
                # 在字典中设置 `title` 字段为 `self._clean_optional_value(parsed.get("title"))`，用于表示前端向用户展示的步骤或业务产物标题。
                "title": self._clean_optional_value(parsed.get("title")),
                # 在字典中设置 `department` 字段为 `self._clean_optional_value(parsed.get("department"))`，用于表示`department` 在当前函数中承载的中间业务值。
                "department": self._clean_optional_value(parsed.get("department")),
                # 在字典中设置 `location` 字段为 `self._clean_optional_value(parsed.get("location"))`，用于表示`location` 在当前函数中承载的中间业务值。
                "location": self._clean_optional_value(parsed.get("location")),
                # 在字典中设置 `salary_range` 字段为 `self._clean_optional_value(parsed.get("salary_range"))`，用于表示`salary_range` 在当前函数中承载的中间业务值。
                "salary_range": self._clean_optional_value(parsed.get("salary_range")),
                # 在字典中设置 `experience_level` 字段为 `self._clean_optional_value(parsed.get("experience_level"))`，用于表示`experience_level` 在当前函数中承载的中间业务值。
                "experience_level": self._clean_optional_value(parsed.get("experience_level")),
                # 在字典中设置 `education` 字段为 `self._clean_optional_value(parsed.get("education"))`，用于表示`education` 在当前函数中承载的中间业务值。
                "education": self._clean_optional_value(parsed.get("education")),
                # 在字典中设置 `job_type` 字段为 `self._clean_optional_value(parsed.get("job_type"))`，用于表示`job_type` 在当前函数中承载的中间业务值。
                "job_type": self._clean_optional_value(parsed.get("job_type")),
                # 在字典中设置 `skills` 字段为 `self._normalize_optional_list(parsed.get("skills"))`，用于表示`skills` 在当前函数中承载的中间业务值。
                "skills": self._normalize_optional_list(parsed.get("skills")),
                # 在字典中设置 `source` 字段为 `"llm"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "llm",
            # 结束 `_AgentServiceEditingMixin._parse_jd_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("JD 修改请求解析失败，使用规则兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("JD 修改请求解析失败，使用规则兜底: %s", exc)
        # 从 `_AgentServiceEditingMixin._parse_jd_edit_request()` 返回 `self._fallback_parse_jd_edit_request(message, memory_context)`，并结束当前函数。
        return self._fallback_parse_jd_edit_request(message, memory_context)

    # 使用 `def` 普通函数 定义 `_fallback_parse_jd_edit_request()`。
    def _fallback_parse_jd_edit_request(self, message: str, memory_context: str = "") -> Dict[str, Any]:
        # 把 `keyword` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`keyword` 在当前函数中承载的中间业务值。
        keyword = None
        # 调用 `re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师...` 完成当前解析、查询或对象构造，并把返回值保存到 `title_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        title_match = re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师))", message)
        # 使用 `if` 判断 `title_match`；条件成立时执行下面分支。
        if title_match:
            # 调用 `title_match.group(1)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
            keyword = title_match.group(1)
        # 使用 `if` 判断 `not keyword`；条件成立时执行下面分支。
        if not keyword:
            # 调用 `re.search(r"(第[一二三四五六七八九十1-9]\s*个|第[一二三四五六七八九十1-9]\s*条|最新|最近|上一个|最后一个)", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `ordinal_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
            ordinal_match = re.search(r"(第[一二三四五六七八九十1-9]\s*个|第[一二三四五六七八九十1-9]\s*条|最新|最近|上一个|最后一个)", message)
            # 使用 `if` 判断 `ordinal_match`；条件成立时执行下面分支。
            if ordinal_match:
                # 调用 `ordinal_match.group(1)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
                keyword = ordinal_match.group(1)
        # 使用 `if` 判断 `not keyword`；条件成立时执行下面分支。
        if not keyword:
            # 使用 `for` 循环 `city in ["北京", "上海", "深圳", "广州", "杭州", "南京", "成都", "重庆", "苏州", "武汉", "西安", "长沙", "济南"]`，逐项处理集合数据。
            for city in ["北京", "上海", "深圳", "广州", "杭州", "南京", "成都", "重庆", "苏州", "武汉", "西安", "长沙", "济南"]:
                # 使用 `if` 判断 `city in message`；条件成立时执行下面分支。
                if city in message:
                    # 计算表达式 `city` 并把结果保存到 `keyword`；该结果在后续流程中用作`keyword` 在当前函数中承载的中间业务值。
                    keyword = city
                    # 使用 `break` 结束当前循环，避免继续执行无效迭代。
                    break
        # 使用 `if` 判断 `not keyword`；条件成立时执行下面分支。
        if not keyword:
            # 调用 `re.search(r"\d+(?:\.\d+)?\s*[kK万]?", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `salary_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
            salary_match = re.search(r"\d+(?:\.\d+)?\s*[kK万]?", message)
            # 使用 `if` 判断 `salary_match`；条件成立时执行下面分支。
            if salary_match:
                # 调用 `salary_match.group(0).replace(" ", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
                keyword = salary_match.group(0).replace(" ", "")
        # 调用 `re.sub(r"请|帮我|麻烦|一下|上次|刚才|刚刚|这个|这份|这条|那个|JD|jd|职位描述|岗位说明书", "", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
        changes = re.sub(r"请|帮我|麻烦|一下|上次|刚才|刚刚|这个|这份|这条|那个|JD|jd|职位描述|岗位说明书", "", message)
        # 调用 `re.sub(r"改改|修改|调整|优化|更新|编辑|润色", "", changes).strip(" ，,。")` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
        changes = re.sub(r"改改|修改|调整|优化|更新|编辑|润色", "", changes).strip(" ，,。")
        # 使用 `if` 判断 `changes and self._is_generic_jd_edit_request(changes)`；条件成立时执行下面分支。
        if changes and self._is_generic_jd_edit_request(changes):
            # 把 `changes` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入`changes` 在当前函数中承载的中间业务值。
            changes = ""
        # 使用 `if` 判断 `not changes and self._is_jd_edit_followup(message, memory_context)`；条件成立时执行下面分支。
        if not changes and self._is_jd_edit_followup(message, memory_context):
            # 调用 `message.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
            changes = message.strip()
        # 从 `_AgentServiceEditingMixin._fallback_parse_jd_edit_request()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `keyword` 字段为 `keyword`，用于表示`keyword` 在当前函数中承载的中间业务值。
            "keyword": keyword,
            # 在字典中设置 `changes` 字段为 `changes or None`，用于表示`changes` 在当前函数中承载的中间业务值。
            "changes": changes or None,
            # 使用 `*` 可迭代对象解包，把 `*self._fallback_parse_jd_edit_fields(message)` 返回的全部元素展开并合并进当前列表或参数序列。
            **self._fallback_parse_jd_edit_fields(message),
            # 在字典中设置 `source` 字段为 `"fallback"`，用于表示`source` 在当前函数中承载的中间业务值。
            "source": "fallback",
        # 结束 `_AgentServiceEditingMixin._fallback_parse_jd_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_parse_criteria_edit_request()`。
    async def _parse_criteria_edit_request(self, message: str, memory_context: str = "") -> Dict[str, Any]:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是 HR Agent 的评分标准修改请求解析器。请严格返回 JSON，不要解释。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "字段：keyword, changes。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "keyword 填用户用于定位评分标准的岗位名或关键词，例如“前端”“AI产品经理”；没有则返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "changes 填用户明确提出的评分标准修改要求；如果用户只是说“帮我改改评分标准”但没有说明改什么，返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果上一轮助手已经询问“你想具体改哪些评分规则”，当前用户消息通常就是 changes。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"当前用户消息：{message}\n"
            # 在字典中设置 `返回格式：{\"keyword\` 字段为 `null,\"changes\":null}"`，用于表示`返回格式：{\"keyword\` 在当前函数中承载的中间业务值。
            "返回格式：{\"keyword\":null,\"changes\":null}"
        # 结束 `_AgentServiceEditingMixin._parse_criteria_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
            if self.llm_service is None:
                # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
                self.llm_service = LLMService()
            # 使用 `await` 异步调用 `self.llm_service.generate_response(prompt)`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = await self.llm_service.generate_response(prompt)
            # 调用 `self._safe_json_loads(response)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed`；它表示从用户自然语言中解析出的结构化招聘参数。
            parsed = self._safe_json_loads(response)
            # 调用 `self._clean_optional_value(parsed.get("changes"))` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
            changes = self._clean_optional_value(parsed.get("changes"))
            # 使用 `if` 判断 `changes and self._is_generic_criteria_edit_request(changes)`；条件成立时执行下面分支。
            if changes and self._is_generic_criteria_edit_request(changes):
                # 把 `changes` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`changes` 在当前函数中承载的中间业务值。
                changes = None
            # 使用 `if` 判断 `not changes and self._is_criteria_edit_followup(message, memory_context)`；条件成立时执行下面分支。
            if not changes and self._is_criteria_edit_followup(message, memory_context):
                # 调用 `message.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
                changes = message.strip()
            # 从 `_AgentServiceEditingMixin._parse_criteria_edit_request()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `keyword` 字段为 `self._clean_optional_value(parsed.get("keyword"))`，用于表示`keyword` 在当前函数中承载的中间业务值。
                "keyword": self._clean_optional_value(parsed.get("keyword")),
                # 在字典中设置 `changes` 字段为 `changes`，用于表示`changes` 在当前函数中承载的中间业务值。
                "changes": changes,
                # 在字典中设置 `source` 字段为 `"llm"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "llm",
            # 结束 `_AgentServiceEditingMixin._parse_criteria_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("评分标准修改请求解析失败，使用规则兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("评分标准修改请求解析失败，使用规则兜底: %s", exc)
        # 从 `_AgentServiceEditingMixin._parse_criteria_edit_request()` 返回 `self._fallback_parse_criteria_edit_request(message, memory_context)`，并结束当前函数。
        return self._fallback_parse_criteria_edit_request(message, memory_context)

    # 使用 `def` 普通函数 定义 `_fallback_parse_criteria_edit_request()`。
    def _fallback_parse_criteria_edit_request(self, message: str, memory_context: str = "") -> Dict[str, Any]:
        # 把 `keyword` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`keyword` 在当前函数中承载的中间业务值。
        keyword = None
        # 调用 `re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师...` 完成当前解析、查询或对象构造，并把返回值保存到 `title_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        title_match = re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师))", message)
        # 使用 `if` 判断 `title_match`；条件成立时执行下面分支。
        if title_match:
            # 调用 `title_match.group(1)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
            keyword = title_match.group(1)
        # 调用 `re.sub(r"请|帮我|麻烦|一下|上次|刚才|刚刚|这个|这份|这条|那个|评分标准|评分规则|打分标准|筛选标准|简历评分", "", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
        changes = re.sub(r"请|帮我|麻烦|一下|上次|刚才|刚刚|这个|这份|这条|那个|评分标准|评分规则|打分标准|筛选标准|简历评分", "", message)
        # 调用 `re.sub(r"改改|修改|调整|优化|更新|编辑|润色", "", changes).strip(" ，,。")` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
        changes = re.sub(r"改改|修改|调整|优化|更新|编辑|润色", "", changes).strip(" ，,。")
        # 使用 `if` 判断 `changes and self._is_generic_criteria_edit_request(changes)`；条件成立时执行下面分支。
        if changes and self._is_generic_criteria_edit_request(changes):
            # 把 `changes` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入`changes` 在当前函数中承载的中间业务值。
            changes = ""
        # 使用 `if` 判断 `not changes and self._is_criteria_edit_followup(message, memory_context)`；条件成立时执行下面分支。
        if not changes and self._is_criteria_edit_followup(message, memory_context):
            # 调用 `message.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `changes`；它表示`changes` 在当前函数中承载的中间业务值。
            changes = message.strip()
        # 从 `_AgentServiceEditingMixin._fallback_parse_criteria_edit_request()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `keyword` 字段为 `keyword`，用于表示`keyword` 在当前函数中承载的中间业务值。
            "keyword": keyword,
            # 在字典中设置 `changes` 字段为 `changes or None`，用于表示`changes` 在当前函数中承载的中间业务值。
            "changes": changes or None,
            # 在字典中设置 `source` 字段为 `"fallback"`，用于表示`source` 在当前函数中承载的中间业务值。
            "source": "fallback",
        # 结束 `_AgentServiceEditingMixin._fallback_parse_criteria_edit_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_fallback_parse_jd_edit_fields()`。
    def _fallback_parse_jd_edit_fields(self, message: str) -> Dict[str, Any]:
        # 调用 `re.search(r"(\d+(?:\.\d+)?\s*[kK万]?)(?:\s*[-~到至]\s*(\d+(?:\.\d+)?\s*[kK万]?))?", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `salary_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        salary_match = re.search(r"(\d+(?:\.\d+)?\s*[kK万]?)(?:\s*[-~到至]\s*(\d+(?:\.\d+)?\s*[kK万]?))?", message)
        # 把 `salary` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`salary` 在当前函数中承载的中间业务值。
        salary = None
        # 使用 `if` 判断 `salary_match and re.search(r"薪资|工资|月薪|待遇|年薪|薪酬|package", message, re.I)`；条件成立时执行下面分支。
        if salary_match and re.search(r"薪资|工资|月薪|待遇|年薪|薪酬|package", message, re.I):
            # 调用 `salary_match.group(0).replace(" ", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `salary`；它表示`salary` 在当前函数中承载的中间业务值。
            salary = salary_match.group(0).replace(" ", "")
        # 把 `location` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`location` 在当前函数中承载的中间业务值。
        location = None
        # 使用 `for` 循环 `city in ["北京", "上海", "深圳", "广州", "杭州", "南京", "成都", "重庆", "苏州", "武汉", "西安", "长沙", "济南"]`，逐项处理集合数据。
        for city in ["北京", "上海", "深圳", "广州", "杭州", "南京", "成都", "重庆", "苏州", "武汉", "西安", "长沙", "济南"]:
            # 使用 `if` 判断 `city in message and re.search(r"地点|城市|工作地|base|改成|改为|换成", message, re.I)`；条件成立时执行下面分支。
            if city in message and re.search(r"地点|城市|工作地|base|改成|改为|换成", message, re.I):
                # 计算表达式 `city` 并把结果保存到 `location`；该结果在后续流程中用作`location` 在当前函数中承载的中间业务值。
                location = city
                # 使用 `break` 结束当前循环，避免继续执行无效迭代。
                break
        # 把 `experience` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`experience` 在当前函数中承载的中间业务值。
        experience = None
        # 调用 `re.search(r"经验(?:改成|改为|换成)?\s*(不限|\d+\s*-\s*\d+年|\d+年以上|\d+年\+?)|工作经验(?:改成|改为|换成)?\s*(不限|\d+\s*-\...` 完成当前解析、查询或对象构造，并把返回值保存到 `experience_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        experience_match = re.search(r"经验(?:改成|改为|换成)?\s*(不限|\d+\s*-\s*\d+年|\d+年以上|\d+年\+?)|工作经验(?:改成|改为|换成)?\s*(不限|\d+\s*-\s*\d+年|\d+年以上|\d+年\+?)", message)
        # 使用 `if` 判断 `experience_match`；条件成立时执行下面分支。
        if experience_match:
            # 调用 `next((group for group in experience_match.groups() if group), experience_match.group(0))` 完成当前解析、查询或对象构造，并把返回值保存到 `experience`；它表示`experience` 在当前函数中承载的中间业务值。
            experience = next((group for group in experience_match.groups() if group), experience_match.group(0))
        # 把 `education` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`education` 在当前函数中承载的中间业务值。
        education = None
        # 调用 `re.search(r"学历(?:改成|改为|换成)?\s*(不限|大专|专科|本科|硕士|博士)|(?:不限|大专|专科|本科|硕士|博士)(?:及以上)?", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `education_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        education_match = re.search(r"学历(?:改成|改为|换成)?\s*(不限|大专|专科|本科|硕士|博士)|(?:不限|大专|专科|本科|硕士|博士)(?:及以上)?", message)
        # 使用 `if` 判断 `education_match and re.search(r"学历|大专|专科|本科|硕士|博士", message)`；条件成立时执行下面分支。
        if education_match and re.search(r"学历|大专|专科|本科|硕士|博士", message):
            # 调用 `education_match.group(1) or education_match.group(0)` 完成当前解析、查询或对象构造，并把返回值保存到 `education`；它表示`education` 在当前函数中承载的中间业务值。
            education = education_match.group(1) or education_match.group(0)
        # 计算表达式 `["Java", "Python", "Go", "Vue", "React", "TypeScript", "JavaScript", "Spring", "MySQL", "Redis", ...` 并把结果保存到 `skill_candidates`；该结果在后续流程中用作`skill_candidates` 在当前函数中承载的中间业务值。
        skill_candidates = ["Java", "Python", "Go", "Vue", "React", "TypeScript", "JavaScript", "Spring", "MySQL", "Redis", "Docker", "Kubernetes", "AI", "大模型"]
        # 计算表达式 `[skill for skill in skill_candidates if skill.lower() in message.lower()]` 并把结果保存到 `skills`；该结果在后续流程中用作`skills` 在当前函数中承载的中间业务值。
        skills = [skill for skill in skill_candidates if skill.lower() in message.lower()]
        # 从 `_AgentServiceEditingMixin._fallback_parse_jd_edit_fields()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `title` 字段为 `None`，用于表示前端向用户展示的步骤或业务产物标题。
            "title": None,
            # 在字典中设置 `department` 字段为 `None`，用于表示`department` 在当前函数中承载的中间业务值。
            "department": None,
            # 在字典中设置 `location` 字段为 `location`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": location,
            # 在字典中设置 `salary_range` 字段为 `salary`，用于表示`salary_range` 在当前函数中承载的中间业务值。
            "salary_range": salary,
            # 在字典中设置 `experience_level` 字段为 `experience`，用于表示`experience_level` 在当前函数中承载的中间业务值。
            "experience_level": experience,
            # 在字典中设置 `education` 字段为 `education`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": education,
            # 在字典中设置 `job_type` 字段为 `None`，用于表示`job_type` 在当前函数中承载的中间业务值。
            "job_type": None,
            # 在字典中设置 `skills` 字段为 `skills or None`，用于表示`skills` 在当前函数中承载的中间业务值。
            "skills": skills or None,
        # 结束 `_AgentServiceEditingMixin._fallback_parse_jd_edit_fields` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_resolve_recent_jd_candidates()`。
    async def _resolve_recent_jd_candidates(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `keyword`，表示`keyword` 在当前函数中承载的中间业务值，类型标注为 `str`，默认值为 `""`。
        keyword: str = "",
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 使用 `await` 异步调用 `self._recent_pending_jd_edit_target(user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `pending_target`；它表示`pending_target` 在当前函数中承载的中间业务值。
        pending_target = await self._recent_pending_jd_edit_target(user_id, conversation_id)
        # 使用 `if` 判断 `pending_target and not keyword`；条件成立时执行下面分支。
        if pending_target and not keyword:
            # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `[pending_target]`，并结束当前函数。
            return [pending_target]

        # 使用 `await` 异步调用 `self._recent_generated_resources("jd", user_id, conversation_id, limit=20)`，等待数据库、Dify 或模型处理完成后把结果保存到 `context_matches`；它表示`context_matches` 在当前函数中承载的中间业务值。
        context_matches = await self._recent_generated_resources("jd", user_id, conversation_id, limit=20)
        # 使用 `await` 异步调用 `self._recent_pending_jd_edit_candidates(user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `pending_candidates`；它表示`pending_candidates` 在当前函数中承载的中间业务值。
        pending_candidates = await self._recent_pending_jd_edit_candidates(user_id, conversation_id)
        # 使用 `if` 判断 `pending_candidates and keyword`；条件成立时执行下面分支。
        if pending_candidates and keyword:
            # 调用 `self._match_jd_selection_candidates(pending_candidates, keyword)` 完成当前解析、查询或对象构造，并把返回值保存到 `selected`；它表示`selected` 在当前函数中承载的中间业务值。
            selected = self._match_jd_selection_candidates(pending_candidates, keyword)
            # 使用 `if` 判断 `selected`；条件成立时执行下面分支。
            if selected:
                # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `selected`，并结束当前函数。
                return selected

        # 使用 `await` 异步调用 `self._list_deletable_resources("jd", user_id, keyword)`，等待数据库、Dify 或模型处理完成后把结果保存到 `resources`；它表示`resources` 在当前函数中承载的中间业务值。
        resources = await self._list_deletable_resources("jd", user_id, keyword)
        # 使用 `if` 判断 `not resources`；条件成立时执行下面分支。
        if not resources:
            # 使用 `if` 判断 `keyword and context_matches`；条件成立时执行下面分支。
            if keyword and context_matches:
                # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `self._match_delete_resources(context_matches, keyword)`，并结束当前函数。
                return self._match_delete_resources(context_matches, keyword)
            # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `context_matches[:1] if context_matches else []`，并结束当前函数。
            return context_matches[:1] if context_matches else []
        # 使用 `if` 判断 `keyword`；条件成立时执行下面分支。
        if keyword:
            # 使用列表字面量开始构造 `matched`；该列表用于保存`matched` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
            matched = [
                # 使用 `*` 可迭代对象解包，把 `self._match_delete_resources(context_matches, keyword)` 返回的全部元素展开并合并进当前列表或参数序列。
                *self._match_delete_resources(context_matches, keyword),
                # 使用 `*` 可迭代对象解包，把 `self._match_delete_resources(resources, keyword)` 返回的全部元素展开并合并进当前列表或参数序列。
                *self._match_delete_resources(resources, keyword),
            # 结束 `_AgentServiceEditingMixin._resolve_recent_jd_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
            # 调用 `self._dedupe_candidates_by_id(matched)` 完成当前解析、查询或对象构造，并把返回值保存到 `matched`；它表示`matched` 在当前函数中承载的中间业务值。
            matched = self._dedupe_candidates_by_id(matched)
            # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `matched or resources[:5]`，并结束当前函数。
            return matched or resources[:5]
        # 使用 `if` 判断 `context_matches`；条件成立时执行下面分支。
        if context_matches:
            # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `context_matches[:1]`，并结束当前函数。
            return context_matches[:1]
        # 从 `_AgentServiceEditingMixin._resolve_recent_jd_candidates()` 返回 `resources[:1]`，并结束当前函数。
        return resources[:1]

    # 使用 `def` 普通函数 定义 `_dedupe_candidates_by_id()`。
    def _dedupe_candidates_by_id(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # 声明参数 `by_key`，表示`by_key` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Dict[str, Any]]`，默认值为 `{}`。
        by_key: Dict[str, Dict[str, Any]] = {}
        # 声明参数 `ordered_keys`，表示`ordered_keys` 在当前函数中承载的中间业务值，类型标注为 `List[str]`，默认值为 `[]`。
        ordered_keys: List[str] = []
        # 使用 `for` 循环 `item in candidates`，逐项处理集合数据。
        for item in candidates:
            # 调用 `item.get("id")` 完成当前解析、查询或对象构造，并把返回值保存到 `item_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
            item_id = item.get("id")
            # 调用 `str(item_id) if item_id else self._normalize_candidate_match_text(item.get("name"))` 完成当前解析、查询或对象构造，并把返回值保存到 `key`；它表示`key` 在当前函数中承载的中间业务值。
            key = str(item_id) if item_id else self._normalize_candidate_match_text(item.get("name"))
            # 使用 `if` 判断 `not key`；条件成立时执行下面分支。
            if not key:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 `if` 判断 `key not in by_key`；条件成立时执行下面分支。
            if key not in by_key:
                # 调用列表的 `append()`，把 `ordered_keys.append(key)` 对应的新元素追加到现有结果末尾。
                ordered_keys.append(key)
                # 计算表达式 `item` 并把结果保存到 `by_key[key]`；该结果在后续流程中用作`by_key` 在当前函数中承载的中间业务值。
                by_key[key] = item
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 调用 `self._merge_candidate_details(by_key[key], item)` 完成当前解析、查询或对象构造，并把返回值保存到 `by_key[key]`；它表示`by_key` 在当前函数中承载的中间业务值。
            by_key[key] = self._merge_candidate_details(by_key[key], item)
        # 从 `_AgentServiceEditingMixin._dedupe_candidates_by_id()` 返回 `[by_key[key] for key in ordered_keys]`，并结束当前函数。
        return [by_key[key] for key in ordered_keys]

    # 使用 `def` 普通函数 定义 `_merge_candidate_details()`。
    def _merge_candidate_details(self, primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
        # 计算表达式 `{**primary}` 并把结果保存到 `merged`；该结果在后续流程中用作`merged` 在当前函数中承载的中间业务值。
        merged = {**primary}
        # 使用 `for` 循环 `key, value in secondary.items()`，逐项处理集合数据。
        for key, value in secondary.items():
            # 使用 `if` 判断 `value and not merged.get(key)`；条件成立时执行下面分支。
            if value and not merged.get(key):
                # 计算表达式 `value` 并把结果保存到 `merged[key]`；该结果在后续流程中用作`merged` 在当前函数中承载的中间业务值。
                merged[key] = value
        # 使用 `if` 判断 `self._candidate_detail_score(secondary) > self._candidate_detail_score(primary)`；条件成立时执行下面分支。
        if self._candidate_detail_score(secondary) > self._candidate_detail_score(primary):
            # 计算表达式 `{**secondary, **merged}` 并把结果保存到 `merged`；该结果在后续流程中用作`merged` 在当前函数中承载的中间业务值。
            merged = {**secondary, **merged}
        # 从 `_AgentServiceEditingMixin._merge_candidate_details()` 返回 `merged`，并结束当前函数。
        return merged

    # 使用 `def` 普通函数 定义 `_candidate_detail_score()`。
    def _candidate_detail_score(self, item: Dict[str, Any]) -> int:
        # 从 `_AgentServiceEditingMixin._candidate_detail_score()` 返回 `sum(1 for key in ["location", "salary_range", "salary", "experience_level", "updated_at"] if item...`，并结束当前函数。
        return sum(1 for key in ["location", "salary_range", "salary", "experience_level", "updated_at"] if item.get(key))

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_pending_jd_edit_candidates()`。
    async def _recent_pending_jd_edit_candidates(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `limit`，表示`limit` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `8`。
        limit: int = 8,
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 使用列表字面量开始构造 `conditions`；该列表用于保存`conditions` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        conditions = [
            # 构造 SQLAlchemy 查询条件 `Conversation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
            Conversation.user_id == user_id,
            # 构造 SQLAlchemy 查询条件 `Message.role == MessageRole.ASSISTANT`，把数据库记录限制为符合该业务字段要求的数据。
            Message.role == MessageRole.ASSISTANT,
        # 结束 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `conversation_id`；条件成立时执行下面分支。
        if conversation_id:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用列表的 `append()`，把 `conditions.append(Message.conversation_id == UUID(str(conversation_id)))` 对应的新元素追加到现有结果末尾。
                conditions.append(Message.conversation_id == UUID(str(conversation_id)))
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 通过模块日志记录器执行 `logger.warning("JD 候选上下文收到非法 conversation_id: %s", conversation_id)`，用于保留运行状态或异常原因。
                logger.warning("JD 候选上下文收到非法 conversation_id: %s", conversation_id)
                # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates()` 返回 `[]`，并结束当前函数。
                return []
        # 计算表达式 `(` 并把结果保存到 `query`；该结果在后续流程中用作发送给模型、Dify 或数据库检索的输入。
        query = (
            # 调用 `select()`，执行 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates` 当前步骤所需的构造、查询或业务处理。
            select(Message)
            # 在 SQLAlchemy 查询链中调用 `join()`，按 `Conversation, Message.conversation_id == Conversation.id` 关联另一张表，使后续条件可以同时使用两张表的字段。
            .join(Conversation, Message.conversation_id == Conversation.id)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `*conditions` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(*conditions)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `Message.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(Message.created_at.desc())
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
        # 结束 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `await` 异步调用 `self.db.execute(query)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(query)
        # 使用 `for` 循环 `message in result.scalars().all()`，逐项处理集合数据。
        for message in result.scalars().all():
            # 计算表达式 `(message.context or {}).get("agent_response") if isinstance(message.context, dict) else None` 并把结果保存到 `response`；该结果在后续流程中用作下游服务或 Agent 流程返回的响应。
            response = (message.context or {}).get("agent_response") if isinstance(message.context, dict) else None
            # 使用 `if` 判断 `not isinstance(response, dict)`；条件成立时执行下面分支。
            if not isinstance(response, dict):
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 `for` 循环 `artifact in response.get("artifacts") or []`，逐项处理集合数据。
            for artifact in response.get("artifacts") or []:
                # 使用 `if` 判断 `artifact.get("type") != "jd_edit_candidates"`；条件成立时执行下面分支。
                if artifact.get("type") != "jd_edit_candidates":
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 调用 `artifact.get("content") if isinstance(artifact.get("content"), dict) else {}` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content = artifact.get("content") if isinstance(artifact.get("content"), dict) else {}
                # 调用 `content.get("candidates") if isinstance(content.get("candidates"), list) else []` 完成当前解析、查询或对象构造，并把返回值保存到 `candidates`；它表示根据用户描述找到、等待进一步选择的候选资源列表。
                candidates = content.get("candidates") if isinstance(content.get("candidates"), list) else []
                # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates()` 返回 `[item for item in candidates if isinstance(item, dict) and item.get("id")]`，并结束当前函数。
                return [item for item in candidates if isinstance(item, dict) and item.get("id")]
        # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_candidates()` 返回 `[]`，并结束当前函数。
        return []

    # 使用 `def` 普通函数 定义 `_match_jd_selection_candidates()`。
    def _match_jd_selection_candidates(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `candidates`，表示根据用户描述找到、等待进一步选择的候选资源列表，类型标注为 `List[Dict[str, Any]]`。
        candidates: List[Dict[str, Any]],
        # 声明参数 `keyword`，表示`keyword` 在当前函数中承载的中间业务值，类型标注为 `str`。
        keyword: str,
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 调用 `self._parse_selection_ordinal(keyword)` 完成当前解析、查询或对象构造，并把返回值保存到 `ordinal`；它表示`ordinal` 在当前函数中承载的中间业务值。
        ordinal = self._parse_selection_ordinal(keyword)
        # 使用 `if` 判断 `ordinal is not None and 0 <= ordinal < len(candidates)`；条件成立时执行下面分支。
        if ordinal is not None and 0 <= ordinal < len(candidates):
            # 从 `_AgentServiceEditingMixin._match_jd_selection_candidates()` 返回 `[candidates[ordinal]]`，并结束当前函数。
            return [candidates[ordinal]]
        # 使用 `if` 判断 `re.search(r"最新|最近|上一个|最后", keyword)`；条件成立时执行下面分支。
        if re.search(r"最新|最近|上一个|最后", keyword):
            # 从 `_AgentServiceEditingMixin._match_jd_selection_candidates()` 返回 `candidates[:1]`，并结束当前函数。
            return candidates[:1]
        # 从 `_AgentServiceEditingMixin._match_jd_selection_candidates()` 返回 `self._match_delete_resources(candidates, keyword)`，并结束当前函数。
        return self._match_delete_resources(candidates, keyword)

    # 使用 `def` 普通函数 定义 `_parse_selection_ordinal()`。
    def _parse_selection_ordinal(self, text: str) -> Optional[int]:
        # 调用 `re.search(r"第\s*([一二三四五六七八九十1-9])\s*(?:个|条)?", text)` 完成当前解析、查询或对象构造，并把返回值保存到 `match`；它表示`match` 在当前函数中承载的中间业务值。
        match = re.search(r"第\s*([一二三四五六七八九十1-9])\s*(?:个|条)?", text)
        # 使用 `if` 判断 `not match`；条件成立时执行下面分支。
        if not match:
            # 从 `_AgentServiceEditingMixin._parse_selection_ordinal()` 返回 `None`，并结束当前函数。
            return None
        # 调用 `match.group(1)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
        value = match.group(1)
        # 计算表达式 `{"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}` 并把结果保存到 `chinese_numbers`；该结果在后续流程中用作`chinese_numbers` 在当前函数中承载的中间业务值。
        chinese_numbers = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}
        # 调用 `int(value) if value.isdigit() else chinese_numbers.get(value)` 完成当前解析、查询或对象构造，并把返回值保存到 `number`；它表示`number` 在当前函数中承载的中间业务值。
        number = int(value) if value.isdigit() else chinese_numbers.get(value)
        # 从 `_AgentServiceEditingMixin._parse_selection_ordinal()` 返回 `number - 1 if number else None`，并结束当前函数。
        return number - 1 if number else None

    # 使用 `def` 普通函数 定义 `_format_jd_candidate_label()`。
    def _format_jd_candidate_label(self, item: Dict[str, Any]) -> str:
        # 调用 `item.get("name") or "未命名 JD"` 完成当前解析、查询或对象构造，并把返回值保存到 `name`；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
        name = item.get("name") or "未命名 JD"
        # 使用列表字面量开始构造 `details`；该列表用于保存`details` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        details = [
            # 调用 `item.get()`，执行 `_AgentServiceEditingMixin._format_jd_candidate_label` 当前步骤所需的构造、查询或业务处理。
            item.get("location"),
            # 调用 `item.get()`，执行 `_AgentServiceEditingMixin._format_jd_candidate_label` 当前步骤所需的构造、查询或业务处理。
            item.get("salary_range") or item.get("salary"),
            # 调用 `item.get()`，执行 `_AgentServiceEditingMixin._format_jd_candidate_label` 当前步骤所需的构造、查询或业务处理。
            item.get("experience_level"),
        # 结束 `_AgentServiceEditingMixin._format_jd_candidate_label` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `item.get("updated_at")`；条件成立时执行下面分支。
        if item.get("updated_at"):
            # 调用列表的 `append()`，把 `details.append(f"更新于 {str(item['updated_at'])[:10]}")` 对应的新元素追加到现有结果末尾。
            details.append(f"更新于 {str(item['updated_at'])[:10]}")
        # 使用 `if` 判断 `item.get("id")`；条件成立时执行下面分支。
        if item.get("id"):
            # 调用列表的 `append()`，把 `details.append(f"ID {str(item['id'])[:8]}")` 对应的新元素追加到现有结果末尾。
            details.append(f"ID {str(item['id'])[:8]}")
        # 计算表达式 `" / ".join(str(value) for value in details if value)` 并把结果保存到 `detail_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        detail_text = " / ".join(str(value) for value in details if value)
        # 从 `_AgentServiceEditingMixin._format_jd_candidate_label()` 返回 `f"{name}（{detail_text}）" if detail_text else name`，并结束当前函数。
        return f"{name}（{detail_text}）" if detail_text else name

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_pending_criteria_edit_target()`。
    async def _recent_pending_criteria_edit_target(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `limit`，表示`limit` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `8`。
        limit: int = 8,
    # 结束函数参数列表，并用 `-> Optional[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> Optional[Dict[str, Any]]:
        # 使用列表字面量开始构造 `conditions`；该列表用于保存`conditions` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        conditions = [
            # 构造 SQLAlchemy 查询条件 `Conversation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
            Conversation.user_id == user_id,
            # 构造 SQLAlchemy 查询条件 `Message.role == MessageRole.ASSISTANT`，把数据库记录限制为符合该业务字段要求的数据。
            Message.role == MessageRole.ASSISTANT,
        # 结束 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `conversation_id`；条件成立时执行下面分支。
        if conversation_id:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用列表的 `append()`，把 `conditions.append(Message.conversation_id == UUID(str(conversation_id)))` 对应的新元素追加到现有结果末尾。
                conditions.append(Message.conversation_id == UUID(str(conversation_id)))
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 通过模块日志记录器执行 `logger.warning("评分标准修改上下文收到非法 conversation_id: %s", conversation_id)`，用于保留运行状态或异常原因。
                logger.warning("评分标准修改上下文收到非法 conversation_id: %s", conversation_id)
                # 从 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target()` 返回 `None`，并结束当前函数。
                return None
        # 计算表达式 `(` 并把结果保存到 `query`；该结果在后续流程中用作发送给模型、Dify 或数据库检索的输入。
        query = (
            # 调用 `select()`，执行 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target` 当前步骤所需的构造、查询或业务处理。
            select(Message)
            # 在 SQLAlchemy 查询链中调用 `join()`，按 `Conversation, Message.conversation_id == Conversation.id` 关联另一张表，使后续条件可以同时使用两张表的字段。
            .join(Conversation, Message.conversation_id == Conversation.id)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `*conditions` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(*conditions)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `Message.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(Message.created_at.desc())
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
        # 结束 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `await` 异步调用 `self.db.execute(query)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(query)
        # 使用 `for` 循环 `message in result.scalars().all()`，逐项处理集合数据。
        for message in result.scalars().all():
            # 计算表达式 `(message.context or {}).get("agent_response") if isinstance(message.context, dict) else None` 并把结果保存到 `response`；该结果在后续流程中用作下游服务或 Agent 流程返回的响应。
            response = (message.context or {}).get("agent_response") if isinstance(message.context, dict) else None
            # 使用 `if` 判断 `not isinstance(response, dict)`；条件成立时执行下面分支。
            if not isinstance(response, dict):
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 `for` 循环 `artifact in response.get("artifacts") or []`，逐项处理集合数据。
            for artifact in response.get("artifacts") or []:
                # 使用 `if` 判断 `artifact.get("type") != "criteria_edit_request"`；条件成立时执行下面分支。
                if artifact.get("type") != "criteria_edit_request":
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 调用 `artifact.get("content") if isinstance(artifact.get("content"), dict) else {}` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content = artifact.get("content") if isinstance(artifact.get("content"), dict) else {}
                # 调用 `content.get("criteria_id")` 完成当前解析、查询或对象构造，并把返回值保存到 `criteria_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
                criteria_id = content.get("criteria_id")
                # 使用 `if` 判断 `not criteria_id`；条件成立时执行下面分支。
                if not criteria_id:
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 调用 `content.get("title") or artifact.get("title") or "待修改的评分标准"` 完成当前解析、查询或对象构造，并把返回值保存到 `title`；它表示前端向用户展示的步骤或业务产物标题。
                title = content.get("title") or artifact.get("title") or "待修改的评分标准"
                # 从 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target()` 返回 `{`，并结束当前函数。
                return {
                    # 在字典中设置 `id` 字段为 `str(criteria_id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(criteria_id),
                    # 在字典中设置 `name` 字段为 `title`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": title,
                    # 在字典中设置 `search_text` 字段为 `title`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": title,
                    # 在字典中设置 `source` 字段为 `"pending_criteria_edit_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "pending_criteria_edit_context",
                # 结束 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
        # 从 `_AgentServiceEditingMixin._recent_pending_criteria_edit_target()` 返回 `None`，并结束当前函数。
        return None

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_pending_jd_edit_target()`。
    async def _recent_pending_jd_edit_target(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `limit`，表示`limit` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `8`。
        limit: int = 8,
    # 结束函数参数列表，并用 `-> Optional[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> Optional[Dict[str, Any]]:
        # 使用列表字面量开始构造 `conditions`；该列表用于保存`conditions` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        conditions = [
            # 构造 SQLAlchemy 查询条件 `Conversation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
            Conversation.user_id == user_id,
            # 构造 SQLAlchemy 查询条件 `Message.role == MessageRole.ASSISTANT`，把数据库记录限制为符合该业务字段要求的数据。
            Message.role == MessageRole.ASSISTANT,
        # 结束 `_AgentServiceEditingMixin._recent_pending_jd_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `conversation_id`；条件成立时执行下面分支。
        if conversation_id:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用列表的 `append()`，把 `conditions.append(Message.conversation_id == UUID(str(conversation_id)))` 对应的新元素追加到现有结果末尾。
                conditions.append(Message.conversation_id == UUID(str(conversation_id)))
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 通过模块日志记录器执行 `logger.warning("JD 修改上下文收到非法 conversation_id: %s", conversation_id)`，用于保留运行状态或异常原因。
                logger.warning("JD 修改上下文收到非法 conversation_id: %s", conversation_id)
                # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_target()` 返回 `None`，并结束当前函数。
                return None
        # 计算表达式 `(` 并把结果保存到 `query`；该结果在后续流程中用作发送给模型、Dify 或数据库检索的输入。
        query = (
            # 调用 `select()`，执行 `_AgentServiceEditingMixin._recent_pending_jd_edit_target` 当前步骤所需的构造、查询或业务处理。
            select(Message)
            # 在 SQLAlchemy 查询链中调用 `join()`，按 `Conversation, Message.conversation_id == Conversation.id` 关联另一张表，使后续条件可以同时使用两张表的字段。
            .join(Conversation, Message.conversation_id == Conversation.id)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `*conditions` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(*conditions)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `Message.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(Message.created_at.desc())
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
        # 结束 `_AgentServiceEditingMixin._recent_pending_jd_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `await` 异步调用 `self.db.execute(query)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(query)
        # 使用 `for` 循环 `message in result.scalars().all()`，逐项处理集合数据。
        for message in result.scalars().all():
            # 计算表达式 `(message.context or {}).get("agent_response") if isinstance(message.context, dict) else None` 并把结果保存到 `response`；该结果在后续流程中用作下游服务或 Agent 流程返回的响应。
            response = (message.context or {}).get("agent_response") if isinstance(message.context, dict) else None
            # 使用 `if` 判断 `not isinstance(response, dict)`；条件成立时执行下面分支。
            if not isinstance(response, dict):
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 `for` 循环 `artifact in response.get("artifacts") or []`，逐项处理集合数据。
            for artifact in response.get("artifacts") or []:
                # 使用 `if` 判断 `artifact.get("type") != "jd_edit_request"`；条件成立时执行下面分支。
                if artifact.get("type") != "jd_edit_request":
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 调用 `artifact.get("content") if isinstance(artifact.get("content"), dict) else {}` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content = artifact.get("content") if isinstance(artifact.get("content"), dict) else {}
                # 调用 `content.get("job_description_id")` 完成当前解析、查询或对象构造，并把返回值保存到 `jd_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
                jd_id = content.get("job_description_id")
                # 使用 `if` 判断 `not jd_id`；条件成立时执行下面分支。
                if not jd_id:
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 调用 `content.get("title") or artifact.get("title") or "待修改的 JD"` 完成当前解析、查询或对象构造，并把返回值保存到 `title`；它表示前端向用户展示的步骤或业务产物标题。
                title = content.get("title") or artifact.get("title") or "待修改的 JD"
                # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_target()` 返回 `{`，并结束当前函数。
                return {
                    # 在字典中设置 `id` 字段为 `str(jd_id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(jd_id),
                    # 在字典中设置 `name` 字段为 `title`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": title,
                    # 在字典中设置 `search_text` 字段为 `title`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": title,
                    # 在字典中设置 `source` 字段为 `"pending_jd_edit_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "pending_jd_edit_context",
                # 结束 `_AgentServiceEditingMixin._recent_pending_jd_edit_target` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
        # 从 `_AgentServiceEditingMixin._recent_pending_jd_edit_target()` 返回 `None`，并结束当前函数。
        return None

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_resolve_recent_criteria_candidates()`。
    async def _resolve_recent_criteria_candidates(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `keyword`，表示`keyword` 在当前函数中承载的中间业务值，类型标注为 `str`，默认值为 `""`。
        keyword: str = "",
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 使用 `await` 异步调用 `self._recent_pending_criteria_edit_target(user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `pending_target`；它表示`pending_target` 在当前函数中承载的中间业务值。
        pending_target = await self._recent_pending_criteria_edit_target(user_id, conversation_id)
        # 使用 `if` 判断 `pending_target and not keyword`；条件成立时执行下面分支。
        if pending_target and not keyword:
            # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `[pending_target]`，并结束当前函数。
            return [pending_target]

        # 使用 `await` 异步调用 `self._recent_generated_resources("criteria", user_id, conversation_id, limit=20)`，等待数据库、Dify 或模型处理完成后把结果保存到 `context_matches`；它表示`context_matches` 在当前函数中承载的中间业务值。
        context_matches = await self._recent_generated_resources("criteria", user_id, conversation_id, limit=20)
        # 使用 `if` 判断 `context_matches`；条件成立时执行下面分支。
        if context_matches:
            # 使用 `if` 判断 `keyword`；条件成立时执行下面分支。
            if keyword:
                # 调用 `self._match_delete_resources(context_matches, keyword)` 完成当前解析、查询或对象构造，并把返回值保存到 `matched`；它表示`matched` 在当前函数中承载的中间业务值。
                matched = self._match_delete_resources(context_matches, keyword)
                # 使用 `if` 判断 `matched`；条件成立时执行下面分支。
                if matched:
                    # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `matched`，并结束当前函数。
                    return matched
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `context_matches[:1]`，并结束当前函数。
                return context_matches[:1]

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `await` 异步调用 `ScoringCriteriaService(self.db).get_scoring_criteria_list(user_id=user_id, page=1, size=100)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await ScoringCriteriaService(self.db).get_scoring_criteria_list(user_id=user_id, page=1, size=100)
            # 使用列表字面量开始构造 `resources`；该列表用于保存`resources` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
            resources = [
                # 使用字典字面量语法开始构造 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `id` 字段为 `str(item.id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(item.id),
                    # 在字典中设置 `name` 字段为 `item.title or item.job_title or "未命名评分标准"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": item.title or item.job_title or "未命名评分标准",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `title` 字段为 `item.title`，用于表示前端向用户展示的步骤或业务产物标题。
                        "title": item.title,
                        # 在字典中设置 `job_title` 字段为 `item.job_title`，用于表示`job_title` 在当前函数中承载的中间业务值。
                        "job_title": item.job_title,
                        # 在字典中设置 `content` 字段为 `item.content`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        "content": item.content,
                    # 结束 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                    # 在字典中设置 `job_description_id` 字段为 `str(item.job_description_id) if item.job_description_id else None`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                    "job_description_id": str(item.job_description_id) if item.job_description_id else None,
                    # 在字典中设置 `created_at` 字段为 `str(item.created_at) if item.created_at else ""`，用于表示`created_at` 在当前函数中承载的中间业务值。
                    "created_at": str(item.created_at) if item.created_at else "",
                    # 在字典中设置 `updated_at` 字段为 `str(item.updated_at) if item.updated_at else ""`，用于表示`updated_at` 在当前函数中承载的中间业务值。
                    "updated_at": str(item.updated_at) if item.updated_at else "",
                # 结束 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 使用 `for` 循环 `item in result.items`，逐项处理集合数据。
                for item in result.items
            # 结束 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("读取评分标准列表失败: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("读取评分标准列表失败: %s", exc)
            # 把 `resources` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`resources` 在当前函数中承载的中间业务值。
            resources = []
        # 使用 `if` 判断 `not resources`；条件成立时执行下面分支。
        if not resources:
            # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `[]`，并结束当前函数。
            return []
        # 使用 `if` 判断 `keyword`；条件成立时执行下面分支。
        if keyword:
            # 调用 `self._match_delete_resources(resources, keyword)` 完成当前解析、查询或对象构造，并把返回值保存到 `matched`；它表示`matched` 在当前函数中承载的中间业务值。
            matched = self._match_delete_resources(resources, keyword)
            # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `self._collapse_criteria_candidates(matched) if matched else resources[:1]`，并结束当前函数。
            return self._collapse_criteria_candidates(matched) if matched else resources[:1]
        # 从 `_AgentServiceEditingMixin._resolve_recent_criteria_candidates()` 返回 `resources[:1]`，并结束当前函数。
        return resources[:1]

    # 使用 `def` 普通函数 定义 `_collapse_criteria_candidates()`。
    def _collapse_criteria_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # 使用 `if` 判断 `len(candidates) <= 1`；条件成立时执行下面分支。
        if len(candidates) <= 1:
            # 从 `_AgentServiceEditingMixin._collapse_criteria_candidates()` 返回 `candidates`，并结束当前函数。
            return candidates
        # 调用 `sorted(` 完成当前解析、查询或对象构造，并把返回值保存到 `sorted_candidates`；它表示`sorted_candidates` 在当前函数中承载的中间业务值。
        sorted_candidates = sorted(
            # 把变量 `candidates` 作为位置参数传入当前多行函数调用；它承载根据用户描述找到、等待进一步选择的候选资源列表。
            candidates,
            # 使用关键字参数语法，把 `lambda item: item.get("updated_at") or item.get("created_at") or ""` 传入 `key` 参数；它表示`key` 在当前函数中承载的中间业务值。
            key=lambda item: item.get("updated_at") or item.get("created_at") or "",
            # 使用关键字参数语法，把 `True` 传入 `reverse` 参数；它表示`reverse` 在当前函数中承载的中间业务值。
            reverse=True,
        # 结束 `_AgentServiceEditingMixin._collapse_criteria_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 声明参数 `by_jd`，表示`by_jd` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Dict[str, Any]]`，默认值为 `{}`。
        by_jd: Dict[str, Dict[str, Any]] = {}
        # 声明参数 `without_jd`，表示`without_jd` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`，默认值为 `[]`。
        without_jd: List[Dict[str, Any]] = []
        # 使用 `for` 循环 `item in sorted_candidates`，逐项处理集合数据。
        for item in sorted_candidates:
            # 调用 `item.get("job_description_id")` 完成当前解析、查询或对象构造，并把返回值保存到 `jd_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
            jd_id = item.get("job_description_id")
            # 使用 `if` 判断 `jd_id`；条件成立时执行下面分支。
            if jd_id:
                # 调用 `by_jd.setdefault()`，执行 `_AgentServiceEditingMixin._collapse_criteria_candidates` 当前步骤所需的构造、查询或业务处理。
                by_jd.setdefault(str(jd_id), item)
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 调用列表的 `append()`，把 `without_jd.append(item)` 对应的新元素追加到现有结果末尾。
                without_jd.append(item)
        # 计算表达式 `[*by_jd.values(), *without_jd]` 并把结果保存到 `collapsed`；该结果在后续流程中用作`collapsed` 在当前函数中承载的中间业务值。
        collapsed = [*by_jd.values(), *without_jd]
        # 使用 `if` 判断 `len(collapsed) > 1`；条件成立时执行下面分支。
        if len(collapsed) > 1:
            # 计算表达式 `{self._normalize_candidate_match_text(item.get("name")) for item in collapsed}` 并把结果保存到 `normalized_names`；该结果在后续流程中用作已统一字段格式、便于后续比较或保存的数据。
            normalized_names = {self._normalize_candidate_match_text(item.get("name")) for item in collapsed}
            # 使用 `if` 判断 `len(normalized_names) == 1`；条件成立时执行下面分支。
            if len(normalized_names) == 1:
                # 从 `_AgentServiceEditingMixin._collapse_criteria_candidates()` 返回 `[collapsed[0]]`，并结束当前函数。
                return [collapsed[0]]
        # 从 `_AgentServiceEditingMixin._collapse_criteria_candidates()` 返回 `collapsed`，并结束当前函数。
        return collapsed

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_rewrite_jd_content()`。
    async def _rewrite_jd_content(self, original_content: str, edit_instructions: str) -> str:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是专业招聘 JD 编辑器。请在保留原 JD 结构和没有被要求修改的信息的前提下，"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "只根据用户修改要求输出修改后的完整 JD 正文。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "不要输出解释、差异说明或 Markdown 代码块。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"原 JD：\n{original_content}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户修改要求：\n{edit_instructions}"
        # 结束 `_AgentServiceEditingMixin._rewrite_jd_content` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
        if self.llm_service is None:
            # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
            self.llm_service = LLMService()
        # 使用 `await` 异步调用 `self.llm_service.generate_response(prompt)`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self.llm_service.generate_response(prompt)
        # 调用 `str(response or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `updated`；它表示`updated` 在当前函数中承载的中间业务值。
        updated = str(response or "").strip()
        # 使用 `if` 判断 `not updated`；条件成立时执行下面分支。
        if not updated:
            # 使用 `raise` 抛出 `ValueError("大模型没有返回修改后的 JD")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("大模型没有返回修改后的 JD")
        # 从 `_AgentServiceEditingMixin._rewrite_jd_content()` 返回 `updated`，并结束当前函数。
        return updated

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_rewrite_criteria_content()`。
    async def _rewrite_criteria_content(self, original_content: str, edit_instructions: str) -> str:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是专业简历评分标准编辑器。请在保留原评分标准结构和未被要求修改的信息的前提下，"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "只根据用户修改要求输出修改后的完整评分标准正文。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果涉及分值调整，请保持总分口径清晰，并同步调整相关维度描述，避免分值前后矛盾。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "不要输出解释、差异说明或 Markdown 代码块。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"原评分标准：\n{original_content}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户修改要求：\n{edit_instructions}"
        # 结束 `_AgentServiceEditingMixin._rewrite_criteria_content` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
        if self.llm_service is None:
            # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
            self.llm_service = LLMService()
        # 使用 `await` 异步调用 `self.llm_service.generate_response(prompt)`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self.llm_service.generate_response(prompt)
        # 调用 `str(response or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `updated`；它表示`updated` 在当前函数中承载的中间业务值。
        updated = str(response or "").strip()
        # 使用 `if` 判断 `not updated`；条件成立时执行下面分支。
        if not updated:
            # 使用 `raise` 抛出 `ValueError("大模型没有返回修改后的评分标准")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("大模型没有返回修改后的评分标准")
        # 从 `_AgentServiceEditingMixin._rewrite_criteria_content()` 返回 `updated`，并结束当前函数。
        return updated

    # 使用 `def` 普通函数 定义 `_build_jd_update_payload()`。
    def _build_jd_update_payload(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `original_data`，表示`original_data` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Any]`。
        original_data: Dict[str, Any],
        # 声明参数 `updated_content`，表示当前业务资源的正文内容，供模型生成、保存或前端展示，类型标注为 `str`。
        updated_content: str,
        # 声明参数 `edit_instructions`，表示用户要求对原内容执行的具体修改说明，类型标注为 `str`。
        edit_instructions: str,
        # 声明参数 `edit_request`，表示准备提交给下游服务的结构化请求参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        edit_request: Optional[Dict[str, Any]] = None,
    # 结束函数参数列表，并用 `-> JobDescriptionUpdate` 标注该函数的返回值类型。
    ) -> JobDescriptionUpdate:
        # 计算表达式 `edit_request or {}` 并把结果保存到 `edit_request`；该结果在后续流程中用作准备提交给下游服务的结构化请求参数。
        edit_request = edit_request or {}
        # 调用 `original_data.get("meta_data") if isinstance(original_data.get("meta_data"), dict) else {}` 完成当前解析、查询或对象构造，并把返回值保存到 `meta_data`；它表示`meta_data` 在当前函数中承载的中间业务值。
        meta_data = original_data.get("meta_data") if isinstance(original_data.get("meta_data"), dict) else {}
        # 调用 `list(meta_data.get("edit_history") or [])` 完成当前解析、查询或对象构造，并把返回值保存到 `edit_history`；它表示`edit_history` 在当前函数中承载的中间业务值。
        edit_history = list(meta_data.get("edit_history") or [])
        # 调用列表的 `append()`，把 `edit_history.append({"source": "hr_agent", "instruction": edit_instructions})` 对应的新元素追加到现有结果末尾。
        edit_history.append({"source": "hr_agent", "instruction": edit_instructions})
        # 计算表达式 `{**meta_data, "last_edit_source": "hr_agent", "edit_history": edit_history[-10:]}` 并把结果保存到 `meta_data`；该结果在后续流程中用作`meta_data` 在当前函数中承载的中间业务值。
        meta_data = {**meta_data, "last_edit_source": "hr_agent", "edit_history": edit_history[-10:]}
        # 使用 `if` 判断 `edit_request.get("salary_range")`；条件成立时执行下面分支。
        if edit_request.get("salary_range"):
            # 计算表达式 `edit_request["salary_range"]` 并把结果保存到 `meta_data["salary"]`；该结果在后续流程中用作`meta_data` 在当前函数中承载的中间业务值。
            meta_data["salary"] = edit_request["salary_range"]
        # 使用 `if` 判断 `edit_request.get("location")`；条件成立时执行下面分支。
        if edit_request.get("location"):
            # 计算表达式 `edit_request["location"]` 并把结果保存到 `meta_data["location"]`；该结果在后续流程中用作`meta_data` 在当前函数中承载的中间业务值。
            meta_data["location"] = edit_request["location"]
        # 从 `_AgentServiceEditingMixin._build_jd_update_payload()` 返回 `JobDescriptionUpdate(`，并结束当前函数。
        return JobDescriptionUpdate(
            # 使用关键字参数语法，把 `edit_request.get("title") or original_data.get("title")` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=edit_request.get("title") or original_data.get("title"),
            # 使用关键字参数语法，把 `edit_request.get("department") or original_data.get("department")` 传入 `department` 参数；它表示`department` 在当前函数中承载的中间业务值。
            department=edit_request.get("department") or original_data.get("department"),
            # 使用关键字参数语法，把 `edit_request.get("location") or original_data.get("location")` 传入 `location` 参数；它表示`location` 在当前函数中承载的中间业务值。
            location=edit_request.get("location") or original_data.get("location"),
            # 使用关键字参数语法，把 `edit_request.get("salary_range") or original_data.get("salary_range")` 传入 `salary_range` 参数；它表示`salary_range` 在当前函数中承载的中间业务值。
            salary_range=edit_request.get("salary_range") or original_data.get("salary_range"),
            # 使用关键字参数语法，把 `edit_request.get("experience_level") or original_data.get("experience_level")` 传入 `experience_level` 参数；它表示`experience_level` 在当前函数中承载的中间业务值。
            experience_level=edit_request.get("experience_level") or original_data.get("experience_level"),
            # 使用关键字参数语法，把 `edit_request.get("education") or original_data.get("education")` 传入 `education` 参数；它表示`education` 在当前函数中承载的中间业务值。
            education=edit_request.get("education") or original_data.get("education"),
            # 使用关键字参数语法，把 `edit_request.get("job_type") or original_data.get("job_type")` 传入 `job_type` 参数；它表示`job_type` 在当前函数中承载的中间业务值。
            job_type=edit_request.get("job_type") or original_data.get("job_type"),
            # 使用关键字参数语法，把 `edit_request.get("skills") or original_data.get("skills")` 传入 `skills` 参数；它表示`skills` 在当前函数中承载的中间业务值。
            skills=edit_request.get("skills") or original_data.get("skills"),
            # 使用关键字参数语法，把 `updated_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=updated_content,
            # 使用关键字参数语法，把 `original_data.get("requirements")` 传入 `requirements` 参数；它表示`requirements` 在当前函数中承载的中间业务值。
            requirements=original_data.get("requirements"),
            # 使用关键字参数语法，把 `original_data.get("status")` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status=original_data.get("status"),
            # 使用关键字参数语法，把 `meta_data` 传入 `meta_data` 参数；它表示`meta_data` 在当前函数中承载的中间业务值。
            meta_data=meta_data,
        # 结束 `_AgentServiceEditingMixin._build_jd_update_payload` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_build_criteria_update_payload()`。
    def _build_criteria_update_payload(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `original_data`，表示`original_data` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Any]`。
        original_data: Dict[str, Any],
        # 声明参数 `updated_content`，表示当前业务资源的正文内容，供模型生成、保存或前端展示，类型标注为 `str`。
        updated_content: str,
        # 声明参数 `edit_instructions`，表示用户要求对原内容执行的具体修改说明，类型标注为 `str`。
        edit_instructions: str,
    # 结束函数参数列表，并用 `-> ScoringCriteriaUpdate` 标注该函数的返回值类型。
    ) -> ScoringCriteriaUpdate:
        # 调用 `original_data.get("meta_data") if isinstance(original_data.get("meta_data"), dict) else {}` 完成当前解析、查询或对象构造，并把返回值保存到 `meta_data`；它表示`meta_data` 在当前函数中承载的中间业务值。
        meta_data = original_data.get("meta_data") if isinstance(original_data.get("meta_data"), dict) else {}
        # 调用 `list(meta_data.get("edit_history") or [])` 完成当前解析、查询或对象构造，并把返回值保存到 `edit_history`；它表示`edit_history` 在当前函数中承载的中间业务值。
        edit_history = list(meta_data.get("edit_history") or [])
        # 调用列表的 `append()`，把 `edit_history.append({"source": "hr_agent", "instruction": edit_instructions})` 对应的新元素追加到现有结果末尾。
        edit_history.append({"source": "hr_agent", "instruction": edit_instructions})
        # 计算表达式 `{**meta_data, "last_edit_source": "hr_agent", "edit_history": edit_history[-10:]}` 并把结果保存到 `meta_data`；该结果在后续流程中用作`meta_data` 在当前函数中承载的中间业务值。
        meta_data = {**meta_data, "last_edit_source": "hr_agent", "edit_history": edit_history[-10:]}
        # 从 `_AgentServiceEditingMixin._build_criteria_update_payload()` 返回 `ScoringCriteriaUpdate(`，并结束当前函数。
        return ScoringCriteriaUpdate(
            # 使用关键字参数语法，把 `original_data.get("title")` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=original_data.get("title"),
            # 使用关键字参数语法，把 `original_data.get("job_title")` 传入 `job_title` 参数；它表示`job_title` 在当前函数中承载的中间业务值。
            job_title=original_data.get("job_title"),
            # 使用关键字参数语法，把 `updated_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=updated_content,
            # 使用关键字参数语法，把 `original_data.get("criteria_data")` 传入 `criteria_data` 参数；它表示`criteria_data` 在当前函数中承载的中间业务值。
            criteria_data=original_data.get("criteria_data"),
            # 使用关键字参数语法，把 `original_data.get("total_score")` 传入 `total_score` 参数；它表示`total_score` 在当前函数中承载的中间业务值。
            total_score=original_data.get("total_score"),
            # 使用关键字参数语法，把 `original_data.get("scoring_dimensions")` 传入 `scoring_dimensions` 参数；它表示`scoring_dimensions` 在当前函数中承载的中间业务值。
            scoring_dimensions=original_data.get("scoring_dimensions"),
            # 使用关键字参数语法，把 `original_data.get("status")` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status=original_data.get("status"),
            # 使用关键字参数语法，把 `meta_data` 传入 `meta_data` 参数；它表示`meta_data` 在当前函数中承载的中间业务值。
            meta_data=meta_data,
            # 使用关键字参数语法，把 `original_data.get("job_description_id")` 传入 `job_description_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
            job_description_id=original_data.get("job_description_id"),
        # 结束 `_AgentServiceEditingMixin._build_criteria_update_payload` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_resolve_interview_exam_followup()`。
    async def _resolve_interview_exam_followup(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 使用 `if` 判断 `not self._is_followup_interview_exam_request(message)`；条件成立时执行下面分支。
        if not self._is_followup_interview_exam_request(message):
            # 从 `_AgentServiceEditingMixin._resolve_interview_exam_followup()` 返回 `{"matched": False}`，并结束当前函数。
            return {"matched": False}
        # 使用 `await` 异步调用 `self._recent_generated_resources("interview", user_id, conversation_id, limit=20)`，等待数据库、Dify 或模型处理完成后把结果保存到 `matches`；它表示`matches` 在当前函数中承载的中间业务值。
        matches = await self._recent_generated_resources("interview", user_id, conversation_id, limit=20)
        # 使用 `if` 判断 `not matches`；条件成立时执行下面分支。
        if not matches:
            # 从 `_AgentServiceEditingMixin._resolve_interview_exam_followup()` 返回 `{"matched": False}`，并结束当前函数。
            return {"matched": False}
        # 计算表达式 `matches[0]` 并把结果保存到 `latest`；该结果在后续流程中用作`latest` 在当前函数中承载的中间业务值。
        latest = matches[0]
        # 调用 `latest.get("content") or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
        content = latest.get("content") or ""
        # 从 `_AgentServiceEditingMixin._resolve_interview_exam_followup()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `matched` 字段为 `True`，用于表示`matched` 在当前函数中承载的中间业务值。
            "matched": True,
            # 在字典中设置 `plan_id` 字段为 `latest.get("id")`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
            "plan_id": latest.get("id"),
            # 在字典中设置 `title` 字段为 `latest.get("name") or "最近生成的面试方案"`，用于表示前端向用户展示的步骤或业务产物标题。
            "title": latest.get("name") or "最近生成的面试方案",
            # 在字典中设置 `content` 字段为 `content`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            "content": content,
            # 在字典中设置 `message` 字段为 `f"{message}\n\n当前面试方案上下文：\n{content[:2500]}"`，用于表示用户本次输入的自然语言消息。
            "message": f"{message}\n\n当前面试方案上下文：\n{content[:2500]}",
        # 结束 `_AgentServiceEditingMixin._resolve_interview_exam_followup` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
