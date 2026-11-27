"""
* 文件作用
  * 这是 HR Agent 的公开编排服务。AgentService 通过多重继承组合 Core、Editing 和 Resources 三个 Mixin，初始化数据库会话、Dify、LLM、意图服务、各 HR 业务 Service、工具注册表和 Skill 调度器；对外方法根据自然语言、附件、会话记忆与确认参数执行普通对话、JD/评分标准生成与编辑、简历筛选、面试方案、试卷生成等流程，并统一返回 steps、artifacts、suggestions 或 SSE 事件。
* 类说明
  * `AgentService`：Agent 对外服务类；组合三个 Mixin，并把所有底层 Service、工具与 Skill 装配成统一自然语言入口。
* 主要函数与方法说明
  * `AgentService.__init__()`：保存 AsyncSession，初始化 DifyService、LLMService、IntentService 和招聘业务 Service，然后构建受控工具注册表与 Skill 分发器。
  * `AgentService.chat()`：Agent 非流式主流程：加载记忆、规划动作、处理普通聊天或调用工具，最后统一组装步骤、产物、建议和确认信息。
  * `AgentService.stream_chat_agent()`：Agent 流式主流程：先发送 thinking/plan，再按意图进入聊天或工具分支，持续产生 delta/artifact，最终发送 final。
  * `AgentService.stream_recruitment_agent()`：执行 JD 生成及评分标准生成链路，并把过程状态和两个业务产物流式返回。
  * `AgentService.stream_resume_screening()`：解析上传简历并调用 ResumeEvaluationService 逐份评分，返回进度和筛选结果。
  * `AgentService.stream_interview_plan()`：读取候选人的简历评价及关联 JD，调用 Dify 生成面试方案并通过 InterviewPlanService 保存。
  * `AgentService.stream_exam_generation()`：解析试卷配置、选择上下文资源、调用 ExamService 生成题目并返回试卷产物。
  * `AgentService.stream_exam_generation_with_documents()`：将上传文件保存为文档后生成试卷，解决考试工具缺少知识来源的问题。
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


# 导入 _AgentServiceCoreMixin，用于组合 Agent 决策、记忆和流式编排能力。
from app.services._agent_service_core import _AgentServiceCoreMixin
# 导入 _AgentServiceEditingMixin，用于组合 JD 与评分标准编辑能力。
from app.services._agent_service_editing import _AgentServiceEditingMixin
# 导入 _AgentServiceResourcesMixin，用于组合业务资源生成、查询与删除能力。
from app.services._agent_service_resources import _AgentServiceResourcesMixin

# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
HR Agent 服务
    第一版聚焦工具编排：理解用户 HR 需求，并按意图调用 JD 生成、简历筛选等工具。

    HR Agent 的核心编排服务，也是最大文件。负责识别用户意图、选择工具、组织ReAct决策、处理普通/流式对话，
    并串联JD生成、评分标准生成、简历筛选、面试方案生成、试卷生成、资源删除等完整 HR工作流。
"""
# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
    兜底机制
    这个文件有很多兜底设计，避免 LLM 或 Dify 失败后系统完全不可用：
        ReAct 决策失败 -> 本地关键词规则。
        JD 字段解析失败 -> 正则提取。
        删除请求解析失败 -> 正则提取。
        流式生成失败 -> 同步生成。
        Dify 返回累计全文 -> 去重。
        普通聊天流式失败 -> 使用默认能力说明。
"""


# 开始 `AgentService` 中的多行函数调用或对象构造，后续缩进行传入具体参数。
class AgentService(
    # 把变量 `_AgentServiceCoreMixin` 作为位置参数传入当前多行函数调用；它承载`_AgentServiceCoreMixin` 在当前函数中承载的中间业务值。
    _AgentServiceCoreMixin,
    # 把变量 `_AgentServiceEditingMixin` 作为位置参数传入当前多行函数调用；它承载`_AgentServiceEditingMixin` 在当前函数中承载的中间业务值。
    _AgentServiceEditingMixin,
    # 把变量 `_AgentServiceResourcesMixin` 作为位置参数传入当前多行函数调用；它承载`_AgentServiceResourcesMixin` 在当前函数中承载的中间业务值。
    _AgentServiceResourcesMixin,
# 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
):
    """HR Agent service"""

    # 使用 `def` 普通函数 定义 `__init__()`。保存 AsyncSession，初始化 DifyService、LLMService、IntentService 和招聘业务 Service，然后构建受控工具注册表与 Skill 分发器。
    def __init__(self, db: AsyncSession):
        """
        初始化
            self.db：异步数据库会话。
            self.dify_service：调用 Dify 工作流。
            self.intent_service：关键词/LLM 意图分类服务。
            self.skill_dispatcher：加载可插拔 Skill。
            self.tool_registry：构建 Agent 可用工具表。
            self.email_send_service：邮件发送服务。
            self.llm_service：懒加载 LLM 服务。
            llm_service 没有在构造时立即创建，而是在真正需要 LLM 时才初始化，避免普通请求不必要地加载模型客户端。
        """
        # 把 `db` 保存为实例属性 `self.db`，供同一 Agent 服务的其他方法复用；该属性表示当前请求使用的异步数据库会话。
        self.db = db
        # 把 `DifyService()` 保存为实例属性 `self.dify_service`，供同一 Agent 服务的其他方法复用；该属性表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
        self.dify_service = DifyService()
        # 把 `IntentService(db)` 保存为实例属性 `self.intent_service`，供同一 Agent 服务的其他方法复用；该属性表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
        self.intent_service = IntentService(db)
        # 扫描并加载 Skill 包，供邮件等可插拔能力按阶段执行。
        self.skill_dispatcher = self._build_skill_dispatcher()
        # 构建受控工具注册表，后续 ReAct 只能从这些已声明能力中选择。
        self.tool_registry = self._build_tool_registry()
        # 把 `EmailSendService(db)` 保存为实例属性 `self.email_send_service`，供同一 Agent 服务的其他方法复用；该属性表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
        self.email_send_service = EmailSendService(db)
        # 把 `None` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
        self.llm_service = None

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `chat()`。Agent 非流式主流程：加载记忆、规划动作、处理普通聊天或调用工具，最后统一组装步骤、产物、建议和确认信息。
    async def chat(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 声明参数 `auto_execute`，表示是否允许信息完整后自动执行低风险生成工具，类型标注为 `bool`，默认值为 `True`。
        auto_execute: bool = True,
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        confirmed_requirements: Optional[Dict[str, Any]] = None,
        # 声明参数 `attachments`，表示用户随消息上传的附件列表，类型标注为 `Optional[List[Dict[str, Any]]]`，默认值为 `None`。
        attachments: Optional[List[Dict[str, Any]]] = None,
        # 声明参数 `agent_plan`，表示规则或 ReAct 产生的本轮执行计划，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        agent_plan: Optional[Dict[str, Any]] = None,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        """
        处理用户消息并执行招聘 Agent 任务

        主入口 chat()
            chat() 是非流式核心入口。它的流程是：
            1.标准化附件。
            2.读取历史对话记忆。
            3.如果用户已经确认 confirmed_requirements，直接进入确认后的工具链。
            4.如果是删除类请求，直接走删除工具，避免 LLM 误判。
            5.否则调用 _plan_agent_action() 做 ReAct 决策。
            6.根据 intent 分发到不同业务分支。

            普通聊天时，返回 intent="general"，不调用业务工具，只给出 Agent 能力说明或 LLM 回复。
        """
        # 1.标准化附件。
        normalized_attachments = self._normalize_attachments(attachments or [])
        # 2.读取历史对话记忆。
        memory_context = await self._build_conversation_memory(conversation_id, user_id, message)
        # 3.如果用户已经确认 confirmed_requirements，直接进入确认后的工具链。
        if confirmed_requirements:
            # 调用 `str(confirmed_requirements.get("action") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `confirmation_action`；它表示触发高风险操作继续执行的人工确认动作。
            confirmation_action = str(confirmed_requirements.get("action") or "").strip()
            # 用用户提交的确认动作匹配等待继续执行的 Skill，例如 send_email。
            confirmed_skill = self.skill_dispatcher.match_confirmation_action(confirmation_action) if confirmation_action else None
            # 使用 `if` 判断 `confirmed_skill`；条件成立时执行下面分支。
            if confirmed_skill:
                # 计算表达式 `confirmed_skill.intent` 并把结果保存到 `intent`；该结果在后续流程中用作识别出的业务意图标签。
                intent = confirmed_skill.intent
                # 使用字典字面量开始构造 `agent_plan`；该字典用于保存规则或 ReAct 产生的本轮执行计划，下面各行会继续写入具体业务字段。
                agent_plan = {
                    # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                    "mode": "tool",
                    # 在字典中设置 `intent` 字段为 `confirmed_skill.intent`，用于表示识别出的业务意图标签。
                    "intent": confirmed_skill.intent,
                    # 在字典中设置 `reason` 字段为 `f"用户已确认 {confirmed_skill.bundle_name} 所需信息，继续执行 skill。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                    "reason": f"用户已确认 {confirmed_skill.bundle_name} 所需信息，继续执行 skill。",
                    # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                    "reply": None,
                    # 在字典中设置 `source` 字段为 `"confirmed_skill_action"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "confirmed_skill_action",
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 计算表达式 `"jd"` 并把结果保存到 `intent`；该结果在后续流程中用作识别出的业务意图标签。
                intent = "jd"
                # 使用字典字面量开始构造 `agent_plan`；该字典用于保存规则或 ReAct 产生的本轮执行计划，下面各行会继续写入具体业务字段。
                agent_plan = {
                    # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                    "mode": "tool",
                    # 在字典中设置 `intent` 字段为 `"jd"`，用于表示识别出的业务意图标签。
                    "intent": "jd",
                    # 在字典中设置 `reason` 字段为 `"用户已确认 JD 生成信息，继续执行 JD 工具链。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                    "reason": "用户已确认 JD 生成信息，继续执行 JD 工具链。",
                    # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                    "reply": None,
                    # 在字典中设置 `source` 字段为 `"confirmed_requirements"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "confirmed_requirements",
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
        # 4.否则调用 _plan_agent_action() 做 ReAct 决策。
        else:
            # 计算表达式 `agent_plan or self._build_rule_agent_plan(message, normalized_attachments, memory_context)` 并把结果保存到 `agent_plan`；该结果在后续流程中用作规则或 ReAct 产生的本轮执行计划。
            agent_plan = agent_plan or self._build_rule_agent_plan(message, normalized_attachments, memory_context)
            # 计算表达式 `agent_plan or await self._plan_agent_action(message, normalized_attachments, memory_context)` 并把结果保存到 `agent_plan`；该结果在后续流程中用作规则或 ReAct 产生的本轮执行计划。
            agent_plan = agent_plan or await self._plan_agent_action(message, normalized_attachments, memory_context)
            # 计算表达式 `agent_plan["intent"]` 并把结果保存到 `intent`；该结果在后续流程中用作识别出的业务意图标签。
            intent = agent_plan["intent"]

        # 5.根据 intent 分发到不同业务分支。
        route_result = self._route_for_intent(intent, message)
        # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
        selected_tool = self.tool_registry.get(intent)

        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
        普通聊天
        """
        # 使用 `if` 判断 `agent_plan["mode"] == "chat" or intent == "general"`；条件成立时执行下面分支。
        if agent_plan["mode"] == "chat" or intent == "general":
            # 调用 `self._clean_optional_value(agent_plan.get("reply")) or self._fallback_message("general", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `reply`；它表示`reply` 在当前函数中承载的中间业务值。
            reply = self._clean_optional_value(agent_plan.get("reply")) or self._fallback_message("general", message)
            # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `reply` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=reply,
                # 使用关键字参数语法，把 `"general"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="general",
                # 使用关键字参数语法，把 `None` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=None,
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `AgentStep()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    AgentStep(
                        # 使用关键字参数语法，把 `"understand"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                        id="understand",
                        # 使用关键字参数语法，把 `"完成需求判断"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="完成需求判断",
                        # 使用关键字参数语法，把 `"completed"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                        status="completed",
                        # 使用关键字参数语法，把 `agent_plan.get("reason") or "判断为普通对话，不调用招聘工具。"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                        detail=agent_plan.get("reason") or "判断为普通对话，不调用招聘工具。",
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `["生成 JD", "评分简历", "基于文档生成试卷"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["生成 JD", "评分简历", "基于文档生成试卷"],
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
        删除资源分支
        删除资源由 _handle_resource_delete() 处理。
        支持删除：
            JD、简历评分记录、面试方案、试卷
        """
        # 使用 `if` 判断 `intent == "resource_delete"`；条件成立时执行下面分支。
        if intent == "resource_delete":
            # 从 `AgentService.chat()` 返回 `await self._handle_resource_delete(message, user_id, selected_tool, conversation_id)`，并结束当前函数。
            return await self._handle_resource_delete(message, user_id, selected_tool, conversation_id)

        # jd编辑
        if intent == "jd_edit":
            # 从 `AgentService.chat()` 返回 `await self._handle_jd_edit(message, user_id, selected_tool, conversation_id, memory_context)`，并结束当前函数。
            return await self._handle_jd_edit(message, user_id, selected_tool, conversation_id, memory_context)
        # 评分标准
        if intent == "criteria_edit":
            # 从 `AgentService.chat()` 返回 `await self._handle_criteria_edit(message, user_id, selected_tool, conversation_id, memory_context)`，并结束当前函数。
            return await self._handle_criteria_edit(message, user_id, selected_tool, conversation_id, memory_context)

        # 如果intent在skills中
        """
        邮件 Skill 分支
            邮件不是硬编码在 agent_service.py 里的完整业务，而是通过 Skill 插件机制接入。
            agent_skills.py 会扫描 backend/skills，读取 skill.json，把 email_notification 注册进工具表。
        邮件流程有两个 phase：
            - draft：生成邮件草稿。
            - send：用户确认后发送。
        在流式聊天中，如果识别到 email_notification，会走 _stream_skill_draft_response()，先用 LLM 流式生成邮件草稿，再调用 _handle_skill_intent()  生成标准 Agent 响应。
        真正发送邮件在/backend/skills/hr-agent-email/scripts/email_workflow.py ，调用 email_service.send_agent_email()。设计上邮件不会自动发送，必须人工确认。
        """
        # 使用 `if` 判断 `intent in self.skill_dispatcher.bundles`；条件成立时执行下面分支。
        if intent in self.skill_dispatcher.bundles:
            # 从 `AgentService.chat()` 返回 `await self._handle_skill_intent(`，并结束当前函数。
            return await self._handle_skill_intent(
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=user_id,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用关键字参数语法，把 `selected_tool` 传入 `selected_tool` 参数；它表示当前意图对应的已注册工具规格。
                selected_tool=selected_tool,
                # 使用关键字参数语法，把 `memory_context` 传入 `memory_context` 参数；它表示从数据库历史消息整理出的短期会话记忆。
                memory_context=memory_context,
                # 使用关键字参数语法，把 `confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
                confirmed_requirements=confirmed_requirements,
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
        简历筛选分支
        在 chat() 中，如果识别为 resume_screening：
            如果消息带了简历附件，返回 resume_upload_request artifact，提示前端让用户选择 JD。
            如果没有附件，返回上传简历的引导。
        """
        # 使用 `if` 判断 `intent == "resume_screening"`；条件成立时执行下面分支。
        if intent == "resume_screening":
            # 调用 `bool(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx"}))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_resume_files`；它表示`has_resume_files` 在当前函数中承载的中间业务值。
            has_resume_files = bool(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx"}))
            # 有简历文件
            if has_resume_files:
                # 调用 `len(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx"}))` 完成当前解析、查询或对象构造，并把返回值保存到 `resume_count`；它表示当前业务对象、题目或候选记录的数量。
                resume_count = len(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx"}))
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"我已经识别到 {resume_count} 份简历附件。下一步请选择用于匹配的 JD，确认后我会调用简历评分工具逐份评分并保存结果。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"我已经识别到 {resume_count} 份简历附件。下一步请选择用于匹配的 JD，确认后我会调用简历评分工具逐份评分并保存结果。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "已选择简历评分工具，并检查到简历附件。"),
                        # 计算表达式 `"upload", title="接收简历文件", status="completed", detail=f"已检测到 {resume_count} 份简历附件"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="upload", title="接收简历文件", status="completed", detail=f"已检测到 {resume_count} 份简历附件"),
                        # 计算表达式 `"select_jd", title="选择匹配 JD", status="running", detail="请选择用于评分匹配的 JD。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="select_jd", title="选择匹配 JD", status="running", detail="请选择用于评分匹配的 JD。"),
                        # 计算表达式 `"evaluate", title="执行 AI 评分", status="pending", detail="选择 JD 后开始逐份评分。", tool="evaluate_resume"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="evaluate", title="执行 AI 评分", status="pending", detail="选择 JD 后开始逐份评分。", tool="evaluate_resume"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"resume_upload_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="resume_upload_request",
                            # 使用关键字参数语法，把 `"选择 JD 并开始评分"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="选择 JD 并开始评分",
                            # 使用关键字参数语法，把 `{"requires_job_description": True, "file_count": resume_count}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            content={"requires_job_description": True, "file_count": resume_count},
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

            # 如果当前消息没有简历附件
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我规划了一下：这个需求需要调用简历评分工具，但当前消息里还没有简历附件。请先在底部消息框点击“上传文件”，添加 PDF、DOC 或 DOCX 简历后再发送；收到简历后我再让你选择用于匹配的 JD。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我规划了一下：这个需求需要调用简历评分工具，但当前消息里还没有简历附件。请先在底部消息框点击“上传文件”，添加 PDF、DOC 或 DOCX 简历后再发送；收到简历后我再让你选择用于匹配的 JD。",
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "需要先满足前置条件：上传简历文件。"),
                    # 计算表达式 `"upload", title="等待上传简历", status="pending", detail="请先上传 PDF、DOC 或 DOCX 简历。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="upload", title="等待上传简历", status="pending", detail="请先上传 PDF、DOC 或 DOCX 简历。"),
                    # 计算表达式 `"select_jd", title="选择匹配 JD", status="pending", detail="收到简历后再选择用于匹配的 JD。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="select_jd", title="选择匹配 JD", status="pending", detail="收到简历后再选择用于匹配的 JD。"),
                    # 计算表达式 `"evaluate", title="执行 AI 评分", status="pending", detail="选择 JD 后开始逐份评分。", tool="evaluate_resume"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="evaluate", title="执行 AI 评分", status="pending", detail="选择 JD 后开始逐份评分。", tool="evaluate_resume"),
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"resume_upload_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="resume_upload_request",
                        # 使用关键字参数语法，把 `"简历筛选工具"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="简历筛选工具",
                        # 使用关键字参数语法，把 `{"requires_job_description": True, "accepted_formats": ["pdf", "doc", "docx"]}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"requires_job_description": True, "accepted_formats": ["pdf", "doc", "docx"]},
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
            面试计划分支
            如果意图是 interview_plan，chat() 不会盲目生成，而是先解析候选人。
            _resolve_interview_candidate() 会读取最近的简历评分记录，然后用 LLM 判断用户说的是哪位候选人：
                matched：匹配到唯一候选人。
                no_match：用户说了候选人，但系统没有评分记录。
                ambiguous：匹配多个候选人，需要用户选择。
                no_specific：没有指定候选人。
            如果匹配成功，返回 interview_plan_execute artifact，前端可以据此调用流式面试方案接口，stream_interview_plan()
        """
        # 使用 `if` 判断 `intent == "interview_plan"`；条件成立时执行下面分支。
        if intent == "interview_plan":
            # 使用 `await` 异步调用 `self._resolve_resume_screening_followup_action(message, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `followup_resume_action`；它表示`followup_resume_action` 在当前函数中承载的中间业务值。
            followup_resume_action = await self._resolve_resume_screening_followup_action(message, user_id, conversation_id)
            # 使用 `if` 判断 `followup_resume_action.get("action") in {"generate_interview", "confirm_delete_low_scores"}`；条件成立时执行下面分支。
            if followup_resume_action.get("action") in {"generate_interview", "confirm_delete_low_scores"}:
                # 从 `AgentService.chat()` 返回 `self._build_resume_screening_followup_response(`，并结束当前函数。
                return self._build_resume_screening_followup_response(
                    # 把变量 `followup_resume_action` 作为位置参数传入当前多行函数调用；它承载`followup_resume_action` 在当前函数中承载的中间业务值。
                    followup_resume_action,
                    # 调用 `route_result.get()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    route_result.get("route"),
                    # 把变量 `selected_tool` 作为位置参数传入当前多行函数调用；它承载当前意图对应的已注册工具规格。
                    selected_tool,
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

            # 查询最近简历评分记录，用 LLM 或本地规则匹配候选人
            candidate_resolution = await self._resolve_interview_candidate(message, user_id, memory_context)
            # 匹配到了候选人
            if candidate_resolution.get("status") == "matched" and candidate_resolution.get("candidate_id"):
                # 调用 `candidate_resolution.get("candidate_name") or "该候选人"` 完成当前解析、查询或对象构造，并把返回值保存到 `candidate_name`；它表示`candidate_name` 在当前函数中承载的中间业务值。
                candidate_name = candidate_resolution.get("candidate_name") or "该候选人"
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `candidate_resolution.get("reply") or f"我已经找到候选人「{candidate_name}」，现在直接调用面试计划工具生成方案。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=candidate_resolution.get("reply") or f"我已经找到候选人「{candidate_name}」，现在直接调用面试计划工具生成方案。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "用户已指定候选人，且已匹配到已评分简历。"),
                        # 计算表达式 `"select_resume", title="匹配候选人", status="completed", detail=f"已匹配：{candidate_name}。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="select_resume", title="匹配候选人", status="completed", detail=f"已匹配：{candidate_name}。"),
                        # 计算表达式 `"generate_plan", title="生成面试计划", status="running", detail="正在调用面试计划工具。", tool="generate_interview...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_plan", title="生成面试计划", status="running", detail="正在调用面试计划工具。", tool="generate_interview_plan"),
                        # 计算表达式 `"save_plan", title="保存面试计划", status="pending", detail="生成后自动保存。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="save_plan", title="保存面试计划", status="pending", detail="生成后自动保存。"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"interview_plan_execute"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="interview_plan_execute",
                            # 使用关键字参数语法，把 `"直接生成面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="直接生成面试计划",
                            # 使用字典字面量开始构造 `content`；该字典用于保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据，下面各行会继续写入具体业务字段。
                            content={
                                # 在字典中设置 `resume_evaluation_id` 字段为 `candidate_resolution["candidate_id"]`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                                "resume_evaluation_id": candidate_resolution["candidate_id"],
                                # 在字典中设置 `candidate_name` 字段为 `candidate_name`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                                "candidate_name": candidate_name,
                            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                            },
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["生成面试计划", "查看该候选人评分", "重新筛选简历"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["生成面试计划", "查看该候选人评分", "重新筛选简历"],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 没有匹配到候选人
            if candidate_resolution.get("status") == "no_match":
                # 调用 `candidate_resolution.get("requested_name") or "该候选人"` 完成当前解析、查询或对象构造，并把返回值保存到 `requested_name`；它表示`requested_name` 在当前函数中承载的中间业务值。
                requested_name = candidate_resolution.get("requested_name") or "该候选人"
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `candidate_resolution.get("reply") or f"我没有在已评分候选人中找到「{requested_name}」。请先上传他的简历并完成评分，然后我再基于评分结果生成...` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=candidate_resolution.get("reply") or f"我没有在已评分候选人中找到「{requested_name}」。请先上传他的简历并完成评分，然后我再基于评分结果生成面试计划。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "用户指定了候选人，但当前没有匹配的已评分简历。"),
                        # 计算表达式 `"match_resume", title="匹配候选人", status="failed", detail=f"未找到：{requested_name}。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="match_resume", title="匹配候选人", status="failed", detail=f"未找到：{requested_name}。"),
                        # 计算表达式 `"upload_resume", title="上传候选人简历", status="running", detail="请上传该候选人的 PDF、DOC 或 DOCX 简历。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="upload_resume", title="上传候选人简历", status="running", detail="请上传该候选人的 PDF、DOC 或 DOCX 简历。"),
                        # 计算表达式 `"evaluate", title="完成简历评分", status="pending", detail="评分完成后再生成面试计划。", tool="evaluate_resume"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="evaluate", title="完成简历评分", status="pending", detail="评分完成后再生成面试计划。", tool="evaluate_resume"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"resume_upload_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="resume_upload_request",
                            # 使用关键字参数语法，把 `"上传候选人简历"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="上传候选人简历",
                            # 使用关键字参数语法，把 `{"candidate_name": requested_name, "accepted_formats": ["pdf", "doc", "docx"]}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            content={"candidate_name": requested_name, "accepted_formats": ["pdf", "doc", "docx"]},
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 匹配到多个候选人
            if candidate_resolution.get("status") == "ambiguous":
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `candidate_resolution.get("reply") or "我找到了多个可能匹配的候选人，还不能确定你指的是哪一位。请在这条消息里选择候选人后，我再生成面试计划。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=candidate_resolution.get("reply") or "我找到了多个可能匹配的候选人，还不能确定你指的是哪一位。请在这条消息里选择候选人后，我再生成面试计划。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "用户指定的候选人存在多个可能匹配项，需要用户确认。"),
                        # 计算表达式 `"select_resume", title="确认候选人", status="running", detail="请选择一条已评分简历。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="select_resume", title="确认候选人", status="running", detail="请选择一条已评分简历。"),
                        # 计算表达式 `"generate_plan", title="生成面试计划", status="pending", detail="选择后开始生成。", tool="generate_interview_pl...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_plan", title="生成面试计划", status="pending", detail="选择后开始生成。", tool="generate_interview_plan"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"interview_plan_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="interview_plan_request",
                            # 使用关键字参数语法，把 `"选择候选人生成面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="选择候选人生成面试计划",
                            # 使用字典字面量开始构造 `content`；该字典用于保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据，下面各行会继续写入具体业务字段。
                            content={
                                # 在字典中设置 `requires_resume_evaluation` 字段为 `True`，用于表示`requires_resume_evaluation` 在当前函数中承载的中间业务值。
                                "requires_resume_evaluation": True,
                                # 在字典中设置 `candidates` 字段为 `candidate_resolution.get("candidates") or []`，用于表示根据用户描述找到、等待进一步选择的候选资源列表。
                                "candidates": candidate_resolution.get("candidates") or [],
                            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                            },
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我规划了一下：生成面试计划需要先选中一位已评分候选人，然后调用面试计划工具生成并保存方案。请在这条消息里选择候选人。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我规划了一下：生成面试计划需要先选中一位已评分候选人，然后调用面试计划工具生成并保存方案。请在这条消息里选择候选人。",
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "需要先满足前置条件：选择已评分候选人。"),
                    # 计算表达式 `"select_resume", title="选择候选人", status="pending", detail="请选择一条已评分简历。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="select_resume", title="选择候选人", status="pending", detail="请选择一条已评分简历。"),
                    # 计算表达式 `"generate_plan", title="生成面试计划", status="pending", detail="选择后开始生成。", tool="generate_interview_pl...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="generate_plan", title="生成面试计划", status="pending", detail="选择后开始生成。", tool="generate_interview_plan"),
                    # 计算表达式 `"save_plan", title="保存面试计划", status="pending", detail="生成后自动保存。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="save_plan", title="保存面试计划", status="pending", detail="生成后自动保存。"),
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"interview_plan_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="interview_plan_request",
                        # 使用关键字参数语法，把 `"选择候选人生成面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="选择候选人生成面试计划",
                        # 使用关键字参数语法，把 `{"requires_resume_evaluation": True}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"requires_resume_evaluation": True},
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
        考试生成分支
            如果意图是 exam_generate，chat() 会检查是否有参考文档附件。
            没有文档时，返回 exam_document_upload_request，提醒先上传文档。
            有文档时，返回 exam_generate_request，要求确认试卷配置，比如标题、方向、题型、题量、分值。
            stream_exam_generation()负责生成并保存试卷
        """
        # 使用 `if` 判断 `intent == "exam_generate"`；条件成立时执行下面分支。
        if intent == "exam_generate":

            # 使用 `await` 异步调用 `self._resolve_interview_exam_followup(message, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `interview_exam_context`；它表示当前业务步骤需要的历史、用户或 Skill 上下文。
            interview_exam_context = await self._resolve_interview_exam_followup(message, user_id, conversation_id)
            # 使用 `if` 判断 `interview_exam_context.get("matched")`；条件成立时执行下面分支。
            if interview_exam_context.get("matched"):
                # 使用 `await` 异步调用 `self._parse_exam_requirements(`，等待数据库、Dify 或模型处理完成后把结果保存到 `parsed_exam`；它表示从用户自然语言或模型输出中解析并校验后的结构化结果。
                parsed_exam = await self._parse_exam_requirements(
                    # 调用 `interview_exam_context.get()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    interview_exam_context.get("message") or message,
                    # 把变量 `conversation_id` 作为位置参数传入当前多行函数调用；它承载当前聊天窗口 ID，用于读取和保存同一会话的历史。
                    conversation_id,
                    # 把变量 `memory_context` 作为位置参数传入当前多行函数调用；它承载从数据库历史消息整理出的短期会话记忆。
                    memory_context,
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                    message=(
                        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                        f"可以。我会基于最近的面试方案「{interview_exam_context.get('title')}」来组织笔试范围。"
                        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                        "不过当前消息里还没有参考文档，请先在底部消息框上传岗位资料、题库材料或技术文档，上传后我再让你确认试卷配置。"
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ),
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "识别到用户想从当前面试方案继续生成笔试试卷。"),
                        # 调用 `AgentStep()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentStep(
                            # 使用关键字参数语法，把 `"load_interview_plan"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                            id="load_interview_plan",
                            # 使用关键字参数语法，把 `"读取面试方案上下文"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="读取面试方案上下文",
                            # 使用关键字参数语法，把 `"completed"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                            status="completed",
                            # 使用关键字参数语法，把 `f"已匹配：{interview_exam_context.get('title')}"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                            detail=f"已匹配：{interview_exam_context.get('title')}",
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        ),
                        # 计算表达式 `"upload_docs", title="等待上传参考文档", status="running", detail="请上传用于出题的参考文档。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="upload_docs", title="等待上传参考文档", status="running", detail="请上传用于出题的参考文档。"),
                        # 计算表达式 `"confirm_exam", title="确认考试配置", status="pending", detail="上传文档后确认题型、题量和分值。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="confirm_exam", title="确认考试配置", status="pending", detail="上传文档后确认题型、题量和分值。"),
                        # 计算表达式 `"generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"exam_document_upload_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="exam_document_upload_request",
                            # 使用关键字参数语法，把 `"上传出题参考文档"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="上传出题参考文档",
                            # 使用字典字面量开始构造 `content`；该字典用于保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据，下面各行会继续写入具体业务字段。
                            content={
                                # 使用 `**` 字典解包，把 `parsed_exam` 中的全部键值作为关键字参数传给当前函数，避免逐个重复传参。
                                **parsed_exam,
                                # 在字典中设置 `requires_document_upload` 字段为 `True`，用于表示`requires_document_upload` 在当前函数中承载的中间业务值。
                                "requires_document_upload": True,
                                # 在字典中设置 `interview_plan_context` 字段为 `interview_exam_context`，用于表示当前业务步骤需要的历史、用户或 Skill 上下文。
                                "interview_plan_context": interview_exam_context,
                            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                            },
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                    # 使用关键字参数语法，把 `False` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
                    requires_confirmation=False,
                    # 使用关键字参数语法，把 `[field for field in ["title", "subject"] if not parsed_exam.get(field)]` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
                    missing_fields=[field for field in ["title", "subject"] if not parsed_exam.get(field)],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

            # 从用户输入中解析 title、subject、duration、total_score、question_counts 等
            parsed_exam = await self._parse_exam_requirements(message, conversation_id, memory_context)
            # 调用 `bool(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx", "txt", "md"}))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_exam_docs`；它表示`has_exam_docs` 在当前函数中承载的中间业务值。
            has_exam_docs = bool(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx", "txt", "md"}))
            # 有上传考试参考文档
            if has_exam_docs:
                # 调用 `len(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx", "txt", "md"}))` 完成当前解析、查询或对象构造，并把返回值保存到 `document_count`；它表示当前业务对象、题目或候选记录的数量。
                document_count = len(self._filter_attachments(normalized_attachments, {"pdf", "doc", "docx", "txt", "md"}))
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"我已经识别到 {document_count} 个参考文档附件。下一步先确认试卷标题、考察方向、题型、题量和分值，再调用文档出题工具生成试卷。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"我已经识别到 {document_count} 个参考文档附件。下一步先确认试卷标题、考察方向、题型、题量和分值，再调用文档出题工具生成试卷。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "已选择基于文档生成试卷工具，并检查到参考文档。"),
                        # 计算表达式 `"upload_docs", title="接收参考文档", status="completed", detail=f"已检测到 {document_count} 个参考文档"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="upload_docs", title="接收参考文档", status="completed", detail=f"已检测到 {document_count} 个参考文档"),
                        # 计算表达式 `"confirm_exam", title="确认考试配置", status="running", detail="请确认试卷标题、题型、题量和分值。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="confirm_exam", title="确认考试配置", status="running", detail="请确认试卷标题、题型、题量和分值。"),
                        # 计算表达式 `"generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),
                        # 计算表达式 `"save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"exam_generate_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="exam_generate_request",
                            # 使用关键字参数语法，把 `"确认考试配置"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="确认考试配置",
                            # 使用关键字参数语法，把 `{**parsed_exam, "document_count": document_count}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            content={**parsed_exam, "document_count": document_count},
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                    # 使用关键字参数语法，把 `[field for field in ["title", "subject"] if not parsed_exam.get(field)]` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
                    missing_fields=[field for field in ["title", "subject"] if not parsed_exam.get(field)],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

            # 有匹配的知识库文档
            matched_knowledge_files = await self._select_exam_knowledge_files(parsed_exam, message, user_id)
            # 使用 `if` 判断 `matched_knowledge_files`；条件成立时执行下面分支。
            if matched_knowledge_files:
                # 计算表达式 `"、".join(file.get("fileName") or "知识库文档" for file in matched_knowledge_files)` 并把结果保存到 `matched_names`；该结果在后续流程中用作`matched_names` 在当前函数中承载的中间业务值。
                matched_names = "、".join(file.get("fileName") or "知识库文档" for file in matched_knowledge_files)
                # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"我已经按你的描述提取了试卷配置，并在知识库中匹配到参考文档：{matched_names}。请确认配置后即可生成试卷。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"我已经按你的描述提取了试卷配置，并在知识库中匹配到参考文档：{matched_names}。请确认配置后即可生成试卷。",
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route_result.get("route"),
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "已选择基于文档生成试卷工具，并匹配到知识库参考文档。"),
                        # 计算表达式 `"select_docs", title="匹配参考文档", status="completed", detail=f"已匹配：{matched_names}"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="select_docs", title="匹配参考文档", status="completed", detail=f"已匹配：{matched_names}"),
                        # 计算表达式 `"confirm_exam", title="确认考试配置", status="running", detail="请确认试卷标题、题型、题量和分值。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="confirm_exam", title="确认考试配置", status="running", detail="请确认试卷标题、题型、题量和分值。"),
                        # 计算表达式 `"generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),
                        # 计算表达式 `"save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"exam_generate_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="exam_generate_request",
                            # 使用关键字参数语法，把 `"确认考试配置"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="确认考试配置",
                            # 使用关键字参数语法，把 `{**parsed_exam, "knowledge_files": matched_knowledge_files}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            content={**parsed_exam, "knowledge_files": matched_knowledge_files},
                        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=[],
                    # 使用关键字参数语法，把 `[field for field in ["title", "subject"] if not parsed_exam.get(field)]` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
                    missing_fields=[field for field in ["title", "subject"] if not parsed_exam.get(field)],
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

            # 没有文档，要上传文档
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我规划了一下：基于文档出题需要先拿到参考文档，但当前消息里还没有可用附件。请先在底部消息框点击“上传文件”添加附件，然后再发送生成试卷的需求。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我规划了一下：基于文档出题需要先拿到参考文档，但当前消息里还没有可用附件。请先在底部消息框点击“上传文件”添加附件，然后再发送生成试卷的需求。",
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "需要先满足前置条件：上传出题参考文档。"),
                    # 计算表达式 `"upload_docs", title="等待上传参考文档", status="pending", detail="请上传 PDF、DOC、DOCX、TXT 或 MD 文档。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="upload_docs", title="等待上传参考文档", status="pending", detail="请上传 PDF、DOC、DOCX、TXT 或 MD 文档。"),
                    # 计算表达式 `"confirm_exam", title="确认考试配置", status="pending", detail="上传文档后再配置试卷。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="confirm_exam", title="确认考试配置", status="pending", detail="上传文档后再配置试卷。"),
                    # 计算表达式 `"generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),
                    # 计算表达式 `"save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="save_exam", title="保存试卷", status="pending", detail="生成后保存到考试管理。"),
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"exam_document_upload_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="exam_document_upload_request",
                        # 使用关键字参数语法，把 `"上传出题参考文档"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="上传出题参考文档",
                        # 使用关键字参数语法，把 `{**parsed_exam, "requires_document_upload": True}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={**parsed_exam, "requires_document_upload": True},
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
                # 使用关键字参数语法，把 `False` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
                requires_confirmation=False,
                # 使用关键字参数语法，把 `[field for field in ["title", "subject"] if not parsed_exam.get(field)]` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
                missing_fields=[field for field in ["title", "subject"] if not parsed_exam.get(field)],
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `intent != "jd"`；条件成立时执行下面分支。
        if intent != "jd":
            # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `self._fallback_message(intent, message)` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=self._fallback_message(intent, message),
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `AgentStep()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    AgentStep(
                        # 使用关键字参数语法，把 `"plan"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                        id="plan",
                        # 使用关键字参数语法，把 `"理解需求并规划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="理解需求并规划",
                        # 使用关键字参数语法，把 `"completed"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                        status="completed",
                        # 使用关键字参数语法，把 `"当前没有匹配到可安全自动执行的招聘工具，我先给出说明或建议。"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                        detail="当前没有匹配到可安全自动执行的招聘工具，我先给出说明或建议。",
                    # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `self._suggestions_for_intent(intent)` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=self._suggestions_for_intent(intent),
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )


        # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        """
        JD 生成分支
            如果意图是 jd，chat() 会先解析招聘需求：
            - _parse_requirements()用 LLM 提取结构化字段。如果 LLM 失败，使用 _fallback_parse()  用正则兜底提取岗位、薪资、经验、学历、地点、技能。
            - _missing_required_fields()  检查必要字段：岗位名称、地点、薪资、经验、学历。

            如果缺字段，它不会直接生成，而是返回 requires_confirmation=True，让前端弹窗补充信息。
            用户确认后，_run_recruitment_agent() 会执行：
                1.生成 JD。
                2.保存 JD。
                3.基于 JD 生成评分标准。
                4.保存评分标准。
                5.返回两个 artifact：job_description 和 scoring_criteria。

            JD生成调用的是 Dify workflow type 1：_generate_jd() 。
            评分标准调用 Dify workflow type 2：_generate_scoring_criteria()

        """
        # 使用 `if` 判断 `not auto_execute`；条件成立时执行下面分支。
        if not auto_execute:
            # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我识别到这是 JD 生成任务。确认信息后我会调用 JD 生成工具并保存到 JD 管理。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我识别到这是 JD 生成任务。确认信息后我会调用 JD 生成工具并保存到 JD 管理。",
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "JD 生成需要先确认结构化招聘信息。"),
                    # 计算表达式 `"confirm", title="等待确认", status="pending", detail="确认后调用 JD 生成工具。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="confirm", title="等待确认", status="pending", detail="确认后调用 JD 生成工具。"),
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
                # 使用关键字参数语法，把 `True` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
                requires_confirmation=True,
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `confirmed_requirements`；条件成立时执行下面分支。
        if confirmed_requirements:
            # 调用 `self._normalize_requirements(confirmed_requirements, message)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed`；它表示从用户自然语言中解析出的结构化招聘参数。
            parsed = self._normalize_requirements(confirmed_requirements, message)
            # 从 `AgentService.chat()` 返回 `await self._run_recruitment_agent(`，并结束当前函数。
            return await self._run_recruitment_agent(
                # 把变量 `message` 作为位置参数传入当前多行函数调用；它承载用户本次输入的自然语言消息。
                message,
                # 把变量 `conversation_id` 作为位置参数传入当前多行函数调用；它承载当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id,
                # 调用 `route_result.get()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                route_result.get("route"),
                # 使用关键字参数语法，把 `parsed` 传入 `parsed_requirements` 参数；它表示从用户自然语言或模型输出中解析并校验后的结构化结果。
                parsed_requirements=parsed,
                # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=user_id,
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 用 LLM 从自然语言中提取岗位字段
        parsed = await self._parse_requirements(message, conversation_id, memory_context)
        #  检查必要字段：岗位名称、地点、薪资、经验、学历。
        missing_fields = self._missing_required_fields(parsed)
        # 如果缺字段，不会直接生成，而是返回 `requires_confirmation=True`，让前端弹窗补充信息。
        if missing_fields:
            # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我先补齐 JD 生成的必要信息，再生成会更稳。请在弹窗里确认或补充这些字段。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我先补齐 JD 生成的必要信息，再生成会更稳。请在弹窗里确认或补充这些字段。",
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route_result.get("route"),
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "已选择 JD 生成工具；先补齐必要字段再执行。"),
                    # 计算表达式 `"parse", title="解析招聘需求", status="completed", detail=self._brief_requirements(parsed), tool="parse...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="parse", title="解析招聘需求", status="completed", detail=self._brief_requirements(parsed), tool="parse_requirements"),
                    # 计算表达式 `"confirm", title="确认招聘信息", status="pending", detail=f"待补充：{'、'.join(self._field_label(field) for ...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="confirm", title="确认招聘信息", status="pending", detail=f"待补充：{'、'.join(self._field_label(field) for field in missing_fields)}"),
                    # 计算表达式 `"jd", title="生成岗位 JD", status="pending", tool="generate_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="jd", title="生成岗位 JD", status="pending", tool="generate_jd"),
                # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[AgentArtifact(type="requirements", title="待确认招聘需求", content=parsed)]` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=[AgentArtifact(type="requirements", title="待确认招聘需求", content=parsed)],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
                # 使用关键字参数语法，把 `True` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
                requires_confirmation=True,
                # 使用关键字参数语法，把 `missing_fields` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
                missing_fields=missing_fields,
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 从 `AgentService.chat()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `"我已提取到 JD 生成所需信息。请在弹窗中确认，确认后我再调用 JD 生成工具。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message="我已提取到 JD 生成所需信息。请在弹窗中确认，确认后我再调用 JD 生成工具。",
            # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent=intent,
            # 使用关键字参数语法，把 `route_result.get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route_result.get("route"),
            # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
            steps=[
                # 调用 `self._planning_step()`，执行 `AgentService.chat` 当前步骤所需的构造、查询或业务处理。
                self._planning_step(selected_tool, "已选择 JD 生成工具；等待用户确认后执行。"),
                # 计算表达式 `"parse", title="解析招聘需求", status="completed", detail=self._brief_requirements(parsed), tool="parse...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="parse", title="解析招聘需求", status="completed", detail=self._brief_requirements(parsed), tool="parse_requirements"),
                # 计算表达式 `"confirm", title="确认招聘信息", status="pending", detail="等待用户确认后执行生成。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="confirm", title="确认招聘信息", status="pending", detail="等待用户确认后执行生成。"),
                # 计算表达式 `"jd", title="生成岗位 JD", status="pending", tool="generate_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="jd", title="生成岗位 JD", status="pending", tool="generate_jd"),
            # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `[AgentArtifact(type="requirements", title="待确认招聘需求", content=parsed)]` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=[AgentArtifact(type="requirements", title="待确认招聘需求", content=parsed)],
            # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=[],
            # 使用关键字参数语法，把 `True` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
            requires_confirmation=True,
        # 结束 `AgentService.chat` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_chat_agent()`。Agent 流式主流程：先发送 thinking/plan，再按意图进入聊天或工具分支，持续产生 delta/artifact，最终发送 final。
    async def stream_chat_agent(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 声明参数 `auto_execute`，表示是否允许信息完整后自动执行低风险生成工具，类型标注为 `bool`，默认值为 `True`。
        auto_execute: bool = True,
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        confirmed_requirements: Optional[Dict[str, Any]] = None,
        # 声明参数 `attachments`，表示用户随消息上传的附件列表，类型标注为 `Optional[List[Dict[str, Any]]]`，默认值为 `None`。
        attachments: Optional[List[Dict[str, Any]]] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """流式处理普通 Agent 聊天：
            先推送思考状态，再返回最终规划/回复。
            """
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `thinking_response`；它表示下游服务返回、等待解析或转发的响应。
        thinking_response = AgentChatResponse(
            # 使用关键字参数语法，把 `"我正在理解你的需求。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message="我正在理解你的需求。",
            # 使用关键字参数语法，把 `"thinking"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="thinking",
            # 使用关键字参数语法，把 `None` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=None,
            # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
            steps=[
                # 调用 `AgentStep()`，执行 `AgentService.stream_chat_agent` 当前步骤所需的构造、查询或业务处理。
                AgentStep(
                    # 使用关键字参数语法，把 `"understand"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    id="understand",
                    # 使用关键字参数语法，把 `"理解用户需求"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title="理解用户需求",
                    # 使用关键字参数语法，把 `"running"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                    status="running",
                    # 使用关键字参数语法，把 `"正在判断是否需要调用 HR 工具。"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                    detail="正在判断是否需要调用 HR 工具。",
                # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 调用 `AgentStep()`，执行 `AgentService.stream_chat_agent` 当前步骤所需的构造、查询或业务处理。
                AgentStep(
                    # 使用关键字参数语法，把 `"plan"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    id="plan",
                    # 使用关键字参数语法，把 `"选择下一步动作"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title="选择下一步动作",
                    # 使用关键字参数语法，把 `"pending"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                    status="pending",
                    # 使用关键字参数语法，把 `"识别完成后选择 chat、ask_user 或 use_tool。"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                    detail="识别完成后选择 chat、ask_user 或 use_tool。",
                # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
            # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=[],
        # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "thinking", "response": thinking_response.model_dump()}

        # 附件
        normalized_attachments = self._normalize_attachments(attachments or [])
        # 读取对话记忆
        memory_context = await self._build_conversation_memory(conversation_id, user_id, message)

        # 把 `agent_plan` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入规则或 ReAct 产生的本轮执行计划。
        agent_plan = None
        # 是否有用户确认后的结构化招聘需求
        if not confirmed_requirements:
            # 规划：下一步动作，轻量意图分类：优先规则和附件上下文，不为路由单独调用大模型
            agent_plan = self._build_rule_agent_plan(message, normalized_attachments, memory_context)
            # 使用 `if` 判断 `not agent_plan`；条件成立时执行下面分支。
            if not agent_plan:
                # 规划：受控LLM ReAct决策
                agent_plan = await self._plan_agent_action(message, normalized_attachments, memory_context)

            # 常规聊天
            if agent_plan.get("mode") == "chat" or agent_plan.get("intent") == "general":
                # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `chat_response`；它表示下游服务返回、等待解析或转发的响应。
                chat_response = AgentChatResponse(
                    # 使用关键字参数语法，把 `""` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message="",
                    # 使用关键字参数语法，把 `"general"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent="general",
                    # 使用关键字参数语法，把 `None` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=None,
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `AgentStep()`，执行 `AgentService.stream_chat_agent` 当前步骤所需的构造、查询或业务处理。
                        AgentStep(
                            # 使用关键字参数语法，把 `"understand"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                            id="understand",
                            # 使用关键字参数语法，把 `"完成需求判断"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="完成需求判断",
                            # 使用关键字参数语法，把 `"completed"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                            status="completed",
                            # 使用关键字参数语法，把 `agent_plan.get("reason") or "判断为普通对话，不调用招聘工具。"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                            detail=agent_plan.get("reason") or "判断为普通对话，不调用招聘工具。",
                        # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["生成 JD", "评分简历", "基于文档生成试卷"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["生成 JD", "评分简历", "基于文档生成试卷"],
                # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "plan", "response": chat_response.model_dump()}

                # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
                full_text = ""
                # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                try:
                    # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
                    if self.llm_service is None:
                        # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
                        self.llm_service = LLMService()
                    # 调用 `self._build_memory_augmented_prompt(message, memory_context)` 完成当前解析、查询或对象构造，并把返回值保存到 `chat_prompt`；它表示准备发送给大模型的完整提示词。
                    chat_prompt = self._build_memory_augmented_prompt(message, memory_context)
                    # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                    async for delta in self.llm_service.stream_response(chat_prompt):
                        # 计算表达式 `delta` 并把结果保存到 `full_text +`；该结果在后续流程中用作`full_text +` 在当前函数中承载的中间业务值。
                        full_text += delta
                        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                        yield {"type": "delta", "delta": delta}
                # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                except Exception as exc:
                    # 通过模块日志记录器执行 `logger.warning("Agent 普通聊天原生流式失败，使用规划回复兜底: %s", exc)`，用于保留运行状态或异常原因。
                    logger.warning("Agent 普通聊天原生流式失败，使用规划回复兜底: %s", exc)
                    # 调用 `self._clean_optional_value(agent_plan.get("reply")) or self._fallback_message("general", message)` 完成当前解析、查询或对象构造，并把返回值保存到 `fallback_reply`；它表示`fallback_reply` 在当前函数中承载的中间业务值。
                    fallback_reply = self._clean_optional_value(agent_plan.get("reply")) or self._fallback_message("general", message)
                    # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                    async for delta in self._stream_text(fallback_reply):
                        # 计算表达式 `delta` 并把结果保存到 `full_text +`；该结果在后续流程中用作`full_text +` 在当前函数中承载的中间业务值。
                        full_text += delta
                        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                        yield {"type": "delta", "delta": delta}

                # 调用 `chat_response.model_copy(update={"message": full_text.strip() or self._fallback_message("general"...` 完成当前解析、查询或对象构造，并把返回值保存到 `final_response`；它表示下游服务返回、等待解析或转发的响应。
                final_response = chat_response.model_copy(update={"message": full_text.strip() or self._fallback_message("general", message)})
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "final", "response": final_response.model_dump()}
                # 从 `AgentService.stream_chat_agent()` 返回 `None`，并结束当前函数。
                return

            # jd编辑
            if agent_plan.get("intent") == "jd_edit":
                # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                async for event in self._stream_jd_edit_response(
                    # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=message,
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                    conversation_id=conversation_id,
                    # 使用关键字参数语法，把 `memory_context` 传入 `memory_context` 参数；它表示从数据库历史消息整理出的短期会话记忆。
                    memory_context=memory_context,
                # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
                ):
                    # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                    yield event
                # 从 `AgentService.stream_chat_agent()` 返回 `None`，并结束当前函数。
                return
            # 评分标准编辑
            if agent_plan.get("intent") == "criteria_edit":
                # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                async for event in self._stream_criteria_edit_response(
                    # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=message,
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                    conversation_id=conversation_id,
                    # 使用关键字参数语法，把 `memory_context` 传入 `memory_context` 参数；它表示从数据库历史消息整理出的短期会话记忆。
                    memory_context=memory_context,
                # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
                ):
                    # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                    yield event
                # 从 `AgentService.stream_chat_agent()` 返回 `None`，并结束当前函数。
                return
            # skill
            if agent_plan.get("intent") in self.skill_dispatcher.bundles:
                # 发送邮件
                if agent_plan.get("intent") == "email_notification":
                    # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                    async for event in self._stream_skill_draft_response(
                        # 使用关键字参数语法，把 `"email_notification"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                        intent="email_notification",
                        # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                        message=message,
                        # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                        user_id=user_id,
                        # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                        conversation_id=conversation_id,
                        # 使用关键字参数语法，把 `self._route_for_intent("email_notification", message).get("route")` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                        route=self._route_for_intent("email_notification", message).get("route"),
                        # 使用关键字参数语法，把 `memory_context` 传入 `memory_context` 参数；它表示从数据库历史消息整理出的短期会话记忆。
                        memory_context=memory_context,
                        # 使用关键字参数语法，把 `confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
                        confirmed_requirements=confirmed_requirements,
                    # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
                    ):
                        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                        yield event
                    # 从 `AgentService.stream_chat_agent()` 返回 `None`，并结束当前函数。
                    return

                # 使用 `await` 异步调用 `self.chat(`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
                response = await self.chat(
                    # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=message,
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                    conversation_id=conversation_id,
                    # 使用关键字参数语法，把 `auto_execute` 传入 `auto_execute` 参数；它表示是否允许信息完整后自动执行低风险生成工具。
                    auto_execute=auto_execute,
                    # 使用关键字参数语法，把 `confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
                    confirmed_requirements=confirmed_requirements,
                    # 使用关键字参数语法，把 `attachments` 传入 `attachments` 参数；它表示用户随消息上传的附件列表。
                    attachments=attachments,
                    # 使用关键字参数语法，把 `agent_plan` 传入 `agent_plan` 参数；它表示规则或 ReAct 产生的本轮执行计划。
                    agent_plan=agent_plan,
                # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {
                    # 在字典中设置 `type` 字段为 `"plan"`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    "type": "plan",
                    # 计算表达式 `{"message": ""}).model_dump(),` 并把结果保存到 `"response": response.model_copy(update`；该结果在后续流程中用作`model_copy(update` 在当前函数中承载的中间业务值。
                    "response": response.model_copy(update={"message": ""}).model_dump(),
                # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 异步消费下游流式事件，收到一段就立即继续向前端转发。
                async for delta in self._stream_text(response.message):
                    # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                    yield {"type": "delta", "delta": delta}
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "final", "response": response.model_dump()}
                # 从 `AgentService.stream_chat_agent()` 返回 `None`，并结束当前函数。
                return

        # 根据 intent 构造响应
        response = await self.chat(
            # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=message,
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=conversation_id,
            # 使用关键字参数语法，把 `auto_execute` 传入 `auto_execute` 参数；它表示是否允许信息完整后自动执行低风险生成工具。
            auto_execute=auto_execute,
            # 使用关键字参数语法，把 `confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
            confirmed_requirements=confirmed_requirements,
            # 使用关键字参数语法，把 `attachments` 传入 `attachments` 参数；它表示用户随消息上传的附件列表。
            attachments=attachments,
            # 使用关键字参数语法，把 `agent_plan` 传入 `agent_plan` 参数；它表示规则或 ReAct 产生的本轮执行计划。
            agent_plan=agent_plan,
        # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 返回执行计划 steps 和 artifact阶段性产物
        yield {
            # 在字典中设置 `type` 字段为 `"plan"`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": "plan",
            # 计算表达式 `{"message": ""}).model_dump(),` 并把结果保存到 `"response": response.model_copy(update`；该结果在后续流程中用作`model_copy(update` 在当前函数中承载的中间业务值。
            "response": response.model_copy(update={"message": ""}).model_dump(),
        # 结束 `AgentService.stream_chat_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for delta in self._stream_text(response.message):
            # 流式文本片段
            yield {"type": "delta", "delta": delta}
        # 最终完整结果
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_recruitment_agent()`。执行 JD 生成及评分标准生成链路，并把过程状态和两个业务产物流式返回。
    async def stream_recruitment_agent(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Dict[str, Any]`。
        confirmed_requirements: Dict[str, Any],
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        流式执行确认后的招聘 Agent任务，向前端推送真实阶段进度

        流式JD生成
        stream_recruitment_agent() 是确认JD需求后的流式版本。它会按阶段返回：
            - progress：状态进度。
            - delta：JD或评分标准正文增量。
            - artifact：阶段产物。
            - final：最终完整响应。

        先流式生成JD，再保存JD，再流式生成评分标准，再保存评分标准。前端可以边收边显示内容。
        """

        # 调用 `self._normalize_requirements(confirmed_requirements, message)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed`；它表示从用户自然语言中解析出的结构化招聘参数。
        parsed = self._normalize_requirements(confirmed_requirements, message)
        # 计算表达式 `"/recruitment/jd-generator"` 并把结果保存到 `route`；该结果在后续流程中用作该意图对应的前端页面路径，供前端跳转或展示下一步入口。
        route = "/recruitment/jd-generator"
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`，默认值为 `[`。
        steps: List[AgentStep] = [
            # 调用 `self._planning_step()`，执行 `AgentService.stream_recruitment_agent` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(self.tool_registry.get("jd"), "招聘信息已确认，按 JD → 评分标准的工具链执行。"),
            # 计算表达式 `"parse", title="确认招聘需求", status="completed", detail=self._brief_requirements(parsed)),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="parse", title="确认招聘需求", status="completed", detail=self._brief_requirements(parsed)),
            # 计算表达式 `"jd", title="生成岗位 JD", status="pending", detail="等待调用大模型", tool="generate_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="jd", title="生成岗位 JD", status="pending", detail="等待调用大模型", tool="generate_jd"),
            # 计算表达式 `"save_jd", title="保存 JD", status="pending", detail="等待 JD 生成完成"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_jd", title="保存 JD", status="pending", detail="等待 JD 生成完成"),
            # 计算表达式 `"criteria", title="生成简历评分标准", status="pending", detail="等待 JD 保存完成", tool="generate_scoring_crite...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="criteria", title="生成简历评分标准", status="pending", detail="等待 JD 保存完成", tool="generate_scoring_criteria"),
            # 计算表达式 `"save_criteria", title="保存评分标准", status="pending", detail="等待评分标准生成完成"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_criteria", title="保存评分标准", status="pending", detail="等待评分标准生成完成"),
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`，默认值为 `[`。
        artifacts: List[AgentArtifact] = [
            # 计算表达式 `"requirements", title="结构化招聘需求", content=parsed)` 并把结果保存到 `AgentArtifact(type`；该结果在后续流程中用作`AgentArtifact(type` 在当前函数中承载的中间业务值。
            AgentArtifact(type="requirements", title="结构化招聘需求", content=parsed)
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

        # 先显示: 已确认招聘需求，准备生成JD
        yield self._stream_event("progress", "已确认招聘需求，准备生成 JD。", steps, artifacts)

        # 生成岗位 JD
        steps[2] = steps[2].model_copy(update={"status": "running", "detail": "正在调用大模型生成 JD..."})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("progress", "正在生成岗位 JD。", steps, artifacts)
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "delta", "delta": "## 岗位 JD\n\n", "section": "jd"}

        # 把 `jd_content` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入提供给评分标准或面试方案生成流程的完整 JD 正文。
        jd_content = ""
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for delta in self._generate_jd_stream(message, parsed, conversation_id):
            # 计算表达式 `delta` 并把结果保存到 `jd_content +`；该结果在后续流程中用作`jd_content +` 在当前函数中承载的中间业务值。
            jd_content += delta
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "delta", "delta": delta, "section": "jd"}
        # 调用 `jd_content.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `jd_content`；它表示提供给评分标准或面试方案生成流程的完整 JD 正文。
        jd_content = jd_content.strip()
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": "JD 已生成。"})
        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"job_description"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="job_description",
            # 使用关键字参数语法，把 `f"{parsed.get('job_title') or '岗位'} JD"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=f"{parsed.get('job_title') or '岗位'} JD",
            # 使用关键字参数语法，把 `jd_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=jd_content,
            # 使用关键字参数语法，把 `{"job_title": parsed.get("job_title"), "route": route}` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
            metadata={"job_title": parsed.get("job_title"), "route": route},
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("artifact", "JD 已生成，正在保存到 JD 管理。", steps, artifacts)

        # 保存 JD
        steps[3] = steps[3].model_copy(update={"status": "running", "detail": "正在保存 JD 记录..."})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("progress", "正在保存 JD。", steps, artifacts)
        # 使用 `await` 异步调用 `self._save_job_description(jd_content, parsed, message, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `saved_jd`；它表示已经持久化并带有远程资源 ID 的 JD 对象。
        saved_jd = await self._save_job_description(jd_content, parsed, message, user_id, conversation_id)
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
        steps[3] = steps[3].model_copy(update={"status": "completed", "detail": f"已保存到 JD 列表，ID：{saved_jd.id}"})
        # 调用 `str(saved_jd.id)` 完成当前解析、查询或对象构造，并把返回值保存到 `artifacts[-1].metadata["saved_jd_id"]`；它表示`metadata` 在当前函数中承载的中间业务值。
        artifacts[-1].metadata["saved_jd_id"] = str(saved_jd.id)

        # 生成简历评分标准
        steps[4] = steps[4].model_copy(update={"status": "running", "detail": "正在基于 JD 生成简历评分标准..."})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("progress", "JD 已保存，正在生成简历评分标准。", steps, artifacts)
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "delta", "delta": "\n\n## 简历评分标准\n\n", "section": "scoring_criteria"}
        # 把 `criteria_content` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入根据 JD 生成的完整简历评分标准正文。
        criteria_content = ""
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for delta in self._generate_scoring_criteria_stream(jd_content, parsed, conversation_id):
            # 计算表达式 `delta` 并把结果保存到 `criteria_content +`；该结果在后续流程中用作`criteria_content +` 在当前函数中承载的中间业务值。
            criteria_content += delta
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "delta", "delta": delta, "section": "scoring_criteria"}
        # 调用 `criteria_content.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `criteria_content`；它表示根据 JD 生成的完整简历评分标准正文。
        criteria_content = criteria_content.strip()
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
        steps[4] = steps[4].model_copy(update={"status": "completed", "detail": "评分标准已生成。"})
        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"scoring_criteria"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="scoring_criteria",
            # 使用关键字参数语法，把 `f"{parsed.get('job_title') or '岗位'}评分标准"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=f"{parsed.get('job_title') or '岗位'}评分标准",
            # 使用关键字参数语法，把 `criteria_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=criteria_content,
            # 使用关键字参数语法，把 `{"job_title": parsed.get("job_title"), "saved_jd_id": str(saved_jd.id)}` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
            metadata={"job_title": parsed.get("job_title"), "saved_jd_id": str(saved_jd.id)},
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("artifact", "评分标准已生成，正在保存。", steps, artifacts)

        # 保存评分标准
        steps[5] = steps[5].model_copy(update={"status": "running", "detail": "正在保存评分标准..."})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._stream_event("progress", "正在保存评分标准。", steps, artifacts)
        # 使用 `await` 异步调用 `self._save_scoring_criteria(`，等待数据库、Dify 或模型处理完成后把结果保存到 `saved_criteria`；它表示已经保存到业务存储并带有资源 ID 的简历评分标准。
        saved_criteria = await self._save_scoring_criteria(
            # 把变量 `criteria_content` 作为位置参数传入当前多行函数调用；它承载根据 JD 生成的完整简历评分标准正文。
            criteria_content,
            # 把变量 `parsed` 作为位置参数传入当前多行函数调用；它承载从用户自然语言中解析出的结构化招聘参数。
            parsed,
            # 把变量 `user_id` 作为位置参数传入当前多行函数调用；它承载当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id,
            # 把变量 `conversation_id` 作为位置参数传入当前多行函数调用；它承载当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id,
            # 把对象字段 `saved_jd.id` 作为位置参数传入当前业务调用，用它关联刚保存的资源或下游记录。
            saved_jd.id,
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[5]`；业务上用于推进前端步骤的状态或说明。
        steps[5] = steps[5].model_copy(update={"status": "completed", "detail": f"已保存评分标准，ID：{saved_criteria.id}"})
        # 调用 `str(saved_criteria.id)` 完成当前解析、查询或对象构造，并把返回值保存到 `artifacts[-1].metadata["saved_criteria_id"]`；它表示`metadata` 在当前函数中承载的中间业务值。
        artifacts[-1].metadata["saved_criteria_id"] = str(saved_criteria.id)

        # 调用 `parsed.get("job_title") or "这个岗位"` 完成当前解析、查询或对象构造，并把返回值保存到 `job_title`；它表示`job_title` 在当前函数中承载的中间业务值。
        job_title = parsed.get("job_title") or "这个岗位"
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `final_response`；它表示下游服务返回、等待解析或转发的响应。
        final_response = AgentChatResponse(
            # 使用关键字参数语法，把 `f"我已经按「{job_title}」生成并保存了 JD，同时生成并保存了简历评分标准。需要筛选简历时，告诉我“筛选简历”，我会提醒你上传简历并调用评分工具。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"我已经按「{job_title}」生成并保存了 JD，同时生成并保存了简历评分标准。需要筛选简历时，告诉我“筛选简历”，我会提醒你上传简历并调用评分工具。",
            # 使用关键字参数语法，把 `"jd"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="jd",
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `["筛选这个 JD 的简历", "去 JD 管理查看职位", "基于 JD 生成面试题"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["筛选这个 JD 的简历", "去 JD 管理查看职位", "基于 JD 生成面试题"],
        # 结束 `AgentService.stream_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": final_response.model_dump()}

        # 返回给前端的内容格式：
        '''
        {
          "intent": "jd",
          "route": "/recruitment/jd-generator",
          "artifacts": [
            {
              "type": "job_description",
              "content": "JD正文",
              "metadata": {
                "saved_jd_id": "..."
              }
            },
            {
              "type": "scoring_criteria",
              "content": "评分标准正文",
              "metadata": {
                "saved_criteria_id": "...",
                "saved_jd_id": "..."
              }
            }
            ...
          ],
          "suggestions": [
            "筛选这个 JD 的简历",
            "去 JD 管理查看职位",
            "基于 JD 生成面试题"
          ]
        }
        '''

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_resume_screening()`。解析上传简历并调用 ResumeEvaluationService 逐份评分，返回进度和筛选结果。
    async def stream_resume_screening(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `job_description_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `UUID`。
        job_description_id: UUID,
        # 声明参数 `files`，表示`files` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`。
        files: List[Dict[str, Any]],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        批量筛选简历并推送真实进度
        简历筛选分支
        在 chat() 中，如果识别为 resume_screening：
        - 如果消息带了简历附件，它返回 resume_upload_request artifact，提示前端让用户选择 JD。
        - 如果没有附件，它返回上传简历的引导。
        真正执行批量评分的是 stream_resume_screening()。接收：
            user_id、job_description_id、files、conversation_id
        执行过程：
            1. 先返回“已收到 N 份简历”。
            2. 遍历每个文件。
            3. 调用 ResumeEvaluationService.evaluate_resume() 评分。
            4. 每评完一份，就返回一段 markdown 增量。
            5. 最后按分数排序。
            6. 生成招聘闭环建议，例如高分候选人进入面试，低分候选人可复核或删除。

        还会生成 resume_screening_followup artifact，用于后续支持“为高分候选人生成面试计划”“删除低分候选人”。
        """
        # 调用 `len(files)` 完成当前解析、查询或对象构造，并把返回值保存到 `total`；它表示`total` 在当前函数中承载的中间业务值。
        total = len(files)
        # 声明参数 `results`，表示`results` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`，默认值为 `[]`。
        results: List[Dict[str, Any]] = []
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 计算表达式 `"upload", title="接收简历文件", status="completed", detail=f"已接收 {total} 份简历"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="upload", title="接收简历文件", status="completed", detail=f"已接收 {total} 份简历"),
            # 计算表达式 `"evaluate", title="批量 AI 评分", status="running", detail="准备开始评分", tool="evaluate_resume"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="evaluate", title="批量 AI 评分", status="running", detail="准备开始评分", tool="evaluate_resume"),
            # 计算表达式 `"summary", title="生成筛选摘要", status="pending", detail="等待评分完成"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="summary", title="生成筛选摘要", status="pending", detail="等待评分完成"),
        # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._resume_stream_event(
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "progress",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"已收到 {total} 份简历，开始按当前 JD 批量评分。",
            # 把变量 `steps` 作为位置参数传入当前多行函数调用；它承载返回前端展示的 Agent 执行步骤。
            steps,
            # 把变量 `results` 作为位置参数传入当前多行函数调用；它承载`results` 在当前函数中承载的中间业务值。
            results,
        # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "delta", "delta": "## 简历评分结果\n\n", "section": "resume_screening"}

        # 逐份处理简历
        evaluation_service = ResumeEvaluationService(self.db)
        # 使用 `for` 循环 `index, file_info in enumerate(files, start=1)`，逐项处理集合数据。
        for index, file_info in enumerate(files, start=1):
            # 计算表达式 `file_info["filename"]` 并把结果保存到 `filename`；该结果在后续流程中用作`filename` 在当前函数中承载的中间业务值。
            filename = file_info["filename"]
            # 使用字典字面量语法开始构造 `AgentService.stream_resume_screening` 当前步骤所需的键值数据。
            steps[1] = steps[1].model_copy(update={
                # 在字典中设置 `status` 字段为 `"running"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                "status": "running",
                # 在字典中设置 `detail` 字段为 `f"正在评分第 {index}/{total} 份：{filename}"`，用于表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                "detail": f"正在评分第 {index}/{total} 份：{filename}",
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            })
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._resume_stream_event(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "progress",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"正在评分：{filename}",
                # 把变量 `steps` 作为位置参数传入当前多行函数调用；它承载返回前端展示的 Agent 执行步骤。
                steps,
                # 把变量 `results` 作为位置参数传入当前多行函数调用；它承载`results` 在当前函数中承载的中间业务值。
                results,
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 简历评估
                result = await evaluation_service.evaluate_resume(
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `file_info["content"]` 传入 `file_content` 参数；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
                    file_content=file_info["content"],
                    # 使用关键字参数语法，把 `filename` 传入 `filename` 参数；它表示`filename` 在当前函数中承载的中间业务值。
                    filename=filename,
                    # 使用关键字参数语法，把 `job_description_id` 传入 `job_description_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
                    job_description_id=job_description_id,
                    # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                    conversation_id=conversation_id,
                # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
                # 使用字典字面量开始构造 `result_data`；该字典用于保存`result_data` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
                result_data = {
                    # 在字典中设置 `id` 字段为 `str(result.get("id"))`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(result.get("id")),
                    # 在字典中设置 `filename` 字段为 `filename`，用于表示`filename` 在当前函数中承载的中间业务值。
                    "filename": filename,
                    # 在字典中设置 `name` 字段为 `result.get("name") or filename`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": result.get("name") or filename,
                    # 在字典中设置 `score` 字段为 `result.get("total_score") or 0`，用于表示`score` 在当前函数中承载的中间业务值。
                    "score": result.get("total_score") or 0,
                    # 在字典中设置 `position` 字段为 `result.get("position")`，用于表示`position` 在当前函数中承载的中间业务值。
                    "position": result.get("position"),
                    # 在字典中设置 `education` 字段为 `result.get("education")`，用于表示`education` 在当前函数中承载的中间业务值。
                    "education": result.get("education"),
                    # 在字典中设置 `work_years` 字段为 `result.get("workYears")`，用于表示`work_years` 在当前函数中承载的中间业务值。
                    "work_years": result.get("workYears"),
                    # 在字典中设置 `metrics` 字段为 `result.get("evaluation_metrics") or []`，用于表示`metrics` 在当前函数中承载的中间业务值。
                    "metrics": result.get("evaluation_metrics") or [],
                    # 在字典中设置 `status` 字段为 `"completed"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                    "status": "completed",
                # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception as exc:
                # 通过模块日志记录器执行 `logger.exception("Agent 批量筛选简历失败: %s", filename)`，用于保留运行状态或异常原因。
                logger.exception("Agent 批量筛选简历失败: %s", filename)
                # 使用字典字面量开始构造 `result_data`；该字典用于保存`result_data` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
                result_data = {
                    # 在字典中设置 `filename` 字段为 `filename`，用于表示`filename` 在当前函数中承载的中间业务值。
                    "filename": filename,
                    # 在字典中设置 `name` 字段为 `filename`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": filename,
                    # 在字典中设置 `score` 字段为 `0`，用于表示`score` 在当前函数中承载的中间业务值。
                    "score": 0,
                    # 在字典中设置 `status` 字段为 `"failed"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                    "status": "failed",
                    # 在字典中设置 `error` 字段为 `str(exc)`，用于表示`error` 在当前函数中承载的中间业务值。
                    "error": str(exc),
                # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }

            # 调用列表的 `append()`，把 `results.append(result_data)` 对应的新元素追加到现有结果末尾。
            results.append(result_data)
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {
                # 在字典中设置 `type` 字段为 `"delta"`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                "type": "delta",
                # 在字典中设置 `delta` 字段为 `self._format_resume_result_delta(result_data, index, total)`，用于表示`delta` 在当前函数中承载的中间业务值。
                "delta": self._format_resume_result_delta(result_data, index, total),
                # 在字典中设置 `section` 字段为 `"resume_screening"`，用于表示`section` 在当前函数中承载的中间业务值。
                "section": "resume_screening",
                # 在字典中设置 `result` 字段为 `result_data`，用于表示当前查询、解析或业务调用产生的结果。
                "result": result_data,
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._resume_stream_event(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "artifact",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"已完成 {index}/{total}：{filename}",
                # 把变量 `steps` 作为位置参数传入当前多行函数调用；它承载返回前端展示的 Agent 执行步骤。
                steps,
                # 把变量 `results` 作为位置参数传入当前多行函数调用；它承载`results` 在当前函数中承载的中间业务值。
                results,
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用关键字参数语法把 `len([item for item in results if item.get("status") == "failed"])` 传给 `failed_count`；该参数表示当前业务对象、题目或候选记录的数量。
        failed_count = len([item for item in results if item.get("status") == "failed"])
        # 计算表达式 `total - failed_count` 并把结果保存到 `success_count`；该结果在后续流程中用作当前业务对象、题目或候选记录的数量。
        success_count = total - failed_count
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": f"完成 {success_count} 份，失败 {failed_count} 份"})
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": "已生成批量筛选结果"})

        # 调用 `sorted(results, key=lambda item: item.get("score") or 0, reverse=True)` 完成当前解析、查询或对象构造，并把返回值保存到 `sorted_results`；它表示当前查询或批处理得到的结果集合。
        sorted_results = sorted(results, key=lambda item: item.get("score") or 0, reverse=True)
        # 调用 `self._build_resume_screening_loop_summary(sorted_results, threshold=60)` 完成当前解析、查询或对象构造，并把返回值保存到 `loop_summary`；它表示`loop_summary` 在当前函数中承载的中间业务值。
        loop_summary = self._build_resume_screening_loop_summary(sorted_results, threshold=60)
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `final_response`；它表示下游服务返回、等待解析或转发的响应。
        final_response = AgentChatResponse(
            # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
            message=(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"批量简历筛选完成：成功 {success_count} 份，失败 {failed_count} 份。结果已保存到简历筛选列表。"
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"{loop_summary['message_suffix']}"
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 使用关键字参数语法，把 `"resume_screening"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="resume_screening",
            # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/resume-screening",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
            artifacts=[
                # 调用 `AgentArtifact()`，执行 `AgentService.stream_resume_screening` 当前步骤所需的构造、查询或业务处理。
                AgentArtifact(
                    # 使用关键字参数语法，把 `"resume_screening_results"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    type="resume_screening_results",
                    # 使用关键字参数语法，把 `"批量简历筛选结果"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title="批量简历筛选结果",
                    # 使用关键字参数语法，把 `sorted_results` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                    content=sorted_results,
                    # 使用字典字面量开始构造 `metadata`；该字典用于保存产物附加信息，例如数据库 ID、候选人和保存状态，下面各行会继续写入具体业务字段。
                    metadata={
                        # 在字典中设置 `job_description_id` 字段为 `str(job_description_id)`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                        "job_description_id": str(job_description_id),
                        # 在字典中设置 `total` 字段为 `total`，用于表示`total` 在当前函数中承载的中间业务值。
                        "total": total,
                        # 在字典中设置 `qualified_threshold` 字段为 `60`，用于表示`qualified_threshold` 在当前函数中承载的中间业务值。
                        "qualified_threshold": 60,
                    # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    },
                # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 调用 `AgentArtifact()`，执行 `AgentService.stream_resume_screening` 当前步骤所需的构造、查询或业务处理。
                AgentArtifact(
                    # 使用关键字参数语法，把 `"resume_screening_followup"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    type="resume_screening_followup",
                    # 使用关键字参数语法，把 `"招聘闭环建议"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title="招聘闭环建议",
                    # 使用关键字参数语法，把 `loop_summary` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                    content=loop_summary,
                    # 使用关键字参数语法，把 `{"threshold": 60}` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
                    metadata={"threshold": 60},
                # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `loop_summary["suggestions"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=loop_summary["suggestions"],
        # 结束 `AgentService.stream_resume_screening` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": final_response.model_dump()}

        # 返回内容格式：
        '''
        {
          "intent": "resume_screening",
          "route": "/recruitment/resume-screening",
          "artifacts": [
            {
              "type": "resume_screening_results",
              "content": [
                {
                  "id": "简历评分ID",
                  "filename": "xxx.pdf",
                  "name": "张三",
                  "score": 85,
                  "status": "completed"
                }
              ]
            },
            {
              "type": "resume_screening_followup",
              "title": "招聘闭环建议",
              "content": {
                "high_score_candidates": "...",
                "low_score_candidates": "..."
              }
            }
          ],
          "suggestions": [
            "生成面试计划",
            "删除低分候选人",
            "查看简历筛选列表"
          ]
        }
        '''

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_interview_plan()`。读取候选人的简历评价及关联 JD，调用 Dify 生成面试方案并通过 InterviewPlanService 保存。
    async def stream_interview_plan(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `resume_evaluation_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `UUID`。
        resume_evaluation_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        根据已评分简历生成并保存面试计划
        真正生成面试方案的是 stream_interview_plan()：
            1.根据 resume_evaluation_id 读取评分记录。
            2.根据评分记录里的 job_description_id 读取JD。
            3.调用 Dify workflow type 4 生成面试计划。
            4.保存到 InterviewPlanService.create_interview_plan()。
            5.把候选人状态改成 ResumeStatus.INTERVIEW。
            6.返回面试方案 artifact 和后续建议。
        """
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 计算表达式 `"load_resume", title="读取候选人资料", status="running", detail="正在读取简历评分记录"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="load_resume", title="读取候选人资料", status="running", detail="正在读取简历评分记录"),
            # 计算表达式 `"generate_plan", title="生成面试计划", status="pending", tool="generate_interview_plan"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="generate_plan", title="生成面试计划", status="pending", tool="generate_interview_plan"),
            # 计算表达式 `"save_plan", title="保存面试计划", status="pending"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_plan", title="保存面试计划", status="pending"),
        # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("progress", "我正在读取候选人的简历评分结果。", "interview_plan", "/recruitment/smart-interview", steps, [])

        # 使用 `await` 异步调用 `self._get_resume_evaluation(user_id, resume_evaluation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `resume`；它表示当前候选人的简历评分 ORM 记录。
        resume = await self._get_resume_evaluation(user_id, resume_evaluation_id)
        # 使用 `await` 异步调用 `JobDescriptionService(self.db).get_job_description(`，等待数据库、Dify 或模型处理完成后把结果保存到 `job_description`；它表示与候选人关联的岗位 JD。
        job_description = await JobDescriptionService(self.db).get_job_description(
            # 使用关键字参数语法，把 `str(resume.job_description_id)` 传入 `jd_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
            jd_id=str(resume.job_description_id),
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
        # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[0]`；业务上用于推进前端步骤的状态或说明。
        steps[0] = steps[0].model_copy(update={"status": "completed", "detail": f"候选人：{resume.candidate_name or resume.original_filename}"})
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "running", "detail": "正在结合 JD、简历和评分结果生成面试计划..."})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("progress", "资料已就绪，开始生成面试计划。", "interview_plan", "/recruitment/smart-interview", steps, [])

        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "delta", "delta": "## 面试计划\n\n", "section": "interview_plan"}
        # 把 `plan_content` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入Dify 流式生成并累计得到的完整面试方案。
        plan_content = ""
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for delta in self._generate_interview_plan_stream(resume, job_description.content, conversation_id):
            # 计算表达式 `delta` 并把结果保存到 `plan_content +`；该结果在后续流程中用作`plan_content +` 在当前函数中承载的中间业务值。
            plan_content += delta
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "delta", "delta": delta, "section": "interview_plan"}
        # 调用 `plan_content.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `plan_content`；它表示Dify 流式生成并累计得到的完整面试方案。
        plan_content = plan_content.strip()
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": "面试计划已生成。"})
        # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
        artifacts = [
            # 调用 `AgentArtifact()`，执行 `AgentService.stream_interview_plan` 当前步骤所需的构造、查询或业务处理。
            AgentArtifact(
                # 使用关键字参数语法，把 `"interview_plan"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                type="interview_plan",
                # 使用关键字参数语法，把 `f"{resume.candidate_name or '候选人'} 面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title=f"{resume.candidate_name or '候选人'} 面试计划",
                # 使用关键字参数语法，把 `plan_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content=plan_content,
                # 使用关键字参数语法，把 `{"resume_evaluation_id": str(resume.id)}` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
                metadata={"resume_evaluation_id": str(resume.id)},
            # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("artifact", "面试计划已生成，正在保存。", "interview_plan", "/recruitment/smart-interview", steps, artifacts)

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "running", "detail": "正在保存到面试计划列表..."})
        # 使用 `await` 异步调用 `InterviewPlanService(self.db).create_interview_plan(`，等待数据库、Dify 或模型处理完成后把结果保存到 `saved_plan`；它表示已成功持久化、可供后续流程通过 ID 继续引用的业务对象。
        saved_plan = await InterviewPlanService(self.db).create_interview_plan(
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 调用 `InterviewPlanCreate(` 完成当前解析、查询或对象构造，并把返回值保存到 `plan_data`；它表示`plan_data` 在当前函数中承载的中间业务值。
            plan_data=InterviewPlanCreate(
                # 使用关键字参数语法，把 `resume.id` 传入 `resume_evaluation_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
                resume_evaluation_id=resume.id,
                # 使用关键字参数语法，把 `resume.candidate_name or resume.original_filename` 传入 `candidate_name` 参数；它表示`candidate_name` 在当前函数中承载的中间业务值。
                candidate_name=resume.candidate_name or resume.original_filename,
                # 使用关键字参数语法，把 `resume.candidate_position or getattr(job_description, "title", "候选岗位")` 传入 `candidate_position` 参数；它表示`candidate_position` 在当前函数中承载的中间业务值。
                candidate_position=resume.candidate_position or getattr(job_description, "title", "候选岗位"),
                # 使用关键字参数语法，把 `plan_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content=plan_content,
            # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
        # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 计算表达式 `ResumeStatus.INTERVIEW` 并把结果保存到 `resume.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
        resume.status = ResumeStatus.INTERVIEW
        # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
        await self.db.commit()
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": f"已保存，ID：{saved_plan.id}；候选人状态已同步为面试"})
        # 调用 `str(saved_plan.id)` 完成当前解析、查询或对象构造，并把返回值保存到 `artifacts[0].metadata["saved_plan_id"]`；它表示`metadata` 在当前函数中承载的中间业务值。
        artifacts[0].metadata["saved_plan_id"] = str(saved_plan.id)
        # 计算表达式 `resume.candidate_name or resume.original_filename` 并把结果保存到 `artifacts[0].metadata["candidate_name"]`；该结果在后续流程中用作`metadata` 在当前函数中承载的中间业务值。
        artifacts[0].metadata["candidate_name"] = resume.candidate_name or resume.original_filename
        # 计算表达式 `resume.candidate_position or getattr(job_description, "title", "候选岗位")` 并把结果保存到 `artifacts[0].metadata["candidate_position"]`；该结果在后续流程中用作`metadata` 在当前函数中承载的中间业务值。
        artifacts[0].metadata["candidate_position"] = resume.candidate_position or getattr(job_description, "title", "候选岗位")
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = AgentChatResponse(
            # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
            message=(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"面试计划已生成并保存。建议围绕「{resume.candidate_name or '该候选人'}」的高匹配项做深挖，"
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "同时对评分中较弱的能力设计追问，避免只凭简历亮点做判断。候选人状态也已推进到“面试”，刷新面试计划页面后会出现在列表中。\n\n"
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "如果你想把招聘链路继续往前推，可以上传岗位资料、技术文档或题库材料，然后告诉我“基于当前面试方案生成笔试试卷”。"
            # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 使用关键字参数语法，把 `"interview_plan"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="interview_plan",
            # 使用关键字参数语法，把 `"/recruitment/smart-interview"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/smart-interview",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `["基于当前面试方案生成笔试试卷", "写面试邀约邮件", "查看面试计划列表"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["基于当前面试方案生成笔试试卷", "写面试邀约邮件", "查看面试计划列表"],
        # 结束 `AgentService.stream_interview_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_exam_generation()`。解析试卷配置、选择上下文资源、调用 ExamService 生成题目并返回试卷产物。
    async def stream_exam_generation(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `exam_requirements`，表示`exam_requirements` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Any]`。
        exam_requirements: Dict[str, Any],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        生成并保存考试试卷

        stream_exam_generation() 负责生成并保存试卷：
            1.标准化考试配置。
            2.如果用户说“基于当前面试方案出题”，会尝试关联最近面试方案。
            3.调用 ExamService.generate_exam() 流式生成。
            4.将内部题目格式转换成前端可读 markdown。
            5.调用 ExamService.save_exam() 保存。
            6.返回分享链接 /exam-share/{id}。

            stream_exam_generation_with_documents() 多做了一步：
                先保存并解析上传的参考文档，再把文档 ID 作为 knowledge_files 传给考试生成服务。
        """
        # 调用 `self._normalize_exam_requirements(exam_requirements)` 完成当前解析、查询或对象构造，并把返回值保存到 `exam`；它表示标准化后的试卷配置或试卷对象。
        exam = self._normalize_exam_requirements(exam_requirements)
        # 使用 `await` 异步调用 `self._hydrate_exam_interview_context(exam, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `exam`；它表示标准化后的试卷配置或试卷对象。
        exam = await self._hydrate_exam_interview_context(exam, user_id, conversation_id)
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 计算表达式 `"confirm_exam", title="确认考试配置", status="completed", detail=self._brief_exam_requirements(exam)),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="confirm_exam", title="确认考试配置", status="completed", detail=self._brief_exam_requirements(exam)),
            # 计算表达式 `"generate_exam", title="生成考试试卷", status="running", detail="正在调用考试生成工具...", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="generate_exam", title="生成考试试卷", status="running", detail="正在调用考试生成工具...", tool="generate_exam"),
            # 计算表达式 `"save_exam", title="保存试卷", status="pending", detail="等待生成完成"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_exam", title="保存试卷", status="pending", detail="等待生成完成"),
        # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("progress", "考试配置已确认，开始生成试卷。", "exam_generate", "/training/exam-generator", steps, [])

        # 调用 `ExamService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `exam_service`；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
        exam_service = ExamService(self.db)
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "delta", "delta": "## 考试试卷\n\n", "section": "exam_generate"}
        # 把 `raw_exam_content` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前业务资源的正文内容，供模型生成、保存或前端展示。
        raw_exam_content = ""
        # 把 `display_exam_content` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前业务资源的正文内容，供模型生成、保存或前端展示。
        display_exam_content = ""
        # 把 `pending_exam_buffer` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入`pending_exam_buffer` 在当前函数中承载的中间业务值。
        pending_exam_buffer = ""
        # 计算表达式 `1` 并把结果保存到 `question_index`；该结果在后续流程中用作当前元素在列表中的位置，用于更新同一条记录。
        question_index = 1
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for raw_delta in self._generate_exam_content_stream(exam_service, exam, user_id, conversation_id):
            # 计算表达式 `raw_delta` 并把结果保存到 `raw_exam_content +`；该结果在后续流程中用作`raw_exam_content +` 在当前函数中承载的中间业务值。
            raw_exam_content += raw_delta
            # 计算表达式 `raw_delta` 并把结果保存到 `pending_exam_buffer +`；该结果在后续流程中用作`pending_exam_buffer +` 在当前函数中承载的中间业务值。
            pending_exam_buffer += raw_delta
            # 调用 `self._drain_exam_display_buffer(` 完成当前解析、查询或对象构造，并把返回值保存到 `display_delta, pending_exam_buffer, question_index`；它表示当前元素在列表中的位置，用于更新同一条记录。
            display_delta, pending_exam_buffer, question_index = self._drain_exam_display_buffer(
                # 把变量 `pending_exam_buffer` 作为位置参数传入当前多行函数调用；它承载`pending_exam_buffer` 在当前函数中承载的中间业务值。
                pending_exam_buffer,
                # 把变量 `question_index` 作为位置参数传入当前多行函数调用；它承载当前元素在列表中的位置，用于更新同一条记录。
                question_index,
            # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 使用 `if` 判断 `display_delta`；条件成立时执行下面分支。
            if display_delta:
                # 计算表达式 `display_delta` 并把结果保存到 `display_exam_content +`；该结果在后续流程中用作`display_exam_content +` 在当前函数中承载的中间业务值。
                display_exam_content += display_delta
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "delta", "delta": display_delta, "section": "exam_generate"}

        # 调用 `self._drain_exam_display_buffer(` 完成当前解析、查询或对象构造，并把返回值保存到 `display_delta, pending_exam_buffer, question_index`；它表示当前元素在列表中的位置，用于更新同一条记录。
        display_delta, pending_exam_buffer, question_index = self._drain_exam_display_buffer(
            # 把变量 `pending_exam_buffer` 作为位置参数传入当前多行函数调用；它承载`pending_exam_buffer` 在当前函数中承载的中间业务值。
            pending_exam_buffer,
            # 把变量 `question_index` 作为位置参数传入当前多行函数调用；它承载当前元素在列表中的位置，用于更新同一条记录。
            question_index,
            # 使用关键字参数语法，把 `True` 传入 `force` 参数；它表示`force` 在当前函数中承载的中间业务值。
            force=True,
        # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `display_delta`；条件成立时执行下面分支。
        if display_delta:
            # 计算表达式 `display_delta` 并把结果保存到 `display_exam_content +`；该结果在后续流程中用作`display_exam_content +` 在当前函数中承载的中间业务值。
            display_exam_content += display_delta
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "delta", "delta": display_delta, "section": "exam_generate"}

        # 使用 `await` 异步调用 `self._generate_missing_exam_questions(`，等待数据库、Dify 或模型处理完成后把结果保存到 `repair_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
        repair_content = await self._generate_missing_exam_questions(
            # 使用关键字参数语法，把 `exam_service` 传入 `exam_service` 参数；它表示负责执行对应领域查询、生成、保存或外部调用的服务对象。
            exam_service=exam_service,
            # 使用关键字参数语法，把 `exam` 传入 `exam` 参数；它表示标准化后的试卷配置或试卷对象。
            exam=exam,
            # 使用关键字参数语法，把 `raw_exam_content` 传入 `existing_content` 参数；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            existing_content=raw_exam_content,
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=conversation_id,
        # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `repair_content`；条件成立时执行下面分支。
        if repair_content:
            # 使用 `if` 判断 `raw_exam_content.strip()`；条件成立时执行下面分支。
            if raw_exam_content.strip():
                # 计算表达式 `f"{raw_exam_content.rstrip()}\n***\n{repair_content.strip()}"` 并把结果保存到 `raw_exam_content`；该结果在后续流程中用作当前业务资源的正文内容，供模型生成、保存或前端展示。
                raw_exam_content = f"{raw_exam_content.rstrip()}\n***\n{repair_content.strip()}"
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 调用 `repair_content.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_exam_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
                raw_exam_content = repair_content.strip()
            # 调用 `self._drain_exam_display_buffer(` 完成当前解析、查询或对象构造，并把返回值保存到 `display_delta, _, question_index`；它表示当前元素在列表中的位置，用于更新同一条记录。
            display_delta, _, question_index = self._drain_exam_display_buffer(
                # 把变量 `repair_content` 作为位置参数传入当前多行函数调用；它承载当前业务资源的正文内容，供模型生成、保存或前端展示。
                repair_content,
                # 把变量 `question_index` 作为位置参数传入当前多行函数调用；它承载当前元素在列表中的位置，用于更新同一条记录。
                question_index,
                # 使用关键字参数语法，把 `True` 传入 `force` 参数；它表示`force` 在当前函数中承载的中间业务值。
                force=True,
            # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 使用 `if` 判断 `display_delta`；条件成立时执行下面分支。
            if display_delta:
                # 计算表达式 `display_delta` 并把结果保存到 `display_exam_content +`；该结果在后续流程中用作`display_exam_content +` 在当前函数中承载的中间业务值。
                display_exam_content += display_delta
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "delta", "delta": display_delta, "section": "exam_generate"}

        # 调用 `raw_exam_content.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `exam_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
        exam_content = raw_exam_content.strip()
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": "试卷已生成。"})
        # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
        artifacts = [
            # 调用 `AgentArtifact()`，执行 `AgentService.stream_exam_generation` 当前步骤所需的构造、查询或业务处理。
            AgentArtifact(
                # 使用关键字参数语法，把 `"exam_paper"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                type="exam_paper",
                # 使用关键字参数语法，把 `exam["title"]` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title=exam["title"],
                # 使用关键字参数语法，把 `display_exam_content.strip() or exam_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content=display_exam_content.strip() or exam_content,
                # 使用关键字参数语法，把 `{"subject": exam["subject"]}` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
                metadata={"subject": exam["subject"]},
            # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("artifact", "试卷已生成，正在保存到考试管理。", "exam_generate", "/training/exam-generator", steps, artifacts)

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "running", "detail": "正在保存试卷..."})
        # 使用 `await` 异步调用 `exam_service.save_exam({**exam, "content": exam_content}, str(user_id))`，等待数据库、Dify 或模型处理完成后把结果保存到 `saved_exam`；它表示已成功持久化、可供后续流程通过 ID 继续引用的业务对象。
        saved_exam = await exam_service.save_exam({**exam, "content": exam_content}, str(user_id))
        # 调用 `saved_exam.get("id")` 完成当前解析、查询或对象构造，并把返回值保存到 `saved_exam_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
        saved_exam_id = saved_exam.get("id")
        # 计算表达式 `f"/exam-share/{saved_exam_id}" if saved_exam_id else None` 并把结果保存到 `share_url`；该结果在后续流程中用作`share_url` 在当前函数中承载的中间业务值。
        share_url = f"/exam-share/{saved_exam_id}" if saved_exam_id else None
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": f"已保存，ID：{saved_exam.get('id')}"})
        # 计算表达式 `saved_exam_id` 并把结果保存到 `artifacts[0].metadata["saved_exam_id"]`；该结果在后续流程中用作`metadata` 在当前函数中承载的中间业务值。
        artifacts[0].metadata["saved_exam_id"] = saved_exam_id
        # 计算表达式 `share_url` 并把结果保存到 `artifacts[0].metadata["share_url"]`；该结果在后续流程中用作`metadata` 在当前函数中承载的中间业务值。
        artifacts[0].metadata["share_url"] = share_url
        # 计算表达式 `f"\n\n分享链接：[打开试卷分享页]({share_url})\n\n也可以复制：`{share_url}`" if share_url else ""` 并把结果保存到 `share_message`；该结果在后续流程中用作`share_message` 在当前函数中承载的中间业务值。
        share_message = f"\n\n分享链接：[打开试卷分享页]({share_url})\n\n也可以复制：`{share_url}`" if share_url else ""
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = AgentChatResponse(
            # 使用关键字参数语法，把 `f"试卷「{exam['title']}」已生成并保存。你可以把它作为候选人的笔试题，也可以继续让我生成考试邀请邮件。{share_message}"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"试卷「{exam['title']}」已生成并保存。你可以把它作为候选人的笔试题，也可以继续让我生成考试邀请邮件。{share_message}",
            # 使用关键字参数语法，把 `"exam_generate"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="exam_generate",
            # 使用关键字参数语法，把 `"/training/exam-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/training/exam-generator",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `["写考试邀请邮件", "去考试管理查看"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["写考试邀请邮件", "去考试管理查看"],
        # 结束 `AgentService.stream_exam_generation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stream_exam_generation_with_documents()`。将上传文件保存为文档后生成试卷，解决考试工具缺少知识来源的问题。
    async def stream_exam_generation_with_documents(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `exam_requirements`，表示`exam_requirements` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, Any]`。
        exam_requirements: Dict[str, Any],
        # 声明参数 `files`，表示`files` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`。
        files: List[Dict[str, Any]],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """上传并处理文档后，基于文档生成并保存考试试卷"""
        # 调用 `self._normalize_exam_requirements(exam_requirements)` 完成当前解析、查询或对象构造，并把返回值保存到 `exam`；它表示标准化后的试卷配置或试卷对象。
        exam = self._normalize_exam_requirements(exam_requirements)
        # 调用 `len(files)` 完成当前解析、查询或对象构造，并把返回值保存到 `total`；它表示`total` 在当前函数中承载的中间业务值。
        total = len(files)
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 计算表达式 `"upload_docs", title="接收参考文档", status="completed", detail=f"已接收 {total} 个文档"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="upload_docs", title="接收参考文档", status="completed", detail=f"已接收 {total} 个文档"),
            # 计算表达式 `"process_docs", title="解析文档内容", status="running", detail="正在提取文档文本"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="process_docs", title="解析文档内容", status="running", detail="正在提取文档文本"),
            # 计算表达式 `"generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="generate_exam", title="基于文档生成试卷", status="pending", tool="generate_exam"),
            # 计算表达式 `"save_exam", title="保存试卷", status="pending"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_exam", title="保存试卷", status="pending"),
        # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`，默认值为 `[]`。
        artifacts: List[AgentArtifact] = []
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("progress", f"已收到 {total} 个参考文档，开始解析文档内容。", "exam_generate", "/training/exam-generator", steps, artifacts)

        # 如果用户是从“当前面试方案”继续生成试卷，会读取最近面试方案上下文
        exam = await self._hydrate_exam_interview_context(exam, user_id, conversation_id)
        # 使用 `if` 判断 `exam.get("interview_plan_context")`；条件成立时执行下面分支。
        if exam.get("interview_plan_context"):
            # 使用字典字面量语法开始构造 `AgentService.stream_exam_generation_with_documents` 当前步骤所需的键值数据。
            steps[0] = steps[0].model_copy(update={
                # 在字典中设置 `detail` 字段为 `f"已接收 {total} 个文档，并关联当前面试方案"`，用于表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                "detail": f"已接收 {total} 个文档，并关联当前面试方案",
            # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            })
            # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
            artifacts.append(AgentArtifact(
                # 使用关键字参数语法，把 `"exam_interview_plan_context"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                type="exam_interview_plan_context",
                # 使用关键字参数语法，把 `"试卷参考面试方案"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title="试卷参考面试方案",
                # 使用字典字面量开始构造 `content`；该字典用于保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据，下面各行会继续写入具体业务字段。
                content={
                    # 在字典中设置 `title` 字段为 `exam["interview_plan_context"].get("title")`，用于表示前端向用户展示的步骤或业务产物标题。
                    "title": exam["interview_plan_context"].get("title"),
                    # 在字典中设置 `plan_id` 字段为 `exam["interview_plan_context"].get("plan_id")`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                    "plan_id": exam["interview_plan_context"].get("plan_id"),
                # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                },
            # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ))
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._tool_stream_event(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "artifact",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"已关联面试方案「{exam['interview_plan_context'].get('title')}」，出题会同时参考文档和面试方案。",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "exam_generate",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "/training/exam-generator",
                # 把变量 `steps` 作为位置参数传入当前多行函数调用；它承载返回前端展示的 Agent 执行步骤。
                steps,
                # 把变量 `artifacts` 作为位置参数传入当前多行函数调用；它承载本轮生成的 JD、评分、面试方案等业务产物。
                artifacts,
            # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 声明参数 `knowledge_files`，表示`knowledge_files` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`，默认值为 `[]`。
        knowledge_files: List[Dict[str, Any]] = []
        # 使用 `for` 循环 `index, file_info in enumerate(files, start=1)`，逐项处理集合数据。
        for index, file_info in enumerate(files, start=1):
            # 计算表达式 `file_info["filename"]` 并把结果保存到 `filename`；该结果在后续流程中用作`filename` 在当前函数中承载的中间业务值。
            filename = file_info["filename"]
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "running", "detail": f"正在解析第 {index}/{total} 个文档：{filename}"})
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._tool_stream_event("progress", f"正在解析文档：{filename}", "exam_generate", "/training/exam-generator", steps, artifacts)

            # 保存上传文档到 Document 表
            document = await self._save_exam_source_document(
                # 使用关键字参数语法，把 `filename` 传入 `filename` 参数；它表示`filename` 在当前函数中承载的中间业务值。
                filename=filename,
                # 使用关键字参数语法，把 `file_info["content"]` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content=file_info["content"],
                # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=user_id,
            # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 使用 `if` 判断 `not document.extracted_content`；条件成立时执行下面分支。
            if not document.extracted_content:
                # 如果 Document 没有 extracted_content，则提取文本
                extracted_content = await extract_text_content(document.file_path, document.mime_type)
                # 使用 `if` 判断 `not extracted_content`；条件成立时执行下面分支。
                if not extracted_content:
                    # 使用 `raise` 抛出 `ValueError(f"{filename} 未能解析出文本内容，请上传 PDF、DOCX、TXT 或 MD 格式的文本型文档")`，立即终止当前失败流程并交给上层处理。
                    raise ValueError(f"{filename} 未能解析出文本内容，请上传 PDF、DOCX、TXT 或 MD 格式的文本型文档")
                # 计算表达式 `extracted_content` 并把结果保存到 `document.extracted_content`；该结果在后续流程中用作当前业务资源的正文内容，供模型生成、保存或前端展示。
                document.extracted_content = extracted_content
                # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
                await self.db.commit()
                # 使用 `await` 等待异步操作 `self.db.refresh(document)` 完成，期间不会阻塞事件循环。
                await self.db.refresh(document)
            # 调用列表的 `append()`，把 `knowledge_files.append({"id": str(document.id), "fileName": document.filename})` 对应的新元素追加到现有结果末尾。
            knowledge_files.append({"id": str(document.id), "fileName": document.filename})

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": f"已完成 {len(knowledge_files)} 个文档解析"})
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "running", "detail": "正在基于文档内容生成试卷..."})
        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"exam_source_documents"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="exam_source_documents",
            # 使用关键字参数语法，把 `"试卷参考文档"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title="试卷参考文档",
            # 使用关键字参数语法，把 `knowledge_files` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=knowledge_files,
        # 结束 `AgentService.stream_exam_generation_with_documents` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._tool_stream_event("artifact", "文档解析完成，开始基于文档生成试卷。", "exam_generate", "/training/exam-generator", steps, artifacts)

        # 进入真正试卷生成流程
        # 异步消费下游流式事件，收到一段就立即继续向前端转发。
        async for event in self.stream_exam_generation(
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `{**exam, "knowledge_files": knowledge_files}` 传入 `exam_requirements` 参数；它表示`exam_requirements` 在当前函数中承载的中间业务值。
            exam_requirements={**exam, "knowledge_files": knowledge_files},
            # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=conversation_id,
        # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
        ):
            # 使用 `if` 判断 `event.get("response") and event["response"].get("steps")`；条件成立时执行下面分支。
            if event.get("response") and event["response"].get("steps"):
                # 计算表达式 `event["response"]` 并把结果保存到 `response`；该结果在后续流程中用作下游服务或 Agent 流程返回的响应。
                response = event["response"]
                # 计算表达式 `[step.model_dump() for step in steps[:2]] + response["steps"][1:]` 并把结果保存到 `response["steps"]`；该结果在后续流程中用作`response` 在当前函数中承载的中间业务值。
                response["steps"] = [step.model_dump() for step in steps[:2]] + response["steps"][1:]
                # 计算表达式 `[artifact.model_dump() for artifact in artifacts] + response.get("artifacts", [])` 并把结果保存到 `response["artifacts"]`；该结果在后续流程中用作`response` 在当前函数中承载的中间业务值。
                response["artifacts"] = [artifact.model_dump() for artifact in artifacts] + response.get("artifacts", [])
                # 使用 `if` 判断 `event.get("type") == "final"`；条件成立时执行下面分支。
                if event.get("type") == "final":
                    # 计算表达式 `f"已基于 {len(knowledge_files)} 个上传文档生成并保存试卷。{response['message']}"` 并把结果保存到 `response["message"]`；该结果在后续流程中用作`response` 在当前函数中承载的中间业务值。
                    response["message"] = f"已基于 {len(knowledge_files)} 个上传文档生成并保存试卷。{response['message']}"
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield event
