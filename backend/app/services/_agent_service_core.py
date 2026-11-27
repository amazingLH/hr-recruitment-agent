"""
* 文件作用
  * 这是 Agent 的决策与上下文核心 Mixin。它声明允许调用的工具，采用“规则优先、受控 ReAct 兜底”选择 chat、use_tool 或 ask_user，读取 conversations/messages 构造短期记忆，检查工具前置条件，并把 JD、简历、面试、试卷和 Skill 的执行过程编码为统一步骤、产物与 SSE 事件。本文件还处理候选人指代解析、考试生成流补题和招聘闭环的后续动作。
* 类说明
  * `_AgentServiceCoreMixin`：实现意图决策、工具规划、会话记忆、Skill 编排、SSE 事件和候选人上下文解析。
* 主要函数与方法说明
  * `_AgentServiceCoreMixin._build_tool_registry()`：声明 Agent 允许使用的工具及前置条件，并将扫描到的 Skill 一并注册；模型不能调用表外函数。
  * `_AgentServiceCoreMixin._classify_agent_intent()`：使用删除/编辑关键词、附件类型、上下文规则和 IntentService 快速判断意图，无法匹配时返回 general。
  * `_AgentServiceCoreMixin._plan_agent_action()`：构造受限工具列表与会话上下文调用 LLM 做 ReAct 决策；解析失败时使用本地规则结果兜底。
  * `_AgentServiceCoreMixin._run_recruitment_agent()`：根据计划分发普通聊天、JD、编辑、简历、面试、考试、删除或 Skill，并统一组装 AgentChatResponse。
  * `_AgentServiceCoreMixin._build_conversation_memory()`：按 conversation_id 与 user_id 读取最近消息，额外多取记录后过滤当前请求，限制单条与总字符数并拼成短期上下文。
  * `_AgentServiceCoreMixin._resolve_interview_candidate()`：结合用户文字、短期记忆和最近简历评分记录定位候选人；多匹配时返回选择要求。
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
    核心编排、流式处理等私有方法.
"""

# 使用 `class` 定义 `_AgentServiceCoreMixin`；该类的业务作用是：实现意图决策、工具规划、会话记忆、Skill 编排、SSE 事件和候选人上下文解析。
class _AgentServiceCoreMixin:
    # 使用 `def` 普通函数 定义 `_build_tool_registry()`。声明 Agent 允许使用的工具及前置条件，并将扫描到的 Skill 一并注册；模型不能调用表外函数。
    def _build_tool_registry(self) -> Dict[str, AgentToolSpec]:
        """声明 Agent 能自主规划的工具和前置条件
        工具注册
            jd：生成岗位 JD，并自动生成评分标准。
            resume_screening：基于 JD 批量评分简历。
            interview_plan：基于已评分简历和 JD 生成面试计划。
            exam_generate：基于上传文档生成试卷。
            resource_delete：删除 JD、简历记录、面试方案、试卷。

            之后它还会扫描 backend/skills 下的 Skill，把 Skill 也注册为 Agent 工具。
                目前项目里有邮件 Skill，对应 email_notification 意图。
        """
        # 使用列表字面量开始构造 `tools`；该列表用于保存`tools` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        tools = [
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"generate_jd"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="generate_jd",
                # 使用关键字参数语法，把 `"jd"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `"生成岗位 JD，并自动生成简历评分标准"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="生成岗位 JD，并自动生成简历评分标准",
                # 使用关键字参数语法，把 `["确认岗位名称、地点、薪资、经验、学历"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["确认岗位名称、地点、薪资、经验、学历"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"edit_jd"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="edit_jd",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `"修改最近生成或保存的岗位 JD"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="修改最近生成或保存的岗位 JD",
                # 使用关键字参数语法，把 `["定位要修改的 JD", "明确修改要求"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["定位要修改的 JD", "明确修改要求"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"edit_scoring_criteria"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="edit_scoring_criteria",
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `"修改最近生成或保存的简历评分标准"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="修改最近生成或保存的简历评分标准",
                # 使用关键字参数语法，把 `["定位要修改的评分标准", "明确修改要求"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["定位要修改的评分标准", "明确修改要求"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"evaluate_resume"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="evaluate_resume",
                # 使用关键字参数语法，把 `"resume_screening"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resume_screening",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `"基于 JD 批量评分简历"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="基于 JD 批量评分简历",
                # 使用关键字参数语法，把 `["上传 PDF/DOC/DOCX 简历", "选择用于匹配的 JD"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["上传 PDF/DOC/DOCX 简历", "选择用于匹配的 JD"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"generate_interview_plan"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="generate_interview_plan",
                # 使用关键字参数语法，把 `"interview_plan"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="interview_plan",
                # 使用关键字参数语法，把 `"/recruitment/smart-interview"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/smart-interview",
                # 使用关键字参数语法，把 `"基于已评分简历和 JD 生成面试计划"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="基于已评分简历和 JD 生成面试计划",
                # 使用关键字参数语法，把 `["选择一位已评分候选人"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["选择一位已评分候选人"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"generate_exam"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="generate_exam",
                # 使用关键字参数语法，把 `"exam_generate"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="exam_generate",
                # 使用关键字参数语法，把 `"/training/exam-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/training/exam-generator",
                # 使用关键字参数语法，把 `"基于上传文档生成考试试卷"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="基于上传文档生成考试试卷",
                # 使用关键字参数语法，把 `["上传参考文档", "确认试卷配置"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["上传参考文档", "确认试卷配置"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 调用 `AgentToolSpec()`，执行 `_AgentServiceCoreMixin._build_tool_registry` 当前步骤所需的构造、查询或业务处理。
            AgentToolSpec(
                # 使用关键字参数语法，把 `"delete_resource"` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name="delete_resource",
                # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resource_delete",
                # 使用关键字参数语法，把 `None` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=None,
                # 使用关键字参数语法，把 `"按用户描述删除已生成的 JD、简历评分记录、面试方案或试卷"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description="按用户描述删除已生成的 JD、简历评分记录、面试方案或试卷",
                # 使用关键字参数语法，把 `["明确要删除的资源类型和名称/候选人/标题"]` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=["明确要删除的资源类型和名称/候选人/标题"],
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
        # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 计算表达式 `{tool.intent: tool for tool in tools}` 并把结果保存到 `registry`；该结果在后续流程中用作`registry` 在当前函数中承载的中间业务值。
        registry = {tool.intent: tool for tool in tools}

        # 使用 `for` 循环 `bundle in self.skill_dispatcher.bundles.values()`，逐项处理集合数据。
        for bundle in self.skill_dispatcher.bundles.values():
            # 调用 `AgentToolSpec(` 完成当前解析、查询或对象构造，并把返回值保存到 `registry[bundle.intent]`；它表示`intent]` 在当前函数中承载的中间业务值。
            registry[bundle.intent] = AgentToolSpec(
                # 使用关键字参数语法，把 `bundle.metadata.name` 传入 `name` 参数；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                name=bundle.metadata.name,
                # 使用关键字参数语法，把 `bundle.intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=bundle.intent,
                # 使用关键字参数语法，把 `bundle.metadata.route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=bundle.metadata.route,
                # 使用关键字参数语法，把 `bundle.metadata.description or f"执行 {bundle.bundle_name} skill"` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description=bundle.metadata.description or f"执行 {bundle.bundle_name} skill",
                # 使用关键字参数语法，把 `list(bundle.metadata.prerequisites)` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
                prerequisites=list(bundle.metadata.prerequisites),
            # 结束 `_AgentServiceCoreMixin._build_tool_registry` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 从 `_AgentServiceCoreMixin._build_tool_registry()` 返回 `registry`，并结束当前函数。
        return registry

    # 使用 `def` 普通函数 定义 `_build_skill_dispatcher()`。
    def _build_skill_dispatcher(self) -> AgentSkillDispatcher:
        """构建运行时 skill dispatcher。"""
        # 从 `_AgentServiceCoreMixin._build_skill_dispatcher()` 返回 `build_default_skill_dispatcher()`，并结束当前函数。
        return build_default_skill_dispatcher()

    # 使用 `def` 普通函数 定义 `_classify_agent_intent()`。使用删除/编辑关键词、附件类型、上下文规则和 IntentService 快速判断意图，无法匹配时返回 general。
    def _classify_agent_intent(self, message: str, attachments: List[Dict[str, Any]]) -> str:
        """轻量意图分类：优先规则和附件上下文，不为路由单独调用大模型。"""
        # 调用 `message.lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `lowered`；它表示`lowered` 在当前函数中承载的中间业务值。
        lowered = message.lower()
        # 使用 `if` 判断 `re.search(r"删除|删掉|移除|清理", message) and re.search(r"jd|职位|岗位|简历|候选人|面试|试卷|考试", lowered)`；条件成立时执行下面分支。
        if re.search(r"删除|删掉|移除|清理", message) and re.search(r"jd|职位|岗位|简历|候选人|面试|试卷|考试", lowered):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"resource_delete"`，并结束当前函数。
            return "resource_delete"
        # 使用 `if` 判断 `self._is_criteria_edit_request(message)`；条件成立时执行下面分支。
        if self._is_criteria_edit_request(message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"criteria_edit"`，并结束当前函数。
            return "criteria_edit"
        # 使用 `if` 判断 `self._is_jd_edit_request(message)`；条件成立时执行下面分支。
        if self._is_jd_edit_request(message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"jd_edit"`，并结束当前函数。
            return "jd_edit"

        # 调用 `self.intent_service.classify_intent_fast(message)` 完成当前解析、查询或对象构造，并把返回值保存到 `fast_intent`；它表示`fast_intent` 在当前函数中承载的中间业务值。
        fast_intent = self.intent_service.classify_intent_fast(message)
        # 使用 `if` 判断 `fast_intent in self.tool_registry`；条件成立时执行下面分支。
        if fast_intent in self.tool_registry:
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `fast_intent`，并结束当前函数。
            return fast_intent

        # 使用 `if` 判断 `self._filter_attachments(attachments, {"pdf", "doc", "docx"}) and re.search(r"评分|筛选|匹配|简历|候选人", m...`；条件成立时执行下面分支。
        if self._filter_attachments(attachments, {"pdf", "doc", "docx"}) and re.search(r"评分|筛选|匹配|简历|候选人", message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"resume_screening"`，并结束当前函数。
            return "resume_screening"
        # 使用 `if` 判断 `self._filter_attachments(attachments, {"pdf", "doc", "docx", "txt", "md"}) and re.search(r"试卷|考试|...`；条件成立时执行下面分支。
        if self._filter_attachments(attachments, {"pdf", "doc", "docx", "txt", "md"}) and re.search(r"试卷|考试|出题|题目|笔试", message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"exam_generate"`，并结束当前函数。
            return "exam_generate"
        # 使用 `if` 判断 `self._is_followup_resume_screening_loop_request(message) or self._is_followup_interview_exam_requ...`；条件成立时执行下面分支。
        if self._is_followup_resume_screening_loop_request(message) or self._is_followup_interview_exam_request(message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"interview_plan" if self._is_followup_resume_screening_loop_request(message) else "exam_generate"`，并结束当前函数。
            return "interview_plan" if self._is_followup_resume_screening_loop_request(message) else "exam_generate"
        # 使用 `if` 判断 `re.search(r"试卷|考试|出题|题目|笔试", message)`；条件成立时执行下面分支。
        if re.search(r"试卷|考试|出题|题目|笔试", message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"exam_generate"`，并结束当前函数。
            return "exam_generate"
        # 使用 `if` 判断 `re.search(r"招聘|岗位|职位|jd|job", lowered)`；条件成立时执行下面分支。
        if re.search(r"招聘|岗位|职位|jd|job", lowered):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"jd"`，并结束当前函数。
            return "jd"
        # 使用 `if` 判断 `re.search(r"面试|候选人|邀约|通知|邮件", message)`；条件成立时执行下面分支。
        if re.search(r"面试|候选人|邀约|通知|邮件", message):
            # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"email_notification" if re.search(r"邮件|通知|邀约|邀请", message) else "interview_plan"`，并结束当前函数。
            return "email_notification" if re.search(r"邮件|通知|邀约|邀请", message) else "interview_plan"
        # 从 `_AgentServiceCoreMixin._classify_agent_intent()` 返回 `"general"`，并结束当前函数。
        return "general"

    # 使用 `def` 普通函数 定义 `_build_rule_agent_plan()`。
    def _build_rule_agent_plan(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `attachments`，表示用户随消息上传的附件列表，类型标注为 `List[Dict[str, Any]]`。
        attachments: List[Dict[str, Any]],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`，默认值为 `""`。
        memory_context: str = "",
    # 结束函数参数列表，并用 `-> Optional[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> Optional[Dict[str, Any]]:

        # 使用 `if` 判断 `self._classify_agent_intent(message, attachments) == "resource_delete"`；条件成立时执行下面分支。
        if self._classify_agent_intent(message, attachments) == "resource_delete":
            # 从 `_AgentServiceCoreMixin._build_rule_agent_plan()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                "mode": "tool",
                # 在字典中设置 `intent` 字段为 `"resource_delete"`，用于表示识别出的业务意图标签。
                "intent": "resource_delete",
                # 在字典中设置 `reason` 字段为 `"用户明确要求删除招聘相关产物，调用删除资源工具。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                "reason": "用户明确要求删除招聘相关产物，调用删除资源工具。",
                # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": None,
                # 在字典中设置 `source` 字段为 `"delete_rule"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "delete_rule",
            # 结束 `_AgentServiceCoreMixin._build_rule_agent_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 使用 `if` 判断 `self._is_criteria_edit_request(message) or self._is_criteria_edit_followup(message, memory_context)`；条件成立时执行下面分支。
        if self._is_criteria_edit_request(message) or self._is_criteria_edit_followup(message, memory_context):
            # 从 `_AgentServiceCoreMixin._build_rule_agent_plan()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                "mode": "tool",
                # 在字典中设置 `intent` 字段为 `"criteria_edit"`，用于表示识别出的业务意图标签。
                "intent": "criteria_edit",
                # 在字典中设置 `reason` 字段为 `"用户要求修改评分标准，先定位最近评分标准并确认修改要求。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                "reason": "用户要求修改评分标准，先定位最近评分标准并确认修改要求。",
                # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": None,
                # 在字典中设置 `source` 字段为 `"criteria_edit_rule"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "criteria_edit_rule",
            # 结束 `_AgentServiceCoreMixin._build_rule_agent_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 使用 `if` 判断 `self._is_jd_edit_request(message) or self._is_jd_edit_followup(message, memory_context)`；条件成立时执行下面分支。
        if self._is_jd_edit_request(message) or self._is_jd_edit_followup(message, memory_context):
            # 从 `_AgentServiceCoreMixin._build_rule_agent_plan()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                "mode": "tool",
                # 在字典中设置 `intent` 字段为 `"jd_edit"`，用于表示识别出的业务意图标签。
                "intent": "jd_edit",
                # 在字典中设置 `reason` 字段为 `"用户要求修改已有 JD，先定位最近 JD 并确认修改要求。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                "reason": "用户要求修改已有 JD，先定位最近 JD 并确认修改要求。",
                # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": None,
                # 在字典中设置 `source` 字段为 `"jd_edit_rule"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "jd_edit_rule",
            # 结束 `_AgentServiceCoreMixin._build_rule_agent_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 调用 `self._classify_agent_intent(message, attachments)` 完成当前解析、查询或对象构造，并把返回值保存到 `rule_intent`；它表示`rule_intent` 在当前函数中承载的中间业务值。
        rule_intent = self._classify_agent_intent(message, attachments)
        # 使用 `if` 判断 `rule_intent in {"jd", "resume_screening", "interview_plan", "exam_generate", "email_notification"}`；条件成立时执行下面分支。
        if rule_intent in {"jd", "resume_screening", "interview_plan", "exam_generate", "email_notification"}:
            # 从 `_AgentServiceCoreMixin._build_rule_agent_plan()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `mode` 字段为 `"tool"`，用于表示`mode` 在当前函数中承载的中间业务值。
                "mode": "tool",
                # 在字典中设置 `intent` 字段为 `rule_intent`，用于表示识别出的业务意图标签。
                "intent": rule_intent,
                # 在字典中设置 `reason` 字段为 `"本地规则已明确识别到 HR 工具任务，优先进入对应业务链路。"`，用于表示`reason` 在当前函数中承载的中间业务值。
                "reason": "本地规则已明确识别到 HR 工具任务，优先进入对应业务链路。",
                # 在字典中设置 `reply` 字段为 `None`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": None,
                # 在字典中设置 `source` 字段为 `"rule_intent"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "rule_intent",
            # 结束 `_AgentServiceCoreMixin._build_rule_agent_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 从 `_AgentServiceCoreMixin._build_rule_agent_plan()` 返回 `None`，并结束当前函数。
        return None

    # 使用 `def` 普通函数 定义 `_allowed_intents()`。
    def _allowed_intents(self) -> List[str]:
        # 从 `_AgentServiceCoreMixin._allowed_intents()` 返回 `sorted({*self.tool_registry.keys(), "general"})`，并结束当前函数。
        return sorted({*self.tool_registry.keys(), "general"})

    # 使用 `def` 普通函数 定义 `_route_for_intent()`。
    def _route_for_intent(self, intent: str, message: str) -> Dict[str, Any]:
        # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
        tool = self.tool_registry.get(intent)
        # 从 `_AgentServiceCoreMixin._route_for_intent()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `intent` 字段为 `intent`，用于表示识别出的业务意图标签。
            "intent": intent,
            # 在字典中设置 `route` 字段为 `tool.route if tool else None`，用于表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            "route": tool.route if tool else None,
            # 在字典中设置 `query` 字段为 `message`，用于表示发送给模型、Dify 或数据库检索的输入。
            "query": message,
            # 在字典中设置 `kb_id` 字段为 `None`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
            "kb_id": None,
        # 结束 `_AgentServiceCoreMixin._route_for_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_normalize_attachments()`。
    def _normalize_attachments(self, attachments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # 把 `normalized` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`normalized` 在当前函数中承载的中间业务值。
        normalized = []
        # 使用 `for` 循环 `item in attachments`，逐项处理集合数据。
        for item in attachments:
            # 调用 `str(item.get("name") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `name`；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
            name = str(item.get("name") or "").strip()
            # 使用 `if` 判断 `not name`；条件成立时执行下面分支。
            if not name:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 调用列表的 `append()`，把 `normalized.append({` 对应的新元素追加到现有结果末尾。
            normalized.append({
                # 在字典中设置 `name` 字段为 `name`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                "name": name,
                # 在字典中设置 `size` 字段为 `item.get("size")`，用于表示附件字节数，供上传限制和页面信息展示。
                "size": item.get("size"),
                # 在字典中设置 `content_type` 字段为 `item.get("content_type")`，用于表示附件 MIME 类型，供后端判断 PDF、DOCX 等文件格式。
                "content_type": item.get("content_type"),
                # 在字典中设置 `extension` 字段为 `name.rsplit(".", 1)[-1].lower() if "." in name else ""`，用于表示`extension` 在当前函数中承载的中间业务值。
                "extension": name.rsplit(".", 1)[-1].lower() if "." in name else "",
            # 结束 `_AgentServiceCoreMixin._normalize_attachments` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            })
        # 从 `_AgentServiceCoreMixin._normalize_attachments()` 返回 `normalized`，并结束当前函数。
        return normalized

    # 使用 `def` 普通函数 定义 `_filter_attachments()`。
    def _filter_attachments(self, attachments: List[Dict[str, Any]], extensions: set[str]) -> List[Dict[str, Any]]:
        # 从 `_AgentServiceCoreMixin._filter_attachments()` 返回 `[item for item in attachments if item.get("extension") in extensions]`，并结束当前函数。
        return [item for item in attachments if item.get("extension") in extensions]

    # 使用 `def` 普通函数 定义 `_planning_step()`。
    def _planning_step(self, tool: Optional[AgentToolSpec], detail: str) -> AgentStep:
        # 使用 `if` 判断 `not tool`；条件成立时执行下面分支。
        if not tool:
            # 从 `_AgentServiceCoreMixin._planning_step()` 返回 `AgentStep(id="plan", title="判断处理方式", status="completed", detail=detail)`，并结束当前函数。
            return AgentStep(id="plan", title="判断处理方式", status="completed", detail=detail)
        # 计算表达式 `"；".join(tool.prerequisites)` 并把结果保存到 `prerequisites`；该结果在后续流程中用作执行 Skill 前必须满足的前置条件列表。
        prerequisites = "；".join(tool.prerequisites)
        # 从 `_AgentServiceCoreMixin._planning_step()` 返回 `AgentStep(`，并结束当前函数。
        return AgentStep(
            # 使用关键字参数语法，把 `"plan"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
            id="plan",
            # 使用关键字参数语法，把 `"选择执行工具"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title="选择执行工具",
            # 使用关键字参数语法，把 `"completed"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status="completed",
            # 使用关键字参数语法，把 `f"选择工具：{tool.description}。前置条件：{prerequisites}。{detail}"` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            detail=f"选择工具：{tool.description}。前置条件：{prerequisites}。{detail}",
            # 使用关键字参数语法，把 `tool.name` 传入 `tool` 参数；它表示Agent 注册表中的业务工具规格。
            tool=tool.name,
        # 结束 `_AgentServiceCoreMixin._planning_step` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_decision_to_plan()`。
    def _decision_to_plan(self, decision: ReActDecision) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._decision_to_plan()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `mode` 字段为 `decision.mode`，用于表示`mode` 在当前函数中承载的中间业务值。
            "mode": decision.mode,
            # 在字典中设置 `intent` 字段为 `decision.intent`，用于表示识别出的业务意图标签。
            "intent": decision.intent,
            # 在字典中设置 `action` 字段为 `decision.action`，用于表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
            "action": decision.action,
            # 在字典中设置 `confidence` 字段为 `decision.confidence`，用于表示`confidence` 在当前函数中承载的中间业务值。
            "confidence": decision.confidence,
            # 在字典中设置 `reason` 字段为 `decision.thought`，用于表示`reason` 在当前函数中承载的中间业务值。
            "reason": decision.thought,
            # 在字典中设置 `observation` 字段为 `decision.observation`，用于表示`observation` 在当前函数中承载的中间业务值。
            "observation": decision.observation,
            # 在字典中设置 `reply` 字段为 `decision.reply`，用于表示`reply` 在当前函数中承载的中间业务值。
            "reply": decision.reply,
            # 在字典中设置 `source` 字段为 `decision.source`，用于表示`source` 在当前函数中承载的中间业务值。
            "source": decision.source,
        # 结束 `_AgentServiceCoreMixin._decision_to_plan` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_plan_agent_action()`。构造受限工具列表与会话上下文调用 LLM 做 ReAct 决策；解析失败时使用本地规则结果兜底。
    async def _plan_agent_action(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `attachments`，表示用户随消息上传的附件列表，类型标注为 `List[Dict[str, Any]]`。
        attachments: List[Dict[str, Any]],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`，默认值为 `""`。
        memory_context: str = "",
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        """使用受控 ReAct 决策选择下一步动作。

        这里的 ReAct 是有限动作空间版本：模型只能在 chat / use_tool / ask_user中选择，
        并且 use_tool 只能选择 tool_registry 声明过的业务工具。
        如果 LLM 失败，就回退到 _classify_agent_intent() 的本地规则。
        """
        # 计算表达式 `"无"` 并把结果保存到 `attachment_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        attachment_text = "无"
        # 使用 `if` 判断 `attachments`；条件成立时执行下面分支。
        if attachments:
            # 计算表达式 `"、".join(` 并把结果保存到 `attachment_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
            attachment_text = "、".join(
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                f"{item.get('name')}({item.get('extension') or '未知格式'})"
                # 使用 `for` 循环 `item in attachments`，逐项处理集合数据。
                for item in attachments
            # 结束 `_AgentServiceCoreMixin._plan_agent_action` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 计算表达式 `"\n".join(` 并把结果保存到 `tool_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        tool_text = "\n".join(
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"- {tool.intent}: {tool.name}，{tool.description}；前置条件：{'、'.join(tool.prerequisites)}"
            # 使用 `for` 循环 `tool in self.tool_registry.values()`，逐项处理集合数据。
            for tool in self.tool_registry.values()
        # 结束 `_AgentServiceCoreMixin._plan_agent_action` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 计算表达式 `"、".join(self._allowed_intents())` 并把结果保存到 `allowed_intents`；该结果在后续流程中用作`allowed_intents` 在当前函数中承载的中间业务值。
        allowed_intents = "、".join(self._allowed_intents())
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是 HR Agent 的 ReAct 控制器。你要按 Thought -> Action -> Observation 的方式，"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "为当前用户消息选择下一步动作。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "请只输出一个 JSON 对象，不要输出 Markdown，不要解释 JSON 之外的内容。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "可用工具：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{tool_text}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "动作空间：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- chat: 普通对话、打招呼、解释能力边界，不调用工具。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- use_tool: 用户明确要执行 HR 任务，选择一个工具 intent。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- ask_user: 用户目标明确但缺少关键前置条件，选择对应工具 intent，并由业务链路追问。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "决策规则：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "1. 不确定时优先 chat，避免误触发工具。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "2. 生成 JD、修改 JD、修改评分标准、简历筛选/评分、面试计划、试卷生成、邮件草稿应选择对应工具。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "3. 缺少附件、JD、候选人、考试配置等前置条件时，选择 ask_user 或 use_tool，后续工具链会生成表单/选择器。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "4. 邮件只能生成草稿，不允许自动发送。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"5. intent 只能是这些值之一：{allowed_intents}。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "6. 用户要求删除 JD、简历记录、面试方案、试卷时，选择 resource_delete。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "7. 用户说“改改上次那个 JD、修改刚才的职位描述、把这个 JD 的薪资改成...”时，选择 jd_edit。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "8. 用户说“修改评分标准、调整简历评分规则、把技能匹配改成40分”时，选择 criteria_edit。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "9. thought 只写一句简短推理摘要，不要展开内部推理。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "10. 用户说“继续、这个、刚才、上一个、按前面”等指代时，优先结合历史对话记忆理解。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "返回 JSON 字段：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "{\n"
            # 在字典中设置 `  \"thought\` 字段为 `\"一句推理摘要\",\n"`，用于表示`  \"thought\` 在当前函数中承载的中间业务值。
            "  \"thought\": \"一句推理摘要\",\n"
            # 在字典中设置 `  \"action\` 字段为 `\"chat/use_tool/ask_user\",\n"`，用于表示`  \"action\` 在当前函数中承载的中间业务值。
            "  \"action\": \"chat/use_tool/ask_user\",\n"
            # 在字典中设置 `  \"intent\` 字段为 `\"...\",\n"`，用于表示`  \"intent\` 在当前函数中承载的中间业务值。
            "  \"intent\": \"...\",\n"
            # 在字典中设置 `  \"tool\` 字段为 `\"工具名或 null\",\n"`，用于表示`  \"tool\` 在当前函数中承载的中间业务值。
            "  \"tool\": \"工具名或 null\",\n"
            # 在字典中设置 `  \"action_input\` 字段为 `{},\n"`，用于表示`  \"action_input\` 在当前函数中承载的中间业务值。
            "  \"action_input\": {},\n"
            # 在字典中设置 `  \"observation\` 字段为 `\"预期观察或已知前置条件\",\n"`，用于表示`  \"observation\` 在当前函数中承载的中间业务值。
            "  \"observation\": \"预期观察或已知前置条件\",\n"
            # 在字典中设置 `  \"confidence\` 字段为 `0.0,\n"`，用于表示`  \"confidence\` 在当前函数中承载的中间业务值。
            "  \"confidence\": 0.0,\n"
            # 计算表达式 `chat 时的自然回复，否则为 null\"\n"` 并把结果保存到 `"  \"reply\": \"action`；该结果在后续流程中用作`"  \"reply\": \"action` 在当前函数中承载的中间业务值。
            "  \"reply\": \"action=chat 时的自然回复，否则为 null\"\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户消息：{message}\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"附件：{attachment_text}"
        # 结束 `_AgentServiceCoreMixin._plan_agent_action` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
            if self.llm_service is None:
                # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
                self.llm_service = LLMService()
            # 使用 `await` 异步调用 `self.llm_service.generate_response(prompt)`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = await self.llm_service.generate_response(prompt)
            # 调用 `self._safe_json_loads(response)` 完成当前解析、查询或对象构造，并把返回值保存到 `planned`；它表示`planned` 在当前函数中承载的中间业务值。
            planned = self._safe_json_loads(response)
            # 调用 `str(planned.get("action") or planned.get("mode") or "").lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `action`；它表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
            action = str(planned.get("action") or planned.get("mode") or "").lower()
            # 调用 `str(planned.get("intent") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `intent`；它表示识别出的业务意图标签。
            intent = str(planned.get("intent") or "").strip()
            # 使用 `if` 判断 `action == "use_tool"`；条件成立时执行下面分支。
            if action == "use_tool":
                # 计算表达式 `"tool"` 并把结果保存到 `mode`；该结果在后续流程中用作`mode` 在当前函数中承载的中间业务值。
                mode = "tool"
            # 前一条件不成立时继续判断 `action == "ask_user"`。
            elif action == "ask_user":
                # 计算表达式 `"tool"` 并把结果保存到 `mode`；该结果在后续流程中用作`mode` 在当前函数中承载的中间业务值。
                mode = "tool"
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 计算表达式 `"chat"` 并把结果保存到 `mode`；该结果在后续流程中用作`mode` 在当前函数中承载的中间业务值。
                mode = "chat"
                # 计算表达式 `"chat"` 并把结果保存到 `action`；该结果在后续流程中用作用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
                action = "chat"
            # 使用 `if` 判断 `mode in {"chat", "tool"} and intent in self._allowed_intents()`；条件成立时执行下面分支。
            if mode in {"chat", "tool"} and intent in self._allowed_intents():
                # 调用 `ReActDecision(` 完成当前解析、查询或对象构造，并把返回值保存到 `decision`；它表示`decision` 在当前函数中承载的中间业务值。
                decision = ReActDecision(
                    # 使用关键字参数语法，把 `mode` 传入 `mode` 参数；它表示`mode` 在当前函数中承载的中间业务值。
                    mode=mode,
                    # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent=intent,
                    # 使用关键字参数语法，把 `action` 传入 `action` 参数；它表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
                    action=action,
                    # 调用 `self._clean_optional_value(planned.get("thought"))` 完成当前解析、查询或对象构造，并把返回值保存到 `thought`；它表示`thought` 在当前函数中承载的中间业务值。
                    thought=self._clean_optional_value(planned.get("thought"))
                    # 使用布尔短路语法提供备用值 `self._clean_optional_value(planned.get("reason"))`；前面的候选值为空时才采用这一项。
                    or self._clean_optional_value(planned.get("reason"))
                    # 使用布尔短路语法提供备用值 `"已完成 ReAct 决策。",`；前面的候选值为空时才采用这一项。
                    or "已完成 ReAct 决策。",
                    # 调用 `self._clean_optional_value(planned.get("observation"))` 完成当前解析、查询或对象构造，并把返回值保存到 `observation`；它表示`observation` 在当前函数中承载的中间业务值。
                    observation=self._clean_optional_value(planned.get("observation"))
                    # 使用布尔短路语法提供备用值 `"等待执行动作后观察结果。",`；前面的候选值为空时才采用这一项。
                    or "等待执行动作后观察结果。",
                    # 使用关键字参数语法，把 `planned.get("confidence")` 传入 `confidence` 参数；它表示`confidence` 在当前函数中承载的中间业务值。
                    confidence=planned.get("confidence"),
                    # 使用关键字参数语法，把 `planned.get("reply")` 传入 `reply` 参数；它表示`reply` 在当前函数中承载的中间业务值。
                    reply=planned.get("reply"),
                    # 使用关键字参数语法，把 `"react_llm"` 传入 `source` 参数；它表示`source` 在当前函数中承载的中间业务值。
                    source="react_llm",
                # 结束 `_AgentServiceCoreMixin._plan_agent_action` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
                # 从 `_AgentServiceCoreMixin._plan_agent_action()` 返回 `self._decision_to_plan(decision)`，并结束当前函数。
                return self._decision_to_plan(decision)
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("Agent ReAct 决策失败，使用规则兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent ReAct 决策失败，使用规则兜底: %s", exc)

        # 调用 `self._classify_agent_intent(message, attachments)` 完成当前解析、查询或对象构造，并把返回值保存到 `fallback_intent`；它表示`fallback_intent` 在当前函数中承载的中间业务值。
        fallback_intent = self._classify_agent_intent(message, attachments)
        # 只有规则识别结果对应已注册工具时才允许进入 use_tool，否则降级为普通聊天。
        fallback_action = "use_tool" if fallback_intent in self.tool_registry else "chat"
        # 调用 `ReActDecision(` 完成当前解析、查询或对象构造，并把返回值保存到 `decision`；它表示`decision` 在当前函数中承载的中间业务值。
        decision = ReActDecision(
            # 只有规则识别结果对应已注册工具时才允许进入 use_tool，否则降级为普通聊天。
            mode="tool" if fallback_intent in self.tool_registry else "chat",
            # 使用关键字参数语法，把 `fallback_intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent=fallback_intent,
            # 使用关键字参数语法，把 `fallback_action` 传入 `action` 参数；它表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
            action=fallback_action,
            # 使用关键字参数语法，把 `"ReAct 决策失败后，使用本地关键词和附件规则选择下一步。"` 传入 `thought` 参数；它表示`thought` 在当前函数中承载的中间业务值。
            thought="ReAct 决策失败后，使用本地关键词和附件规则选择下一步。",
            # 使用关键字参数语法，把 `"本地规则已给出可执行意图。" if fallback_action == "use_tool" else "未匹配到需要调用的工具。"` 传入 `observation` 参数；它表示`observation` 在当前函数中承载的中间业务值。
            observation="本地规则已给出可执行意图。" if fallback_action == "use_tool" else "未匹配到需要调用的工具。",
            # 使用关键字参数语法，把 `"fallback"` 传入 `source` 参数；它表示`source` 在当前函数中承载的中间业务值。
            source="fallback",
        # 结束 `_AgentServiceCoreMixin._plan_agent_action` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceCoreMixin._plan_agent_action()` 返回 `self._decision_to_plan(decision)`，并结束当前函数。
        return self._decision_to_plan(decision)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_run_recruitment_agent()`。根据计划分发普通聊天、JD、编辑、简历、面试、考试、删除或 Skill，并统一组装 AgentChatResponse。
    async def _run_recruitment_agent(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `Optional[str]`。
        route: Optional[str],
        # 声明参数 `parsed_requirements`，表示从用户自然语言或模型输出中解析并校验后的结构化结果，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        parsed_requirements: Optional[Dict[str, Any]] = None,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `Optional[UUID]`，默认值为 `None`。
        user_id: Optional[UUID] = None,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        """用户确认后，_run_recruitment_agent() 会执行：
            1. 生成 JD。
            2. 保存 JD。
            3. 基于 JD 生成评分标准。
            4. 保存评分标准。
            5. 返回两个 artifact：`job_description` 和 `scoring_criteria`。
            """
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`，默认值为 `[`。
        steps: List[AgentStep] = [
            # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._run_recruitment_agent` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(self.tool_registry.get("jd"), "招聘信息已确认，开始执行生成链路。"),
            # 计算表达式 `"parse", title="解析招聘需求", status="running", tool="parse_requirements"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="parse", title="解析招聘需求", status="running", tool="parse_requirements"),
            # 计算表达式 `"jd", title="生成岗位 JD", status="pending", tool="generate_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="jd", title="生成岗位 JD", status="pending", tool="generate_jd"),
            # 计算表达式 `"criteria", title="生成简历评分标准", status="pending", tool="generate_scoring_criteria"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="criteria", title="生成简历评分标准", status="pending", tool="generate_scoring_criteria"),
            # 计算表达式 `"next", title="规划下一步", status="pending"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="next", title="规划下一步", status="pending"),
        # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`，默认值为 `[]`。
        artifacts: List[AgentArtifact] = []

        # 计算表达式 `parsed_requirements or await self._parse_requirements(message, conversation_id)` 并把结果保存到 `parsed`；该结果在后续流程中用作从用户自然语言中解析出的结构化招聘参数。
        parsed = parsed_requirements or await self._parse_requirements(message, conversation_id)
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": self._brief_requirements(parsed)})
        # 计算表达式 `"requirements", title="结构化招聘需求", content=parsed))` 并把结果保存到 `artifacts.append(AgentArtifact(type`；该结果在后续流程中用作`append(AgentArtifact(type` 在当前函数中承载的中间业务值。
        artifacts.append(AgentArtifact(type="requirements", title="结构化招聘需求", content=parsed))

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "running"})
        # 使用 `await` 异步调用 `self._generate_jd(message, parsed, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `jd_content`；它表示提供给评分标准或面试方案生成流程的完整 JD 正文。
        jd_content = await self._generate_jd(message, parsed, conversation_id)
        # 把 `saved_jd` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入已经持久化并带有远程资源 ID 的 JD 对象。
        saved_jd = None
        # 使用 `if` 判断 `user_id`；条件成立时执行下面分支。
        if user_id:
            # 使用 `await` 异步调用 `self._save_job_description(jd_content, parsed, message, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `saved_jd`；它表示已经持久化并带有远程资源 ID 的 JD 对象。
            saved_jd = await self._save_job_description(jd_content, parsed, message, user_id, conversation_id)
        # 计算表达式 `"已生成可编辑岗位 JD。"` 并把结果保存到 `jd_detail`；该结果在后续流程中用作返回前端展示的 JD 生成与保存状态说明。
        jd_detail = "已生成可编辑岗位 JD。"
        # 使用 `if` 判断 `saved_jd`；条件成立时执行下面分支。
        if saved_jd:
            # 计算表达式 `f"已生成并保存到 JD 列表，ID：{saved_jd.id}"` 并把结果保存到 `jd_detail`；该结果在后续流程中用作返回前端展示的 JD 生成与保存状态说明。
            jd_detail = f"已生成并保存到 JD 列表，ID：{saved_jd.id}"
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": jd_detail})
        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"job_description"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="job_description",
            # 使用关键字参数语法，把 `f"{parsed.get('job_title') or '岗位'} JD"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=f"{parsed.get('job_title') or '岗位'} JD",
            # 使用关键字参数语法，把 `jd_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=jd_content,
            # 使用字典字面量开始构造 `metadata`；该字典用于保存产物附加信息，例如数据库 ID、候选人和保存状态，下面各行会继续写入具体业务字段。
            metadata={
                # 在字典中设置 `job_title` 字段为 `parsed.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                "job_title": parsed.get("job_title"),
                # 在字典中设置 `route` 字段为 `"/recruitment/jd-generator"`，用于表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                "route": "/recruitment/jd-generator",
                # 在字典中设置 `saved_jd_id` 字段为 `str(saved_jd.id) if saved_jd else None`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "saved_jd_id": str(saved_jd.id) if saved_jd else None,
            # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))

        # 把 `saved_criteria` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入已经保存到业务存储并带有资源 ID 的简历评分标准。
        saved_criteria = None
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
        steps[3] = steps[3].model_copy(update={"status": "running", "detail": "正在基于 JD 生成评分标准..."})
        # 使用 `await` 异步调用 `self._generate_scoring_criteria(jd_content, parsed, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `criteria_content`；它表示根据 JD 生成的完整简历评分标准正文。
        criteria_content = await self._generate_scoring_criteria(jd_content, parsed, conversation_id)
        # 使用 `if` 判断 `user_id`；条件成立时执行下面分支。
        if user_id:
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
                # 使用条件表达式计算位置参数 `saved_jd.id if saved_jd else None`，根据对象是否存在决定传真实 ID 还是空值。
                saved_jd.id if saved_jd else None,
            # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 计算表达式 `"已生成简历评分标准。"` 并把结果保存到 `criteria_detail`；该结果在后续流程中用作返回前端展示的评分标准生成与保存状态说明。
        criteria_detail = "已生成简历评分标准。"
        # 使用 `if` 判断 `saved_criteria`；条件成立时执行下面分支。
        if saved_criteria:
            # 计算表达式 `f"已生成并保存评分标准，ID：{saved_criteria.id}"` 并把结果保存到 `criteria_detail`；该结果在后续流程中用作返回前端展示的评分标准生成与保存状态说明。
            criteria_detail = f"已生成并保存评分标准，ID：{saved_criteria.id}"
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
        steps[3] = steps[3].model_copy(update={"status": "completed", "detail": criteria_detail})
        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"scoring_criteria"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="scoring_criteria",
            # 使用关键字参数语法，把 `f"{parsed.get('job_title') or '岗位'}评分标准"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=f"{parsed.get('job_title') or '岗位'}评分标准",
            # 使用关键字参数语法，把 `criteria_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=criteria_content,
            # 使用字典字面量开始构造 `metadata`；该字典用于保存产物附加信息，例如数据库 ID、候选人和保存状态，下面各行会继续写入具体业务字段。
            metadata={
                # 在字典中设置 `job_title` 字段为 `parsed.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                "job_title": parsed.get("job_title"),
                # 在字典中设置 `saved_criteria_id` 字段为 `str(saved_criteria.id) if saved_criteria else None`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "saved_criteria_id": str(saved_criteria.id) if saved_criteria else None,
                # 在字典中设置 `saved_jd_id` 字段为 `str(saved_jd.id) if saved_jd else None`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "saved_jd_id": str(saved_jd.id) if saved_jd else None,
            # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
        steps[4] = steps[4].model_copy(update={"status": "completed", "detail": "如需筛选简历，可以继续告诉我“筛选简历”。"})

        # 调用 `parsed.get("job_title") or "这个岗位"` 完成当前解析、查询或对象构造，并把返回值保存到 `job_title`；它表示`job_title` 在当前函数中承载的中间业务值。
        job_title = parsed.get("job_title") or "这个岗位"
        # 从 `_AgentServiceCoreMixin._run_recruitment_agent()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `f"我已经按「{job_title}」生成并保存了 JD，同时生成了简历评分标准。你可以继续让我筛选简历、生成面试题或修改 JD。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"我已经按「{job_title}」生成并保存了 JD，同时生成了简历评分标准。你可以继续让我筛选简历、生成面试题或修改 JD。",
            # 使用关键字参数语法，把 `"jd"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="jd",
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用列表字面量开始构造 `suggestions`；该列表用于保存任务结束后提供给用户的下一步操作建议，下面各行会继续追加有顺序的业务元素。
            suggestions=[
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "筛选这个 JD 的简历",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "基于 JD 生成面试题",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "继续修改 JD",
            # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
        # 结束 `_AgentServiceCoreMixin._run_recruitment_agent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_stream_jd_edit_response()`。
    async def _stream_jd_edit_response(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
        selected_tool = self.tool_registry.get("jd_edit")
        # 使用 `await` 异步调用 `self._parse_jd_edit_request(message, memory_context)`，等待数据库、Dify 或模型处理完成后把结果保存到 `edit_request`；它表示准备提交给下游服务的结构化请求参数。
        edit_request = await self._parse_jd_edit_request(message, memory_context)
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._stream_jd_edit_response` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(selected_tool, "识别到 JD 修改请求，先定位目标 JD。"),
            # 计算表达式 `"locate_jd", title="定位要修改的 JD", status="running", detail="优先查找当前对话最近生成的 JD。", tool="edit_jd"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="locate_jd", title="定位要修改的 JD", status="running", detail="优先查找当前对话最近生成的 JD。", tool="edit_jd"),
            # 计算表达式 `"collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),
            # 计算表达式 `"rewrite_jd", title="改写 JD 内容", status="pending", detail="基于原 JD 和修改要求生成新版。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="rewrite_jd", title="改写 JD 内容", status="pending", detail="基于原 JD 和修改要求生成新版。"),
            # 计算表达式 `"save_jd", title="保存修改", status="pending", detail="更新原 JD 记录。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_jd", title="保存修改", status="pending", detail="更新原 JD 记录。"),
        # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`，默认值为 `[]`。
        artifacts: List[AgentArtifact] = []
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._jd_edit_stream_event("plan", "正在定位要修改的 JD。", steps, artifacts)

        # 使用 `await` 异步调用 `self._resolve_recent_jd_candidates(user_id, conversation_id, edit_request.get("keyword") or "")`，等待数据库、Dify 或模型处理完成后把结果保存到 `candidates`；它表示根据用户描述找到、等待进一步选择的候选资源列表。
        candidates = await self._resolve_recent_jd_candidates(user_id, conversation_id, edit_request.get("keyword") or "")
        # 使用 `if` 判断 `not candidates`；条件成立时执行下面分支。
        if not candidates:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "failed", "detail": "没有找到可修改的 JD。"})
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `"我还没有找到可以修改的 JD。请先生成或保存一个 JD，或者明确告诉我要修改哪个岗位的 JD。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我还没有找到可以修改的 JD。请先生成或保存一个 JD，或者明确告诉我要修改哪个岗位的 JD。",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["生成一个新的 JD", "说明要修改的岗位名称", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["生成一个新的 JD", "说明要修改的岗位名称", "去 JD 管理查看职位"],
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_jd_edit_response()` 返回 `None`，并结束当前函数。
            return

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
            # 计算表达式 `"jd_edit_candidates", title="待确认 JD", content={"candidates": candidates[:5]}))` 并把结果保存到 `artifacts.append(AgentArtifact(type`；该结果在后续流程中用作`append(AgentArtifact(type` 在当前函数中承载的中间业务值。
            artifacts.append(AgentArtifact(type="jd_edit_candidates", title="待确认 JD", content={"candidates": candidates[:5]}))
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                message=(
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    "我找到了多个可能要修改的 JD。为避免改错，请告诉我具体是哪一个：\n"
                    # 使用 `+` 将 `"\n".join(f"- {self._format_jd_candidate_label(item)}" for item in candidates[:5])` 拼接到前面的文本或集合，组成完整业务内容。
                    + "\n".join(f"- {self._format_jd_candidate_label(item)}" for item in candidates[:5])
                # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["修改最新的那个 JD", "说明岗位名称", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["修改最新的那个 JD", "说明岗位名称", "去 JD 管理查看职位"],
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_jd_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 计算表达式 `candidates[0]` 并把结果保存到 `target`；该结果在后续流程中用作经会话记忆和候选列表消歧后确定的目标业务资源。
        target = candidates[0]
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": f"已定位：{target.get('name') or '最近的 JD'}。"})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._jd_edit_stream_event("progress", f"已定位「{target.get('name') or '最近的 JD'}」。", steps, artifacts)

        # 调用 `self._clean_optional_value(edit_request.get("changes"))` 完成当前解析、查询或对象构造，并把返回值保存到 `edit_instructions`；它表示用户要求对原内容执行的具体修改说明。
        edit_instructions = self._clean_optional_value(edit_request.get("changes"))
        # 使用 `if` 判断 `not edit_instructions`；条件成立时执行下面分支。
        if not edit_instructions:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
            steps[2] = steps[2].model_copy(update={"status": "pending", "detail": "等待用户说明要修改哪些内容。"})
            # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
            artifacts.append(AgentArtifact(
                # 使用关键字参数语法，把 `"jd_edit_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                type="jd_edit_request",
                # 使用关键字参数语法，把 `"等待修改要求"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title="等待修改要求",
                # 使用关键字参数语法，把 `{"job_description_id": target.get("id"), "title": target.get("name")}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content={"job_description_id": target.get("id"), "title": target.get("name")},
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ))
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `f"可以，我找到了「{target.get('name') or '最近的 JD'}」。你想具体改哪些内容？比如薪资、地点、职责、技能要求或福利。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"可以，我找到了「{target.get('name') or '最近的 JD'}」。你想具体改哪些内容？比如薪资、地点、职责、技能要求或福利。",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["薪资改成 20-30K", "增加 React 和性能优化要求", "把工作地点改成长沙"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["薪资改成 20-30K", "增加 React 和性能优化要求", "把工作地点改成长沙"],
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_jd_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": edit_instructions})
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
        steps[3] = steps[3].model_copy(update={"status": "running", "detail": "正在读取原 JD 并生成修改版。"})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._jd_edit_stream_event("progress", "已收到修改要求，正在改写 JD。", steps, artifacts)

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
            # 使用 `await` 异步调用 `self._rewrite_jd_content(original_content, edit_instructions)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            updated_content = await self._rewrite_jd_content(original_content, edit_instructions)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
            steps[3] = steps[3].model_copy(update={"status": "completed", "detail": "已生成修改后的 JD。"})
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._jd_edit_stream_event("progress", "JD 修改版已生成，正在保存。", steps, artifacts)

            # 调用 `self._build_jd_update_payload(original_data, updated_content, edit_instructions, edit_request)` 完成当前解析、查询或对象构造，并把返回值保存到 `update_data`；它表示`update_data` 在当前函数中承载的中间业务值。
            update_data = self._build_jd_update_payload(original_data, updated_content, edit_instructions, edit_request)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "running", "detail": "正在保存到原 JD。"})
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._jd_edit_stream_event("progress", "正在保存修改。", steps, artifacts)
            # 使用 `await` 异步调用 `service.update_job_description(str(target["id"]), update_data, user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated_jd`；它表示`updated_jd` 在当前函数中承载的中间业务值。
            updated_jd = await service.update_job_description(str(target["id"]), update_data, user_id)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "completed", "detail": f"已更新 JD：{updated_jd.id}"})
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `True)` 并把结果保存到 `logger.warning("Agent 流式修改 JD 失败: %s", exc, exc_info`；该结果在后续流程中用作`warning("Agent 流式修改 JD 失败: %s", exc, exc_info` 在当前函数中承载的中间业务值。
            logger.warning("Agent 流式修改 JD 失败: %s", exc, exc_info=True)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "failed", "detail": str(exc)})
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `f"我找到了 JD，但保存修改时失败了：{str(exc)}"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"我找到了 JD，但保存修改时失败了：{str(exc)}",
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["稍后重试", "去 JD 管理手动编辑", "重新生成 JD"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["稍后重试", "去 JD 管理手动编辑", "重新生成 JD"],
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_jd_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
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
            # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = AgentChatResponse(
            # 使用关键字参数语法，把 `f"已按你的要求更新「{updated_jd.title}」。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"已按你的要求更新「{updated_jd.title}」。",
            # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="jd_edit",
            # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/jd-generator",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `["继续修改 JD", "基于这个 JD 筛选简历", "去 JD 管理查看职位"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["继续修改 JD", "基于这个 JD 筛选简历", "去 JD 管理查看职位"],
        # 结束 `_AgentServiceCoreMixin._stream_jd_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `def` 普通函数 定义 `_jd_edit_stream_event()`。
    def _jd_edit_stream_event(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `event_type`，表示`event_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        event_type: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`。
        artifacts: List[AgentArtifact],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._jd_edit_stream_event()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `event_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": event_type,
            # 在字典中设置 `response` 字段为 `AgentChatResponse(`，用于表示下游服务或 Agent 流程返回的响应。
            "response": AgentChatResponse(
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `"jd_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd_edit",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 完成 AgentStep 或 AgentArtifact 的 Pydantic 对象构造，再调用 `model_dump()` 转成普通字典，便于封装进 SSE JSON 事件。
            ).model_dump(),
        # 结束 `_AgentServiceCoreMixin._jd_edit_stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_stream_criteria_edit_response()`。
    async def _stream_criteria_edit_response(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
        selected_tool = self.tool_registry.get("criteria_edit")
        # 使用 `await` 异步调用 `self._parse_criteria_edit_request(message, memory_context)`，等待数据库、Dify 或模型处理完成后把结果保存到 `edit_request`；它表示准备提交给下游服务的结构化请求参数。
        edit_request = await self._parse_criteria_edit_request(message, memory_context)
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._stream_criteria_edit_response` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(selected_tool, "识别到评分标准修改请求，先定位目标评分标准。"),
            # 计算表达式 `"locate_criteria", title="定位评分标准", status="running", detail="优先查找当前对话最近生成的评分标准。", tool="edit_scor...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="locate_criteria", title="定位评分标准", status="running", detail="优先查找当前对话最近生成的评分标准。", tool="edit_scoring_criteria"),
            # 计算表达式 `"collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="collect_changes", title="确认修改要求", status="pending", detail="拿到明确修改点后再改写。"),
            # 计算表达式 `"rewrite_criteria", title="改写评分标准", status="pending", detail="基于原评分标准和修改要求生成新版。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="rewrite_criteria", title="改写评分标准", status="pending", detail="基于原评分标准和修改要求生成新版。"),
            # 计算表达式 `"save_criteria", title="保存修改", status="pending", detail="更新原评分标准。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="save_criteria", title="保存修改", status="pending", detail="更新原评分标准。"),
        # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`，默认值为 `[]`。
        artifacts: List[AgentArtifact] = []
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._criteria_edit_stream_event("plan", "正在定位要修改的评分标准。", steps, artifacts)

        # 使用 `await` 异步调用 `self._resolve_recent_criteria_candidates(user_id, conversation_id, edit_request.get("keyword") or...`，等待数据库、Dify 或模型处理完成后把结果保存到 `candidates`；它表示根据用户描述找到、等待进一步选择的候选资源列表。
        candidates = await self._resolve_recent_criteria_candidates(user_id, conversation_id, edit_request.get("keyword") or "")
        # 使用 `if` 判断 `not candidates`；条件成立时执行下面分支。
        if not candidates:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "failed", "detail": "没有找到可修改的评分标准。"})
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `"我还没有找到可以修改的评分标准。请先生成并保存评分标准，或者明确告诉我要修改哪个岗位的评分标准。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我还没有找到可以修改的评分标准。请先生成并保存评分标准，或者明确告诉我要修改哪个岗位的评分标准。",
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["生成评分标准", "说明岗位名称", "查看简历筛选"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["生成评分标准", "说明岗位名称", "查看简历筛选"],
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_criteria_edit_response()` 返回 `None`，并结束当前函数。
            return

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
            steps[1] = steps[1].model_copy(update={"status": "running", "detail": f"找到 {len(candidates)} 个可能的评分标准，需要确认。"})
            # 计算表达式 `"criteria_edit_candidates", title="待确认评分标准", content={"candidates": candidates[:5]}))` 并把结果保存到 `artifacts.append(AgentArtifact(type`；该结果在后续流程中用作`append(AgentArtifact(type` 在当前函数中承载的中间业务值。
            artifacts.append(AgentArtifact(type="criteria_edit_candidates", title="待确认评分标准", content={"candidates": candidates[:5]}))
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `"我找到了多个可能要修改的评分标准。请告诉我具体是哪一个：\n" + "\n".join(f"- {item['name']}" for item in candidates[:5])` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我找到了多个可能要修改的评分标准。请告诉我具体是哪一个：\n" + "\n".join(f"- {item['name']}" for item in candidates[:5]),
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["修改最新的评分标准", "说明岗位名称", "查看评分标准列表"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["修改最新的评分标准", "说明岗位名称", "查看评分标准列表"],
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_criteria_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 计算表达式 `candidates[0]` 并把结果保存到 `target`；该结果在后续流程中用作经会话记忆和候选列表消歧后确定的目标业务资源。
        target = candidates[0]
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": f"已定位：{target.get('name') or '最近的评分标准'}。"})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._criteria_edit_stream_event("progress", f"已定位「{target.get('name') or '最近的评分标准'}」。", steps, artifacts)

        # 调用 `self._clean_optional_value(edit_request.get("changes"))` 完成当前解析、查询或对象构造，并把返回值保存到 `edit_instructions`；它表示用户要求对原内容执行的具体修改说明。
        edit_instructions = self._clean_optional_value(edit_request.get("changes"))
        # 使用 `if` 判断 `not edit_instructions`；条件成立时执行下面分支。
        if not edit_instructions:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
            steps[2] = steps[2].model_copy(update={"status": "pending", "detail": "等待用户说明要修改哪些评分规则。"})
            # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
            artifacts.append(AgentArtifact(
                # 使用关键字参数语法，把 `"criteria_edit_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                type="criteria_edit_request",
                # 使用关键字参数语法，把 `"等待修改要求"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title="等待修改要求",
                # 使用关键字参数语法，把 `{"criteria_id": target.get("id"), "title": target.get("name")}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                content={"criteria_id": target.get("id"), "title": target.get("name")},
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ))
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `f"可以，我找到了「{target.get('name') or '最近的评分标准'}」。你想具体改哪些评分规则？比如分值、维度、淘汰项或加分项。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"可以，我找到了「{target.get('name') or '最近的评分标准'}」。你想具体改哪些评分规则？比如分值、维度、淘汰项或加分项。",
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["技能匹配改成 40 分", "增加大模型项目经验加分项", "降低学历权重"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["技能匹配改成 40 分", "增加大模型项目经验加分项", "降低学历权重"],
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_criteria_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[2]`；业务上用于推进前端步骤的状态或说明。
        steps[2] = steps[2].model_copy(update={"status": "completed", "detail": edit_instructions})
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
        steps[3] = steps[3].model_copy(update={"status": "running", "detail": "正在读取原评分标准并生成修改版。"})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield self._criteria_edit_stream_event("progress", "已收到修改要求，正在改写评分标准。", steps, artifacts)

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `ScoringCriteriaService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `service`；它表示`service` 在当前函数中承载的中间业务值。
            service = ScoringCriteriaService(self.db)
            # 使用 `await` 异步调用 `service.get_scoring_criteria(str(target["id"]), user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `original`；它表示`original` 在当前函数中承载的中间业务值。
            original = await service.get_scoring_criteria(str(target["id"]), user_id)
            # 调用 `original.model_dump(mode="json")` 完成当前解析、查询或对象构造，并把返回值保存到 `original_data`；它表示`original_data` 在当前函数中承载的中间业务值。
            original_data = original.model_dump(mode="json")
            # 使用 `await` 异步调用 `self._rewrite_criteria_content(original_data.get("content") or "", edit_instructions)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            updated_content = await self._rewrite_criteria_content(original_data.get("content") or "", edit_instructions)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[3]`；业务上用于推进前端步骤的状态或说明。
            steps[3] = steps[3].model_copy(update={"status": "completed", "detail": "已生成修改后的评分标准。"})
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._criteria_edit_stream_event("progress", "评分标准修改版已生成，正在保存。", steps, artifacts)

            # 调用 `self._build_criteria_update_payload(original_data, updated_content, edit_instructions)` 完成当前解析、查询或对象构造，并把返回值保存到 `update_data`；它表示`update_data` 在当前函数中承载的中间业务值。
            update_data = self._build_criteria_update_payload(original_data, updated_content, edit_instructions)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "running", "detail": "正在保存到原评分标准。"})
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield self._criteria_edit_stream_event("progress", "正在保存修改。", steps, artifacts)
            # 使用 `await` 异步调用 `service.update_scoring_criteria(str(target["id"]), update_data, user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `updated`；它表示`updated` 在当前函数中承载的中间业务值。
            updated = await service.update_scoring_criteria(str(target["id"]), update_data, user_id)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "completed", "detail": f"已更新评分标准：{updated.id}"})
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 计算表达式 `True)` 并把结果保存到 `logger.warning("Agent 流式修改评分标准失败: %s", exc, exc_info`；该结果在后续流程中用作`warning("Agent 流式修改评分标准失败: %s", exc, exc_info` 在当前函数中承载的中间业务值。
            logger.warning("Agent 流式修改评分标准失败: %s", exc, exc_info=True)
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[4]`；业务上用于推进前端步骤的状态或说明。
            steps[4] = steps[4].model_copy(update={"status": "failed", "detail": str(exc)})
            # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
            response = AgentChatResponse(
                # 使用关键字参数语法，把 `f"我找到了评分标准，但保存修改时失败了：{str(exc)}"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"我找到了评分标准，但保存修改时失败了：{str(exc)}",
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `["稍后重试", "重新生成评分标准", "查看简历筛选"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["稍后重试", "重新生成评分标准", "查看简历筛选"],
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield {"type": "final", "response": response.model_dump()}
            # 从 `_AgentServiceCoreMixin._stream_criteria_edit_response()` 返回 `None`，并结束当前函数。
            return

        # 调用列表的 `append()`，把 `artifacts.append(AgentArtifact(` 对应的新元素追加到现有结果末尾。
        artifacts.append(AgentArtifact(
            # 使用关键字参数语法，把 `"scoring_criteria"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            type="scoring_criteria",
            # 使用关键字参数语法，把 `updated.title` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=updated.title,
            # 使用关键字参数语法，把 `updated.content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=updated.content,
            # 使用字典字面量开始构造 `metadata`；该字典用于保存产物附加信息，例如数据库 ID、候选人和保存状态，下面各行会继续写入具体业务字段。
            metadata={
                # 在字典中设置 `job_title` 字段为 `updated.job_title`，用于表示`job_title` 在当前函数中承载的中间业务值。
                "job_title": updated.job_title,
                # 在字典中设置 `saved_criteria_id` 字段为 `str(updated.id)`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "saved_criteria_id": str(updated.id),
                # 在字典中设置 `edit_instructions` 字段为 `edit_instructions`，用于表示用户要求对原内容执行的具体修改说明。
                "edit_instructions": edit_instructions,
                # 在字典中设置 `route` 字段为 `"/recruitment/resume-screening"`，用于表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                "route": "/recruitment/resume-screening",
            # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ))
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = AgentChatResponse(
            # 使用关键字参数语法，把 `f"已按你的要求更新「{updated.title}」。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"已按你的要求更新「{updated.title}」。",
            # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="criteria_edit",
            # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/resume-screening",
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `["继续修改评分标准", "基于这个标准筛选简历", "重新生成评分标准"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["继续修改评分标准", "基于这个标准筛选简历", "重新生成评分标准"],
        # 结束 `_AgentServiceCoreMixin._stream_criteria_edit_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `def` 普通函数 定义 `_criteria_edit_stream_event()`。
    def _criteria_edit_stream_event(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `event_type`，表示`event_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        event_type: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`。
        artifacts: List[AgentArtifact],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._criteria_edit_stream_event()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `event_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": event_type,
            # 在字典中设置 `response` 字段为 `AgentChatResponse(`，用于表示下游服务或 Agent 流程返回的响应。
            "response": AgentChatResponse(
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `"criteria_edit"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="criteria_edit",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 完成 AgentStep 或 AgentArtifact 的 Pydantic 对象构造，再调用 `model_dump()` 转成普通字典，便于封装进 SSE JSON 事件。
            ).model_dump(),
        # 结束 `_AgentServiceCoreMixin._criteria_edit_stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_stream_text()`。
    async def _stream_text(self, text: str) -> AsyncGenerator[str, None]:
        """将完整文本拆成小片段输出，供前端呈现流式打字效果。"""
        # 使用 `if` 判断 `not text`；条件成立时执行下面分支。
        if not text:
            # 从 `_AgentServiceCoreMixin._stream_text()` 返回 `None`，并结束当前函数。
            return
        # 调用 `re.findall(r".{1,4}(?:\s+|$)|[^\s]{1,4}", text, flags=re.S)` 完成当前解析、查询或对象构造，并把返回值保存到 `parts`；它表示按 SKILL.md 前置元数据分隔符切开的文本片段。
        parts = re.findall(r".{1,4}(?:\s+|$)|[^\s]{1,4}", text, flags=re.S)
        # 使用 `for` 循环 `part in parts`，逐项处理集合数据。
        for part in parts:
            # 使用 `if` 判断 `not part`；条件成立时执行下面分支。
            if not part:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
            yield part
            # 使用 `await` 等待异步操作 `asyncio.sleep(0.035)` 完成，期间不会阻塞事件循环。
            await asyncio.sleep(0.035)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_stream_skill_draft_response()`。
    async def _stream_skill_draft_response(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `intent`，表示识别出的业务意图标签，类型标注为 `str`。
        intent: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `Optional[str]`。
        route: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        confirmed_requirements: Optional[Dict[str, Any]] = None,
    # 结束函数参数列表，并用 `-> AsyncGenerator[Dict[str, Any], None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # 根据已识别意图取得对应 Skill 能力包，再选择 draft、send 等执行阶段。
        bundle = self.skill_dispatcher.get_bundle(intent)
        # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
        selected_tool = self.tool_registry.get(intent)
        # 调用 `AgentChatResponse(` 完成当前解析、查询或对象构造，并把返回值保存到 `plan_response`；它表示下游服务返回、等待解析或转发的响应。
        plan_response = AgentChatResponse(
            # 使用关键字参数语法，把 `""` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message="",
            # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent=intent,
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
            steps=[
                # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._stream_skill_draft_response` 当前步骤所需的构造、查询或业务处理。
                self._planning_step(selected_tool, f"已选择 {bundle.bundle_name} skill，开始流式生成草稿。"),
                # 计算表达式 `"draft", title="生成邮件草稿", status="running", detail="正在逐步生成邮件草稿。", tool="draft"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="draft", title="生成邮件草稿", status="running", detail="正在逐步生成邮件草稿。", tool="draft"),
                # 计算表达式 `"confirm_send", title="等待人工确认", status="pending", detail="草稿生成后请确认收件人、主题和正文。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="confirm_send", title="等待人工确认", status="pending", detail="草稿生成后请确认收件人、主题和正文。"),
                # 计算表达式 `"send_email", title="提交邮件", status="pending", detail="确认后才会提交给 SMTP 服务器。", tool="send"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="send_email", title="提交邮件", status="pending", detail="确认后才会提交给 SMTP 服务器。", tool="send"),
            # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=[],
        # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "plan", "response": plan_response.model_dump()}

        # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
        full_text = ""
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `self.llm_service is None`；条件成立时执行下面分支。
            if self.llm_service is None:
                # 把 `LLMService()` 保存为实例属性 `self.llm_service`，供同一 Agent 服务的其他方法复用；该属性表示直接调用千问兼容接口的模型服务。
                self.llm_service = LLMService()
            # 调用 `bundle.get_phase("draft")` 完成当前解析、查询或对象构造，并把返回值保存到 `phase`；它表示Skill 当前需要执行的阶段名称。
            phase = bundle.get_phase("draft")
            # 调用 `phase.load_skill_instructions()` 完成当前解析、查询或对象构造，并把返回值保存到 `skill_markdown`；它表示SKILL.md 的完整规则文本。
            skill_markdown = phase.load_skill_instructions()
            # 调用 `self._build_skill_email_draft_prompt(message, memory_context, skill_markdown)` 完成当前解析、查询或对象构造，并把返回值保存到 `prompt`；它表示发送给 LLM 的完整提示词。
            prompt = self._build_skill_email_draft_prompt(message, memory_context, skill_markdown)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self.llm_service.stream_response(prompt):
                # 计算表达式 `delta` 并把结果保存到 `full_text +`；该结果在后续流程中用作`full_text +` 在当前函数中承载的中间业务值。
                full_text += delta
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "delta", "delta": delta}
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("Agent 邮件草稿真实流式生成失败，使用模板兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 邮件草稿真实流式生成失败，使用模板兜底: %s", exc)
            # 调用 `bundle.get_phase("draft")` 完成当前解析、查询或对象构造，并把返回值保存到 `phase`；它表示Skill 当前需要执行的阶段名称。
            phase = bundle.get_phase("draft")
            # 使用 `await` 异步调用 `phase.run(`，等待数据库、Dify 或模型处理完成后把结果保存到 `fallback_result`；它表示当前业务调用完成后返回的单个处理结果。
            fallback_result = await phase.run(
                # 使用字典字面量语法开始构造 `_AgentServiceCoreMixin._stream_skill_draft_response` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `message` 字段为 `message`，用于表示用户本次输入的自然语言消息。
                    "message": message,
                    # 在字典中设置 `memory_context` 字段为 `memory_context`，用于表示从数据库历史消息整理出的短期会话记忆。
                    "memory_context": memory_context,
                    # 在字典中设置 `confirmed_requirements` 字段为 `confirmed_requirements or {}`，用于表示用户补充或确认后的结构化业务参数。
                    "confirmed_requirements": confirmed_requirements or {},
                    # 在字典中设置 `llm_service` 字段为 `self.llm_service`，用于表示直接调用千问兼容接口的模型服务。
                    "llm_service": self.llm_service,
                    # 在字典中设置 `email_service` 字段为 `self.email_send_service`，用于表示负责读取邮箱配置并通过 SMTP 真正发送邮件的服务对象。
                    "email_service": self.email_send_service,
                    # 在字典中设置 `user_id` 字段为 `user_id`，用于表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    "user_id": user_id,
                    # 在字典中设置 `confirmation_action` 字段为 `bundle.metadata.confirmation_action`，用于表示触发高风险操作继续执行的人工确认动作。
                    "confirmation_action": bundle.metadata.confirmation_action,
                # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
            # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 调用 `str(fallback_result.get("message") or "")` 完成当前解析、查询或对象构造，并把返回值保存到 `full_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
            full_text = str(fallback_result.get("message") or "")
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self._stream_text(full_text):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield {"type": "delta", "delta": delta}

        # 使用 `await` 异步调用 `self._handle_skill_intent(`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self._handle_skill_intent(
            # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent=intent,
            # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=message,
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用关键字参数语法，把 `selected_tool` 传入 `selected_tool` 参数；它表示当前意图对应的已注册工具规格。
            selected_tool=selected_tool,
            # 使用关键字参数语法，把 `memory_context` 传入 `memory_context` 参数；它表示从数据库历史消息整理出的短期会话记忆。
            memory_context=memory_context,
            # 使用字典字面量开始构造 `confirmed_requirements`；该字典用于保存用户补充或确认后的结构化业务参数，下面各行会继续写入具体业务字段。
            confirmed_requirements={
                # 使用 `*` 可迭代对象解包，把 `*(confirmed_requirements or {})` 返回的全部元素展开并合并进当前列表或参数序列。
                **(confirmed_requirements or {}),
                # 在字典中设置 `__draft_text` 字段为 `full_text.strip()`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                "__draft_text": full_text.strip(),
            # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceCoreMixin._stream_skill_draft_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `response.model_copy(update={"message": full_text.strip() or response.message})` 完成当前解析、查询或对象构造，并把返回值保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = response.model_copy(update={"message": full_text.strip() or response.message})
        # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
        yield {"type": "final", "response": response.model_dump()}

    # 使用 `def` 普通函数 定义 `_build_skill_email_draft_prompt()`。
    def _build_skill_email_draft_prompt(self, message: str, memory_context: str, skill_markdown: str) -> str:
        # 计算表达式 `f"历史上下文：\n{memory_context}\n" if memory_context else ""` 并把结果保存到 `memory_block`；该结果在后续流程中用作整理后准备加入模型提示词的会话历史文本块。
        memory_block = f"历史上下文：\n{memory_context}\n" if memory_context else ""
        # 从 `_AgentServiceCoreMixin._build_skill_email_draft_prompt()` 返回 `(`，并结束当前函数。
        return (
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你正在执行 hr-agent-email skill。请先遵循下面的 SKILL.md 工作流，再完成当前任务。\n\n"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{skill_markdown}\n\n"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "当前执行阶段：draft phase。\n"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "请根据 skill 工作流生成一封中文候选人邮件草稿。\n"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "要求：包含主题、称呼、正文、下一步动作、署名占位；如果关键信息缺失，用【待补充】标注，不要编造具体时间地点。\n"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{memory_block}"
            # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户需求：{message}"
        # 结束 `_AgentServiceCoreMixin._build_skill_email_draft_prompt` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_handle_skill_intent()`。
    async def _handle_skill_intent(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `intent`，表示识别出的业务意图标签，类型标注为 `str`。
        intent: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `Optional[str]`。
        route: Optional[str],
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`。
        memory_context: str,
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        confirmed_requirements: Optional[Dict[str, Any]] = None,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 根据intent获取skill资源
        # 根据已识别意图取得对应 Skill 能力包，再选择 draft、send 等执行阶段。
        bundle = self.skill_dispatcher.get_bundle(intent)
        # 调用 `bundle.resolve_phase(confirmed_requirements)` 完成当前解析、查询或对象构造，并把返回值保存到 `phase_name`；它表示本次准备执行的 Skill 阶段名称。
        phase_name = bundle.resolve_phase(confirmed_requirements)
        # 调用 `bundle.get_phase(phase_name)` 完成当前解析、查询或对象构造，并把返回值保存到 `phase`；它表示Skill 当前需要执行的阶段名称。
        phase = bundle.get_phase(phase_name)
        # 使用 `await` 异步调用 `phase.run(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await phase.run(
            # 使用字典字面量语法开始构造 `_AgentServiceCoreMixin._handle_skill_intent` 当前步骤所需的键值数据。
            {
                # 在字典中设置 `message` 字段为 `message`，用于表示用户本次输入的自然语言消息。
                "message": message,
                # 在字典中设置 `memory_context` 字段为 `memory_context`，用于表示从数据库历史消息整理出的短期会话记忆。
                "memory_context": memory_context,
                # 在字典中设置 `confirmed_requirements` 字段为 `confirmed_requirements or {}`，用于表示用户补充或确认后的结构化业务参数。
                "confirmed_requirements": confirmed_requirements or {},
                # 在字典中设置 `llm_service` 字段为 `self.llm_service`，用于表示直接调用千问兼容接口的模型服务。
                "llm_service": self.llm_service,
                # 在字典中设置 `email_service` 字段为 `self.email_send_service`，用于表示负责读取邮箱配置并通过 SMTP 真正发送邮件的服务对象。
                "email_service": self.email_send_service,
                # 在字典中设置 `user_id` 字段为 `user_id`，用于表示当前登录用户的唯一 ID，用于权限和数据隔离。
                "user_id": user_id,
                # 在字典中设置 `confirmation_action` 字段为 `bundle.metadata.confirmation_action`，用于表示触发高风险操作继续执行的人工确认动作。
                "confirmation_action": bundle.metadata.confirmation_action,
                # 在字典中设置 `draft_text` 字段为 `(confirmed_requirements or {}).get("__draft_text")`，用于表示等待用户确认的完整邮件草稿。
                "draft_text": (confirmed_requirements or {}).get("__draft_text"),
            # 结束 `_AgentServiceCoreMixin._handle_skill_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 结束 `_AgentServiceCoreMixin._handle_skill_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceCoreMixin._handle_skill_intent()` 返回 `self._build_skill_response(`，并结束当前函数。
        return self._build_skill_response(
            # 使用关键字参数语法，把 `bundle` 传入 `bundle` 参数；它表示一个 Skill 的元数据和全部执行阶段。
            bundle=bundle,
            # 使用关键字参数语法，把 `result` 传入 `result` 参数；它表示当前查询、解析或业务调用产生的结果。
            result=result,
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用关键字参数语法，把 `selected_tool` 传入 `selected_tool` 参数；它表示当前意图对应的已注册工具规格。
            selected_tool=selected_tool,
            # 使用关键字参数语法，把 `phase_name` 传入 `phase_name` 参数；它表示本次准备执行的 Skill 阶段名称。
            phase_name=phase_name,
            # 使用关键字参数语法，把 `confirmed_requirements` 传入 `confirmed_requirements` 参数；它表示用户补充或确认后的结构化业务参数。
            confirmed_requirements=confirmed_requirements,
        # 结束 `_AgentServiceCoreMixin._handle_skill_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_build_skill_response()`。
    def _build_skill_response(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `bundle`，表示一个 Skill 的元数据和全部执行阶段，类型标注为 `AgentSkillBundle`。
        bundle: AgentSkillBundle,
        # 声明参数 `result`，表示当前查询、解析或业务调用产生的结果，类型标注为 `Dict[str, Any]`。
        result: Dict[str, Any],
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `Optional[str]`。
        route: Optional[str],
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
        # 声明参数 `phase_name`，表示本次准备执行的 Skill 阶段名称，类型标注为 `str`。
        phase_name: str,
        # 声明参数 `confirmed_requirements`，表示用户补充或确认后的结构化业务参数，类型标注为 `Optional[Dict[str, Any]]`，默认值为 `None`。
        confirmed_requirements: Optional[Dict[str, Any]] = None,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 调用 `result.get("steps") or []` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_steps`；它表示`raw_steps` 在当前函数中承载的中间业务值。
        raw_steps = result.get("steps") or []
        # 计算表达式 `[AgentStep.model_validate(step) for step in raw_steps]` 并把结果保存到 `steps`；该结果在后续流程中用作返回前端展示的 Agent 执行步骤。
        steps = [AgentStep.model_validate(step) for step in raw_steps]
        # 使用 `if` 判断 `steps and selected_tool`；条件成立时执行下面分支。
        if steps and selected_tool:
            # 计算表达式 `(` 并把结果保存到 `summary`；该结果在后续流程中用作`summary` 在当前函数中承载的中间业务值。
            summary = (
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "用户已确认 skill 所需信息，开始执行下一阶段。"
                # 使用 `if` 判断 `confirmed_requirements`；条件成立时执行下面分支。
                if confirmed_requirements
                # 完成条件表达式的另一分支 `else f"已选择 {bundle.bundle_name} skill，当前执行 {phase_name} 阶段。"`，根据条件选择本轮实际使用的步骤说明或业务值。
                else f"已选择 {bundle.bundle_name} skill，当前执行 {phase_name} 阶段。"
            # 结束 `_AgentServiceCoreMixin._build_skill_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 计算表达式 `[self._planning_step(selected_tool, summary), *steps]` 并把结果保存到 `steps`；该结果在后续流程中用作返回前端展示的 Agent 执行步骤。
            steps = [self._planning_step(selected_tool, summary), *steps]

        # 计算表达式 `[AgentArtifact.model_validate(artifact) for artifact in (result.get("artifacts") or [])]` 并把结果保存到 `artifacts`；该结果在后续流程中用作本轮生成的 JD、评分、面试方案等业务产物。
        artifacts = [AgentArtifact.model_validate(artifact) for artifact in (result.get("artifacts") or [])]
        # 从 `_AgentServiceCoreMixin._build_skill_response()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `str(result.get("message") or "")` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=str(result.get("message") or ""),
            # 使用关键字参数语法，把 `bundle.intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent=bundle.intent,
            # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=route,
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
            artifacts=artifacts,
            # 使用关键字参数语法，把 `[str(item) for item in (result.get("suggestions") or [])]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=[str(item) for item in (result.get("suggestions") or [])],
            # 使用关键字参数语法，把 `bool(result.get("requires_confirmation"))` 传入 `requires_confirmation` 参数；它表示后端是否暂停执行并要求用户人工确认。
            requires_confirmation=bool(result.get("requires_confirmation")),
            # 使用关键字参数语法，把 `[str(item) for item in (result.get("missing_fields") or [])]` 传入 `missing_fields` 参数；它表示继续执行工具前仍缺少的字段名称列表。
            missing_fields=[str(item) for item in (result.get("missing_fields") or [])],
        # 结束 `_AgentServiceCoreMixin._build_skill_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_stream_event()`。
    def _stream_event(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `event_type`，表示`event_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        event_type: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`。
        artifacts: List[AgentArtifact],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._stream_event()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `event_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": event_type,
            # 在字典中设置 `response` 字段为 `AgentChatResponse(`，用于表示下游服务或 Agent 流程返回的响应。
            "response": AgentChatResponse(
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `"jd"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="jd",
                # 使用关键字参数语法，把 `"/recruitment/jd-generator"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/jd-generator",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 完成 AgentStep 或 AgentArtifact 的 Pydantic 对象构造，再调用 `model_dump()` 转成普通字典，便于封装进 SSE JSON 事件。
            ).model_dump(),
        # 结束 `_AgentServiceCoreMixin._stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_format_resume_result_delta()`。
    def _format_resume_result_delta(self, result: Dict[str, Any], index: int, total: int) -> str:
        # 调用 `result.get("filename") or "未知文件"` 完成当前解析、查询或对象构造，并把返回值保存到 `filename`；它表示`filename` 在当前函数中承载的中间业务值。
        filename = result.get("filename") or "未知文件"
        # 调用 `result.get("name") or filename` 完成当前解析、查询或对象构造，并把返回值保存到 `name`；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
        name = result.get("name") or filename
        # 使用 `if` 判断 `result.get("status") == "failed"`；条件成立时执行下面分支。
        if result.get("status") == "failed":
            # 从 `_AgentServiceCoreMixin._format_resume_result_delta()` 返回 `f"{index}. {filename}：评分失败（{result.get('error') or '未知错误'}）\n\n"`，并结束当前函数。
            return f"{index}. {filename}：评分失败（{result.get('error') or '未知错误'}）\n\n"

        # 调用 `result.get("score") or 0` 完成当前解析、查询或对象构造，并把返回值保存到 `score`；它表示`score` 在当前函数中承载的中间业务值。
        score = result.get("score") or 0
        # 调用 `result.get("position") or "未识别岗位"` 完成当前解析、查询或对象构造，并把返回值保存到 `position`；它表示`position` 在当前函数中承载的中间业务值。
        position = result.get("position") or "未识别岗位"
        # 调用 `result.get("education") or "学历未识别"` 完成当前解析、查询或对象构造，并把返回值保存到 `education`；它表示`education` 在当前函数中承载的中间业务值。
        education = result.get("education") or "学历未识别"
        # 调用 `result.get("work_years")` 完成当前解析、查询或对象构造，并把返回值保存到 `work_years`；它表示`work_years` 在当前函数中承载的中间业务值。
        work_years = result.get("work_years")
        # 计算表达式 `f"{work_years} 年经验" if work_years not in (None, "", 0) else "经验未识别"` 并把结果保存到 `work_years_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        work_years_text = f"{work_years} 年经验" if work_years not in (None, "", 0) else "经验未识别"
        # 调用 `result.get("metrics") or []` 完成当前解析、查询或对象构造，并把返回值保存到 `metrics`；它表示`metrics` 在当前函数中承载的中间业务值。
        metrics = result.get("metrics") or []
        # 调用 `self._summarize_resume_metrics(metrics)` 完成当前解析、查询或对象构造，并把返回值保存到 `metric_summary`；它表示`metric_summary` 在当前函数中承载的中间业务值。
        metric_summary = self._summarize_resume_metrics(metrics)

        # 使用列表字面量开始构造 `lines`；该列表用于保存按换行拆开的文本行，下面各行会继续追加有顺序的业务元素。
        lines = [
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{index}. {name}（{filename}）",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"- 综合评分：{score} 分",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"- 候选信息：{position}，{education}，{work_years_text}",
        # 结束 `_AgentServiceCoreMixin._format_resume_result_delta` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `metric_summary`；条件成立时执行下面分支。
        if metric_summary:
            # 调用列表的 `append()`，把 `lines.append(f"- 评分摘要：{metric_summary}")` 对应的新元素追加到现有结果末尾。
            lines.append(f"- 评分摘要：{metric_summary}")
        # 调用列表的 `append()`，把 `lines.append("")` 对应的新元素追加到现有结果末尾。
        lines.append("")
        # 从 `_AgentServiceCoreMixin._format_resume_result_delta()` 返回 `"\n".join(lines)`，并结束当前函数。
        return "\n".join(lines)

    # 使用 `def` 普通函数 定义 `_summarize_resume_metrics()`。
    def _summarize_resume_metrics(self, metrics: List[Dict[str, Any]]) -> str:
        # 使用 `if` 判断 `not metrics`；条件成立时执行下面分支。
        if not metrics:
            # 从 `_AgentServiceCoreMixin._summarize_resume_metrics()` 返回 `""`，并结束当前函数。
            return ""
        # 把 `summaries` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`summaries` 在当前函数中承载的中间业务值。
        summaries = []
        # 使用 `for` 循环 `metric in metrics[:3]`，逐项处理集合数据。
        for metric in metrics[:3]:
            # 调用 `metric.get("name") or "指标"` 完成当前解析、查询或对象构造，并把返回值保存到 `name`；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
            name = metric.get("name") or "指标"
            # 调用 `metric.get("score")` 完成当前解析、查询或对象构造，并把返回值保存到 `score`；它表示`score` 在当前函数中承载的中间业务值。
            score = metric.get("score")
            # 调用 `metric.get("max")` 完成当前解析、查询或对象构造，并把返回值保存到 `max_score`；它表示`max_score` 在当前函数中承载的中间业务值。
            max_score = metric.get("max")
            # 计算表达式 `(metric.get("reason") or "").strip()` 并把结果保存到 `reason`；该结果在后续流程中用作`reason` 在当前函数中承载的中间业务值。
            reason = (metric.get("reason") or "").strip()
            # 计算表达式 `f"{score}/{max_score}" if max_score is not None else str(score)` 并把结果保存到 `score_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
            score_text = f"{score}/{max_score}" if max_score is not None else str(score)
            # 调用列表的 `append()`，把 `summaries.append(f"{name} {score_text}" + (f"（{reason[:40]}）" if reason else ""))` 对应的新元素追加到现有结果末尾。
            summaries.append(f"{name} {score_text}" + (f"（{reason[:40]}）" if reason else ""))
        # 从 `_AgentServiceCoreMixin._summarize_resume_metrics()` 返回 `"；".join(summaries)`，并结束当前函数。
        return "；".join(summaries)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_exam_content_stream()`。
    async def _generate_exam_content_stream(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam_service`，表示负责执行对应领域查询、生成、保存或外部调用的服务对象，类型标注为 `ExamService`。
        exam_service: ExamService,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> AsyncGenerator[str, None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[str, None]:
        # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
        full_text = ""
        # 调用 `self._build_exam_special_requirements(exam)` 完成当前解析、查询或对象构造，并把返回值保存到 `special_requirements`；它表示`special_requirements` 在当前函数中承载的中间业务值。
        special_requirements = self._build_exam_special_requirements(exam)
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `await` 异步调用 `exam_service.generate_exam(`，等待数据库、Dify 或模型处理完成后把结果保存到 `stream`；它表示`stream` 在当前函数中承载的中间业务值。
            stream = await exam_service.generate_exam(
                # 使用关键字参数语法，把 `exam["title"]` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title=exam["title"],
                # 使用关键字参数语法，把 `exam["subject"]` 传入 `subject` 参数；它表示待发送邮件的主题。
                subject=exam["subject"],
                # 使用关键字参数语法，把 `exam["total_score"]` 传入 `total_score` 参数；它表示`total_score` 在当前函数中承载的中间业务值。
                total_score=exam["total_score"],
                # 使用关键字参数语法，把 `str(user_id)` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=str(user_id),
                # 使用关键字参数语法，把 `exam.get("description")` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description=exam.get("description"),
                # 使用关键字参数语法，把 `exam.get("difficulty")` 传入 `difficulty` 参数；它表示`difficulty` 在当前函数中承载的中间业务值。
                difficulty=exam.get("difficulty"),
                # 使用关键字参数语法，把 `exam.get("duration")` 传入 `duration` 参数；它表示`duration` 在当前函数中承载的中间业务值。
                duration=exam.get("duration"),
                # 使用关键字参数语法，把 `exam.get("question_types")` 传入 `question_types` 参数；它表示`question_types` 在当前函数中承载的中间业务值。
                question_types=exam.get("question_types"),
                # 使用关键字参数语法，把 `exam.get("question_counts")` 传入 `question_counts` 参数；它表示`question_counts` 在当前函数中承载的中间业务值。
                question_counts=exam.get("question_counts"),
                # 使用关键字参数语法，把 `exam.get("knowledge_files")` 传入 `knowledge_files` 参数；它表示`knowledge_files` 在当前函数中承载的中间业务值。
                knowledge_files=exam.get("knowledge_files"),
                # 使用关键字参数语法，把 `special_requirements` 传入 `special_requirements` 参数；它表示`special_requirements` 在当前函数中承载的中间业务值。
                special_requirements=special_requirements,
                # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=None,
                # 使用关键字参数语法，把 `True` 传入 `stream` 参数；它表示`stream` 在当前函数中承载的中间业务值。
                stream=True,
            # 结束 `_AgentServiceCoreMixin._generate_exam_content_stream` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for chunk in stream:
                # 调用 `self._extract_stream_delta(self._normalize_sse_chunk(chunk))` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_delta`；它表示`raw_delta` 在当前函数中承载的中间业务值。
                raw_delta = self._extract_stream_delta(self._normalize_sse_chunk(chunk))
                # 调用 `self._dedupe_stream_delta(raw_delta, full_text)` 完成当前解析、查询或对象构造，并把返回值保存到 `delta`；它表示`delta` 在当前函数中承载的中间业务值。
                delta = self._dedupe_stream_delta(raw_delta, full_text)
                # 使用 `if` 判断 `not delta`；条件成立时执行下面分支。
                if not delta:
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 计算表达式 `delta` 并把结果保存到 `full_text +`；该结果在后续流程中用作`full_text +` 在当前函数中承载的中间业务值。
                full_text += delta
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield delta
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("Agent 试卷流式生成失败，回退同步生成: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 试卷流式生成失败，回退同步生成: %s", exc)

        # 使用 `if` 判断 `not full_text.strip()`；条件成立时执行下面分支。
        if not full_text.strip():
            # 使用 `await` 异步调用 `exam_service.generate_exam(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await exam_service.generate_exam(
                # 使用关键字参数语法，把 `exam["title"]` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title=exam["title"],
                # 使用关键字参数语法，把 `exam["subject"]` 传入 `subject` 参数；它表示待发送邮件的主题。
                subject=exam["subject"],
                # 使用关键字参数语法，把 `exam["total_score"]` 传入 `total_score` 参数；它表示`total_score` 在当前函数中承载的中间业务值。
                total_score=exam["total_score"],
                # 使用关键字参数语法，把 `str(user_id)` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=str(user_id),
                # 使用关键字参数语法，把 `exam.get("description")` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
                description=exam.get("description"),
                # 使用关键字参数语法，把 `exam.get("difficulty")` 传入 `difficulty` 参数；它表示`difficulty` 在当前函数中承载的中间业务值。
                difficulty=exam.get("difficulty"),
                # 使用关键字参数语法，把 `exam.get("duration")` 传入 `duration` 参数；它表示`duration` 在当前函数中承载的中间业务值。
                duration=exam.get("duration"),
                # 使用关键字参数语法，把 `exam.get("question_types")` 传入 `question_types` 参数；它表示`question_types` 在当前函数中承载的中间业务值。
                question_types=exam.get("question_types"),
                # 使用关键字参数语法，把 `exam.get("question_counts")` 传入 `question_counts` 参数；它表示`question_counts` 在当前函数中承载的中间业务值。
                question_counts=exam.get("question_counts"),
                # 使用关键字参数语法，把 `exam.get("knowledge_files")` 传入 `knowledge_files` 参数；它表示`knowledge_files` 在当前函数中承载的中间业务值。
                knowledge_files=exam.get("knowledge_files"),
                # 使用关键字参数语法，把 `special_requirements` 传入 `special_requirements` 参数；它表示`special_requirements` 在当前函数中承载的中间业务值。
                special_requirements=special_requirements,
                # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=None,
                # 使用关键字参数语法，把 `False` 传入 `stream` 参数；它表示`stream` 在当前函数中承载的中间业务值。
                stream=False,
            # 结束 `_AgentServiceCoreMixin._generate_exam_content_stream` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 调用 `self._extract_answer(result).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `fallback`；它表示`fallback` 在当前函数中承载的中间业务值。
            fallback = self._extract_answer(result).strip()
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self._stream_text(fallback):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield delta

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_missing_exam_questions()`。
    async def _generate_missing_exam_questions(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam_service`，表示负责执行对应领域查询、生成、保存或外部调用的服务对象，类型标注为 `ExamService`。
        exam_service: ExamService,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `existing_content`，表示当前业务资源的正文内容，供模型生成、保存或前端展示，类型标注为 `str`。
        existing_content: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> str` 标注该函数的返回值类型。
    ) -> str:
        # 调用 `self._missing_exam_question_counts(exam_service, exam, existing_content)` 完成当前解析、查询或对象构造，并把返回值保存到 `missing_counts`；它表示`missing_counts` 在当前函数中承载的中间业务值。
        missing_counts = self._missing_exam_question_counts(exam_service, exam, existing_content)
        # 使用 `if` 判断 `not any(missing_counts.values())`；条件成立时执行下面分支。
        if not any(missing_counts.values()):
            # 从 `_AgentServiceCoreMixin._generate_missing_exam_questions()` 返回 `""`，并结束当前函数。
            return ""

        # 通过模块日志记录器执行 `logger.warning("试卷题量不足，尝试补齐缺失题目: %s", missing_counts)`，用于保留运行状态或异常原因。
        logger.warning("试卷题量不足，尝试补齐缺失题目: %s", missing_counts)
        # 使用字典字面量开始构造 `repair_exam`；该字典用于保存`repair_exam` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        repair_exam = {
            # 使用 `**` 字典解包，把 `exam` 中的全部键值作为关键字参数传给当前函数，避免逐个重复传参。
            **exam,
            # 在字典中设置 `title` 字段为 `f"{exam.get('title') or '试卷'}补题"`，用于表示前端向用户展示的步骤或业务产物标题。
            "title": f"{exam.get('title') or '试卷'}补题",
            # 在字典中设置 `question_counts` 字段为 `missing_counts`，用于表示`question_counts` 在当前函数中承载的中间业务值。
            "question_counts": missing_counts,
            # 在字典中设置 `question_types` 字段为 `[key for key, value in missing_counts.items() if value > 0]`，用于表示`question_types` 在当前函数中承载的中间业务值。
            "question_types": [key for key, value in missing_counts.items() if value > 0],
            # 在字典中设置 `total_score` 字段为 `sum(`，用于表示`total_score` 在当前函数中承载的中间业务值。
            "total_score": sum(
                # 调用 `sum()`，执行 `_AgentServiceCoreMixin._generate_missing_exam_questions` 当前步骤所需的构造、查询或业务处理。
                sum(scores)
                # 使用 `for` 循环 `scores in self._missing_exam_question_scores(exam, existing_content, missing_counts).values()`，逐项处理集合数据。
                for scores in self._missing_exam_question_scores(exam, existing_content, missing_counts).values()
            # 结束前一个取值表达式，再用 `or` 按顺序选择备用值 `exam.get("total_score") or 100,`，保证缺少首选数据时仍有可用结果。
            ) or exam.get("total_score") or 100,
            # 在字典中设置 `special_requirements` 字段为 `"\n".join([`，用于表示`special_requirements` 在当前函数中承载的中间业务值。
            "special_requirements": "\n".join([
                # 调用 `self._clean_optional_value()`，执行 `_AgentServiceCoreMixin._generate_missing_exam_questions` 当前步骤所需的构造、查询或业务处理。
                self._clean_optional_value(exam.get("special_requirements")) or "",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "这是补题请求。只生成缺失题目，不要重复已生成题目。",
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "已生成题目如下，请避开相同题干和知识点：",
                # 把切片或索引结果 `existing_content[:3000]` 作为当前位置参数传入；业务上用于限制提示词长度或选取指定记录。
                existing_content[:3000],
            # 完成前面的文本拼接，并调用 `strip()` 删除首尾空白，得到可直接发送给模型或前端的干净内容。
            ]).strip(),
        # 结束 `_AgentServiceCoreMixin._generate_missing_exam_questions` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `await` 异步调用 `exam_service.generate_exam(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await exam_service.generate_exam(
            # 使用关键字参数语法，把 `repair_exam["title"]` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=repair_exam["title"],
            # 使用关键字参数语法，把 `repair_exam["subject"]` 传入 `subject` 参数；它表示待发送邮件的主题。
            subject=repair_exam["subject"],
            # 使用关键字参数语法，把 `repair_exam["total_score"]` 传入 `total_score` 参数；它表示`total_score` 在当前函数中承载的中间业务值。
            total_score=repair_exam["total_score"],
            # 使用关键字参数语法，把 `str(user_id)` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=str(user_id),
            # 使用关键字参数语法，把 `repair_exam.get("description")` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
            description=repair_exam.get("description"),
            # 使用关键字参数语法，把 `repair_exam.get("difficulty")` 传入 `difficulty` 参数；它表示`difficulty` 在当前函数中承载的中间业务值。
            difficulty=repair_exam.get("difficulty"),
            # 使用关键字参数语法，把 `repair_exam.get("duration")` 传入 `duration` 参数；它表示`duration` 在当前函数中承载的中间业务值。
            duration=repair_exam.get("duration"),
            # 使用关键字参数语法，把 `repair_exam.get("question_types")` 传入 `question_types` 参数；它表示`question_types` 在当前函数中承载的中间业务值。
            question_types=repair_exam.get("question_types"),
            # 使用关键字参数语法，把 `repair_exam.get("question_counts")` 传入 `question_counts` 参数；它表示`question_counts` 在当前函数中承载的中间业务值。
            question_counts=repair_exam.get("question_counts"),
            # 使用关键字参数语法，把 `repair_exam.get("knowledge_files")` 传入 `knowledge_files` 参数；它表示`knowledge_files` 在当前函数中承载的中间业务值。
            knowledge_files=repair_exam.get("knowledge_files"),
            # 使用关键字参数语法，把 `repair_exam.get("special_requirements")` 传入 `special_requirements` 参数；它表示`special_requirements` 在当前函数中承载的中间业务值。
            special_requirements=repair_exam.get("special_requirements"),
            # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=None,
            # 使用关键字参数语法，把 `False` 传入 `stream` 参数；它表示`stream` 在当前函数中承载的中间业务值。
            stream=False,
        # 结束 `_AgentServiceCoreMixin._generate_missing_exam_questions` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `self._extract_answer(result).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `repair_content`；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
        repair_content = self._extract_answer(result).strip()
        # 调用 `exam_service._parse_exam_content(repair_content)` 完成当前解析、查询或对象构造，并把返回值保存到 `repair_questions`；它表示`repair_questions` 在当前函数中承载的中间业务值。
        repair_questions = exam_service._parse_exam_content(repair_content)
        # 调用 `sum(missing_counts.values())` 完成当前解析、查询或对象构造，并把返回值保存到 `expected_repair_count`；它表示当前业务对象、题目或候选记录的数量。
        expected_repair_count = sum(missing_counts.values())
        # 使用 `if` 判断 `len(repair_questions) < expected_repair_count`；条件成立时执行下面分支。
        if len(repair_questions) < expected_repair_count:
            # 通过模块日志记录器执行 `logger.warning("试卷补题仍不足，期望 %s 题，实际 %s 题", expected_repair_count, len(repair_questions))`，用于保留运行状态或异常原因。
            logger.warning("试卷补题仍不足，期望 %s 题，实际 %s 题", expected_repair_count, len(repair_questions))
        # 从 `_AgentServiceCoreMixin._generate_missing_exam_questions()` 返回 `repair_content`，并结束当前函数。
        return repair_content

    # 使用 `def` 普通函数 定义 `_missing_exam_question_counts()`。
    def _missing_exam_question_counts(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam_service`，表示负责执行对应领域查询、生成、保存或外部调用的服务对象，类型标注为 `ExamService`。
        exam_service: ExamService,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `content`，表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据，类型标注为 `str`。
        content: str,
    # 结束函数参数列表，并用 `-> Dict[str, int]` 标注该函数的返回值类型。
    ) -> Dict[str, int]:
        # 使用字典字面量开始构造 `expected`；该字典用于保存`expected` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        expected = {
            # 在字典中设置 `single_choice` 字段为 `int((exam.get("question_counts") or {}).get("single_choice") or 0)`，用于表示`single_choice` 在当前函数中承载的中间业务值。
            "single_choice": int((exam.get("question_counts") or {}).get("single_choice") or 0),
            # 在字典中设置 `multiple_choice` 字段为 `int((exam.get("question_counts") or {}).get("multiple_choice") or 0)`，用于表示`multiple_choice` 在当前函数中承载的中间业务值。
            "multiple_choice": int((exam.get("question_counts") or {}).get("multiple_choice") or 0),
            # 在字典中设置 `short_answer` 字段为 `int((exam.get("question_counts") or {}).get("short_answer") or 0)`，用于表示`short_answer` 在当前函数中承载的中间业务值。
            "short_answer": int((exam.get("question_counts") or {}).get("short_answer") or 0),
        # 结束 `_AgentServiceCoreMixin._missing_exam_question_counts` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `if` 判断 `not any(expected.values())`；条件成立时执行下面分支。
        if not any(expected.values()):
            # 从 `_AgentServiceCoreMixin._missing_exam_question_counts()` 返回 `{"single_choice": 0, "multiple_choice": 0, "short_answer": 0}`，并结束当前函数。
            return {"single_choice": 0, "multiple_choice": 0, "short_answer": 0}

        # 计算表达式 `{"single_choice": 0, "multiple_choice": 0, "short_answer": 0}` 并把结果保存到 `actual`；该结果在后续流程中用作`actual` 在当前函数中承载的中间业务值。
        actual = {"single_choice": 0, "multiple_choice": 0, "short_answer": 0}
        # 使用字典字面量开始构造 `type_map`；该字典用于保存`type_map` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        type_map = {
            # 在字典中设置 `单选题` 字段为 `"single_choice"`，用于表示`单选题` 在当前函数中承载的中间业务值。
            "单选题": "single_choice",
            # 在字典中设置 `单选` 字段为 `"single_choice"`，用于表示`单选` 在当前函数中承载的中间业务值。
            "单选": "single_choice",
            # 在字典中设置 `多选题` 字段为 `"multiple_choice"`，用于表示`多选题` 在当前函数中承载的中间业务值。
            "多选题": "multiple_choice",
            # 在字典中设置 `多选` 字段为 `"multiple_choice"`，用于表示`多选` 在当前函数中承载的中间业务值。
            "多选": "multiple_choice",
            # 在字典中设置 `简答题` 字段为 `"short_answer"`，用于表示`简答题` 在当前函数中承载的中间业务值。
            "简答题": "short_answer",
            # 在字典中设置 `简答` 字段为 `"short_answer"`，用于表示`简答` 在当前函数中承载的中间业务值。
            "简答": "short_answer",
        # 结束 `_AgentServiceCoreMixin._missing_exam_question_counts` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `for` 循环 `question in exam_service._parse_exam_content(content)`，逐项处理集合数据。
        for question in exam_service._parse_exam_content(content):
            # 调用 `type_map.get(str(question.get("type") or "").strip())` 完成当前解析、查询或对象构造，并把返回值保存到 `key`；它表示`key` 在当前函数中承载的中间业务值。
            key = type_map.get(str(question.get("type") or "").strip())
            # 使用 `if` 判断 `key`；条件成立时执行下面分支。
            if key:
                # 计算表达式 `1` 并把结果保存到 `actual[key] +`；该结果在后续流程中用作`actual[key] +` 在当前函数中承载的中间业务值。
                actual[key] += 1

        # 从 `_AgentServiceCoreMixin._missing_exam_question_counts()` 返回 `{`，并结束当前函数。
        return {
            # 声明参数 `key`，表示`key` 在当前函数中承载的中间业务值，类型标注为 `max(0, expected.get(key, 0) - actual.get(key, 0))`。
            key: max(0, expected.get(key, 0) - actual.get(key, 0))
            # 使用 `for` 循环 `key in expected`，逐项处理集合数据。
            for key in expected
        # 结束 `_AgentServiceCoreMixin._missing_exam_question_counts` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_missing_exam_question_scores()`。
    def _missing_exam_question_scores(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `content`，表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据，类型标注为 `str`。
        content: str,
        # 声明参数 `missing_counts`，表示`missing_counts` 在当前函数中承载的中间业务值，类型标注为 `Dict[str, int]`。
        missing_counts: Dict[str, int],
    # 结束函数参数列表，并用 `-> Dict[str, List[int]]` 标注该函数的返回值类型。
    ) -> Dict[str, List[int]]:
        # 调用 `ExamService(self.db)._allocate_question_scores(` 完成当前解析、查询或对象构造，并把返回值保存到 `question_scores`；它表示`question_scores` 在当前函数中承载的中间业务值。
        question_scores = ExamService(self.db)._allocate_question_scores(
            # 调用 `int()`，执行 `_AgentServiceCoreMixin._missing_exam_question_scores` 当前步骤所需的构造、查询或业务处理。
            int(exam.get("total_score") or 100),
            # 调用 `exam.get()`，执行 `_AgentServiceCoreMixin._missing_exam_question_scores` 当前步骤所需的构造、查询或业务处理。
            exam.get("question_counts") or {},
        # 结束 `_AgentServiceCoreMixin._missing_exam_question_scores` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceCoreMixin._missing_exam_question_scores()` 返回 `{`，并结束当前函数。
        return {
            # 声明参数 `key`，表示`key` 在当前函数中承载的中间业务值，类型标注为 `values[-missing_counts[key]:] if missing_counts.get(key) else []`。
            key: values[-missing_counts[key]:] if missing_counts.get(key) else []
            # 使用 `for` 循环 `key, values in question_scores.items()`，逐项处理集合数据。
            for key, values in question_scores.items()
        # 结束 `_AgentServiceCoreMixin._missing_exam_question_scores` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_drain_exam_display_buffer()`。
    def _drain_exam_display_buffer(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `buffer`，表示`buffer` 在当前函数中承载的中间业务值，类型标注为 `str`。
        buffer: str,
        # 声明参数 `start_index`，表示当前元素在列表中的位置，用于更新同一条记录，类型标注为 `int`。
        start_index: int,
        # 声明参数 `force`，表示`force` 在当前函数中承载的中间业务值，类型标注为 `bool`，默认值为 `False`。
        force: bool = False,
    # 结束函数参数列表，并用 `-> tuple[str, str, int]` 标注该函数的返回值类型。
    ) -> tuple[str, str, int]:
        # 使用 `if` 判断 `not buffer`；条件成立时执行下面分支。
        if not buffer:
            # 从 `_AgentServiceCoreMixin._drain_exam_display_buffer()` 返回 `"", "", start_index`，并结束当前函数。
            return "", "", start_index

        # 调用 `buffer.split("***")` 完成当前解析、查询或对象构造，并把返回值保存到 `parts`；它表示按 SKILL.md 前置元数据分隔符切开的文本片段。
        parts = buffer.split("***")
        # 使用 `if` 判断 `force`；条件成立时执行下面分支。
        if force:
            # 计算表达式 `parts` 并把结果保存到 `complete_parts`；该结果在后续流程中用作`complete_parts` 在当前函数中承载的中间业务值。
            complete_parts = parts
            # 把 `remaining` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入`remaining` 在当前函数中承载的中间业务值。
            remaining = ""
        # 前一条件不成立时继续判断 `len(parts) <= 1`。
        elif len(parts) <= 1:
            # 从 `_AgentServiceCoreMixin._drain_exam_display_buffer()` 返回 `"", buffer, start_index`，并结束当前函数。
            return "", buffer, start_index
        # 前面的 `if/elif` 条件均不成立时进入该备用分支。
        else:
            # 计算表达式 `parts[:-1]` 并把结果保存到 `complete_parts`；该结果在后续流程中用作`complete_parts` 在当前函数中承载的中间业务值。
            complete_parts = parts[:-1]
            # 计算表达式 `parts[-1]` 并把结果保存到 `remaining`；该结果在后续流程中用作`remaining` 在当前函数中承载的中间业务值。
            remaining = parts[-1]

        # 声明参数 `markdown_parts`，表示`markdown_parts` 在当前函数中承载的中间业务值，类型标注为 `List[str]`，默认值为 `[]`。
        markdown_parts: List[str] = []
        # 计算表达式 `start_index` 并把结果保存到 `question_index`；该结果在后续流程中用作当前元素在列表中的位置，用于更新同一条记录。
        question_index = start_index
        # 使用 `for` 循环 `part in complete_parts`，逐项处理集合数据。
        for part in complete_parts:
            # 调用 `part.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `block`；它表示`block` 在当前函数中承载的中间业务值。
            block = part.strip()
            # 使用 `if` 判断 `not block`；条件成立时执行下面分支。
            if not block:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 调用 `self._format_exam_question_block(block, question_index)` 完成当前解析、查询或对象构造，并把返回值保存到 `markdown`；它表示`markdown` 在当前函数中承载的中间业务值。
            markdown = self._format_exam_question_block(block, question_index)
            # 调用列表的 `append()`，把 `markdown_parts.append(markdown)` 对应的新元素追加到现有结果末尾。
            markdown_parts.append(markdown)
            # 计算表达式 `1` 并把结果保存到 `question_index +`；该结果在后续流程中用作`question_index +` 在当前函数中承载的中间业务值。
            question_index += 1

        # 从 `_AgentServiceCoreMixin._drain_exam_display_buffer()` 返回 `"".join(markdown_parts), remaining, question_index`，并结束当前函数。
        return "".join(markdown_parts), remaining, question_index

    # 使用 `def` 普通函数 定义 `_format_exam_question_block()`。
    def _format_exam_question_block(self, block: str, question_index: int) -> str:
        # 计算表达式 `[part.strip() for part in block.split("|")]` 并把结果保存到 `parts`；该结果在后续流程中用作按 SKILL.md 前置元数据分隔符切开的文本片段。
        parts = [part.strip() for part in block.split("|")]
        # 使用 `if` 判断 `len(parts) != 6`；条件成立时执行下面分支。
        if len(parts) != 6:
            # 从 `_AgentServiceCoreMixin._format_exam_question_block()` 返回 `f"{block}\n\n"`，并结束当前函数。
            return f"{block}\n\n"

        # 计算表达式 `parts` 并把结果保存到 `question_text, question_type, options_text, correct_answers, score, explanation`；该结果在后续流程中用作`question_text, question_type, options_text, correct_answers, score, explanation` 在当前函数中承载的中间业务值。
        question_text, question_type, options_text, correct_answers, score, explanation = parts
        # 使用字典字面量开始构造 `question_type_label`；该字典用于保存`question_type_label` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        question_type_label = {
            # 在字典中设置 `single_choice` 字段为 `"单选题"`，用于表示`single_choice` 在当前函数中承载的中间业务值。
            "single_choice": "单选题",
            # 在字典中设置 `multiple_choice` 字段为 `"多选题"`，用于表示`multiple_choice` 在当前函数中承载的中间业务值。
            "multiple_choice": "多选题",
            # 在字典中设置 `short_answer` 字段为 `"简答题"`，用于表示`short_answer` 在当前函数中承载的中间业务值。
            "short_answer": "简答题",
            # 在字典中设置 `single` 字段为 `"单选题"`，用于表示`single` 在当前函数中承载的中间业务值。
            "single": "单选题",
            # 在字典中设置 `multiple` 字段为 `"多选题"`，用于表示`multiple` 在当前函数中承载的中间业务值。
            "multiple": "多选题",
            # 在字典中设置 `short` 字段为 `"简答题"`，用于表示`short` 在当前函数中承载的中间业务值。
            "short": "简答题",
        # 结束前面的业务类型映射字典，并调用 `get()` 按 `question_type, question_type` 取值；键不存在时使用本行给出的默认名称、路由或建议。
        }.get(question_type, question_type)
        # 使用列表字面量开始构造 `lines`；该列表用于保存按换行拆开的文本行，下面各行会继续追加有顺序的业务元素。
        lines = [
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"### {question_index}. {question_type_label}（{score}分）",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "",
            # 把变量 `question_text` 作为位置参数传入当前多行函数调用；它承载当前步骤整理后的文本，供解析、匹配或模型调用。
            question_text,
        # 结束 `_AgentServiceCoreMixin._format_exam_question_block` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `options_text`；条件成立时执行下面分支。
        if options_text:
            # 使用列表字面量开始构造 `option_items`；该列表用于保存`option_items` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
            option_items = [
                # 调用 `option.strip()`，执行 `_AgentServiceCoreMixin._format_exam_question_block` 当前步骤所需的构造、查询或业务处理。
                option.strip()
                # 使用 `for` 循环 `option in re.split(r"[;；]", options_text)`，逐项处理集合数据。
                for option in re.split(r"[;；]", options_text)
                # 使用 `if` 判断 `option.strip()`；条件成立时执行下面分支。
                if option.strip()
            # 结束 `_AgentServiceCoreMixin._format_exam_question_block` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
            # 使用 `if` 判断 `option_items`；条件成立时执行下面分支。
            if option_items:
                # 调用 `lines.extend()`，执行 `_AgentServiceCoreMixin._format_exam_question_block` 当前步骤所需的构造、查询或业务处理。
                lines.extend(["", "**选项：**"])
                # 使用 `for` 循环 `option_index, option in enumerate(option_items)`，逐项处理集合数据。
                for option_index, option in enumerate(option_items):
                    # 调用 `chr(65 + option_index)` 完成当前解析、查询或对象构造，并把返回值保存到 `label`；它表示`label` 在当前函数中承载的中间业务值。
                    label = chr(65 + option_index)
                    # 调用 `re.sub(r"^[A-ZＡ-Ｚ][\\.．、:：]\\s*", "", option)` 完成当前解析、查询或对象构造，并把返回值保存到 `option_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    option_text = re.sub(r"^[A-ZＡ-Ｚ][\\.．、:：]\\s*", "", option)
                    # 调用列表的 `append()`，把 `lines.append(f"- {label}. {option_text}")` 对应的新元素追加到现有结果末尾。
                    lines.append(f"- {label}. {option_text}")

        # 调用 `lines.extend()`，执行 `_AgentServiceCoreMixin._format_exam_question_block` 当前步骤所需的构造、查询或业务处理。
        lines.extend([
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"**参考答案：** {correct_answers or '略'}",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"**解析：** {explanation or '略'}",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "",
        # 结束 `_AgentServiceCoreMixin._format_exam_question_block` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ])
        # 从 `_AgentServiceCoreMixin._format_exam_question_block()` 返回 `"\n".join(lines)`，并结束当前函数。
        return "\n".join(lines)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_hydrate_exam_interview_context()`。
    async def _hydrate_exam_interview_context(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 使用 `if` 判断 `exam.get("interview_plan_context")`；条件成立时执行下面分支。
        if exam.get("interview_plan_context"):
            # 从 `_AgentServiceCoreMixin._hydrate_exam_interview_context()` 返回 `exam`，并结束当前函数。
            return exam
        # 计算表达式 `" ".join([` 并把结果保存到 `text`；该结果在后续流程中用作`text` 在当前函数中承载的中间业务值。
        text = " ".join([
            # 调用 `str()`，执行 `_AgentServiceCoreMixin._hydrate_exam_interview_context` 当前步骤所需的构造、查询或业务处理。
            str(exam.get("special_requirements") or ""),
            # 调用 `str()`，执行 `_AgentServiceCoreMixin._hydrate_exam_interview_context` 当前步骤所需的构造、查询或业务处理。
            str(exam.get("description") or ""),
            # 调用 `str()`，执行 `_AgentServiceCoreMixin._hydrate_exam_interview_context` 当前步骤所需的构造、查询或业务处理。
            str(exam.get("title") or ""),
        # 结束 `_AgentServiceCoreMixin._hydrate_exam_interview_context` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ])
        # 使用 `if` 判断 `not self._is_followup_interview_exam_request(text)`；条件成立时执行下面分支。
        if not self._is_followup_interview_exam_request(text):
            # 从 `_AgentServiceCoreMixin._hydrate_exam_interview_context()` 返回 `exam`，并结束当前函数。
            return exam
        # 使用 `await` 异步调用 `self._resolve_interview_exam_followup(text, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `context`；它表示传给 Skill 阶段函数的运行上下文。
        context = await self._resolve_interview_exam_followup(text, user_id, conversation_id)
        # 使用 `if` 判断 `not context.get("matched")`；条件成立时执行下面分支。
        if not context.get("matched"):
            # 从 `_AgentServiceCoreMixin._hydrate_exam_interview_context()` 返回 `exam`，并结束当前函数。
            return exam
        # 从 `_AgentServiceCoreMixin._hydrate_exam_interview_context()` 返回 `{`，并结束当前函数。
        return {
            # 使用 `**` 字典解包，把 `exam` 中的全部键值作为关键字参数传给当前函数，避免逐个重复传参。
            **exam,
            # 在字典中设置 `interview_plan_context` 字段为 `context`，用于表示当前业务步骤需要的历史、用户或 Skill 上下文。
            "interview_plan_context": context,
        # 结束 `_AgentServiceCoreMixin._hydrate_exam_interview_context` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_build_exam_special_requirements()`。
    def _build_exam_special_requirements(self, exam: Dict[str, Any]) -> str:
        # 调用 `self._clean_optional_value(exam.get("special_requirements")) or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `base_requirements`；它表示`base_requirements` 在当前函数中承载的中间业务值。
        base_requirements = self._clean_optional_value(exam.get("special_requirements")) or ""
        # 调用 `exam.get("interview_plan_context") if isinstance(exam.get("interview_plan_context"), dict) else None` 完成当前解析、查询或对象构造，并把返回值保存到 `context`；它表示传给 Skill 阶段函数的运行上下文。
        context = exam.get("interview_plan_context") if isinstance(exam.get("interview_plan_context"), dict) else None
        # 使用 `if` 判断 `not context`；条件成立时执行下面分支。
        if not context:
            # 从 `_AgentServiceCoreMixin._build_exam_special_requirements()` 返回 `base_requirements`，并结束当前函数。
            return base_requirements

        # 调用 `self._clean_optional_value(context.get("content")) or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `context_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
        context_text = self._clean_optional_value(context.get("content")) or ""
        # 调用 `self._clean_optional_value(context.get("title")) or "当前面试方案"` 完成当前解析、查询或对象构造，并把返回值保存到 `context_title`；它表示`context_title` 在当前函数中承载的中间业务值。
        context_title = self._clean_optional_value(context.get("title")) or "当前面试方案"
        # 把 `parts` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入按 SKILL.md 前置元数据分隔符切开的文本片段。
        parts = []
        # 使用 `if` 判断 `base_requirements`；条件成立时执行下面分支。
        if base_requirements:
            # 调用列表的 `append()`，把 `parts.append(base_requirements)` 对应的新元素追加到现有结果末尾。
            parts.append(base_requirements)
        # 调用列表的 `append()`，把 `parts.append(` 对应的新元素追加到现有结果末尾。
        parts.append(
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "请按 8:2 配比生成笔试试卷：约 80% 题目来自上传文档知识点，约 20% 题目来自面试方案中的能力验证点、风险点或追问方向。"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "文档题要依据上传文档；面试方案导向题可以设计情境题或简答题，但要尽量结合文档业务背景。"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "不要把面试流程、面试安排、候选人评价本身直接改写成题目。"
        # 结束 `_AgentServiceCoreMixin._build_exam_special_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `context_text`；条件成立时执行下面分支。
        if context_text:
            # 调用列表的 `append()`，把 `parts.append(f"当前面试方案《{context_title}》（仅作为出题方向和权重参考，不作为题目事实来源）：\n{context_text[:1500]}")` 对应的新元素追加到现有结果末尾。
            parts.append(f"当前面试方案《{context_title}》（仅作为出题方向和权重参考，不作为题目事实来源）：\n{context_text[:1500]}")
        # 从 `_AgentServiceCoreMixin._build_exam_special_requirements()` 返回 `"\n\n".join(parts)`，并结束当前函数。
        return "\n\n".join(parts)

    # 使用 `def` 普通函数 定义 `_tool_stream_event()`。
    def _tool_stream_event(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `event_type`，表示`event_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        event_type: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `intent`，表示识别出的业务意图标签，类型标注为 `str`。
        intent: str,
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `str`。
        route: str,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `artifacts`，表示本轮生成的 JD、评分、面试方案等业务产物，类型标注为 `List[AgentArtifact]`。
        artifacts: List[AgentArtifact],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._tool_stream_event()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `event_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": event_type,
            # 在字典中设置 `response` 字段为 `AgentChatResponse(`，用于表示下游服务或 Agent 流程返回的响应。
            "response": AgentChatResponse(
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent=intent,
                # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route,
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `artifacts` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=artifacts,
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 完成 AgentStep 或 AgentArtifact 的 Pydantic 对象构造，再调用 `model_dump()` 转成普通字典，便于封装进 SSE JSON 事件。
            ).model_dump(),
        # 结束 `_AgentServiceCoreMixin._tool_stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_resume_stream_event()`。
    def _resume_stream_event(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `event_type`，表示`event_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        event_type: str,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `results`，表示`results` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`。
        results: List[Dict[str, Any]],
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._resume_stream_event()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `event_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": event_type,
            # 在字典中设置 `response` 字段为 `AgentChatResponse(`，用于表示下游服务或 Agent 流程返回的响应。
            "response": AgentChatResponse(
                # 使用关键字参数语法，把 `message` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=message,
                # 使用关键字参数语法，把 `"resume_screening"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resume_screening",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceCoreMixin._resume_stream_event` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"resume_screening_results"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="resume_screening_results",
                        # 使用关键字参数语法，把 `"批量简历筛选结果"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="批量简历筛选结果",
                        # 使用关键字参数语法，把 `results` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content=results,
                    # 结束 `_AgentServiceCoreMixin._resume_stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束列表推导式，并用条件 `results else [],` 过滤空值，确保后续模型提示词只包含有效业务内容。
                ] if results else [],
                # 使用关键字参数语法，把 `[]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[],
            # 完成 AgentStep 或 AgentArtifact 的 Pydantic 对象构造，再调用 `model_dump()` 转成普通字典，便于封装进 SSE JSON 事件。
            ).model_dump(),
        # 结束 `_AgentServiceCoreMixin._resume_stream_event` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_build_conversation_memory()`。按 conversation_id 与 user_id 读取最近消息，额外多取记录后过滤当前请求，限制单条与总字符数并拼成短期上下文。
    async def _build_conversation_memory(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `current_message`，表示`current_message` 在当前函数中承载的中间业务值，类型标注为 `str`。
        current_message: str,
        # 声明参数 `limit`，表示`limit` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `12`。
        limit: int = 12,
        # 声明参数 `max_chars`，表示`max_chars` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `6000`。
        max_chars: int = 6000,
    # 结束函数参数列表，并用 `-> str` 标注该函数的返回值类型。
    ) -> str:
        """
        会话记忆
        _build_conversation_memory()  根据 conversation_id 读取最近消息，拼成文本记忆。这个记忆主要用于：
            ReAct 决策。
            普通聊天。
            JD 字段解析。
            候选人指代解析。
            “基于当前面试方案生成试卷”。
            删除“刚才生成的内容”。
        会限制条数和字符数，避免 prompt 太长。

        """

        # 使用 `if` 判断 `not conversation_id`；条件成立时执行下面分支。
        if not conversation_id:
            # 从 `_AgentServiceCoreMixin._build_conversation_memory()` 返回 `""`，并结束当前函数。
            return ""
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `UUID(str(conversation_id))` 完成当前解析、查询或对象构造，并把返回值保存到 `conversation_uuid`；它表示`conversation_uuid` 在当前函数中承载的中间业务值。
            conversation_uuid = UUID(str(conversation_id))
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception:
            # 从 `_AgentServiceCoreMixin._build_conversation_memory()` 返回 `""`，并结束当前函数。
            return ""

        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `conversation_result`；它表示当前业务调用完成后返回的单个处理结果。
            conversation_result = await self.db.execute(
                # 调用 `select()`，执行 `_AgentServiceCoreMixin._build_conversation_memory` 当前步骤所需的构造、查询或业务处理。
                select(Conversation).where(
                    # 构造 SQLAlchemy 查询条件 `Conversation.id == conversation_uuid`，把数据库记录限制为符合该业务字段要求的数据。
                    Conversation.id == conversation_uuid,
                    # 构造 SQLAlchemy 查询条件 `Conversation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
                    Conversation.user_id == user_id,
                # 结束 `_AgentServiceCoreMixin._build_conversation_memory` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 结束 `_AgentServiceCoreMixin._build_conversation_memory` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 使用 `if` 判断 `not conversation_result.scalar_one_or_none()`；条件成立时执行下面分支。
            if not conversation_result.scalar_one_or_none():
                # 从 `_AgentServiceCoreMixin._build_conversation_memory()` 返回 `""`，并结束当前函数。
                return ""

            # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await self.db.execute(
                # 调用 `select()`，执行 `_AgentServiceCoreMixin._build_conversation_memory` 当前步骤所需的构造、查询或业务处理。
                select(Message)
                # 在 SQLAlchemy 查询链中调用 `where()`，应用 `Message.conversation_id == conversation_uuid` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
                .where(Message.conversation_id == conversation_uuid)
                # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `Message.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
                .order_by(Message.created_at.desc())
                # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit + 3` 条记录，避免会话记忆或候选资源查询无限增长。
                .limit(limit + 3)
            # 结束 `_AgentServiceCoreMixin._build_conversation_memory` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 调用 `list(reversed(result.scalars().all()))` 完成当前解析、查询或对象构造，并把返回值保存到 `messages`；它表示`messages` 在当前函数中承载的中间业务值。
            messages = list(reversed(result.scalars().all()))
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("读取 Agent 对话记忆失败: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("读取 Agent 对话记忆失败: %s", exc)
            # 从 `_AgentServiceCoreMixin._build_conversation_memory()` 返回 `""`，并结束当前函数。
            return ""

        # 调用 `self._normalize_memory_text(current_message)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized_current`；它表示已统一字段格式、便于后续比较或保存的数据。
        normalized_current = self._normalize_memory_text(current_message)
        # 声明参数 `memory_lines`，表示`memory_lines` 在当前函数中承载的中间业务值，类型标注为 `List[str]`，默认值为 `[]`。
        memory_lines: List[str] = []
        # 使用 `for` 循环 `item in messages`，逐项处理集合数据。
        for item in messages:
            # 调用 `self._normalize_memory_text(item.content)` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content = self._normalize_memory_text(item.content)
            # 使用 `if` 判断 `not content`；条件成立时执行下面分支。
            if not content:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 `if` 判断 `item.role == MessageRole.USER and normalized_current and content.startswith(normalized_current)`；条件成立时执行下面分支。
            if item.role == MessageRole.USER and normalized_current and content.startswith(normalized_current):
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用关键字参数语法把 `"用户" if item.role == MessageRole.USER else "助手"` 传给 `role_label`；该参数表示`role_label` 在当前函数中承载的中间业务值。
            role_label = "用户" if item.role == MessageRole.USER else "助手"
            # 使用 `if` 判断 `item.role == MessageRole.SYSTEM`；条件成立时执行下面分支。
            if item.role == MessageRole.SYSTEM:
                # 计算表达式 `"系统"` 并把结果保存到 `role_label`；该结果在后续流程中用作`role_label` 在当前函数中承载的中间业务值。
                role_label = "系统"
            # 调用列表的 `append()`，把 `memory_lines.append(f"{role_label}: {content[:900]}")` 对应的新元素追加到现有结果末尾。
            memory_lines.append(f"{role_label}: {content[:900]}")

        # 计算表达式 `"\n".join(memory_lines)` 并把结果保存到 `memory`；该结果在后续流程中用作`memory` 在当前函数中承载的中间业务值。
        memory = "\n".join(memory_lines)
        # 使用 `if` 判断 `len(memory) > max_chars`；条件成立时执行下面分支。
        if len(memory) > max_chars:
            # 计算表达式 `memory[-max_chars:]` 并把结果保存到 `memory`；该结果在后续流程中用作`memory` 在当前函数中承载的中间业务值。
            memory = memory[-max_chars:]
        # 从 `_AgentServiceCoreMixin._build_conversation_memory()` 返回 `memory.strip()`，并结束当前函数。
        return memory.strip()

    # 使用 `def` 普通函数 定义 `_normalize_memory_text()`。
    def _normalize_memory_text(self, text: Optional[str]) -> str:
        # 使用 `if` 判断 `not text`；条件成立时执行下面分支。
        if not text:
            # 从 `_AgentServiceCoreMixin._normalize_memory_text()` 返回 `""`，并结束当前函数。
            return ""
        # 调用 `re.sub(r"\s+", " ", str(text)).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized`；它表示`normalized` 在当前函数中承载的中间业务值。
        normalized = re.sub(r"\s+", " ", str(text)).strip()
        # 从 `_AgentServiceCoreMixin._normalize_memory_text()` 返回 `normalized`，并结束当前函数。
        return normalized

    # 使用 `def` 普通函数 定义 `_format_memory_for_prompt()`。
    def _format_memory_for_prompt(self, memory_context: str) -> str:
        # 使用 `if` 判断 `not memory_context`；条件成立时执行下面分支。
        if not memory_context:
            # 从 `_AgentServiceCoreMixin._format_memory_for_prompt()` 返回 `""`，并结束当前函数。
            return ""
        # 从 `_AgentServiceCoreMixin._format_memory_for_prompt()` 返回 `(`，并结束当前函数。
        return (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "历史对话记忆（仅用于理解上下文和指代，不要逐字复述）：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{memory_context}\n\n"
        # 结束 `_AgentServiceCoreMixin._format_memory_for_prompt` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_build_memory_augmented_prompt()`。
    def _build_memory_augmented_prompt(self, message: str, memory_context: str) -> str:
        # 使用 `if` 判断 `not memory_context`；条件成立时执行下面分支。
        if not memory_context:
            # 从 `_AgentServiceCoreMixin._build_memory_augmented_prompt()` 返回 `message`，并结束当前函数。
            return message
        # 从 `_AgentServiceCoreMixin._build_memory_augmented_prompt()` 返回 `(`，并结束当前函数。
        return (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "请结合以下历史对话记忆回答当前用户消息。"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果用户使用“继续、这个、刚才、上一个”等指代，请从历史中解析；"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果历史无关，不要强行引用。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"当前用户消息：{message}"
        # 结束 `_AgentServiceCoreMixin._build_memory_augmented_prompt` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_resolve_interview_candidate()`。结合用户文字、短期记忆和最近简历评分记录定位候选人；多匹配时返回选择要求。
    async def _resolve_interview_candidate(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`，默认值为 `""`。
        memory_context: str = "",
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 使用 `await` 异步调用 `self._recent_resume_candidates(user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `candidates`；它表示根据用户描述找到、等待进一步选择的候选资源列表。
        candidates = await self._recent_resume_candidates(user_id)
        # 计算表达式 `"\n".join(` 并把结果保存到 `candidate_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        candidate_text = "\n".join(
            # 开始 `_AgentServiceCoreMixin._resolve_interview_candidate` 中的多行函数调用或对象构造，后续缩进行传入具体参数。
            (
                # 计算表达式 `{item['id']}；姓名={item['name'] or '未提取'}；"` 并把结果保存到 `f"- id`；该结果在后续流程中用作`f"- id` 在当前函数中承载的中间业务值。
                f"- id={item['id']}；姓名={item['name'] or '未提取'}；"
                # 计算表达式 `{item['filename']}；岗位={item['position'] or '未提及'}；"` 并把结果保存到 `f"文件`；该结果在后续流程中用作`f"文件` 在当前函数中承载的中间业务值。
                f"文件={item['filename']}；岗位={item['position'] or '未提及'}；"
                # 计算表达式 `{item['score'] if item['score'] is not None else '未评分'}"` 并把结果保存到 `f"分数`；该结果在后续流程中用作`f"分数` 在当前函数中承载的中间业务值。
                f"分数={item['score'] if item['score'] is not None else '未评分'}"
            # 结束 `_AgentServiceCoreMixin._resolve_interview_candidate` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
            # 使用 `for` 循环 `item in candidates`，逐项处理集合数据。
            for item in candidates
        # 结束前一个取值表达式，再用 `or` 按顺序选择备用值 `"无已评分候选人"`，保证缺少首选数据时仍有可用结果。
        ) or "无已评分候选人"
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是 HR Agent 的候选人解析器。请判断用户是否指定了要生成面试计划的候选人，"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "并在已评分候选人列表中匹配。只返回 JSON，不要解释。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "规则：\n"
            # 计算表达式 `no_specific。\n"` 并把结果保存到 `"1. 如果用户没有明确候选人姓名/文件名/“他、这位、刚才那位”等指代，返回 status`；该结果在后续流程中用作` 如果用户没有明确候选人姓名/文件名/“他、这位、刚才那位”等指代，返回 status` 在当前函数中承载的中间业务值。
            "1. 如果用户没有明确候选人姓名/文件名/“他、这位、刚才那位”等指代，返回 status=no_specific。\n"
            # 计算表达式 `matched 和 candidate_id。\n"` 并把结果保存到 `"2. 如果用户指定了候选人，并且候选人列表里唯一匹配，返回 status`；该结果在后续流程中用作` 如果用户指定了候选人，并且候选人列表里唯一匹配，返回 status` 在当前函数中承载的中间业务值。
            "2. 如果用户指定了候选人，并且候选人列表里唯一匹配，返回 status=matched 和 candidate_id。\n"
            # 计算表达式 `no_match，并在 reply 中说明未找到该候选人，提醒上传他的简历并先评分。\n"` 并把结果保存到 `"3. 如果用户指定了候选人，但候选人列表没有匹配，返回 status`；该结果在后续流程中用作` 如果用户指定了候选人，但候选人列表没有匹配，返回 status` 在当前函数中承载的中间业务值。
            "3. 如果用户指定了候选人，但候选人列表没有匹配，返回 status=no_match，并在 reply 中说明未找到该候选人，提醒上传他的简历并先评分。\n"
            # 计算表达式 `ambiguous，并给出 candidates。\n"` 并把结果保存到 `"4. 如果匹配多个候选人，返回 status`；该结果在后续流程中用作` 如果匹配多个候选人，返回 status` 在当前函数中承载的中间业务值。
            "4. 如果匹配多个候选人，返回 status=ambiguous，并给出 candidates。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "5. reply 要自然、简洁，不能假装已找到不存在的候选人。\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "返回 JSON 字段：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "{\n"
            # 在字典中设置 `  \"status\` 字段为 `\"no_specific/matched/no_match/ambiguous\",\n"`，用于表示`  \"status\` 在当前函数中承载的中间业务值。
            "  \"status\": \"no_specific/matched/no_match/ambiguous\",\n"
            # 在字典中设置 `  \"requested_name\` 字段为 `\"用户指定的人名或null\",\n"`，用于表示`  \"requested_name\` 在当前函数中承载的中间业务值。
            "  \"requested_name\": \"用户指定的人名或null\",\n"
            # 在字典中设置 `  \"candidate_id\` 字段为 `\"匹配到的id或null\",\n"`，用于表示`  \"candidate_id\` 在当前函数中承载的中间业务值。
            "  \"candidate_id\": \"匹配到的id或null\",\n"
            # 在字典中设置 `  \"candidate_name\` 字段为 `\"匹配到的姓名或null\",\n"`，用于表示`  \"candidate_name\` 在当前函数中承载的中间业务值。
            "  \"candidate_name\": \"匹配到的姓名或null\",\n"
            # 在字典中设置 `  \"candidates\` 字段为 `[],\n"`，用于表示`  \"candidates\` 在当前函数中承载的中间业务值。
            "  \"candidates\": [],\n"
            # 在字典中设置 `  \"reply\` 字段为 `\"给用户的自然回复或null\"\n"`，用于表示`  \"reply\` 在当前函数中承载的中间业务值。
            "  \"reply\": \"给用户的自然回复或null\"\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"当前用户消息：{message}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"已评分候选人列表：\n{candidate_text}"
        # 结束 `_AgentServiceCoreMixin._resolve_interview_candidate` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
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
            # 从 `_AgentServiceCoreMixin._resolve_interview_candidate()` 返回 `self._normalize_candidate_resolution(parsed, candidates, message)`，并结束当前函数。
            return self._normalize_candidate_resolution(parsed, candidates, message)
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("解析面试计划候选人失败，使用本地规则兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("解析面试计划候选人失败，使用本地规则兜底: %s", exc)
            # 从 `_AgentServiceCoreMixin._resolve_interview_candidate()` 返回 `self._fallback_candidate_resolution(message, candidates)`，并结束当前函数。
            return self._fallback_candidate_resolution(message, candidates)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_resume_candidates()`。
    async def _recent_resume_candidates(self, user_id: UUID, limit: int = 50) -> List[Dict[str, Any]]:
        # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(
            # 调用 `select()`，执行 `_AgentServiceCoreMixin._recent_resume_candidates` 当前步骤所需的构造、查询或业务处理。
            select(ResumeEvaluation)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `ResumeEvaluation.user_id == user_id` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(ResumeEvaluation.user_id == user_id)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `ResumeEvaluation.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(ResumeEvaluation.created_at.desc())
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
        # 结束 `_AgentServiceCoreMixin._recent_resume_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 把 `candidates` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入根据用户描述找到、等待进一步选择的候选资源列表。
        candidates = []
        # 使用 `for` 循环 `resume in result.scalars().all()`，逐项处理集合数据。
        for resume in result.scalars().all():
            # 调用列表的 `append()`，把 `candidates.append({` 对应的新元素追加到现有结果末尾。
            candidates.append({
                # 在字典中设置 `id` 字段为 `str(resume.id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                "id": str(resume.id),
                # 在字典中设置 `name` 字段为 `resume.candidate_name`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                "name": resume.candidate_name,
                # 在字典中设置 `filename` 字段为 `resume.original_filename`，用于表示`filename` 在当前函数中承载的中间业务值。
                "filename": resume.original_filename,
                # 在字典中设置 `position` 字段为 `resume.candidate_position`，用于表示`position` 在当前函数中承载的中间业务值。
                "position": resume.candidate_position,
                # 在字典中设置 `score` 字段为 `resume.total_score`，用于表示`score` 在当前函数中承载的中间业务值。
                "score": resume.total_score,
            # 结束 `_AgentServiceCoreMixin._recent_resume_candidates` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            })
        # 从 `_AgentServiceCoreMixin._recent_resume_candidates()` 返回 `candidates`，并结束当前函数。
        return candidates

    # 使用 `def` 普通函数 定义 `_normalize_candidate_resolution()`。
    def _normalize_candidate_resolution(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `candidates`，表示根据用户描述找到、等待进一步选择的候选资源列表，类型标注为 `List[Dict[str, Any]]`。
        candidates: List[Dict[str, Any]],
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 调用 `str(parsed.get("status") or "").strip().lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `status`；它表示步骤当前状态，例如 pending、running、completed 或 failed。
        status = str(parsed.get("status") or "").strip().lower()
        # 调用 `self._clean_optional_value(parsed.get("candidate_id"))` 完成当前解析、查询或对象构造，并把返回值保存到 `candidate_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
        candidate_id = self._clean_optional_value(parsed.get("candidate_id"))
        # 计算表达式 `{item["id"]: item for item in candidates}` 并把结果保存到 `candidate_lookup`；该结果在后续流程中用作`candidate_lookup` 在当前函数中承载的中间业务值。
        candidate_lookup = {item["id"]: item for item in candidates}
        # 使用 `if` 判断 `candidate_id and candidate_id in candidate_lookup`；条件成立时执行下面分支。
        if candidate_id and candidate_id in candidate_lookup:
            # 计算表达式 `candidate_lookup[candidate_id]` 并把结果保存到 `matched`；该结果在后续流程中用作`matched` 在当前函数中承载的中间业务值。
            matched = candidate_lookup[candidate_id]
            # 从 `_AgentServiceCoreMixin._normalize_candidate_resolution()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `status` 字段为 `"matched"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                "status": "matched",
                # 在字典中设置 `candidate_id` 字段为 `candidate_id`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "candidate_id": candidate_id,
                # 在字典中设置 `candidate_name` 字段为 `matched.get("name") or matched.get("filename")`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                "candidate_name": matched.get("name") or matched.get("filename"),
                # 在字典中设置 `requested_name` 字段为 `self._clean_optional_value(parsed.get("requested_name"))`，用于表示`requested_name` 在当前函数中承载的中间业务值。
                "requested_name": self._clean_optional_value(parsed.get("requested_name")),
                # 在字典中设置 `reply` 字段为 `self._clean_optional_value(parsed.get("reply"))`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": self._clean_optional_value(parsed.get("reply")),
            # 结束 `_AgentServiceCoreMixin._normalize_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 使用 `if` 判断 `status in {"no_match", "ambiguous", "no_specific"}`；条件成立时执行下面分支。
        if status in {"no_match", "ambiguous", "no_specific"}:
            # 从 `_AgentServiceCoreMixin._normalize_candidate_resolution()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `status` 字段为 `status`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                "status": status,
                # 在字典中设置 `requested_name` 字段为 `self._clean_optional_value(parsed.get("requested_name"))`，用于表示`requested_name` 在当前函数中承载的中间业务值。
                "requested_name": self._clean_optional_value(parsed.get("requested_name")),
                # 在字典中设置 `reply` 字段为 `self._clean_optional_value(parsed.get("reply"))`，用于表示`reply` 在当前函数中承载的中间业务值。
                "reply": self._clean_optional_value(parsed.get("reply")),
                # 在字典中设置 `candidates` 字段为 `parsed.get("candidates") if isinstance(parsed.get("candidates"), list) else []`，用于表示根据用户描述找到、等待进一步选择的候选资源列表。
                "candidates": parsed.get("candidates") if isinstance(parsed.get("candidates"), list) else [],
            # 结束 `_AgentServiceCoreMixin._normalize_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 从 `_AgentServiceCoreMixin._normalize_candidate_resolution()` 返回 `self._fallback_candidate_resolution(message, candidates)`，并结束当前函数。
        return self._fallback_candidate_resolution(message, candidates)

    # 使用 `def` 普通函数 定义 `_fallback_candidate_resolution()`。
    def _fallback_candidate_resolution(self, message: str, candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
        # 调用 `self._normalize_candidate_match_text(message)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized_message`；它表示已统一字段格式、便于后续比较或保存的数据。
        normalized_message = self._normalize_candidate_match_text(message)
        # 使用列表字面量开始构造 `matches`；该列表用于保存`matches` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        matches = [
            # 使用列表推导式遍历 `item for item in candidates`，逐条筛选或转换候选业务资源，结果会汇总进当前列表。
            item for item in candidates
            # 使用 `if` 判断 `any(`；条件成立时执行下面分支。
            if any(
                # 在列表推导式或条件表达式中组合判断 `name and (name in normalized_message or normalized_message in name)`；只有名称存在且与用户消息相互包含时，才把该候选资源视为匹配项。
                name and (name in normalized_message or normalized_message in name)
                # 使用 `for` 循环 `name in [`，逐项处理集合数据。
                for name in [
                    # 调用 `self._normalize_candidate_match_text()`，执行 `_AgentServiceCoreMixin._fallback_candidate_resolution` 当前步骤所需的构造、查询或业务处理。
                    self._normalize_candidate_match_text(item.get("name")),
                    # 调用 `self._normalize_candidate_match_text()`，执行 `_AgentServiceCoreMixin._fallback_candidate_resolution` 当前步骤所需的构造、查询或业务处理。
                    self._normalize_candidate_match_text(item.get("filename")),
                # 结束 `_AgentServiceCoreMixin._fallback_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ]
            # 结束 `_AgentServiceCoreMixin._fallback_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `_AgentServiceCoreMixin._fallback_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `len(matches) == 1`；条件成立时执行下面分支。
        if len(matches) == 1:
            # 从 `_AgentServiceCoreMixin._fallback_candidate_resolution()` 返回 `{`，并结束当前函数。
            return {
                # 在字典中设置 `status` 字段为 `"matched"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
                "status": "matched",
                # 在字典中设置 `candidate_id` 字段为 `matches[0]["id"]`，用于表示用于关联对应数据库记录或业务资源的唯一标识。
                "candidate_id": matches[0]["id"],
                # 在字典中设置 `candidate_name` 字段为 `matches[0].get("name") or matches[0].get("filename")`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                "candidate_name": matches[0].get("name") or matches[0].get("filename"),
            # 结束 `_AgentServiceCoreMixin._fallback_candidate_resolution` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
        # 使用 `if` 判断 `len(matches) > 1`；条件成立时执行下面分支。
        if len(matches) > 1:
            # 从 `_AgentServiceCoreMixin._fallback_candidate_resolution()` 返回 `{"status": "ambiguous", "candidates": matches[:5]}`，并结束当前函数。
            return {"status": "ambiguous", "candidates": matches[:5]}
        # 调用 `bool(re.search(r"生成(.{1,20}?)(?:的)?面试|(.{1,20}?)(?:的)?面试(?:计划|方案)", message))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_specific_name`；它表示`has_specific_name` 在当前函数中承载的中间业务值。
        has_specific_name = bool(re.search(r"生成(.{1,20}?)(?:的)?面试|(.{1,20}?)(?:的)?面试(?:计划|方案)", message))
        # 从 `_AgentServiceCoreMixin._fallback_candidate_resolution()` 返回 `{"status": "no_match" if has_specific_name else "no_specific", "requested_name": None}`，并结束当前函数。
        return {"status": "no_match" if has_specific_name else "no_specific", "requested_name": None}

    # 使用 `def` 普通函数 定义 `_normalize_candidate_match_text()`。
    def _normalize_candidate_match_text(self, text: Optional[str]) -> str:
        # 从 `_AgentServiceCoreMixin._normalize_candidate_match_text()` 返回 `re.sub(r"[\s_\-()[\]【】（）.。·、,，]+", "", str(text or "").lower())`，并结束当前函数。
        return re.sub(r"[\s_\-()[\]【】（）.。·、,，]+", "", str(text or "").lower())

    # 使用 `def` 普通函数 定义 `_is_followup_resume_screening_loop_request()`。
    def _is_followup_resume_screening_loop_request(self, message: str) -> bool:
        # 从 `_AgentServiceCoreMixin._is_followup_resume_screening_loop_request()` 返回 `bool(`，并结束当前函数。
        return bool(
            # 调用 `re.search()`，执行 `_AgentServiceCoreMixin._is_followup_resume_screening_loop_request` 当前步骤所需的构造、查询或业务处理。
            re.search(r"高分|通过|合格|大于\s*60|超过\s*60|60\s*分以上|分数高|低分|不合格|小于\s*60|低于\s*60", message)
            # 使用 `and` 追加正则匹配条件 `re.search(r"面试|方案|计划|删除|淘汰|候选人|简历", message)`；只有用户消息也命中这些业务关键词时，整个意图判断才成立。
            and re.search(r"面试|方案|计划|删除|淘汰|候选人|简历", message)
        # 结束 `_AgentServiceCoreMixin._is_followup_resume_screening_loop_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_is_followup_interview_exam_request()`。
    def _is_followup_interview_exam_request(self, message: str) -> bool:
        # 从 `_AgentServiceCoreMixin._is_followup_interview_exam_request()` 返回 `bool(`，并结束当前函数。
        return bool(
            # 调用 `re.search()`，执行 `_AgentServiceCoreMixin._is_followup_interview_exam_request` 当前步骤所需的构造、查询或业务处理。
            re.search(r"当前|这个|该|刚才|刚生成|上一个|面试方案|面试计划", message)
            # 使用 `and` 追加正则匹配条件 `re.search(r"试卷|考试|笔试|出题|题目|测评", message)`；只有用户消息也命中这些业务关键词时，整个意图判断才成立。
            and re.search(r"试卷|考试|笔试|出题|题目|测评", message)
        # 结束 `_AgentServiceCoreMixin._is_followup_interview_exam_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_build_resume_screening_loop_summary()`。
    def _build_resume_screening_loop_summary(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `results`，表示`results` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`。
        results: List[Dict[str, Any]],
        # 声明参数 `threshold`，表示`threshold` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `60`。
        threshold: int = 60,
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 使用关键字参数语法把 `[item for item in results if item.get("status") != "failed"]` 传给 `completed`；该参数表示`completed` 在当前函数中承载的中间业务值。
        completed = [item for item in results if item.get("status") != "failed"]
        # 使用关键字参数语法把 `[item for item in completed if float(item.get("score") or 0) >= threshold]` 传给 `qualified`；该参数表示`qualified` 在当前函数中承载的中间业务值。
        qualified = [item for item in completed if float(item.get("score") or 0) >= threshold]
        # 计算表达式 `[item for item in completed if float(item.get("score") or 0) < threshold]` 并把结果保存到 `low_scores`；该结果在后续流程中用作`low_scores` 在当前函数中承载的中间业务值。
        low_scores = [item for item in completed if float(item.get("score") or 0) < threshold]
        # 计算表达式 `qualified[:3]` 并把结果保存到 `top_candidates`；该结果在后续流程中用作`top_candidates` 在当前函数中承载的中间业务值。
        top_candidates = qualified[:3]

        # 把 `message_parts` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`message_parts` 在当前函数中承载的中间业务值。
        message_parts = []
        # 使用 `if` 判断 `qualified`；条件成立时执行下面分支。
        if qualified:
            # 计算表达式 `"、".join(self._resume_candidate_label(item) for item in top_candidates)` 并把结果保存到 `names`；该结果在后续流程中用作`names` 在当前函数中承载的中间业务值。
            names = "、".join(self._resume_candidate_label(item) for item in top_candidates)
            # 调用列表的 `append()`，把 `message_parts.append(f"{len(qualified)} 位候选人达到 {threshold} 分及以上（{names}），建议进入面试计划阶段。")` 对应的新元素追加到现有结果末尾。
            message_parts.append(f"{len(qualified)} 位候选人达到 {threshold} 分及以上（{names}），建议进入面试计划阶段。")
        # 使用 `if` 判断 `low_scores`；条件成立时执行下面分支。
        if low_scores:
            # 计算表达式 `"、".join(self._resume_candidate_label(item) for item in low_scores[:3])` 并把结果保存到 `names`；该结果在后续流程中用作`names` 在当前函数中承载的中间业务值。
            names = "、".join(self._resume_candidate_label(item) for item in low_scores[:3])
            # 调用列表的 `append()`，把 `message_parts.append(f"{len(low_scores)} 位候选人低于 {threshold} 分（{names}），建议标记淘汰或删除记录前先人工复核。")` 对应的新元素追加到现有结果末尾。
            message_parts.append(f"{len(low_scores)} 位候选人低于 {threshold} 分（{names}），建议标记淘汰或删除记录前先人工复核。")

        # 把 `suggestions` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入任务结束后提供给用户的下一步操作建议。
        suggestions = []
        # 使用 `if` 判断 `qualified`；条件成立时执行下面分支。
        if qualified:
            # 调用列表的 `append()`，把 `suggestions.append("为高分候选人生成面试计划")` 对应的新元素追加到现有结果末尾。
            suggestions.append("为高分候选人生成面试计划")
        # 使用 `if` 判断 `low_scores`；条件成立时执行下面分支。
        if low_scores:
            # 调用列表的 `append()`，把 `suggestions.append("删除低分候选人的简历记录")` 对应的新元素追加到现有结果末尾。
            suggestions.append("删除低分候选人的简历记录")
        # 调用 `suggestions.extend()`，执行 `_AgentServiceCoreMixin._build_resume_screening_loop_summary` 当前步骤所需的构造、查询或业务处理。
        suggestions.extend(["查看完整评分详情", "继续上传更多简历"])

        # 从 `_AgentServiceCoreMixin._build_resume_screening_loop_summary()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `threshold` 字段为 `threshold`，用于表示`threshold` 在当前函数中承载的中间业务值。
            "threshold": threshold,
            # 在字典中设置 `qualified_candidates` 字段为 `qualified`，用于表示`qualified_candidates` 在当前函数中承载的中间业务值。
            "qualified_candidates": qualified,
            # 在字典中设置 `low_score_candidates` 字段为 `low_scores`，用于表示`low_score_candidates` 在当前函数中承载的中间业务值。
            "low_score_candidates": low_scores,
            # 在字典中设置 `message_suffix` 字段为 `("\n\n下一步建议：" + " ".join(message_parts)) if message_parts else ""`，用于表示`message_suffix` 在当前函数中承载的中间业务值。
            "message_suffix": ("\n\n下一步建议：" + " ".join(message_parts)) if message_parts else "",
            # 在字典中设置 `suggestions` 字段为 `suggestions[:4]`，用于表示任务结束后提供给用户的下一步操作建议。
            "suggestions": suggestions[:4],
        # 结束 `_AgentServiceCoreMixin._build_resume_screening_loop_summary` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_resume_candidate_label()`。
    def _resume_candidate_label(self, item: Dict[str, Any]) -> str:
        # 调用 `item.get("name") or item.get("filename") or "候选人"` 完成当前解析、查询或对象构造，并把返回值保存到 `name`；它表示附件原始名称，供 Agent 判断文件类型并在页面展示。
        name = item.get("name") or item.get("filename") or "候选人"
        # 调用 `item.get("score")` 完成当前解析、查询或对象构造，并把返回值保存到 `score`；它表示`score` 在当前函数中承载的中间业务值。
        score = item.get("score")
        # 从 `_AgentServiceCoreMixin._resume_candidate_label()` 返回 `f"{name} {score}分" if score is not None else str(name)`，并结束当前函数。
        return f"{name} {score}分" if score is not None else str(name)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_resolve_resume_screening_followup_action()`。
    async def _resolve_resume_screening_followup_action(
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
        # 使用 `if` 判断 `not self._is_followup_resume_screening_loop_request(message)`；条件成立时执行下面分支。
        if not self._is_followup_resume_screening_loop_request(message):
            # 从 `_AgentServiceCoreMixin._resolve_resume_screening_followup_action()` 返回 `{"action": None}`，并结束当前函数。
            return {"action": None}
        # 使用 `await` 异步调用 `self._recent_resume_screening_groups(conversation_id, user_id, threshold=60)`，等待数据库、Dify 或模型处理完成后把结果保存到 `groups`；它表示`groups` 在当前函数中承载的中间业务值。
        groups = await self._recent_resume_screening_groups(conversation_id, user_id, threshold=60)
        # 使用 `if` 判断 `not groups.get("has_context")`；条件成立时执行下面分支。
        if not groups.get("has_context"):
            # 从 `_AgentServiceCoreMixin._resolve_resume_screening_followup_action()` 返回 `{"action": "missing_context"}`，并结束当前函数。
            return {"action": "missing_context"}
        # 使用 `if` 判断 `re.search(r"删除|删掉|移除|清理|淘汰", message) and re.search(r"低分|不合格|小于\s*60|低于\s*60|没通过|未通过", message)`；条件成立时执行下面分支。
        if re.search(r"删除|删掉|移除|清理|淘汰", message) and re.search(r"低分|不合格|小于\s*60|低于\s*60|没通过|未通过", message):
            # 从 `_AgentServiceCoreMixin._resolve_resume_screening_followup_action()` 返回 `{"action": "confirm_delete_low_scores", **groups}`，并结束当前函数。
            return {"action": "confirm_delete_low_scores", **groups}
        # 使用 `if` 判断 `re.search(r"面试|方案|计划", message) and re.search(r"高分|通过|合格|大于\s*60|超过\s*60|60\s*分以上|分数高", message)`；条件成立时执行下面分支。
        if re.search(r"面试|方案|计划", message) and re.search(r"高分|通过|合格|大于\s*60|超过\s*60|60\s*分以上|分数高", message):
            # 从 `_AgentServiceCoreMixin._resolve_resume_screening_followup_action()` 返回 `{"action": "generate_interview", **groups}`，并结束当前函数。
            return {"action": "generate_interview", **groups}
        # 从 `_AgentServiceCoreMixin._resolve_resume_screening_followup_action()` 返回 `{"action": None}`，并结束当前函数。
        return {"action": None}

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_resume_screening_groups()`。
    async def _recent_resume_screening_groups(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `threshold`，表示`threshold` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `60`。
        threshold: int = 60,
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 使用 `await` 异步调用 `self._recent_generated_resources("resume", user_id, conversation_id, limit=20)`，等待数据库、Dify 或模型处理完成后把结果保存到 `resources`；它表示`resources` 在当前函数中承载的中间业务值。
        resources = await self._recent_generated_resources("resume", user_id, conversation_id, limit=20)
        # 把 `candidate_ids` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入用于批量关联数据库记录或业务资源的唯一标识集合。
        candidate_ids = []
        # 使用 `for` 循环 `item in resources`，逐项处理集合数据。
        for item in resources:
            # 使用 `if` 判断 `item.get("id") and item["id"] not in candidate_ids`；条件成立时执行下面分支。
            if item.get("id") and item["id"] not in candidate_ids:
                # 调用列表的 `append()`，把 `candidate_ids.append(item["id"])` 对应的新元素追加到现有结果末尾。
                candidate_ids.append(item["id"])

        # 使用 `if` 判断 `not candidate_ids`；条件成立时执行下面分支。
        if not candidate_ids:
            # 从 `_AgentServiceCoreMixin._recent_resume_screening_groups()` 返回 `{"has_context": False, "qualified_candidates": [], "low_score_candidates": []}`，并结束当前函数。
            return {"has_context": False, "qualified_candidates": [], "low_score_candidates": []}

        # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(
            # 调用 `select()`，执行 `_AgentServiceCoreMixin._recent_resume_screening_groups` 当前步骤所需的构造、查询或业务处理。
            select(ResumeEvaluation).where(
                # 构造 SQLAlchemy 查询条件 `ResumeEvaluation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
                ResumeEvaluation.user_id == user_id,
                # 调用 `ResumeEvaluation.id.in_()`，执行 `_AgentServiceCoreMixin._recent_resume_screening_groups` 当前步骤所需的构造、查询或业务处理。
                ResumeEvaluation.id.in_([UUID(item) for item in candidate_ids]),
            # 结束 `_AgentServiceCoreMixin._recent_resume_screening_groups` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `_AgentServiceCoreMixin._recent_resume_screening_groups` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `list(result.scalars().all())` 完成当前解析、查询或对象构造，并把返回值保存到 `evaluations`；它表示`evaluations` 在当前函数中承载的中间业务值。
        evaluations = list(result.scalars().all())
        # 计算表达式 `{candidate_id: index for index, candidate_id in enumerate(candidate_ids)}` 并把结果保存到 `order_map`；该结果在后续流程中用作`order_map` 在当前函数中承载的中间业务值。
        order_map = {candidate_id: index for index, candidate_id in enumerate(candidate_ids)}
        # 计算表达式 `lambda item: order_map.get(str(item.id), 999))` 并把结果保存到 `evaluations.sort(key`；该结果在后续流程中用作`sort(key` 在当前函数中承载的中间业务值。
        evaluations.sort(key=lambda item: order_map.get(str(item.id), 999))

        # 把 `qualified` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`qualified` 在当前函数中承载的中间业务值。
        qualified = []
        # 把 `low_scores` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`low_scores` 在当前函数中承载的中间业务值。
        low_scores = []
        # 使用 `for` 循环 `evaluation in evaluations`，逐项处理集合数据。
        for evaluation in evaluations:
            # 调用 `self._resume_evaluation_to_candidate_dict(evaluation)` 完成当前解析、查询或对象构造，并把返回值保存到 `item`；它表示`item` 在当前函数中承载的中间业务值。
            item = self._resume_evaluation_to_candidate_dict(evaluation)
            # 使用 `if` 判断 `float(evaluation.total_score or 0) >= threshold`；条件成立时执行下面分支。
            if float(evaluation.total_score or 0) >= threshold:
                # 调用列表的 `append()`，把 `qualified.append(item)` 对应的新元素追加到现有结果末尾。
                qualified.append(item)
            # 前面的 `if/elif` 条件均不成立时进入该备用分支。
            else:
                # 调用列表的 `append()`，把 `low_scores.append(item)` 对应的新元素追加到现有结果末尾。
                low_scores.append(item)
        # 从 `_AgentServiceCoreMixin._recent_resume_screening_groups()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `has_context` 字段为 `True`，用于表示当前业务步骤需要的历史、用户或 Skill 上下文。
            "has_context": True,
            # 在字典中设置 `threshold` 字段为 `threshold`，用于表示`threshold` 在当前函数中承载的中间业务值。
            "threshold": threshold,
            # 在字典中设置 `qualified_candidates` 字段为 `qualified`，用于表示`qualified_candidates` 在当前函数中承载的中间业务值。
            "qualified_candidates": qualified,
            # 在字典中设置 `low_score_candidates` 字段为 `low_scores`，用于表示`low_score_candidates` 在当前函数中承载的中间业务值。
            "low_score_candidates": low_scores,
        # 结束 `_AgentServiceCoreMixin._recent_resume_screening_groups` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_resume_evaluation_to_candidate_dict()`。
    def _resume_evaluation_to_candidate_dict(self, evaluation: ResumeEvaluation) -> Dict[str, Any]:
        # 从 `_AgentServiceCoreMixin._resume_evaluation_to_candidate_dict()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `id` 字段为 `str(evaluation.id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
            "id": str(evaluation.id),
            # 在字典中设置 `name` 字段为 `evaluation.candidate_name or evaluation.original_filename`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
            "name": evaluation.candidate_name or evaluation.original_filename,
            # 在字典中设置 `filename` 字段为 `evaluation.original_filename`，用于表示`filename` 在当前函数中承载的中间业务值。
            "filename": evaluation.original_filename,
            # 在字典中设置 `score` 字段为 `evaluation.total_score or 0`，用于表示`score` 在当前函数中承载的中间业务值。
            "score": evaluation.total_score or 0,
            # 在字典中设置 `position` 字段为 `evaluation.candidate_position`，用于表示`position` 在当前函数中承载的中间业务值。
            "position": evaluation.candidate_position,
            # 在字典中设置 `education` 字段为 `evaluation.education_level`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": evaluation.education_level,
            # 在字典中设置 `status` 字段为 `evaluation.status.value if evaluation.status else None`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
            "status": evaluation.status.value if evaluation.status else None,
        # 结束 `_AgentServiceCoreMixin._resume_evaluation_to_candidate_dict` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_build_resume_screening_followup_response()`。
    def _build_resume_screening_followup_response(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `action`，表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段，类型标注为 `Dict[str, Any]`。
        action: Dict[str, Any],
        # 声明参数 `route`，表示该意图对应的前端页面路径，供前端跳转或展示下一步入口，类型标注为 `Optional[str]`。
        route: Optional[str],
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 调用 `action.get("threshold") or 60` 完成当前解析、查询或对象构造，并把返回值保存到 `threshold`；它表示`threshold` 在当前函数中承载的中间业务值。
        threshold = action.get("threshold") or 60
        # 调用 `action.get("qualified_candidates") or []` 完成当前解析、查询或对象构造，并把返回值保存到 `qualified`；它表示`qualified` 在当前函数中承载的中间业务值。
        qualified = action.get("qualified_candidates") or []
        # 调用 `action.get("low_score_candidates") or []` 完成当前解析、查询或对象构造，并把返回值保存到 `low_scores`；它表示`low_scores` 在当前函数中承载的中间业务值。
        low_scores = action.get("low_score_candidates") or []

        # 使用 `if` 判断 `action.get("action") == "generate_interview"`；条件成立时执行下面分支。
        if action.get("action") == "generate_interview":
            # 使用 `if` 判断 `not qualified`；条件成立时执行下面分支。
            if not qualified:
                # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"最近这批评分里没有达到 {threshold} 分及以上的候选人，我不建议直接生成面试计划。你可以重新评分、调整 JD，或指定某个候选人继续生成。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"最近这批评分里没有达到 {threshold} 分及以上的候选人，我不建议直接生成面试计划。你可以重新评分、调整 JD，或指定某个候选人继续生成。",
                    # 使用关键字参数语法，把 `"interview_plan"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent="interview_plan",
                    # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route,
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "识别到基于评分结果推进面试计划的请求。"),
                        # 计算表达式 `"check_scores", title="检查高分候选人", status="failed", detail=f"未找到 {threshold} 分及以上候选人。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="check_scores", title="检查高分候选人", status="failed", detail=f"未找到 {threshold} 分及以上候选人。"),
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["重新筛选简历", "指定候选人生成面试计划", "调整评分标准"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["重新筛选简历", "指定候选人生成面试计划", "调整评分标准"],
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 使用 `if` 判断 `len(qualified) == 1`；条件成立时执行下面分支。
            if len(qualified) == 1:
                # 计算表达式 `qualified[0]` 并把结果保存到 `candidate`；该结果在后续流程中用作`candidate` 在当前函数中承载的中间业务值。
                candidate = qualified[0]
                # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"最近评分结果里，{candidate['name']} 达到 {candidate['score']} 分，符合进入面试的阈值。我已准备好直接生成他的面试计划。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"最近评分结果里，{candidate['name']} 达到 {candidate['score']} 分，符合进入面试的阈值。我已准备好直接生成他的面试计划。",
                    # 使用关键字参数语法，把 `"interview_plan"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent="interview_plan",
                    # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route=route,
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "用户要求为高分候选人生成面试计划。"),
                        # 计算表达式 `"select_resume", title="锁定高分候选人", status="completed", detail=self._resume_candidate_label(candida...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="select_resume", title="锁定高分候选人", status="completed", detail=self._resume_candidate_label(candidate)),
                        # 计算表达式 `"generate_plan", title="生成面试计划", status="running", detail="正在调用面试计划工具。", tool="generate_interview...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="generate_plan", title="生成面试计划", status="running", detail="正在调用面试计划工具。", tool="generate_interview_plan"),
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                    artifacts=[
                        # 调用 `AgentArtifact()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                        AgentArtifact(
                            # 使用关键字参数语法，把 `"interview_plan_execute"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                            type="interview_plan_execute",
                            # 使用关键字参数语法，把 `"直接生成面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                            title="直接生成面试计划",
                            # 使用关键字参数语法，把 `{"resume_evaluation_id": candidate["id"], "candidate_name": candidate["name"]}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                            content={"resume_evaluation_id": candidate["id"], "candidate_name": candidate["name"]},
                        # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        )
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["生成面试计划", "查看该候选人评分", "删除低分候选人"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["生成面试计划", "查看该候选人评分", "删除低分候选人"],
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                message=(
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    f"最近这批有 {len(qualified)} 位候选人达到 {threshold} 分及以上。为避免一次生成太多方案，"
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    "请先选择要推进的候选人；如果你明确说“全部生成”，我再逐个生成。"
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 使用关键字参数语法，把 `"interview_plan"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="interview_plan",
                # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=route,
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(selected_tool, "用户要求为高分候选人生成面试计划，但存在多个候选人。"),
                    # 计算表达式 `"select_resume", title="选择高分候选人", status="running", detail=f"候选人数量：{len(qualified)}"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="select_resume", title="选择高分候选人", status="running", detail=f"候选人数量：{len(qualified)}"),
                    # 计算表达式 `"generate_plan", title="生成面试计划", status="pending", tool="generate_interview_plan"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="generate_plan", title="生成面试计划", status="pending", tool="generate_interview_plan"),
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"interview_plan_request"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="interview_plan_request",
                        # 使用关键字参数语法，把 `"选择高分候选人生成面试计划"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="选择高分候选人生成面试计划",
                        # 使用关键字参数语法，把 `{"requires_resume_evaluation": True, "candidates": qualified}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"requires_resume_evaluation": True, "candidates": qualified},
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `[f"生成{qualified[0]['name']}的面试计划", "全部生成面试计划", "先查看评分详情"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=[f"生成{qualified[0]['name']}的面试计划", "全部生成面试计划", "先查看评分详情"],
            # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `action.get("action") == "confirm_delete_low_scores"`；条件成立时执行下面分支。
        if action.get("action") == "confirm_delete_low_scores":
            # 使用 `if` 判断 `not low_scores`；条件成立时执行下面分支。
            if not low_scores:
                # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `f"最近这批评分里没有低于 {threshold} 分的候选人，暂时不需要删除低分简历记录。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message=f"最近这批评分里没有低于 {threshold} 分的候选人，暂时不需要删除低分简历记录。",
                    # 使用关键字参数语法，把 `"resume_screening"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent="resume_screening",
                    # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route="/recruitment/resume-screening",
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(self.tool_registry.get("resume_screening"), "识别到低分候选人处理请求。"),
                        # 计算表达式 `"check_scores", title="检查低分候选人", status="completed", detail="没有低分候选人。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="check_scores", title="检查低分候选人", status="completed", detail="没有低分候选人。"),
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["为高分候选人生成面试计划", "继续上传简历", "查看评分详情"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["为高分候选人生成面试计划", "继续上传简历", "查看评分详情"],
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 计算表达式 `"、".join(self._resume_candidate_label(item) for item in low_scores[:5])` 并把结果保存到 `names`；该结果在后续流程中用作`names` 在当前函数中承载的中间业务值。
            names = "、".join(self._resume_candidate_label(item) for item in low_scores[:5])
            # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                message=(
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    f"我找到了 {len(low_scores)} 位低于 {threshold} 分的候选人：{names}。"
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    "为避免误删，我不会自动删除；如果确认删除，请回复“确认删除这些低分候选人”。"
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resource_delete",
                # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route="/recruitment/resume-screening",
                # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                steps=[
                    # 调用 `self._planning_step()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                    self._planning_step(self.tool_registry.get("resource_delete"), "识别到低分候选人删除建议。"),
                    # 计算表达式 `"locate_resource", title="定位低分候选人", status="completed", detail=f"匹配到 {len(low_scores)} 位低分候选人。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="locate_resource", title="定位低分候选人", status="completed", detail=f"匹配到 {len(low_scores)} 位低分候选人。"),
                    # 计算表达式 `"delete_resource", title="等待确认删除", status="pending", detail="用户确认后再删除。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                    AgentStep(id="delete_resource", title="等待确认删除", status="pending", detail="用户确认后再删除。"),
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"delete_resource_candidates"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="delete_resource_candidates",
                        # 使用关键字参数语法，把 `"低分候选人待确认"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="低分候选人待确认",
                        # 使用关键字参数语法，把 `{"resource_type": "resume", "keyword": "低分候选人", "candidates": low_scores}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"resource_type": "resume", "keyword": "低分候选人", "candidates": low_scores},
                    # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `["确认删除这些低分候选人", "暂不删除，查看评分详情", "为高分候选人生成面试计划"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["确认删除这些低分候选人", "暂不删除，查看评分详情", "为高分候选人生成面试计划"],
            # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 从 `_AgentServiceCoreMixin._build_resume_screening_followup_response()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `"我还没有找到最近一轮简历评分结果。请先完成简历评分，再让我继续生成面试计划或处理低分候选人。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message="我还没有找到最近一轮简历评分结果。请先完成简历评分，再让我继续生成面试计划或处理低分候选人。",
            # 使用关键字参数语法，把 `"resume_screening"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="resume_screening",
            # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route="/recruitment/resume-screening",
            # 按识别出的意图从注册表取得对应工具规格，供前置条件检查和步骤展示使用。
            steps=[self._planning_step(self.tool_registry.get("resume_screening"), "缺少可用于闭环推进的评分上下文。")],
            # 使用关键字参数语法，把 `["上传简历评分", "选择 JD 后开始评分", "查看历史评分"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=["上传简历评分", "选择 JD 后开始评分", "查看历史评分"],
        # 结束 `_AgentServiceCoreMixin._build_resume_screening_followup_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
