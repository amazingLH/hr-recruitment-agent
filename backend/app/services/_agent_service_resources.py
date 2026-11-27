"""
* 文件作用
  * 这是 Agent 的业务资源与生成工具 Mixin。它负责解析 JD/考试需求、选择知识文档、调用 Dify 流式生成 JD、评分标准和面试方案、保存业务对象、规范化模型输出，以及查询和删除 JD、简历评分、面试方案与试卷。删除流程先定位资源并要求确认，再调用具体 Service，避免模型直接操作任意数据库记录。
* 类说明
  * `_AgentServiceResourcesMixin`：实现业务资源生成、查询、规范化、保存与受控删除。
* 主要函数与方法说明
  * `_AgentServiceResourcesMixin._handle_resource_delete()`：解析资源类型和目标，先返回确认信息；确认后校验归属并调用对应 Service 删除。
  * `_AgentServiceResourcesMixin._parse_requirements()`：调用模型把自然语言岗位需求解析为结构化字段，失败时用正则与默认值兜底。
  * `_AgentServiceResourcesMixin._parse_exam_requirements()`：把自然语言考试需求转换为标题、科目、题型数量、总分、时长和特殊要求。
  * `_AgentServiceResourcesMixin._generate_jd_stream()`：以 Dify type=1 流式生成 JD，并清洗重复 SSE 分片后向上游产出。
  * `_AgentServiceResourcesMixin._generate_interview_plan_stream()`：以 Dify type=4 根据候选人简历、评分和 JD 流式生成面试方案。
  * `_AgentServiceResourcesMixin._generate_scoring_criteria_stream()`：以 Dify type=2 根据 JD 流式生成评分标准。
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
    资源管理、内容生成和数据规范化.
"""

# 使用 `class` 定义 `_AgentServiceResourcesMixin`；该类的业务作用是：实现业务资源生成、查询、规范化、保存与受控删除。
class _AgentServiceResourcesMixin:

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_handle_resource_delete()`。解析资源类型和目标，先返回确认信息；确认后校验归属并调用对应 Service 删除。
    async def _handle_resource_delete(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `selected_tool`，表示当前意图对应的已注册工具规格，类型标注为 `Optional[AgentToolSpec]`。
        selected_tool: Optional[AgentToolSpec],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        """
        资源删除的流程是：
            1._parse_delete_request() 用 LLM 解析删除目标。
            2.解析失败时 _fallback_parse_delete_request() 用正则兜底。
            3.如果用户说“删除刚才那个”“删除最新的”，会先查最近对话上下文。
            4.如果没有上下文，就从对应业务列表查询可删除资源。
            5.如果匹配多个，要求用户说得更具体。
            6.如果唯一匹配，调用 _delete_resource_by_type()  执行删除。

            这里比较关键的是 _recent_generated_resources() ：
                会从历史 assistant 消息的 context.agent_response.artifacts 里找最近生成的产物，因此才能理解“刚才那个”。
        """

        # 使用 `await` 异步调用 `self._parse_delete_request(message)`，等待数据库、Dify 或模型处理完成后把结果保存到 `delete_request`；它表示准备提交给下游服务的结构化请求参数。
        delete_request = await self._parse_delete_request(message)
        # 调用 `delete_request.get("type")` 完成当前解析、查询或对象构造，并把返回值保存到 `resource_type`；它表示`resource_type` 在当前函数中承载的中间业务值。
        resource_type = delete_request.get("type")
        # 调用 `delete_request.get("keyword") or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
        keyword = delete_request.get("keyword") or ""
        # 使用 `if` 判断 `(`；条件成立时执行下面分支。
        if (
            # 使用关键字参数语法把 `= "resume"` 传给 `resource_type`；该参数表示`resource_type` 在当前函数中承载的中间业务值。
            resource_type == "resume"
            # 使用 `and` 追加必要条件 `not delete_request.get("confirm_context_low_scores")`；只有前面条件和本行条件同时成立，才允许进入该业务分支。
            and not delete_request.get("confirm_context_low_scores")
            # 使用 `and` 追加正则匹配条件 `re.search(r"低分|不合格|小于\s*60|低于\s*60|没通过|未通过", message)`；只有用户消息也命中这些业务关键词时，整个意图判断才成立。
            and re.search(r"低分|不合格|小于\s*60|低于\s*60|没通过|未通过", message)
        # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
        ):
            # 使用 `await` 异步调用 `self._resolve_resume_screening_followup_action(message, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `followup_action`；它表示`followup_action` 在当前函数中承载的中间业务值。
            followup_action = await self._resolve_resume_screening_followup_action(message, user_id, conversation_id)
            # 使用 `if` 判断 `followup_action.get("action") == "confirm_delete_low_scores"`；条件成立时执行下面分支。
            if followup_action.get("action") == "confirm_delete_low_scores":
                # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `self._build_resume_screening_followup_response(`，并结束当前函数。
                return self._build_resume_screening_followup_response(
                    # 把变量 `followup_action` 作为位置参数传入当前多行函数调用；它承载`followup_action` 在当前函数中承载的中间业务值。
                    followup_action,
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    "/recruitment/resume-screening",
                    # 把变量 `selected_tool` 作为位置参数传入当前多行函数调用；它承载当前意图对应的已注册工具规格。
                    selected_tool,
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
        # 使用 `if` 判断 `resource_type == "resume" and delete_request.get("confirm_context_low_scores")`；条件成立时执行下面分支。
        if resource_type == "resume" and delete_request.get("confirm_context_low_scores"):
            # 使用 `await` 异步调用 `self._recent_low_score_resume_resources(user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `low_score_matches`；它表示`low_score_matches` 在当前函数中承载的中间业务值。
            low_score_matches = await self._recent_low_score_resume_resources(user_id, conversation_id)
            # 使用 `if` 判断 `not low_score_matches`；条件成立时执行下面分支。
            if not low_score_matches:
                # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `AgentChatResponse(`，并结束当前函数。
                return AgentChatResponse(
                    # 使用关键字参数语法，把 `"我没有在当前对话最近的评分结果里找到低分候选人，因此没有执行删除。你可以先完成简历评分，或明确说出候选人姓名。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                    message="我没有在当前对话最近的评分结果里找到低分候选人，因此没有执行删除。你可以先完成简历评分，或明确说出候选人姓名。",
                    # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                    intent="resource_delete",
                    # 使用关键字参数语法，把 `"/recruitment/resume-screening"` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                    route="/recruitment/resume-screening",
                    # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
                    steps=[
                        # 调用 `self._planning_step()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
                        self._planning_step(selected_tool, "识别到确认删除低分候选人的请求。"),
                        # 计算表达式 `"locate_resource", title="定位低分候选人", status="failed", detail="没有找到可删除的低分候选人。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                        AgentStep(id="locate_resource", title="定位低分候选人", status="failed", detail="没有找到可删除的低分候选人。"),
                    # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    ],
                    # 使用关键字参数语法，把 `["先评分简历", "说出候选人姓名", "查看简历筛选列表"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                    suggestions=["先评分简历", "说出候选人姓名", "查看简历筛选列表"],
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
            steps = [
                # 调用 `self._planning_step()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
                self._planning_step(selected_tool, "用户确认删除最近评分中的低分候选人。"),
                # 计算表达式 `"locate_resource", title="定位低分候选人", status="completed", detail=f"匹配到 {len(low_score_matches)} 位低分...` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="locate_resource", title="定位低分候选人", status="completed", detail=f"匹配到 {len(low_score_matches)} 位低分候选人。"),
                # 计算表达式 `"delete_resource", title="执行删除操作", status="running", detail="正在删除低分简历记录。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
                AgentStep(id="delete_resource", title="执行删除操作", status="running", detail="正在删除低分简历记录。"),
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
            # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `await self._delete_matched_resources(`，并结束当前函数。
            return await self._delete_matched_resources(
                # 使用关键字参数语法，把 `"resume"` 传入 `resource_type` 参数；它表示`resource_type` 在当前函数中承载的中间业务值。
                resource_type="resume",
                # 使用关键字参数语法，把 `low_score_matches` 传入 `matches` 参数；它表示`matches` 在当前函数中承载的中间业务值。
                matches=low_score_matches,
                # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=user_id,
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `"低分候选人"` 传入 `keyword` 参数；它表示`keyword` 在当前函数中承载的中间业务值。
                keyword="低分候选人",
                # 使用关键字参数语法，把 `True` 传入 `matched_from_context` 参数；它表示当前业务步骤需要的历史、用户或 Skill 上下文。
                matched_from_context=True,
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 调用 `bool(delete_request.get("context_reference")) and not keyword` 完成当前解析、查询或对象构造，并把返回值保存到 `context_first`；它表示`context_first` 在当前函数中承载的中间业务值。
        context_first = bool(delete_request.get("context_reference")) and not keyword
        # 使用列表字面量开始构造 `steps`；该列表用于保存返回前端展示的 Agent 执行步骤，下面各行会继续追加有顺序的业务元素。
        steps = [
            # 调用 `self._planning_step()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
            self._planning_step(selected_tool, "识别到删除类请求，准备定位目标资源。"),
            # 调用 `AgentStep()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
            AgentStep(
                # 使用关键字参数语法，把 `"locate_resource"` 传入 `id` 参数；它表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                id="locate_resource",
                # 使用关键字参数语法，把 `"定位删除目标"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                title="定位删除目标",
                # 使用关键字参数语法，把 `"running"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
                status="running",
                # 使用关键字参数语法，把 `self._delete_target_detail(resource_type, keyword)` 传入 `detail` 参数；它表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
                detail=self._delete_target_detail(resource_type, keyword),
                # 使用关键字参数语法，把 `"delete_resource"` 传入 `tool` 参数；它表示Agent 注册表中的业务工具规格。
                tool="delete_resource",
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ),
            # 计算表达式 `"delete_resource", title="执行删除操作", status="pending", detail="匹配到唯一目标后执行删除。"),` 并把结果保存到 `AgentStep(id`；该结果在后续流程中用作`AgentStep(id` 在当前函数中承载的中间业务值。
            AgentStep(id="delete_resource", title="执行删除操作", status="pending", detail="匹配到唯一目标后执行删除。"),
        # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `not resource_type`；条件成立时执行下面分支。
        if not resource_type:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "failed", "detail": "缺少要删除的资源类型。"})
            # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `"我还不确定你要删除哪类内容。请明确说明要删除 JD、简历记录、面试方案还是试卷，例如：帮我删除产品经理的 JD。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message="我还不确定你要删除哪类内容。请明确说明要删除 JD、简历记录、面试方案还是试卷，例如：帮我删除产品经理的 JD。",
                # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resource_delete",
                # 使用关键字参数语法，把 `None` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=None,
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用关键字参数语法，把 `[]` 传入 `artifacts` 参数；它表示本轮生成的 JD、评分、面试方案等业务产物。
                artifacts=[],
                # 使用关键字参数语法，把 `["删除产品经理的 JD", "删除张三的简历记录", "删除 Java 试卷"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["删除产品经理的 JD", "删除张三的简历记录", "删除 Java 试卷"],
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `context_first`；条件成立时执行下面分支。
        if context_first:
            # 使用 `await` 异步调用 `self._recent_generated_resources(resource_type, user_id, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `context_matches`；它表示`context_matches` 在当前函数中承载的中间业务值。
            context_matches = await self._recent_generated_resources(resource_type, user_id, conversation_id)
            # 使用 `if` 判断 `context_matches`；条件成立时执行下面分支。
            if context_matches:
                # 计算表达式 `context_matches if delete_request.get("delete_all") else context_matches[:1]` 并把结果保存到 `matches`；该结果在后续流程中用作`matches` 在当前函数中承载的中间业务值。
                matches = context_matches if delete_request.get("delete_all") else context_matches[:1]
                # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `await self._delete_matched_resources(`，并结束当前函数。
                return await self._delete_matched_resources(
                    # 使用关键字参数语法，把 `resource_type` 传入 `resource_type` 参数；它表示`resource_type` 在当前函数中承载的中间业务值。
                    resource_type=resource_type,
                    # 使用关键字参数语法，把 `matches` 传入 `matches` 参数；它表示`matches` 在当前函数中承载的中间业务值。
                    matches=matches,
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                    steps=steps,
                    # 使用关键字参数语法，把 `keyword` 传入 `keyword` 参数；它表示`keyword` 在当前函数中承载的中间业务值。
                    keyword=keyword,
                    # 使用关键字参数语法，把 `True` 传入 `matched_from_context` 参数；它表示当前业务步骤需要的历史、用户或 Skill 上下文。
                    matched_from_context=True,
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

        # 使用 `await` 异步调用 `self._list_deletable_resources(resource_type, user_id, keyword)`，等待数据库、Dify 或模型处理完成后把结果保存到 `resources`；它表示`resources` 在当前函数中承载的中间业务值。
        resources = await self._list_deletable_resources(resource_type, user_id, keyword)
        # 调用 `self._match_delete_resources(resources, keyword, delete_request.get("latest"))` 完成当前解析、查询或对象构造，并把返回值保存到 `matches`；它表示`matches` 在当前函数中承载的中间业务值。
        matches = self._match_delete_resources(resources, keyword, delete_request.get("latest"))
        # 使用 `if` 判断 `not matches`；条件成立时执行下面分支。
        if not matches:
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "failed", "detail": "没有找到匹配资源。"})
            # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 使用关键字参数语法，把 `f"没有找到匹配的{self._resource_type_label(resource_type)}。你可以换个名称再试，或先到对应列表确认名称。"` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
                message=f"没有找到匹配的{self._resource_type_label(resource_type)}。你可以换个名称再试，或先到对应列表确认名称。",
                # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resource_delete",
                # 使用关键字参数语法，把 `self._resource_type_route(resource_type)` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=self._resource_type_route(resource_type),
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"delete_resource_result"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="delete_resource_result",
                        # 使用关键字参数语法，把 `"删除结果"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="删除结果",
                        # 使用关键字参数语法，把 `{"resource_type": resource_type, "keyword": keyword, "deleted": [], "matches": []}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"resource_type": resource_type, "keyword": keyword, "deleted": [], "matches": []},
                    # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `self._delete_suggestions(resource_type)` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=self._delete_suggestions(resource_type),
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 使用 `if` 判断 `len(matches) > 1 and not delete_request.get("delete_all")`；条件成立时执行下面分支。
        if len(matches) > 1 and not delete_request.get("delete_all"):
            # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
            steps[1] = steps[1].model_copy(update={"status": "running", "detail": f"找到 {len(matches)} 个匹配项，需要用户确认。"})
            # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `AgentChatResponse(`，并结束当前函数。
            return AgentChatResponse(
                # 计算表达式 `(` 并把结果保存到 `message`；该结果在后续流程中用作用户本次输入的自然语言消息。
                message=(
                    # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                    f"我找到了 {len(matches)} 个匹配的{self._resource_type_label(resource_type)}，为避免误删，请说得更具体一点：\n"
                    # 使用 `+` 将 `"\n".join(f"- {item['name']}" for item in matches[:8])` 拼接到前面的文本或集合，组成完整业务内容。
                    + "\n".join(f"- {item['name']}" for item in matches[:8])
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ),
                # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
                intent="resource_delete",
                # 使用关键字参数语法，把 `self._resource_type_route(resource_type)` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
                route=self._resource_type_route(resource_type),
                # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
                steps=steps,
                # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
                artifacts=[
                    # 调用 `AgentArtifact()`，执行 `_AgentServiceResourcesMixin._handle_resource_delete` 当前步骤所需的构造、查询或业务处理。
                    AgentArtifact(
                        # 使用关键字参数语法，把 `"delete_resource_candidates"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                        type="delete_resource_candidates",
                        # 使用关键字参数语法，把 `"待确认删除目标"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                        title="待确认删除目标",
                        # 使用关键字参数语法，把 `{"resource_type": resource_type, "keyword": keyword, "candidates": matches[:8]}` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        content={"resource_type": resource_type, "keyword": keyword, "candidates": matches[:8]},
                    # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    )
                # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                ],
                # 使用关键字参数语法，把 `["补充更完整的名称", "说“删除最新的那个”", "到列表中确认后再删除"]` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
                suggestions=["补充更完整的名称", "说“删除最新的那个”", "到列表中确认后再删除"],
            # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )

        # 从 `_AgentServiceResourcesMixin._handle_resource_delete()` 返回 `await self._delete_matched_resources(`，并结束当前函数。
        return await self._delete_matched_resources(
            # 使用关键字参数语法，把 `resource_type` 传入 `resource_type` 参数；它表示`resource_type` 在当前函数中承载的中间业务值。
            resource_type=resource_type,
            # 使用关键字参数语法，把 `matches` 传入 `matches` 参数；它表示`matches` 在当前函数中承载的中间业务值。
            matches=matches,
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用关键字参数语法，把 `keyword` 传入 `keyword` 参数；它表示`keyword` 在当前函数中承载的中间业务值。
            keyword=keyword,
            # 使用关键字参数语法，把 `False` 传入 `matched_from_context` 参数；它表示当前业务步骤需要的历史、用户或 Skill 上下文。
            matched_from_context=False,
        # 结束 `_AgentServiceResourcesMixin._handle_resource_delete` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_parse_delete_request()`。
    async def _parse_delete_request(self, message: str) -> Dict[str, Any]:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是 HR Agent 的删除请求解析器。请从用户消息中识别要删除的招聘资源，并严格返回 JSON，不要解释。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "resource_type 只能是 jd、resume、interview、exam、unknown。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "含义：\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- jd：JD、岗位、职位、职位描述、招聘需求、岗位说明书。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- resume：简历、候选人、简历评分/筛选记录。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- interview：面试方案、面试计划、面试安排。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- exam：试卷、考试、笔试题、测评题。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "keyword 填用户用于定位目标的名称/关键词，例如“AI训练师”“产品经理”“张三”“Java基础”。不要包含“删除、帮我、JD、岗位、试卷”等动作词或类型词。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "context_reference 表示用户是否用“这个/这份/这条/该/刚才/刚生成的/上一个/它”等指代最近生成的产物。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "latest 为用户是否表达最新/刚才/上一个/最近。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "delete_all 为用户是否明确表达全部/所有/都删。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "confirm_context_low_scores 表示用户是否在上文低分候选人确认后，明确说“确认删除这些低分候选人/确认删除不合格简历”。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "如果资源类型不明确，resource_type 返回 unknown。\n"
            # 在字典中设置 `返回格式：{\"resource_type\` 字段为 `\"jd|resume|interview|exam|unknown\",\"keyword\":\"...\",\"context_reference\":false,\"latest\":f...`，用于表示`返回格式：{\"resource_type\` 在当前函数中承载的中间业务值。
            "返回格式：{\"resource_type\":\"jd|resume|interview|exam|unknown\",\"keyword\":\"...\",\"context_reference\":false,\"latest\":false,\"delete_all\":false,\"confirm_context_low_scores\":false}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户消息：{message}"
        # 结束 `_AgentServiceResourcesMixin._parse_delete_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
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
            # 调用 `str(parsed.get("resource_type") or parsed.get("type") or "").strip().lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `resource_type`；它表示`resource_type` 在当前函数中承载的中间业务值。
            resource_type = str(parsed.get("resource_type") or parsed.get("type") or "").strip().lower()
            # 使用 `if` 判断 `resource_type in {"jd", "resume", "interview", "exam"}`；条件成立时执行下面分支。
            if resource_type in {"jd", "resume", "interview", "exam"}:
                # 调用 `self._clean_optional_value(parsed.get("keyword")) or ""` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
                keyword = self._clean_optional_value(parsed.get("keyword")) or ""
                # 从 `_AgentServiceResourcesMixin._parse_delete_request()` 返回 `{`，并结束当前函数。
                return {
                    # 在字典中设置 `type` 字段为 `resource_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    "type": resource_type,
                    # 在字典中设置 `keyword` 字段为 `keyword`，用于表示`keyword` 在当前函数中承载的中间业务值。
                    "keyword": keyword,
                    # 在字典中设置 `context_reference` 字段为 `bool(parsed.get("context_reference"))`，用于表示`context_reference` 在当前函数中承载的中间业务值。
                    "context_reference": bool(parsed.get("context_reference")),
                    # 在字典中设置 `latest` 字段为 `bool(parsed.get("latest"))`，用于表示`latest` 在当前函数中承载的中间业务值。
                    "latest": bool(parsed.get("latest")),
                    # 在字典中设置 `delete_all` 字段为 `bool(parsed.get("delete_all"))`，用于表示`delete_all` 在当前函数中承载的中间业务值。
                    "delete_all": bool(parsed.get("delete_all")),
                    # 在字典中设置 `confirm_context_low_scores` 字段为 `bool(parsed.get("confirm_context_low_scores"))`，用于表示`confirm_context_low_scores` 在当前函数中承载的中间业务值。
                    "confirm_context_low_scores": bool(parsed.get("confirm_context_low_scores")),
                    # 在字典中设置 `source` 字段为 `"llm"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "llm",
                # 结束 `_AgentServiceResourcesMixin._parse_delete_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("删除请求大模型解析失败，使用规则兜底: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("删除请求大模型解析失败，使用规则兜底: %s", exc)

        # 从 `_AgentServiceResourcesMixin._parse_delete_request()` 返回 `self._fallback_parse_delete_request(message)`，并结束当前函数。
        return self._fallback_parse_delete_request(message)

    # 使用 `def` 普通函数 定义 `_fallback_parse_delete_request()`。
    def _fallback_parse_delete_request(self, message: str) -> Dict[str, Any]:
        # 调用 `message.lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `lowered`；它表示`lowered` 在当前函数中承载的中间业务值。
        lowered = message.lower()
        # 把 `resource_type` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`resource_type` 在当前函数中承载的中间业务值。
        resource_type = None
        # 使用 `if` 判断 `re.search(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位", lowered)`；条件成立时执行下面分支。
        if re.search(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位", lowered):
            # 计算表达式 `"jd"` 并把结果保存到 `resource_type`；该结果在后续流程中用作`resource_type` 在当前函数中承载的中间业务值。
            resource_type = "jd"
        # 前一条件不成立时继续判断 `re.search(r"面试方案|面试计划|面试", lowered)`。
        elif re.search(r"面试方案|面试计划|面试", lowered):
            # 计算表达式 `"interview"` 并把结果保存到 `resource_type`；该结果在后续流程中用作`resource_type` 在当前函数中承载的中间业务值。
            resource_type = "interview"
        # 前一条件不成立时继续判断 `re.search(r"试卷|考试|笔试", lowered)`。
        elif re.search(r"试卷|考试|笔试", lowered):
            # 计算表达式 `"exam"` 并把结果保存到 `resource_type`；该结果在后续流程中用作`resource_type` 在当前函数中承载的中间业务值。
            resource_type = "exam"
        # 前一条件不成立时继续判断 `re.search(r"简历|候选人", lowered)`。
        elif re.search(r"简历|候选人", lowered):
            # 计算表达式 `"resume"` 并把结果保存到 `resource_type`；该结果在后续流程中用作`resource_type` 在当前函数中承载的中间业务值。
            resource_type = "resume"

        # 调用 `re.sub(r"请|帮我|麻烦|一下|这个|这份|这条|记录|生成的|已生成的", "", message, flags=re.I)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
        keyword = re.sub(r"请|帮我|麻烦|一下|这个|这份|这条|记录|生成的|已生成的", "", message, flags=re.I)
        # 调用 `re.sub(r"删除|删掉|移除|清理|取消", "", keyword, flags=re.I)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
        keyword = re.sub(r"删除|删掉|移除|清理|取消", "", keyword, flags=re.I)
        # 调用 `re.sub(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位|简历评分|简历记录|简历|候选人|面试方案|面试计划|面试|试卷|考试|笔试", "", keyword, flags=re.I)` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
        keyword = re.sub(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位|简历评分|简历记录|简历|候选人|面试方案|面试计划|面试|试卷|考试|笔试", "", keyword, flags=re.I)
        # 调用 `keyword.strip(" 的：:，,。.?？!！「」『』【】[]()（）")` 完成当前解析、查询或对象构造，并把返回值保存到 `keyword`；它表示`keyword` 在当前函数中承载的中间业务值。
        keyword = keyword.strip(" 的：:，,。.?？!！「」『』【】[]()（）")
        # 从 `_AgentServiceResourcesMixin._fallback_parse_delete_request()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `type` 字段为 `resource_type`，用于表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
            "type": resource_type,
            # 在字典中设置 `keyword` 字段为 `keyword`，用于表示`keyword` 在当前函数中承载的中间业务值。
            "keyword": keyword,
            # 在字典中设置 `context_reference` 字段为 `bool(re.search(r"这个|这份|这条|该|刚才|刚刚|上一个|上一条|最近|最新|它|其|刚生成", message))`，用于表示`context_reference` 在当前函数中承载的中间业务值。
            "context_reference": bool(re.search(r"这个|这份|这条|该|刚才|刚刚|上一个|上一条|最近|最新|它|其|刚生成", message)),
            # 在字典中设置 `latest` 字段为 `bool(re.search(r"最近|最新|刚才|上一个|最后", message))`，用于表示`latest` 在当前函数中承载的中间业务值。
            "latest": bool(re.search(r"最近|最新|刚才|上一个|最后", message)),
            # 在字典中设置 `delete_all` 字段为 `bool(re.search(r"全部|所有|都删|全删", message))`，用于表示`delete_all` 在当前函数中承载的中间业务值。
            "delete_all": bool(re.search(r"全部|所有|都删|全删", message)),
            # 在字典中设置 `confirm_context_low_scores` 字段为 `bool(re.search(r"确认|确定", message) and re.search(r"低分|不合格|没通过|未通过|这些", message))`，用于表示`confirm_context_low_scores` 在当前函数中承载的中间业务值。
            "confirm_context_low_scores": bool(re.search(r"确认|确定", message) and re.search(r"低分|不合格|没通过|未通过|这些", message)),
            # 在字典中设置 `source` 字段为 `"fallback"`，用于表示`source` 在当前函数中承载的中间业务值。
            "source": "fallback",
        # 结束 `_AgentServiceResourcesMixin._fallback_parse_delete_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_low_score_resume_resources()`。
    async def _recent_low_score_resume_resources(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `threshold`，表示`threshold` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `60`。
        threshold: int = 60,
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 使用 `await` 异步调用 `self._recent_resume_screening_groups(conversation_id, user_id, threshold)`，等待数据库、Dify 或模型处理完成后把结果保存到 `groups`；它表示`groups` 在当前函数中承载的中间业务值。
        groups = await self._recent_resume_screening_groups(conversation_id, user_id, threshold)
        # 从 `_AgentServiceResourcesMixin._recent_low_score_resume_resources()` 返回 `[`，并结束当前函数。
        return [
            # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._recent_low_score_resume_resources` 当前步骤所需的键值数据。
            {
                # 在字典中设置 `id` 字段为 `item["id"]`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                "id": item["id"],
                # 在字典中设置 `name` 字段为 `item.get("name") or item.get("filename") or "低分候选人"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                "name": item.get("name") or item.get("filename") or "低分候选人",
                # 在字典中设置 `search_text` 字段为 `self._resource_search_text(item, ["name", "filename", "position"])`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                "search_text": self._resource_search_text(item, ["name", "filename", "position"]),
                # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "conversation_context",
            # 结束 `_AgentServiceResourcesMixin._recent_low_score_resume_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            }
            # 使用 `for` 循环 `item in groups.get("low_score_candidates") or []`，逐项处理集合数据。
            for item in groups.get("low_score_candidates") or []
            # 使用 `if` 判断 `item.get("id")`；条件成立时执行下面分支。
            if item.get("id")
        # 结束 `_AgentServiceResourcesMixin._recent_low_score_resume_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

    # 使用 `def` 普通函数 定义 `_delete_target_detail()`。
    def _delete_target_detail(self, resource_type: Optional[str], keyword: str) -> str:
        # 调用 `self._resource_type_label(resource_type) if resource_type else "资源"` 完成当前解析、查询或对象构造，并把返回值保存到 `label`；它表示`label` 在当前函数中承载的中间业务值。
        label = self._resource_type_label(resource_type) if resource_type else "资源"
        # 从 `_AgentServiceResourcesMixin._delete_target_detail()` 返回 `f"目标类型：{label}；关键词：{keyword or '未提供'}。"`，并结束当前函数。
        return f"目标类型：{label}；关键词：{keyword or '未提供'}。"

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_recent_generated_resources()`。
    async def _recent_generated_resources(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `resource_type`，表示`resource_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        resource_type: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 声明参数 `limit`，表示`limit` 在当前函数中承载的中间业务值，类型标注为 `int`，默认值为 `12`。
        limit: int = 12,
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 使用列表字面量开始构造 `conditions`；该列表用于保存`conditions` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        conditions = [
            # 构造 SQLAlchemy 查询条件 `Conversation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
            Conversation.user_id == user_id,
            # 构造 SQLAlchemy 查询条件 `Message.role == MessageRole.ASSISTANT`，把数据库记录限制为符合该业务字段要求的数据。
            Message.role == MessageRole.ASSISTANT,
        # 结束 `_AgentServiceResourcesMixin._recent_generated_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `conversation_id`；条件成立时执行下面分支。
        if conversation_id:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用列表的 `append()`，把 `conditions.append(Message.conversation_id == UUID(str(conversation_id)))` 对应的新元素追加到现有结果末尾。
                conditions.append(Message.conversation_id == UUID(str(conversation_id)))
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 通过模块日志记录器执行 `logger.warning("删除指代匹配收到非法 conversation_id: %s", conversation_id)`，用于保留运行状态或异常原因。
                logger.warning("删除指代匹配收到非法 conversation_id: %s", conversation_id)
        # 计算表达式 `(` 并把结果保存到 `query`；该结果在后续流程中用作发送给模型、Dify 或数据库检索的输入。
        query = (
            # 调用 `select()`，执行 `_AgentServiceResourcesMixin._recent_generated_resources` 当前步骤所需的构造、查询或业务处理。
            select(Message)
            # 在 SQLAlchemy 查询链中调用 `join()`，按 `Conversation, Message.conversation_id == Conversation.id` 关联另一张表，使后续条件可以同时使用两张表的字段。
            .join(Conversation, Message.conversation_id == Conversation.id)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `*conditions` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(*conditions)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `Message.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(Message.created_at.desc())
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
        # 结束 `_AgentServiceResourcesMixin._recent_generated_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `await` 异步调用 `self.db.execute(query)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(query)
        # 声明参数 `resources`，表示`resources` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`，默认值为 `[]`。
        resources: List[Dict[str, Any]] = []
        # 使用 `for` 循环 `message in result.scalars().all()`，逐项处理集合数据。
        for message in result.scalars().all():
            # 计算表达式 `(message.context or {}).get("agent_response") if isinstance(message.context, dict) else None` 并把结果保存到 `response`；该结果在后续流程中用作下游服务或 Agent 流程返回的响应。
            response = (message.context or {}).get("agent_response") if isinstance(message.context, dict) else None
            # 使用 `if` 判断 `not isinstance(response, dict)`；条件成立时执行下面分支。
            if not isinstance(response, dict):
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 调用 `resources.extend()`，执行 `_AgentServiceResourcesMixin._recent_generated_resources` 当前步骤所需的构造、查询或业务处理。
            resources.extend(self._resources_from_agent_response(response, resource_type))
            # 使用 `if` 判断 `resources`；条件成立时执行下面分支。
            if resources:
                # 从 `_AgentServiceResourcesMixin._recent_generated_resources()` 返回 `resources`，并结束当前函数。
                return resources
        # 从 `_AgentServiceResourcesMixin._recent_generated_resources()` 返回 `resources`，并结束当前函数。
        return resources

    # 使用 `def` 普通函数 定义 `_resources_from_agent_response()`。
    def _resources_from_agent_response(self, response: Dict[str, Any], resource_type: str) -> List[Dict[str, Any]]:
        # 声明参数 `resources`，表示`resources` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`，默认值为 `[]`。
        resources: List[Dict[str, Any]] = []
        # 使用 `for` 循环 `artifact in response.get("artifacts") or []`，逐项处理集合数据。
        for artifact in response.get("artifacts") or []:
            # 调用 `artifact.get("metadata") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `metadata`；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
            metadata = artifact.get("metadata") or {}
            # 调用 `artifact.get("content")` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content = artifact.get("content")
            # 使用 `if` 判断 `resource_type == "jd" and metadata.get("saved_jd_id")`；条件成立时执行下面分支。
            if resource_type == "jd" and metadata.get("saved_jd_id"):
                # 调用列表的 `append()`，把 `resources.append({` 对应的新元素追加到现有结果末尾。
                resources.append({
                    # 在字典中设置 `id` 字段为 `str(metadata["saved_jd_id"])`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(metadata["saved_jd_id"]),
                    # 在字典中设置 `name` 字段为 `artifact.get("title") or metadata.get("job_title") or "最近生成的 JD"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": artifact.get("title") or metadata.get("job_title") or "最近生成的 JD",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `title` 字段为 `artifact.get("title")`，用于表示前端向用户展示的步骤或业务产物标题。
                        "title": artifact.get("title"),
                        # 在字典中设置 `job_title` 字段为 `metadata.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                        "job_title": metadata.get("job_title"),
                        # 在字典中设置 `content` 字段为 `content if isinstance(content, str) else ""`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        "content": content if isinstance(content, str) else "",
                    # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                    # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "conversation_context",
                # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                })
            # 前一条件不成立时继续判断 `resource_type == "criteria" and metadata.get("saved_criteria_id")`。
            elif resource_type == "criteria" and metadata.get("saved_criteria_id"):
                # 调用列表的 `append()`，把 `resources.append({` 对应的新元素追加到现有结果末尾。
                resources.append({
                    # 在字典中设置 `id` 字段为 `str(metadata["saved_criteria_id"])`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(metadata["saved_criteria_id"]),
                    # 在字典中设置 `name` 字段为 `artifact.get("title") or metadata.get("job_title") or "最近生成的评分标准"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": artifact.get("title") or metadata.get("job_title") or "最近生成的评分标准",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `title` 字段为 `artifact.get("title")`，用于表示前端向用户展示的步骤或业务产物标题。
                        "title": artifact.get("title"),
                        # 在字典中设置 `job_title` 字段为 `metadata.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                        "job_title": metadata.get("job_title"),
                        # 在字典中设置 `content` 字段为 `content if isinstance(content, str) else ""`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        "content": content if isinstance(content, str) else "",
                    # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                    # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "conversation_context",
                # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                })
            # 前一条件不成立时继续判断 `resource_type == "resume" and artifact.get("type") == "resume_screening_results" and isinstance(c...`。
            elif resource_type == "resume" and artifact.get("type") == "resume_screening_results" and isinstance(content, list):
                # 使用 `for` 循环 `item in content`，逐项处理集合数据。
                for item in content:
                    # 使用 `if` 判断 `item.get("id") and item.get("status") != "failed"`；条件成立时执行下面分支。
                    if item.get("id") and item.get("status") != "failed":
                        # 调用列表的 `append()`，把 `resources.append({` 对应的新元素追加到现有结果末尾。
                        resources.append({
                            # 在字典中设置 `id` 字段为 `str(item["id"])`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                            "id": str(item["id"]),
                            # 在字典中设置 `name` 字段为 `item.get("name") or item.get("filename") or "最近评分的简历"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                            "name": item.get("name") or item.get("filename") or "最近评分的简历",
                            # 在字典中设置 `search_text` 字段为 `self._resource_search_text(item, ["name", "filename", "position", "summary"])`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                            "search_text": self._resource_search_text(item, ["name", "filename", "position", "summary"]),
                            # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                            "source": "conversation_context",
                        # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                        })
            # 前一条件不成立时继续判断 `resource_type == "interview" and metadata.get("saved_plan_id")`。
            elif resource_type == "interview" and metadata.get("saved_plan_id"):
                # 调用列表的 `append()`，把 `resources.append({` 对应的新元素追加到现有结果末尾。
                resources.append({
                    # 在字典中设置 `id` 字段为 `str(metadata["saved_plan_id"])`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(metadata["saved_plan_id"]),
                    # 在字典中设置 `name` 字段为 `artifact.get("title") or metadata.get("candidate_name") or "最近生成的面试方案"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": artifact.get("title") or metadata.get("candidate_name") or "最近生成的面试方案",
                    # 在字典中设置 `content` 字段为 `content if isinstance(content, str) else ""`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                    "content": content if isinstance(content, str) else "",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `title` 字段为 `artifact.get("title")`，用于表示前端向用户展示的步骤或业务产物标题。
                        "title": artifact.get("title"),
                        # 在字典中设置 `candidate_name` 字段为 `metadata.get("candidate_name")`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                        "candidate_name": metadata.get("candidate_name"),
                        # 在字典中设置 `content` 字段为 `content if isinstance(content, str) else ""`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        "content": content if isinstance(content, str) else "",
                    # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                    # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "conversation_context",
                # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                })
            # 前一条件不成立时继续判断 `resource_type == "exam" and metadata.get("saved_exam_id")`。
            elif resource_type == "exam" and metadata.get("saved_exam_id"):
                # 调用列表的 `append()`，把 `resources.append({` 对应的新元素追加到现有结果末尾。
                resources.append({
                    # 在字典中设置 `id` 字段为 `str(metadata["saved_exam_id"])`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(metadata["saved_exam_id"]),
                    # 在字典中设置 `name` 字段为 `artifact.get("title") or metadata.get("subject") or "最近生成的试卷"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": artifact.get("title") or metadata.get("subject") or "最近生成的试卷",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `title` 字段为 `artifact.get("title")`，用于表示前端向用户展示的步骤或业务产物标题。
                        "title": artifact.get("title"),
                        # 在字典中设置 `subject` 字段为 `metadata.get("subject")`，用于表示待发送邮件的主题。
                        "subject": metadata.get("subject"),
                        # 在字典中设置 `content` 字段为 `content if isinstance(content, str) else ""`，用于表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                        "content": content if isinstance(content, str) else "",
                    # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                    # 在字典中设置 `source` 字段为 `"conversation_context"`，用于表示`source` 在当前函数中承载的中间业务值。
                    "source": "conversation_context",
                # 结束 `_AgentServiceResourcesMixin._resources_from_agent_response` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                })
        # 从 `_AgentServiceResourcesMixin._resources_from_agent_response()` 返回 `resources`，并结束当前函数。
        return resources

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_delete_matched_resources()`。
    async def _delete_matched_resources(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `resource_type`，表示`resource_type` 在当前函数中承载的中间业务值，类型标注为 `str`。
        resource_type: str,
        # 声明参数 `matches`，表示`matches` 在当前函数中承载的中间业务值，类型标注为 `List[Dict[str, Any]]`。
        matches: List[Dict[str, Any]],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `steps`，表示返回前端展示的 Agent 执行步骤，类型标注为 `List[AgentStep]`。
        steps: List[AgentStep],
        # 声明参数 `keyword`，表示`keyword` 在当前函数中承载的中间业务值，类型标注为 `str`。
        keyword: str,
        # 声明参数 `matched_from_context`，表示当前业务步骤需要的历史、用户或 Skill 上下文，类型标注为 `bool`，默认值为 `False`。
        matched_from_context: bool = False,
    # 结束函数参数列表，并用 `-> AgentChatResponse` 标注该函数的返回值类型。
    ) -> AgentChatResponse:
        # 把 `deleted` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`deleted` 在当前函数中承载的中间业务值。
        deleted = []
        # 把 `failures` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`failures` 在当前函数中承载的中间业务值。
        failures = []
        # 使用 `for` 循环 `item in matches`，逐项处理集合数据。
        for item in matches:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 使用 `await` 异步调用 `self._delete_resource_by_type(resource_type, item["id"], user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `delete_result`；它表示当前业务调用完成后返回的单个处理结果。
                delete_result = await self._delete_resource_by_type(resource_type, item["id"], user_id)
                # 调用列表的 `append()`，把 `deleted.append({**item, "delete_result": delete_result or {}})` 对应的新元素追加到现有结果末尾。
                deleted.append({**item, "delete_result": delete_result or {}})
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception as exc:
                # 计算表达式 `%s id=%s: %s", resource_type, item.get("id"), exc)` 并把结果保存到 `logger.warning("Agent 删除资源失败 type`；该结果在后续流程中用作`warning("Agent 删除资源失败 type` 在当前函数中承载的中间业务值。
                logger.warning("Agent 删除资源失败 type=%s id=%s: %s", resource_type, item.get("id"), exc)
                # 调用列表的 `append()`，把 `failures.append({"id": item.get("id"), "name": item.get("name"), "error": str(exc)})` 对应的新元素追加到现有结果末尾。
                failures.append({"id": item.get("id"), "name": item.get("name"), "error": str(exc)})

        # 计算表达式 `"从最近对话产物中匹配到目标。" if matched_from_context else f"匹配到 {len(matches)} 个目标。"` 并把结果保存到 `match_detail`；该结果在后续流程中用作`match_detail` 在当前函数中承载的中间业务值。
        match_detail = "从最近对话产物中匹配到目标。" if matched_from_context else f"匹配到 {len(matches)} 个目标。"
        # 用 Pydantic `model_copy(update=...)` 复制并更新该 Agent 步骤，再写回 `steps[1]`；业务上用于推进前端步骤的状态或说明。
        steps[1] = steps[1].model_copy(update={"status": "completed", "detail": match_detail})
        # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._delete_matched_resources` 当前步骤所需的键值数据。
        steps[2] = steps[2].model_copy(update={
            # 在字典中设置 `status` 字段为 `"completed" if deleted and not failures else ("failed" if not deleted else "completed")`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
            "status": "completed" if deleted and not failures else ("failed" if not deleted else "completed"),
            # 在字典中设置 `detail` 字段为 `f"已删除 {len(deleted)} 个，失败 {len(failures)} 个。"`，用于表示步骤的补充说明，用于告诉用户当前处理内容或失败原因。
            "detail": f"已删除 {len(deleted)} 个，失败 {len(failures)} 个。",
        # 结束 `_AgentServiceResourcesMixin._delete_matched_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        })
        # 计算表达式 `"、".join(item["name"] for item in deleted[:5])` 并把结果保存到 `deleted_names`；该结果在后续流程中用作`deleted_names` 在当前函数中承载的中间业务值。
        deleted_names = "、".join(item["name"] for item in deleted[:5])
        # 计算表达式 `f"；{len(failures)} 个删除失败" if failures else ""` 并把结果保存到 `failure_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        failure_text = f"；{len(failures)} 个删除失败" if failures else ""
        # 计算表达式 `"（已按最近对话中的“这个/刚才”指代匹配）" if matched_from_context else ""` 并把结果保存到 `context_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        context_text = "（已按最近对话中的“这个/刚才”指代匹配）" if matched_from_context else ""
        # 调用 `sum(len((item.get("delete_result") or {}).get("deleted_scoring_criteria") or []) for item in dele...` 完成当前解析、查询或对象构造，并把返回值保存到 `cascade_count`；它表示当前业务对象、题目或候选记录的数量。
        cascade_count = sum(len((item.get("delete_result") or {}).get("deleted_scoring_criteria") or []) for item in deleted)
        # 使用关键字参数语法把 `f"，并删除关联评分标准 {cascade_count} 条" if resource_type == "jd" and cascade_count else ""` 传给 `cascade_text`；该参数表示当前步骤整理后的文本，供解析、匹配或模型调用。
        cascade_text = f"，并删除关联评分标准 {cascade_count} 条" if resource_type == "jd" and cascade_count else ""
        # 从 `_AgentServiceResourcesMixin._delete_matched_resources()` 返回 `AgentChatResponse(`，并结束当前函数。
        return AgentChatResponse(
            # 使用关键字参数语法，把 `f"已删除{self._resource_type_label(resource_type)}{context_text}：{deleted_names or '无'}{cascade_text...` 传入 `message` 参数；它表示用户本次输入的自然语言消息。
            message=f"已删除{self._resource_type_label(resource_type)}{context_text}：{deleted_names or '无'}{cascade_text}{failure_text}。",
            # 使用关键字参数语法，把 `"resource_delete"` 传入 `intent` 参数；它表示识别出的业务意图标签。
            intent="resource_delete",
            # 使用关键字参数语法，把 `self._resource_type_route(resource_type)` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
            route=self._resource_type_route(resource_type),
            # 使用关键字参数语法，把 `steps` 传入 `steps` 参数；它表示返回前端展示的 Agent 执行步骤。
            steps=steps,
            # 使用列表字面量开始构造 `artifacts`；该列表用于保存本轮生成的 JD、评分、面试方案等业务产物，下面各行会继续追加有顺序的业务元素。
            artifacts=[
                # 调用 `AgentArtifact()`，执行 `_AgentServiceResourcesMixin._delete_matched_resources` 当前步骤所需的构造、查询或业务处理。
                AgentArtifact(
                    # 使用关键字参数语法，把 `"delete_resource_result"` 传入 `type` 参数；它表示业务产物类型，供前端选择 JD、评分结果、试卷等展示组件。
                    type="delete_resource_result",
                    # 使用关键字参数语法，把 `"删除结果"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
                    title="删除结果",
                    # 使用字典字面量开始构造 `content`；该字典用于保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据，下面各行会继续写入具体业务字段。
                    content={
                        # 在字典中设置 `resource_type` 字段为 `resource_type`，用于表示`resource_type` 在当前函数中承载的中间业务值。
                        "resource_type": resource_type,
                        # 在字典中设置 `keyword` 字段为 `keyword`，用于表示`keyword` 在当前函数中承载的中间业务值。
                        "keyword": keyword,
                        # 在字典中设置 `deleted` 字段为 `deleted`，用于表示`deleted` 在当前函数中承载的中间业务值。
                        "deleted": deleted,
                        # 在字典中设置 `failures` 字段为 `failures`，用于表示`failures` 在当前函数中承载的中间业务值。
                        "failures": failures,
                        # 在字典中设置 `matched_from_context` 字段为 `matched_from_context`，用于表示当前业务步骤需要的历史、用户或 Skill 上下文。
                        "matched_from_context": matched_from_context,
                    # 结束 `_AgentServiceResourcesMixin._delete_matched_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    },
                # 结束 `_AgentServiceResourcesMixin._delete_matched_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )
            # 结束 `_AgentServiceResourcesMixin._delete_matched_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 使用关键字参数语法，把 `self._delete_suggestions(resource_type)` 传入 `suggestions` 参数；它表示任务结束后提供给用户的下一步操作建议。
            suggestions=self._delete_suggestions(resource_type),
        # 结束 `_AgentServiceResourcesMixin._delete_matched_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_list_deletable_resources()`。
    async def _list_deletable_resources(self, resource_type: str, user_id: UUID, keyword: str = "") -> List[Dict[str, Any]]:
        # 使用 `if` 判断 `resource_type == "jd"`；条件成立时执行下面分支。
        if resource_type == "jd":
            # 使用 `await` 异步调用 `JobDescriptionService(self.db).list_job_descriptions(user_id=user_id, page=1, size=100)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await JobDescriptionService(self.db).list_job_descriptions(user_id=user_id, page=1, size=100)
            # 从 `_AgentServiceResourcesMixin._list_deletable_resources()` 返回 `[`，并结束当前函数。
            return [
                # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._list_deletable_resources` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `id` 字段为 `str(item.get("id"))`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(item.get("id")),
                    # 在字典中设置 `name` 字段为 `item.get("title") or item.get("job_title") or item.get("name") or "未命名 JD"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": item.get("title") or item.get("job_title") or item.get("name") or "未命名 JD",
                    # 在字典中设置 `location` 字段为 `item.get("location")`，用于表示`location` 在当前函数中承载的中间业务值。
                    "location": item.get("location"),
                    # 在字典中设置 `salary_range` 字段为 `item.get("salary_range")`，用于表示`salary_range` 在当前函数中承载的中间业务值。
                    "salary_range": item.get("salary_range"),
                    # 在字典中设置 `experience_level` 字段为 `item.get("experience_level")`，用于表示`experience_level` 在当前函数中承载的中间业务值。
                    "experience_level": item.get("experience_level"),
                    # 在字典中设置 `updated_at` 字段为 `str(item.get("updated_at")) if item.get("updated_at") else ""`，用于表示`updated_at` 在当前函数中承载的中间业务值。
                    "updated_at": str(item.get("updated_at")) if item.get("updated_at") else "",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text(item, ["id", "title", "job_title", "department", "location", "salary_r...`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text(item, ["id", "title", "job_title", "department", "location", "salary_range", "experience_level", "content", "requirements"]),
                # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 使用 `for` 循环 `item in result.get("items", [])`，逐项处理集合数据。
                for item in result.get("items", [])
                # 使用 `if` 判断 `item.get("id")`；条件成立时执行下面分支。
                if item.get("id")
            # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
        # 使用 `if` 判断 `resource_type == "resume"`；条件成立时执行下面分支。
        if resource_type == "resume":
            # 使用 `await` 异步调用 `ResumeEvaluationService(self.db).get_evaluation_history(user_id=user_id, skip=0, limit=100)`，等待数据库、Dify 或模型处理完成后把结果保存到 `evaluations, _`；它表示`evaluations, _` 在当前函数中承载的中间业务值。
            evaluations, _ = await ResumeEvaluationService(self.db).get_evaluation_history(user_id=user_id, skip=0, limit=100)
            # 从 `_AgentServiceResourcesMixin._list_deletable_resources()` 返回 `[`，并结束当前函数。
            return [
                # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._list_deletable_resources` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `id` 字段为 `str(item.id)`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(item.id),
                    # 在字典中设置 `name` 字段为 `item.candidate_name or item.original_filename or "未命名简历"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": item.candidate_name or item.original_filename or "未命名简历",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text({`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text({
                        # 在字典中设置 `candidate_name` 字段为 `item.candidate_name`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                        "candidate_name": item.candidate_name,
                        # 在字典中设置 `original_filename` 字段为 `item.original_filename`，用于表示`original_filename` 在当前函数中承载的中间业务值。
                        "original_filename": item.original_filename,
                        # 在字典中设置 `candidate_position` 字段为 `item.candidate_position`，用于表示`candidate_position` 在当前函数中承载的中间业务值。
                        "candidate_position": item.candidate_position,
                        # 在字典中设置 `education_level` 字段为 `item.education_level`，用于表示`education_level` 在当前函数中承载的中间业务值。
                        "education_level": item.education_level,
                    # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                    }),
                # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 使用 `for` 循环 `item in evaluations`，逐项处理集合数据。
                for item in evaluations
            # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
        # 使用 `if` 判断 `resource_type == "interview"`；条件成立时执行下面分支。
        if resource_type == "interview":
            # 使用 `await` 异步调用 `InterviewPlanService(self.db).list_interview_plans(user_id=user_id, page=1, size=100)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await InterviewPlanService(self.db).list_interview_plans(user_id=user_id, page=1, size=100)
            # 调用 `result.get("items") or result.get("data") or []` 完成当前解析、查询或对象构造，并把返回值保存到 `items`；它表示`items` 在当前函数中承载的中间业务值。
            items = result.get("items") or result.get("data") or []
            # 从 `_AgentServiceResourcesMixin._list_deletable_resources()` 返回 `[`，并结束当前函数。
            return [
                # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._list_deletable_resources` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `id` 字段为 `str(item.get("id"))`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(item.get("id")),
                    # 在字典中设置 `name` 字段为 `item.get("title") or item.get("candidate_name") or item.get("name") or "未命名面试方案"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": item.get("title") or item.get("candidate_name") or item.get("name") or "未命名面试方案",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text(item, ["title", "candidate_name", "candidate_position", "job_title", "...`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text(item, ["title", "candidate_name", "candidate_position", "job_title", "content"]),
                # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 使用 `for` 循环 `item in items`，逐项处理集合数据。
                for item in items
                # 使用 `if` 判断 `item.get("id")`；条件成立时执行下面分支。
                if item.get("id")
            # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
        # 使用 `if` 判断 `resource_type == "exam"`；条件成立时执行下面分支。
        if resource_type == "exam":
            # 使用 `await` 异步调用 `ExamService(self.db).get_exam_list(skip=0, limit=100, search=keyword or None)`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
            result = await ExamService(self.db).get_exam_list(skip=0, limit=100, search=keyword or None)
            # 从 `_AgentServiceResourcesMixin._list_deletable_resources()` 返回 `[`，并结束当前函数。
            return [
                # 使用字典字面量语法开始构造 `_AgentServiceResourcesMixin._list_deletable_resources` 当前步骤所需的键值数据。
                {
                    # 在字典中设置 `id` 字段为 `str(item.get("id"))`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
                    "id": str(item.get("id")),
                    # 在字典中设置 `name` 字段为 `item.get("title") or item.get("subject") or "未命名试卷"`，用于表示附件原始名称，供 Agent 判断文件类型并在页面展示。
                    "name": item.get("title") or item.get("subject") or "未命名试卷",
                    # 在字典中设置 `search_text` 字段为 `self._resource_search_text(item, ["title", "subject", "description", "special_requirements"])`，用于表示当前步骤整理后的文本，供解析、匹配或模型调用。
                    "search_text": self._resource_search_text(item, ["title", "subject", "description", "special_requirements"]),
                # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                }
                # 使用 `for` 循环 `item in result.get("items", [])`，逐项处理集合数据。
                for item in result.get("items", [])
                # 使用 `if` 判断 `item.get("id")`；条件成立时执行下面分支。
                if item.get("id")
            # 结束 `_AgentServiceResourcesMixin._list_deletable_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ]
        # 从 `_AgentServiceResourcesMixin._list_deletable_resources()` 返回 `[]`，并结束当前函数。
        return []

    # 使用 `def` 普通函数 定义 `_match_delete_resources()`。
    def _match_delete_resources(self, resources: List[Dict[str, Any]], keyword: str, latest: bool = False) -> List[Dict[str, Any]]:
        # 使用 `if` 判断 `latest and resources`；条件成立时执行下面分支。
        if latest and resources:
            # 从 `_AgentServiceResourcesMixin._match_delete_resources()` 返回 `[resources[0]]`，并结束当前函数。
            return [resources[0]]
        # 调用 `self._normalize_candidate_match_text(keyword)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized_keyword`；它表示已统一字段格式、便于后续比较或保存的数据。
        normalized_keyword = self._normalize_candidate_match_text(keyword)
        # 使用 `if` 判断 `not normalized_keyword`；条件成立时执行下面分支。
        if not normalized_keyword:
            # 从 `_AgentServiceResourcesMixin._match_delete_resources()` 返回 `resources[:1] if latest else []`，并结束当前函数。
            return resources[:1] if latest else []
        # 使用列表字面量开始构造 `exact_matches`；该列表用于保存`exact_matches` 在当前函数中承载的中间业务值，下面各行会继续追加有顺序的业务元素。
        exact_matches = [
            # 使用列表推导式遍历 `item for item in resources`，逐条筛选或转换候选业务资源，结果会汇总进当前列表。
            item for item in resources
            # 使用 `if` 判断 `normalized_keyword == self._normalize_candidate_match_text(item.get("name"))`；条件成立时执行下面分支。
            if normalized_keyword == self._normalize_candidate_match_text(item.get("name"))
        # 结束 `_AgentServiceResourcesMixin._match_delete_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 使用 `if` 判断 `exact_matches`；条件成立时执行下面分支。
        if exact_matches:
            # 从 `_AgentServiceResourcesMixin._match_delete_resources()` 返回 `exact_matches`，并结束当前函数。
            return exact_matches
        # 从 `_AgentServiceResourcesMixin._match_delete_resources()` 返回 `[`，并结束当前函数。
        return [
            # 使用列表推导式遍历 `item for item in resources`，逐条筛选或转换候选业务资源，结果会汇总进当前列表。
            item for item in resources
            # 使用 `if` 判断 `normalized_keyword in self._normalize_candidate_match_text(item.get("search_text"))`；条件成立时执行下面分支。
            if normalized_keyword in self._normalize_candidate_match_text(item.get("search_text"))
            # 使用布尔短路语法提供备用值 `self._normalize_candidate_match_text(item.get("name")) in normalized_keyword`；前面的候选值为空时才采用这一项。
            or self._normalize_candidate_match_text(item.get("name")) in normalized_keyword
        # 结束 `_AgentServiceResourcesMixin._match_delete_resources` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_delete_resource_by_type()`。
    async def _delete_resource_by_type(self, resource_type: str, resource_id: str, user_id: UUID) -> Optional[Dict[str, Any]]:
        # 使用 `if` 判断 `resource_type == "jd"`；条件成立时执行下面分支。
        if resource_type == "jd":
            # 从 `_AgentServiceResourcesMixin._delete_resource_by_type()` 返回 `await JobDescriptionService(self.db).delete_job_description(resource_id, user_id)`，并结束当前函数。
            return await JobDescriptionService(self.db).delete_job_description(resource_id, user_id)
        # 使用 `if` 判断 `resource_type == "resume"`；条件成立时执行下面分支。
        if resource_type == "resume":
            # 使用 `await` 异步调用 `ResumeEvaluationService(self.db).delete_evaluation(UUID(resource_id), user_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `success`；它表示`success` 在当前函数中承载的中间业务值。
            success = await ResumeEvaluationService(self.db).delete_evaluation(UUID(resource_id), user_id)
            # 使用 `if` 判断 `not success`；条件成立时执行下面分支。
            if not success:
                # 使用 `raise` 抛出 `ValueError("简历记录不存在")`，立即终止当前失败流程并交给上层处理。
                raise ValueError("简历记录不存在")
            # 从 `_AgentServiceResourcesMixin._delete_resource_by_type()` 返回 `{}`，并结束当前函数。
            return {}
        # 使用 `if` 判断 `resource_type == "interview"`；条件成立时执行下面分支。
        if resource_type == "interview":
            # 使用 `await` 等待异步操作 `InterviewPlanService(self.db).delete_interview_plan(UUID(resource_id), user_id)` 完成，期间不会阻塞事件循环。
            await InterviewPlanService(self.db).delete_interview_plan(UUID(resource_id), user_id)
            # 从 `_AgentServiceResourcesMixin._delete_resource_by_type()` 返回 `{}`，并结束当前函数。
            return {}
        # 使用 `if` 判断 `resource_type == "exam"`；条件成立时执行下面分支。
        if resource_type == "exam":
            # 使用 `await` 等待异步操作 `ExamService(self.db).delete_exam(resource_id)` 完成，期间不会阻塞事件循环。
            await ExamService(self.db).delete_exam(resource_id)
            # 从 `_AgentServiceResourcesMixin._delete_resource_by_type()` 返回 `{}`，并结束当前函数。
            return {}
        # 使用 `raise` 抛出 `ValueError("不支持的删除类型")`，立即终止当前失败流程并交给上层处理。
        raise ValueError("不支持的删除类型")

    # 使用 `def` 普通函数 定义 `_resource_search_text()`。
    def _resource_search_text(self, item: Any, keys: Optional[List[str]] = None) -> str:
        # 使用 `if` 判断 `not isinstance(item, dict)`；条件成立时执行下面分支。
        if not isinstance(item, dict):
            # 从 `_AgentServiceResourcesMixin._resource_search_text()` 返回 `""`，并结束当前函数。
            return ""
        # 把 `values` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`values` 在当前函数中承载的中间业务值。
        values = []
        # 计算表达式 `keys or list(item.keys())` 并把结果保存到 `source_keys`；该结果在后续流程中用作`source_keys` 在当前函数中承载的中间业务值。
        source_keys = keys or list(item.keys())
        # 使用 `for` 循环 `key in source_keys`，逐项处理集合数据。
        for key in source_keys:
            # 调用 `item.get(key)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
            value = item.get(key)
            # 使用 `if` 判断 `value is None`；条件成立时执行下面分支。
            if value is None:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 调用列表的 `append()`，把 `values.append(str(value))` 对应的新元素追加到现有结果末尾。
            values.append(str(value))
        # 从 `_AgentServiceResourcesMixin._resource_search_text()` 返回 `" ".join(values)`，并结束当前函数。
        return " ".join(values)

    # 使用 `def` 普通函数 定义 `_resource_type_label()`。
    def _resource_type_label(self, resource_type: Optional[str]) -> str:
        # 从 `_AgentServiceResourcesMixin._resource_type_label()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `jd` 字段为 `"JD"`，用于表示`jd` 在当前函数中承载的中间业务值。
            "jd": "JD",
            # 在字典中设置 `resume` 字段为 `"简历记录"`，用于表示当前候选人的简历评分 ORM 记录。
            "resume": "简历记录",
            # 在字典中设置 `interview` 字段为 `"面试方案"`，用于表示`interview` 在当前函数中承载的中间业务值。
            "interview": "面试方案",
            # 在字典中设置 `exam` 字段为 `"试卷"`，用于表示标准化后的试卷配置或试卷对象。
            "exam": "试卷",
        # 结束前面的业务类型映射字典，并调用 `get()` 按 `resource_type or "", "资源"` 取值；键不存在时使用本行给出的默认名称、路由或建议。
        }.get(resource_type or "", "资源")

    # 使用 `def` 普通函数 定义 `_resource_type_route()`。
    def _resource_type_route(self, resource_type: Optional[str]) -> Optional[str]:
        # 从 `_AgentServiceResourcesMixin._resource_type_route()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `jd` 字段为 `"/recruitment/jd-generator"`，用于表示`jd` 在当前函数中承载的中间业务值。
            "jd": "/recruitment/jd-generator",
            # 在字典中设置 `resume` 字段为 `"/recruitment/resume-screening"`，用于表示当前候选人的简历评分 ORM 记录。
            "resume": "/recruitment/resume-screening",
            # 在字典中设置 `interview` 字段为 `"/recruitment/smart-interview"`，用于表示`interview` 在当前函数中承载的中间业务值。
            "interview": "/recruitment/smart-interview",
            # 在字典中设置 `exam` 字段为 `"/training/exam-generator"`，用于表示标准化后的试卷配置或试卷对象。
            "exam": "/training/exam-generator",
        # 结束前面的业务类型映射字典，并调用 `get()` 按 `resource_type or ""` 取值；键不存在时使用本行给出的默认名称、路由或建议。
        }.get(resource_type or "")

    # 使用 `def` 普通函数 定义 `_delete_suggestions()`。
    def _delete_suggestions(self, resource_type: str) -> List[str]:
        # 从 `_AgentServiceResourcesMixin._delete_suggestions()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `jd` 字段为 `["查看 JD 列表", "重新生成 JD"]`，用于表示`jd` 在当前函数中承载的中间业务值。
            "jd": ["查看 JD 列表", "重新生成 JD"],
            # 在字典中设置 `resume` 字段为 `["查看简历筛选列表", "重新评分简历"]`，用于表示当前候选人的简历评分 ORM 记录。
            "resume": ["查看简历筛选列表", "重新评分简历"],
            # 在字典中设置 `interview` 字段为 `["查看面试方案列表", "重新生成面试方案"]`，用于表示`interview` 在当前函数中承载的中间业务值。
            "interview": ["查看面试方案列表", "重新生成面试方案"],
            # 在字典中设置 `exam` 字段为 `["查看试卷列表", "重新生成试卷"]`，用于表示标准化后的试卷配置或试卷对象。
            "exam": ["查看试卷列表", "重新生成试卷"],
        # 结束前面的业务类型映射字典，并调用 `get()` 按 `resource_type, ["查看列表", "换个名称再试"]` 取值；键不存在时使用本行给出的默认名称、路由或建议。
        }.get(resource_type, ["查看列表", "换个名称再试"])

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_parse_requirements()`。调用模型把自然语言岗位需求解析为结构化字段，失败时用正则与默认值兜底。
    async def _parse_requirements(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `text`，表示`text` 在当前函数中承载的中间业务值，类型标注为 `str`。
        text: str,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`，默认值为 `""`。
        memory_context: str = "",
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
        prompt = (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "你是招聘助手。请从用户需求中提取结构化字段，并严格返回 JSON，不要解释。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "字段：job_title, department, location, salary, experience, education, job_type, skills, benefits, additional_requirements。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "优先结合历史对话记忆理解“继续、这个岗位、刚才”等指代；仍未明确的信息返回 null。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "只提取用户或历史对话中明确提到的信息；未提到的字段必须返回 null，skills/benefits 未提到时返回 []，不要自行推断默认值。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "additional_requirements 只填写用户明确提出的额外岗位要求、加分项、福利或特殊约束；不要填入历史对话原文、当前用户原句或已结构化到其他字段的信息。\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"{self._format_memory_for_prompt(memory_context)}"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"用户需求：{text}"
        # 结束 `_AgentServiceResourcesMixin._parse_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
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
            # 使用 `if` 判断 `parsed`；条件成立时执行下面分支。
            if parsed:
                # 从 `_AgentServiceResourcesMixin._parse_requirements()` 返回 `self._normalize_requirements(parsed, text)`，并结束当前函数。
                return self._normalize_requirements(parsed, text)
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("Agent 需求解析失败，使用本地兜底解析: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 需求解析失败，使用本地兜底解析: %s", exc)
        # 如果 LLM 失败，使用 _fallback_parse()用正则兜底提取岗位、薪资、经验、学历、地点、技能。
        return self._fallback_parse(text)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_parse_exam_requirements()`。把自然语言考试需求转换为标题、科目、题型数量、总分、时长和特殊要求。
    async def _parse_exam_requirements(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `text`，表示`text` 在当前函数中承载的中间业务值，类型标注为 `str`。
        text: str,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `memory_context`，表示从数据库历史消息整理出的短期会话记忆，类型标注为 `str`，默认值为 `""`。
        memory_context: str = "",
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 调用 `self._fallback_exam_parse("\n".join([memory_context, text]) if memory_context else text)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed`；它表示从用户自然语言中解析出的结构化招聘参数。
        parsed = self._fallback_exam_parse("\n".join([memory_context, text]) if memory_context else text)
        # 从 `_AgentServiceResourcesMixin._parse_exam_requirements()` 返回 `self._normalize_exam_requirements(parsed, original_text=text)`，并结束当前函数。
        return self._normalize_exam_requirements(parsed, original_text=text)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_select_exam_knowledge_files()`。
    async def _select_exam_knowledge_files(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `exam`，表示标准化后的试卷配置或试卷对象，类型标注为 `Dict[str, Any]`。
        exam: Dict[str, Any],
        # 声明参数 `message`，表示用户本次输入的自然语言消息，类型标注为 `str`。
        message: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
    # 结束函数参数列表，并用 `-> List[Dict[str, Any]]` 标注该函数的返回值类型。
    ) -> List[Dict[str, Any]]:
        # 计算表达式 `" ".join(` 并把结果保存到 `query`；该结果在后续流程中用作发送给模型、Dify 或数据库检索的输入。
        query = " ".join(
            # 使用列表字面量语法开始构造 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 当前步骤所需的有序元素集合。
            item for item in [
                # 调用 `self._clean_optional_value()`，执行 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 当前步骤所需的构造、查询或业务处理。
                self._clean_optional_value(exam.get("subject")),
                # 调用 `self._clean_optional_value()`，执行 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 当前步骤所需的构造、查询或业务处理。
                self._clean_optional_value(exam.get("title")),
                # 调用 `self._clean_optional_value()`，执行 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 当前步骤所需的构造、查询或业务处理。
                self._clean_optional_value(exam.get("special_requirements")),
                # 把变量 `message` 作为位置参数传入当前多行函数调用；它承载用户本次输入的自然语言消息。
                message,
            # 结束列表推导式，并用条件 `item` 过滤空值，确保后续模型提示词只包含有效业务内容。
            ] if item
        # 完成前面的文本拼接，并调用 `strip()` 删除首尾空白，得到可直接发送给模型或前端的干净内容。
        ).strip()
        # 使用 `if` 判断 `not query`；条件成立时执行下面分支。
        if not query:
            # 从 `_AgentServiceResourcesMixin._select_exam_knowledge_files()` 返回 `[]`，并结束当前函数。
            return []
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `KBSelectionService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `selector`；它表示`selector` 在当前函数中承载的中间业务值。
            selector = KBSelectionService(self.db)
            # 使用 `await` 异步调用 `selector.select_kb_for_question(`，等待数据库、Dify 或模型处理完成后把结果保存到 `selection`；它表示`selection` 在当前函数中承载的中间业务值。
            selection = await selector.select_kb_for_question(
                # 使用关键字参数语法，把 `query` 传入 `question` 参数；它表示`question` 在当前函数中承载的中间业务值。
                question=query,
                # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                user_id=user_id,
                # 使用关键字参数语法，把 `200` 传入 `max_candidates` 参数；它表示`max_candidates` 在当前函数中承载的中间业务值。
                max_candidates=200,
            # 结束 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as exc:
            # 通过模块日志记录器执行 `logger.warning("Agent 试卷知识库文档匹配失败: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 试卷知识库文档匹配失败: %s", exc)
            # 从 `_AgentServiceResourcesMixin._select_exam_knowledge_files()` 返回 `[]`，并结束当前函数。
            return []
        # 使用 `if` 判断 `not selection or not selection.get("document_id")`；条件成立时执行下面分支。
        if not selection or not selection.get("document_id"):
            # 从 `_AgentServiceResourcesMixin._select_exam_knowledge_files()` 返回 `[]`，并结束当前函数。
            return []
        # 调用 `float(selection.get("confidence") or 0)` 完成当前解析、查询或对象构造，并把返回值保存到 `confidence`；它表示`confidence` 在当前函数中承载的中间业务值。
        confidence = float(selection.get("confidence") or 0)
        # 使用 `if` 判断 `confidence and confidence < 0.35`；条件成立时执行下面分支。
        if confidence and confidence < 0.35:
            # 从 `_AgentServiceResourcesMixin._select_exam_knowledge_files()` 返回 `[]`，并结束当前函数。
            return []
        # 从 `_AgentServiceResourcesMixin._select_exam_knowledge_files()` 返回 `[{`，并结束当前函数。
        return [{
            # 在字典中设置 `id` 字段为 `str(selection.get("document_id"))`，用于表示当前执行步骤的唯一标识，供前端定位并更新同一个步骤。
            "id": str(selection.get("document_id")),
            # 在字典中设置 `fileName` 字段为 `selection.get("filename") or "知识库文档"`，用于表示`fileName` 在当前函数中承载的中间业务值。
            "fileName": selection.get("filename") or "知识库文档",
        # 结束 `_AgentServiceResourcesMixin._select_exam_knowledge_files` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }]

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_jd()`。
    async def _generate_jd(self, original_text: str, parsed: Dict[str, Any], conversation_id: Optional[str]) -> str:
        # 调用 `self._build_jd_query(original_text, parsed)` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_jd_query(original_text, parsed)
        # 使用 `await` 异步调用 `self.dify_service.call_workflow_sync(`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self.dify_service.call_workflow_sync(
            # 使用关键字参数语法，把 `1` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
            workflow_type=1,
            # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
            query=query,
            # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=None,
            # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
            additional_inputs={
                # 在字典中设置 `position_title` 字段为 `parsed.get("job_title")`，用于表示`position_title` 在当前函数中承载的中间业务值。
                "position_title": parsed.get("job_title"),
                # 在字典中设置 `department` 字段为 `parsed.get("department")`，用于表示`department` 在当前函数中承载的中间业务值。
                "department": parsed.get("department"),
                # 在字典中设置 `experience_level` 字段为 `parsed.get("experience")`，用于表示`experience_level` 在当前函数中承载的中间业务值。
                "experience_level": parsed.get("experience"),
                # 在字典中设置 `task` 字段为 `"agent_generate_jd"`，用于表示`task` 在当前函数中承载的中间业务值。
                "task": "agent_generate_jd",
            # 结束 `_AgentServiceResourcesMixin._generate_jd` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceResourcesMixin._generate_jd` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceResourcesMixin._generate_jd()` 返回 `self._extract_answer(response).strip()`，并结束当前函数。
        return self._extract_answer(response).strip()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_jd_stream()`。以 Dify type=1 流式生成 JD，并清洗重复 SSE 分片后向上游产出。
    async def _generate_jd_stream(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `original_text`，表示当前步骤整理后的文本，供解析、匹配或模型调用，类型标注为 `str`。
        original_text: str,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> AsyncGenerator[str, None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[str, None]:
        # 调用 `self._build_jd_query(original_text, parsed)` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_jd_query(original_text, parsed)
        # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
        full_text = ""
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for chunk in self.dify_service.call_workflow_stream(
                # 使用关键字参数语法，把 `1` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
                workflow_type=1,
                # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
                query=query,
                # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=None,
                # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
                additional_inputs={
                    # 在字典中设置 `position_title` 字段为 `parsed.get("job_title")`，用于表示`position_title` 在当前函数中承载的中间业务值。
                    "position_title": parsed.get("job_title"),
                    # 在字典中设置 `department` 字段为 `parsed.get("department")`，用于表示`department` 在当前函数中承载的中间业务值。
                    "department": parsed.get("department"),
                    # 在字典中设置 `experience_level` 字段为 `parsed.get("experience")`，用于表示`experience_level` 在当前函数中承载的中间业务值。
                    "experience_level": parsed.get("experience"),
                    # 在字典中设置 `task` 字段为 `"agent_generate_jd"`，用于表示`task` 在当前函数中承载的中间业务值。
                    "task": "agent_generate_jd",
                # 结束 `_AgentServiceResourcesMixin._generate_jd_stream` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                },
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 调用 `self._extract_stream_delta(chunk)` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_delta`；它表示`raw_delta` 在当前函数中承载的中间业务值。
                raw_delta = self._extract_stream_delta(chunk)
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
            # 通过模块日志记录器执行 `logger.warning("Agent JD 流式生成失败，回退同步生成: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent JD 流式生成失败，回退同步生成: %s", exc)

        # 使用 `if` 判断 `not full_text.strip()`；条件成立时执行下面分支。
        if not full_text.strip():
            # 使用 `await` 异步调用 `self._generate_jd(original_text, parsed, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `fallback`；它表示`fallback` 在当前函数中承载的中间业务值。
            fallback = await self._generate_jd(original_text, parsed, conversation_id)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self._stream_text(fallback):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield delta

    # 使用 `def` 普通函数 定义 `_build_jd_query()`。
    def _build_jd_query(self, original_text: str, parsed: Dict[str, Any]) -> str:
        # 计算表达式 `[f"请基于以下招聘需求生成一份专业、完整、适合发布的中文岗位JD：{original_text}"]` 并把结果保存到 `query_parts`；该结果在后续流程中用作`query_parts` 在当前函数中承载的中间业务值。
        query_parts = [f"请基于以下招聘需求生成一份专业、完整、适合发布的中文岗位JD：{original_text}"]
        # 使用字典字面量开始构造 `field_map`；该字典用于保存`field_map` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        field_map = {
            # 在字典中设置 `job_title` 字段为 `"岗位名称"`，用于表示`job_title` 在当前函数中承载的中间业务值。
            "job_title": "岗位名称",
            # 在字典中设置 `department` 字段为 `"部门"`，用于表示`department` 在当前函数中承载的中间业务值。
            "department": "部门",
            # 在字典中设置 `location` 字段为 `"工作地点"`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": "工作地点",
            # 在字典中设置 `salary` 字段为 `"薪资范围"`，用于表示`salary` 在当前函数中承载的中间业务值。
            "salary": "薪资范围",
            # 在字典中设置 `experience` 字段为 `"经验要求"`，用于表示`experience` 在当前函数中承载的中间业务值。
            "experience": "经验要求",
            # 在字典中设置 `education` 字段为 `"学历要求"`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": "学历要求",
        # 结束 `_AgentServiceResourcesMixin._build_jd_query` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `for` 循环 `key, label in field_map.items()`，逐项处理集合数据。
        for key, label in field_map.items():
            # 使用 `if` 判断 `parsed.get(key)`；条件成立时执行下面分支。
            if parsed.get(key):
                # 调用列表的 `append()`，把 `query_parts.append(f"{label}：{parsed[key]}")` 对应的新元素追加到现有结果末尾。
                query_parts.append(f"{label}：{parsed[key]}")
        # 使用 `if` 判断 `parsed.get("skills")`；条件成立时执行下面分支。
        if parsed.get("skills"):
            # 调用列表的 `append()`，把 `query_parts.append(f"技能要求：{'、'.join(parsed['skills'])}")` 对应的新元素追加到现有结果末尾。
            query_parts.append(f"技能要求：{'、'.join(parsed['skills'])}")
        # 从 `_AgentServiceResourcesMixin._build_jd_query()` 返回 `"\n".join(query_parts)`，并结束当前函数。
        return "\n".join(query_parts)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_interview_plan_content()`。
    async def _generate_interview_plan_content(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `resume`，表示当前候选人的简历评分 ORM 记录，类型标注为 `ResumeEvaluation`。
        resume: ResumeEvaluation,
        # 声明参数 `jd_content`，表示提供给评分标准或面试方案生成流程的完整 JD 正文，类型标注为 `str`。
        jd_content: str,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> str` 标注该函数的返回值类型。
    ) -> str:
        # 调用 `self._build_interview_plan_query()` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_interview_plan_query()
        # 使用 `await` 异步调用 `self.dify_service.call_workflow_sync(`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self.dify_service.call_workflow_sync(
            # 使用关键字参数语法，把 `4` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
            workflow_type=4,
            # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
            query=query,
            # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=None,
            # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
            additional_inputs={
                # 在字典中设置 `jianli` 字段为 `resume.resume_content`，用于表示`jianli` 在当前函数中承载的中间业务值。
                "jianli": resume.resume_content,
                # 在字典中设置 `jd` 字段为 `jd_content`，用于表示`jd` 在当前函数中承载的中间业务值。
                "jd": jd_content,
                # 在字典中设置 `evaluation_metrics` 字段为 `resume.evaluation_metrics or []`，用于表示`evaluation_metrics` 在当前函数中承载的中间业务值。
                "evaluation_metrics": resume.evaluation_metrics or [],
                # 在字典中设置 `total_score` 字段为 `resume.total_score`，用于表示`total_score` 在当前函数中承载的中间业务值。
                "total_score": resume.total_score,
                # 在字典中设置 `candidate_name` 字段为 `resume.candidate_name`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                "candidate_name": resume.candidate_name,
                # 在字典中设置 `task` 字段为 `"agent_generate_interview_plan"`，用于表示`task` 在当前函数中承载的中间业务值。
                "task": "agent_generate_interview_plan",
            # 结束 `_AgentServiceResourcesMixin._generate_interview_plan_content` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceResourcesMixin._generate_interview_plan_content` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceResourcesMixin._generate_interview_plan_content()` 返回 `self._extract_answer(response).strip()`，并结束当前函数。
        return self._extract_answer(response).strip()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_interview_plan_stream()`。以 Dify type=4 根据候选人简历、评分和 JD 流式生成面试方案。
    async def _generate_interview_plan_stream(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `resume`，表示当前候选人的简历评分 ORM 记录，类型标注为 `ResumeEvaluation`。
        resume: ResumeEvaluation,
        # 声明参数 `jd_content`，表示提供给评分标准或面试方案生成流程的完整 JD 正文，类型标注为 `str`。
        jd_content: str,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> AsyncGenerator[str, None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[str, None]:
        # 调用 `self._build_interview_plan_query()` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_interview_plan_query()
        # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
        full_text = ""
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for chunk in self.dify_service.call_workflow_stream(
                # 使用关键字参数语法，把 `4` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
                workflow_type=4,
                # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
                query=query,
                # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=None,
                # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
                additional_inputs={
                    # 在字典中设置 `jianli` 字段为 `resume.resume_content`，用于表示`jianli` 在当前函数中承载的中间业务值。
                    "jianli": resume.resume_content,
                    # 在字典中设置 `jd` 字段为 `jd_content`，用于表示`jd` 在当前函数中承载的中间业务值。
                    "jd": jd_content,
                    # 在字典中设置 `evaluation_metrics` 字段为 `resume.evaluation_metrics or []`，用于表示`evaluation_metrics` 在当前函数中承载的中间业务值。
                    "evaluation_metrics": resume.evaluation_metrics or [],
                    # 在字典中设置 `total_score` 字段为 `resume.total_score`，用于表示`total_score` 在当前函数中承载的中间业务值。
                    "total_score": resume.total_score,
                    # 在字典中设置 `candidate_name` 字段为 `resume.candidate_name`，用于表示`candidate_name` 在当前函数中承载的中间业务值。
                    "candidate_name": resume.candidate_name,
                    # 在字典中设置 `task` 字段为 `"agent_generate_interview_plan"`，用于表示`task` 在当前函数中承载的中间业务值。
                    "task": "agent_generate_interview_plan",
                # 结束 `_AgentServiceResourcesMixin._generate_interview_plan_stream` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                },
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 调用 `self._extract_stream_delta(chunk)` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_delta`；它表示`raw_delta` 在当前函数中承载的中间业务值。
                raw_delta = self._extract_stream_delta(chunk)
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
            # 通过模块日志记录器执行 `logger.warning("Agent 面试计划流式生成失败，回退同步生成: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 面试计划流式生成失败，回退同步生成: %s", exc)

        # 使用 `if` 判断 `not full_text.strip()`；条件成立时执行下面分支。
        if not full_text.strip():
            # 使用 `await` 异步调用 `self._generate_interview_plan_content(resume, jd_content, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `fallback`；它表示`fallback` 在当前函数中承载的中间业务值。
            fallback = await self._generate_interview_plan_content(resume, jd_content, conversation_id)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self._stream_text(fallback):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield delta

    # 使用 `def` 普通函数 定义 `_build_interview_plan_query()`。
    def _build_interview_plan_query(self) -> str:
        # 从 `_AgentServiceResourcesMixin._build_interview_plan_query()` 返回 `"请根据候选人简历、JD要求和简历评分结果生成一份结构化面试计划。"`，并结束当前函数。
        return "请根据候选人简历、JD要求和简历评分结果生成一份结构化面试计划。"

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_get_resume_evaluation()`。
    async def _get_resume_evaluation(self, user_id: UUID, resume_evaluation_id: UUID) -> ResumeEvaluation:
        # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = await self.db.execute(
            # 调用 `select()`，执行 `_AgentServiceResourcesMixin._get_resume_evaluation` 当前步骤所需的构造、查询或业务处理。
            select(ResumeEvaluation).where(
                # 构造 SQLAlchemy 查询条件 `ResumeEvaluation.id == resume_evaluation_id`，把数据库记录限制为符合该业务字段要求的数据。
                ResumeEvaluation.id == resume_evaluation_id,
                # 构造 SQLAlchemy 查询条件 `ResumeEvaluation.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
                ResumeEvaluation.user_id == user_id,
            # 结束 `_AgentServiceResourcesMixin._get_resume_evaluation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `_AgentServiceResourcesMixin._get_resume_evaluation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `result.scalar_one_or_none()` 完成当前解析、查询或对象构造，并把返回值保存到 `resume`；它表示当前候选人的简历评分 ORM 记录。
        resume = result.scalar_one_or_none()
        # 使用 `if` 判断 `not resume`；条件成立时执行下面分支。
        if not resume:
            # 使用 `raise` 抛出 `ValueError("简历评价记录未找到或无权限访问")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("简历评价记录未找到或无权限访问")
        # 从 `_AgentServiceResourcesMixin._get_resume_evaluation()` 返回 `resume`，并结束当前函数。
        return resume

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_save_exam_source_document()`。
    async def _save_exam_source_document(self, filename: str, content: bytes, user_id: UUID) -> Document:
        # 调用 `hashlib.sha256(content).hexdigest()` 完成当前解析、查询或对象构造，并把返回值保存到 `file_hash`；它表示`file_hash` 在当前函数中承载的中间业务值。
        file_hash = hashlib.sha256(content).hexdigest()
        # 使用 `await` 异步调用 `self.db.execute(`，等待数据库、Dify 或模型处理完成后把结果保存到 `existing_result`；它表示当前业务调用完成后返回的单个处理结果。
        existing_result = await self.db.execute(
            # 调用 `select()`，执行 `_AgentServiceResourcesMixin._save_exam_source_document` 当前步骤所需的构造、查询或业务处理。
            select(Document).where(
                # 构造 SQLAlchemy 查询条件 `Document.file_hash == file_hash`，把数据库记录限制为符合该业务字段要求的数据。
                Document.file_hash == file_hash,
                # 构造 SQLAlchemy 查询条件 `Document.user_id == user_id`，把数据库记录限制为符合该业务字段要求的数据。
                Document.user_id == user_id,
            # 结束 `_AgentServiceResourcesMixin._save_exam_source_document` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 结束 `_AgentServiceResourcesMixin._save_exam_source_document` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `existing_result.scalar_one_or_none()` 完成当前解析、查询或对象构造，并把返回值保存到 `existing_document`；它表示`existing_document` 在当前函数中承载的中间业务值。
        existing_document = existing_result.scalar_one_or_none()
        # 使用 `if` 判断 `existing_document`；条件成立时执行下面分支。
        if existing_document:
            # 从 `_AgentServiceResourcesMixin._save_exam_source_document()` 返回 `existing_document`，并结束当前函数。
            return existing_document

        # 调用 `os.path.join(settings.UPLOAD_DIR, str(user_id))` 完成当前解析、查询或对象构造，并把返回值保存到 `upload_dir`；它表示`upload_dir` 在当前函数中承载的中间业务值。
        upload_dir = os.path.join(settings.UPLOAD_DIR, str(user_id))
        # 计算表达式 `True)` 并把结果保存到 `os.makedirs(upload_dir, exist_ok`；该结果在后续流程中用作`makedirs(upload_dir, exist_ok` 在当前函数中承载的中间业务值。
        os.makedirs(upload_dir, exist_ok=True)
        # 调用 `os.path.join(upload_dir, filename)` 完成当前解析、查询或对象构造，并把返回值保存到 `file_path`；它表示当前需要读取、加载或写入的文件路径。
        file_path = os.path.join(upload_dir, filename)
        # 调用 `os.path.splitext(filename)` 完成当前解析、查询或对象构造，并把返回值保存到 `base_name, extension`；它表示`base_name, extension` 在当前函数中承载的中间业务值。
        base_name, extension = os.path.splitext(filename)
        # 计算表达式 `1` 并把结果保存到 `counter`；该结果在后续流程中用作`counter` 在当前函数中承载的中间业务值。
        counter = 1
        # 使用 `while` 在 `os.path.exists(file_path)` 成立期间重复执行下面逻辑。
        while os.path.exists(file_path):
            # 调用 `os.path.join(upload_dir, f"{base_name}_{counter}{extension}")` 完成当前解析、查询或对象构造，并把返回值保存到 `file_path`；它表示当前需要读取、加载或写入的文件路径。
            file_path = os.path.join(upload_dir, f"{base_name}_{counter}{extension}")
            # 计算表达式 `1` 并把结果保存到 `counter +`；该结果在后续流程中用作`counter +` 在当前函数中承载的中间业务值。
            counter += 1

        # 使用上下文管理器 `with open(file_path, "wb") as file`，结束后自动释放文件或网络资源。
        with open(file_path, "wb") as file:
            # 调用 `file.write()`，执行 `_AgentServiceResourcesMixin._save_exam_source_document` 当前步骤所需的构造、查询或业务处理。
            file.write(content)

        # 调用 `Document(` 完成当前解析、查询或对象构造，并把返回值保存到 `document`；它表示`document` 在当前函数中承载的中间业务值。
        document = Document(
            # 使用关键字参数语法，把 `os.path.basename(file_path)` 传入 `filename` 参数；它表示`filename` 在当前函数中承载的中间业务值。
            filename=os.path.basename(file_path),
            # 使用关键字参数语法，把 `filename` 传入 `original_filename` 参数；它表示`original_filename` 在当前函数中承载的中间业务值。
            original_filename=filename,
            # 使用关键字参数语法，把 `file_path` 传入 `file_path` 参数；它表示当前需要读取、加载或写入的文件路径。
            file_path=file_path,
            # 使用关键字参数语法，把 `len(content)` 传入 `file_size` 参数；它表示`file_size` 在当前函数中承载的中间业务值。
            file_size=len(content),
            # 使用关键字参数语法，把 `file_hash` 传入 `file_hash` 参数；它表示`file_hash` 在当前函数中承载的中间业务值。
            file_hash=file_hash,
            # 使用关键字参数语法，把 `self._get_document_mime_type(filename)` 传入 `mime_type` 参数；它表示`mime_type` 在当前函数中承载的中间业务值。
            mime_type=self._get_document_mime_type(filename),
            # 使用关键字参数语法，把 `None` 传入 `extracted_content` 参数；它表示当前业务资源的正文内容，供模型生成、保存或前端展示。
            extracted_content=None,
            # 使用关键字参数语法，把 `None` 传入 `embedding` 参数；它表示`embedding` 在当前函数中承载的中间业务值。
            embedding=None,
            # 使用关键字参数语法，把 `"exam_source"` 传入 `category` 参数；它表示`category` 在当前函数中承载的中间业务值。
            category="exam_source",
            # 使用关键字参数语法，把 `["hr_agent", "exam_generation"]` 传入 `tags` 参数；它表示`tags` 在当前函数中承载的中间业务值。
            tags=["hr_agent", "exam_generation"],
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `None` 传入 `knowledge_base_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
            knowledge_base_id=None,
        # 结束 `_AgentServiceResourcesMixin._save_exam_source_document` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `self.db.add()`，执行 `_AgentServiceResourcesMixin._save_exam_source_document` 当前步骤所需的构造、查询或业务处理。
        self.db.add(document)
        # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
        await self.db.commit()
        # 使用 `await` 等待异步操作 `self.db.refresh(document)` 完成，期间不会阻塞事件循环。
        await self.db.refresh(document)
        # 从 `_AgentServiceResourcesMixin._save_exam_source_document()` 返回 `document`，并结束当前函数。
        return document

    # 使用 `def` 普通函数 定义 `_get_document_mime_type()`。
    def _get_document_mime_type(self, filename: str) -> str:
        # 调用 `filename.lower().rsplit(".", 1)[-1] if "." in filename else ""` 完成当前解析、查询或对象构造，并把返回值保存到 `extension`；它表示`extension` 在当前函数中承载的中间业务值。
        extension = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
        # 使用字典字面量开始构造 `mime_types`；该字典用于保存`mime_types` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        mime_types = {
            # 在字典中设置 `pdf` 字段为 `"application/pdf"`，用于表示`pdf` 在当前函数中承载的中间业务值。
            "pdf": "application/pdf",
            # 在字典中设置 `doc` 字段为 `"application/msword"`，用于表示`doc` 在当前函数中承载的中间业务值。
            "doc": "application/msword",
            # 在字典中设置 `docx` 字段为 `"application/vnd.openxmlformats-officedocument.wordprocessingml.document"`，用于表示`docx` 在当前函数中承载的中间业务值。
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            # 在字典中设置 `txt` 字段为 `"text/plain"`，用于表示`txt` 在当前函数中承载的中间业务值。
            "txt": "text/plain",
            # 在字典中设置 `md` 字段为 `"text/markdown"`，用于表示`md` 在当前函数中承载的中间业务值。
            "md": "text/markdown",
        # 结束 `_AgentServiceResourcesMixin._get_document_mime_type` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 从 `_AgentServiceResourcesMixin._get_document_mime_type()` 返回 `mime_types.get(extension, "application/octet-stream")`，并结束当前函数。
        return mime_types.get(extension, "application/octet-stream")

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_scoring_criteria()`。
    async def _generate_scoring_criteria(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `jd_content`，表示提供给评分标准或面试方案生成流程的完整 JD 正文，类型标注为 `str`。
        jd_content: str,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> str` 标注该函数的返回值类型。
    ) -> str:
        # 调用 `self._build_scoring_criteria_query(jd_content)` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_scoring_criteria_query(jd_content)
        # 使用 `await` 异步调用 `self.dify_service.call_workflow_sync(`，等待数据库、Dify 或模型处理完成后把结果保存到 `response`；它表示下游服务或 Agent 流程返回的响应。
        response = await self.dify_service.call_workflow_sync(
            # 使用关键字参数语法，把 `2` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
            workflow_type=2,
            # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
            query=query,
            # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=None,
            # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
            additional_inputs={
                # 在字典中设置 `job_title` 字段为 `parsed.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                "job_title": parsed.get("job_title"),
                # 在字典中设置 `task` 字段为 `"agent_generate_scoring_criteria"`，用于表示`task` 在当前函数中承载的中间业务值。
                "task": "agent_generate_scoring_criteria",
            # 结束 `_AgentServiceResourcesMixin._generate_scoring_criteria` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
        # 结束 `_AgentServiceResourcesMixin._generate_scoring_criteria` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 从 `_AgentServiceResourcesMixin._generate_scoring_criteria()` 返回 `self._extract_answer(response).strip()`，并结束当前函数。
        return self._extract_answer(response).strip()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_generate_scoring_criteria_stream()`。以 Dify type=2 根据 JD 流式生成评分标准。
    async def _generate_scoring_criteria_stream(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `jd_content`，表示提供给评分标准或面试方案生成流程的完整 JD 正文，类型标注为 `str`。
        jd_content: str,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束函数参数列表，并用 `-> AsyncGenerator[str, None]` 标注该函数的返回值类型。
    ) -> AsyncGenerator[str, None]:
        # 调用 `self._build_scoring_criteria_query(jd_content)` 完成当前解析、查询或对象构造，并把返回值保存到 `query`；它表示发送给模型、Dify 或数据库检索的输入。
        query = self._build_scoring_criteria_query(jd_content)
        # 把 `full_text` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入当前步骤整理后的文本，供解析、匹配或模型调用。
        full_text = ""
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for chunk in self.dify_service.call_workflow_stream(
                # 使用关键字参数语法，把 `2` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
                workflow_type=2,
                # 使用关键字参数语法，把 `query` 传入 `query` 参数；它表示发送给模型、Dify 或数据库检索的输入。
                query=query,
                # 使用关键字参数语法，把 `None` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
                conversation_id=None,
                # 使用字典字面量开始构造 `additional_inputs`；该字典用于保存`additional_inputs` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
                additional_inputs={
                    # 在字典中设置 `job_title` 字段为 `parsed.get("job_title")`，用于表示`job_title` 在当前函数中承载的中间业务值。
                    "job_title": parsed.get("job_title"),
                    # 在字典中设置 `task` 字段为 `"agent_generate_scoring_criteria"`，用于表示`task` 在当前函数中承载的中间业务值。
                    "task": "agent_generate_scoring_criteria",
                # 结束 `_AgentServiceResourcesMixin._generate_scoring_criteria_stream` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                },
            # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
            ):
                # 调用 `self._extract_stream_delta(chunk)` 完成当前解析、查询或对象构造，并把返回值保存到 `raw_delta`；它表示`raw_delta` 在当前函数中承载的中间业务值。
                raw_delta = self._extract_stream_delta(chunk)
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
            # 通过模块日志记录器执行 `logger.warning("Agent 评分标准流式生成失败，回退同步生成: %s", exc)`，用于保留运行状态或异常原因。
            logger.warning("Agent 评分标准流式生成失败，回退同步生成: %s", exc)

        # 使用 `if` 判断 `not full_text.strip()`；条件成立时执行下面分支。
        if not full_text.strip():
            # 使用 `await` 异步调用 `self._generate_scoring_criteria(jd_content, parsed, conversation_id)`，等待数据库、Dify 或模型处理完成后把结果保存到 `fallback`；它表示`fallback` 在当前函数中承载的中间业务值。
            fallback = await self._generate_scoring_criteria(jd_content, parsed, conversation_id)
            # 异步消费下游流式事件，收到一段就立即继续向前端转发。
            async for delta in self._stream_text(fallback):
                # 产出当前 SSE 或文本分片，同时保留异步生成器状态等待下一段结果。
                yield delta

    # 使用 `def` 普通函数 定义 `_build_scoring_criteria_query()`。
    def _build_scoring_criteria_query(self, jd_content: str) -> str:
        # 从 `_AgentServiceResourcesMixin._build_scoring_criteria_query()` 返回 `(`，并结束当前函数。
        return (
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            f"请基于以下JD内容，生成简历初筛评分标准，总分100分。\n\n{jd_content}\n\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "请包含技能匹配、工作经验、教育背景、项目经验、加分项和淘汰项，并给出可操作的分值区间。"
        # 结束 `_AgentServiceResourcesMixin._build_scoring_criteria_query` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_save_job_description()`。
    async def _save_job_description(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `jd_content`，表示提供给评分标准或面试方案生成流程的完整 JD 正文，类型标注为 `str`。
        jd_content: str,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `original_text`，表示当前步骤整理后的文本，供解析、匹配或模型调用，类型标注为 `str`。
        original_text: str,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
    # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
    ):
        # 调用 `JobDescriptionCreate(` 完成当前解析、查询或对象构造，并把返回值保存到 `jd_data`；它表示`jd_data` 在当前函数中承载的中间业务值。
        jd_data = JobDescriptionCreate(
            # 使用关键字参数语法，把 `parsed.get("job_title") or "未命名岗位"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=parsed.get("job_title") or "未命名岗位",
            # 使用关键字参数语法，把 `parsed.get("department")` 传入 `department` 参数；它表示`department` 在当前函数中承载的中间业务值。
            department=parsed.get("department"),
            # 使用关键字参数语法，把 `parsed.get("location")` 传入 `location` 参数；它表示`location` 在当前函数中承载的中间业务值。
            location=parsed.get("location"),
            # 使用关键字参数语法，把 `parsed.get("salary")` 传入 `salary_range` 参数；它表示`salary_range` 在当前函数中承载的中间业务值。
            salary_range=parsed.get("salary"),
            # 使用关键字参数语法，把 `parsed.get("experience")` 传入 `experience_level` 参数；它表示`experience_level` 在当前函数中承载的中间业务值。
            experience_level=parsed.get("experience"),
            # 使用关键字参数语法，把 `parsed.get("education")` 传入 `education` 参数；它表示`education` 在当前函数中承载的中间业务值。
            education=parsed.get("education"),
            # 使用关键字参数语法，把 `parsed.get("job_type") or "全职"` 传入 `job_type` 参数；它表示`job_type` 在当前函数中承载的中间业务值。
            job_type=parsed.get("job_type") or "全职",
            # 使用关键字参数语法，把 `parsed.get("skills") or []` 传入 `skills` 参数；它表示`skills` 在当前函数中承载的中间业务值。
            skills=parsed.get("skills") or [],
            # 使用关键字参数语法，把 `jd_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=jd_content,
            # 使用关键字参数语法，把 `parsed.get("additional_requirements") or original_text` 传入 `requirements` 参数；它表示`requirements` 在当前函数中承载的中间业务值。
            requirements=parsed.get("additional_requirements") or original_text,
            # 使用关键字参数语法，把 `"draft"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status="draft",
            # 使用字典字面量开始构造 `meta_data`；该字典用于保存`meta_data` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
            meta_data={
                # 在字典中设置 `source` 字段为 `"hr_agent"`，用于表示`source` 在当前函数中承载的中间业务值。
                "source": "hr_agent",
                # 在字典中设置 `salary` 字段为 `parsed.get("salary")`，用于表示`salary` 在当前函数中承载的中间业务值。
                "salary": parsed.get("salary"),
                # 在字典中设置 `location` 字段为 `parsed.get("location")`，用于表示`location` 在当前函数中承载的中间业务值。
                "location": parsed.get("location"),
                # 在字典中设置 `benefits` 字段为 `parsed.get("benefits") or []`，用于表示`benefits` 在当前函数中承载的中间业务值。
                "benefits": parsed.get("benefits") or [],
            # 结束 `_AgentServiceResourcesMixin._save_job_description` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
            # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=conversation_id,
            # 使用关键字参数语法，把 `"agent_jd_generation"` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
            workflow_type="agent_jd_generation",
        # 结束 `_AgentServiceResourcesMixin._save_job_description` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `JobDescriptionService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `service`；它表示`service` 在当前函数中承载的中间业务值。
        service = JobDescriptionService(self.db)
        # 从 `_AgentServiceResourcesMixin._save_job_description()` 返回 `await service.create_job_description(jd_data, user_id)`，并结束当前函数。
        return await service.create_job_description(jd_data, user_id)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_save_scoring_criteria()`。
    async def _save_scoring_criteria(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `criteria_content`，表示根据 JD 生成的完整简历评分标准正文，类型标注为 `str`。
        criteria_content: str,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `conversation_id`，表示当前聊天窗口 ID，用于读取和保存同一会话的历史，类型标注为 `Optional[str]`。
        conversation_id: Optional[str],
        # 声明参数 `saved_jd_id`，表示用于关联对应数据库记录或业务资源的唯一标识，类型标注为 `Optional[UUID]`，默认值为 `None`。
        saved_jd_id: Optional[UUID] = None,
    # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
    ):
        # 调用 `ScoringCriteriaCreate(` 完成当前解析、查询或对象构造，并把返回值保存到 `criteria_data`；它表示`criteria_data` 在当前函数中承载的中间业务值。
        criteria_data = ScoringCriteriaCreate(
            # 使用关键字参数语法，把 `f"{parsed.get('job_title') or '岗位'}简历评分标准"` 传入 `title` 参数；它表示前端向用户展示的步骤或业务产物标题。
            title=f"{parsed.get('job_title') or '岗位'}简历评分标准",
            # 使用关键字参数语法，把 `parsed.get("job_title")` 传入 `job_title` 参数；它表示`job_title` 在当前函数中承载的中间业务值。
            job_title=parsed.get("job_title"),
            # 使用关键字参数语法，把 `criteria_content` 传入 `content` 参数；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
            content=criteria_content,
            # 使用关键字参数语法，把 `"100"` 传入 `total_score` 参数；它表示`total_score` 在当前函数中承载的中间业务值。
            total_score="100",
            # 使用关键字参数语法，把 `"draft"` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status="draft",
            # 使用关键字参数语法，把 `{"source": "hr_agent"}` 传入 `meta_data` 参数；它表示`meta_data` 在当前函数中承载的中间业务值。
            meta_data={"source": "hr_agent"},
            # 使用关键字参数语法，把 `conversation_id` 传入 `conversation_id` 参数；它表示当前聊天窗口 ID，用于读取和保存同一会话的历史。
            conversation_id=conversation_id,
            # 使用关键字参数语法，把 `"agent_scoring_criteria_generation"` 传入 `workflow_type` 参数；它表示`workflow_type` 在当前函数中承载的中间业务值。
            workflow_type="agent_scoring_criteria_generation",
            # 使用关键字参数语法，把 `saved_jd_id` 传入 `job_description_id` 参数；它表示用于关联对应数据库记录或业务资源的唯一标识。
            job_description_id=saved_jd_id,
        # 结束 `_AgentServiceResourcesMixin._save_scoring_criteria` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `ScoringCriteriaService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `service`；它表示`service` 在当前函数中承载的中间业务值。
        service = ScoringCriteriaService(self.db)
        # 从 `_AgentServiceResourcesMixin._save_scoring_criteria()` 返回 `await service.save_scoring_criteria(criteria_data, user_id)`，并结束当前函数。
        return await service.save_scoring_criteria(criteria_data, user_id)

    # 使用 `def` 普通函数 定义 `_extract_answer()`。
    def _extract_answer(self, response: Any) -> str:
        # 使用 `if` 判断 `isinstance(response, dict)`；条件成立时执行下面分支。
        if isinstance(response, dict):
            # 使用 `if` 判断 `response.get("answer")`；条件成立时执行下面分支。
            if response.get("answer"):
                # 从 `_AgentServiceResourcesMixin._extract_answer()` 返回 `str(response["answer"])`，并结束当前函数。
                return str(response["answer"])
            # 调用 `response.get("data")` 完成当前解析、查询或对象构造，并把返回值保存到 `data`；它表示当前 JSON、SKILL.md 元数据或业务对象解析得到的字典。
            data = response.get("data")
            # 使用 `if` 判断 `isinstance(data, dict) and data.get("answer")`；条件成立时执行下面分支。
            if isinstance(data, dict) and data.get("answer"):
                # 从 `_AgentServiceResourcesMixin._extract_answer()` 返回 `str(data["answer"])`，并结束当前函数。
                return str(data["answer"])
            # 使用 `if` 判断 `response.get("text")`；条件成立时执行下面分支。
            if response.get("text"):
                # 从 `_AgentServiceResourcesMixin._extract_answer()` 返回 `str(response["text"])`，并结束当前函数。
                return str(response["text"])
            # 从 `_AgentServiceResourcesMixin._extract_answer()` 返回 `json.dumps(response, ensure_ascii=False)`，并结束当前函数。
            return json.dumps(response, ensure_ascii=False)
        # 从 `_AgentServiceResourcesMixin._extract_answer()` 返回 `str(response)`，并结束当前函数。
        return str(response)

    # 使用 `def` 普通函数 定义 `_extract_stream_delta()`。
    def _extract_stream_delta(self, chunk: Any) -> str:
        # 使用 `if` 判断 `isinstance(chunk, str)`；条件成立时执行下面分支。
        if isinstance(chunk, str):
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用 `json.loads(chunk)` 完成当前解析、查询或对象构造，并把返回值保存到 `chunk`；它表示`chunk` 在当前函数中承载的中间业务值。
                chunk = json.loads(chunk)
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except json.JSONDecodeError:
                # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `chunk`，并结束当前函数。
                return chunk
        # 使用 `if` 判断 `not isinstance(chunk, dict)`；条件成立时执行下面分支。
        if not isinstance(chunk, dict):
            # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `str(chunk)`，并结束当前函数。
            return str(chunk)

        # 调用 `chunk.get("event")` 完成当前解析、查询或对象构造，并把返回值保存到 `event`；它表示`event` 在当前函数中承载的中间业务值。
        event = chunk.get("event")
        # 使用 `if` 判断 `event in {`；条件成立时执行下面分支。
        if event in {
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "message_end",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "agent_message_end",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "workflow_finished",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "node_finished",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "tts_message",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "tts_message_end",
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "message_file",
        # 结束 `_AgentServiceResourcesMixin._extract_stream_delta` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }:
            # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `""`，并结束当前函数。
            return ""
        # 使用 `if` 判断 `event and event not in {"message", "agent_message", "text_chunk"}`；条件成立时执行下面分支。
        if event and event not in {"message", "agent_message", "text_chunk"}:
            # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `""`，并结束当前函数。
            return ""

        # 使用 `for` 循环 `key in ("answer", "delta", "text")`，逐项处理集合数据。
        for key in ("answer", "delta", "text"):
            # 调用 `chunk.get(key)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
            value = chunk.get(key)
            # 使用 `if` 判断 `isinstance(value, str)`；条件成立时执行下面分支。
            if isinstance(value, str):
                # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `value`，并结束当前函数。
                return value

        # 调用 `chunk.get("data")` 完成当前解析、查询或对象构造，并把返回值保存到 `data`；它表示当前 JSON、SKILL.md 元数据或业务对象解析得到的字典。
        data = chunk.get("data")
        # 使用 `if` 判断 `isinstance(data, dict)`；条件成立时执行下面分支。
        if isinstance(data, dict):
            # 使用 `for` 循环 `key in ("answer", "delta", "text")`，逐项处理集合数据。
            for key in ("answer", "delta", "text"):
                # 调用 `data.get(key)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
                value = data.get(key)
                # 使用 `if` 判断 `isinstance(value, str)`；条件成立时执行下面分支。
                if isinstance(value, str):
                    # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `value`，并结束当前函数。
                    return value
        # 从 `_AgentServiceResourcesMixin._extract_stream_delta()` 返回 `""`，并结束当前函数。
        return ""

    # 使用 `def` 普通函数 定义 `_normalize_sse_chunk()`。
    def _normalize_sse_chunk(self, chunk: Any) -> Any:
        # 使用 `if` 判断 `not isinstance(chunk, str)`；条件成立时执行下面分支。
        if not isinstance(chunk, str):
            # 从 `_AgentServiceResourcesMixin._normalize_sse_chunk()` 返回 `chunk`，并结束当前函数。
            return chunk
        # 调用 `chunk.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `text`；它表示`text` 在当前函数中承载的中间业务值。
        text = chunk.strip()
        # 使用 `if` 判断 `not text`；条件成立时执行下面分支。
        if not text:
            # 从 `_AgentServiceResourcesMixin._normalize_sse_chunk()` 返回 `""`，并结束当前函数。
            return ""
        # 把 `lines` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入按换行拆开的文本行。
        lines = []
        # 使用 `for` 循环 `line in text.splitlines()`，逐项处理集合数据。
        for line in text.splitlines():
            # 调用 `line.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `line`；它表示`line` 在当前函数中承载的中间业务值。
            line = line.strip()
            # 使用 `if` 判断 `line.startswith("data:")`；条件成立时执行下面分支。
            if line.startswith("data:"):
                # 计算表达式 `line[5:].strip()` 并把结果保存到 `line`；该结果在后续流程中用作`line` 在当前函数中承载的中间业务值。
                line = line[5:].strip()
            # 使用 `if` 判断 `line and line != "[DONE]"`；条件成立时执行下面分支。
            if line and line != "[DONE]":
                # 调用列表的 `append()`，把 `lines.append(line)` 对应的新元素追加到现有结果末尾。
                lines.append(line)
        # 从 `_AgentServiceResourcesMixin._normalize_sse_chunk()` 返回 `"\n".join(lines)`，并结束当前函数。
        return "\n".join(lines)

    # 使用 `def` 普通函数 定义 `_dedupe_stream_delta()`。
    def _dedupe_stream_delta(self, delta: str, full_text: str) -> str:
        """兼容 Dify 流式返回增量/累计全文/最终全文，避免前端重复拼接。"""
        # 使用 `if` 判断 `not delta`；条件成立时执行下面分支。
        if not delta:
            # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `""`，并结束当前函数。
            return ""
        # 使用 `if` 判断 `not full_text`；条件成立时执行下面分支。
        if not full_text:
            # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `delta`，并结束当前函数。
            return delta
        # 使用 `if` 判断 `delta == full_text or delta.strip() == full_text.strip()`；条件成立时执行下面分支。
        if delta == full_text or delta.strip() == full_text.strip():
            # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `""`，并结束当前函数。
            return ""
        # 使用 `if` 判断 `delta.startswith(full_text)`；条件成立时执行下面分支。
        if delta.startswith(full_text):
            # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `delta[len(full_text):]`，并结束当前函数。
            return delta[len(full_text):]

        # 较大的 chunk 往往是累计全文或最终全文，尝试按前后缀重叠裁剪。
        # 小 token 不做重叠裁剪，避免把正常重复字符误删。
        if len(delta) < 20:
            # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `delta`，并结束当前函数。
            return delta

        # 调用 `min(len(full_text), len(delta))` 完成当前解析、查询或对象构造，并把返回值保存到 `max_overlap`；它表示`max_overlap` 在当前函数中承载的中间业务值。
        max_overlap = min(len(full_text), len(delta))
        # 使用 `for` 循环 `size in range(max_overlap, 0, -1)`，逐项处理集合数据。
        for size in range(max_overlap, 0, -1):
            # 使用 `if` 判断 `full_text.endswith(delta[:size])`；条件成立时执行下面分支。
            if full_text.endswith(delta[:size]):
                # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `delta[size:]`，并结束当前函数。
                return delta[size:]
        # 从 `_AgentServiceResourcesMixin._dedupe_stream_delta()` 返回 `delta`，并结束当前函数。
        return delta

    # 使用 `def` 普通函数 定义 `_safe_json_loads()`。
    def _safe_json_loads(self, text: str) -> Dict[str, Any]:
        # 调用 `text.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `json_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
        json_text = text.strip()
        # 使用 `if` 判断 `"```" in json_text`；条件成立时执行下面分支。
        if "```" in json_text:
            # 调用 `re.sub(r"^```(?:json)?", "", json_text, flags=re.IGNORECASE).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `json_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
            json_text = re.sub(r"^```(?:json)?", "", json_text, flags=re.IGNORECASE).strip()
            # 调用 `re.sub(r"```$", "", json_text).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `json_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
            json_text = re.sub(r"```$", "", json_text).strip()
        # 调用 `re.search(r"\{.*\}", json_text, re.S)` 完成当前解析、查询或对象构造，并把返回值保存到 `match`；它表示`match` 在当前函数中承载的中间业务值。
        match = re.search(r"\{.*\}", json_text, re.S)
        # 使用 `if` 判断 `match`；条件成立时执行下面分支。
        if match:
            # 调用 `match.group(0)` 完成当前解析、查询或对象构造，并把返回值保存到 `json_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
            json_text = match.group(0)
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `json.loads(json_text)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
            value = json.loads(json_text)
            # 从 `_AgentServiceResourcesMixin._safe_json_loads()` 返回 `value if isinstance(value, dict) else {}`，并结束当前函数。
            return value if isinstance(value, dict) else {}
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception:
            # 从 `_AgentServiceResourcesMixin._safe_json_loads()` 返回 `{}`，并结束当前函数。
            return {}

    # 使用 `def` 普通函数 定义 `_normalize_requirements()`。
    def _normalize_requirements(self, parsed: Dict[str, Any], original_text: str) -> Dict[str, Any]:
        # 使用字典字面量开始构造 `normalized`；该字典用于保存`normalized` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        normalized = {
            # 在字典中设置 `job_title` 字段为 `self._clean_optional_value(parsed.get("job_title"))`，用于表示`job_title` 在当前函数中承载的中间业务值。
            "job_title": self._clean_optional_value(parsed.get("job_title")),
            # 在字典中设置 `department` 字段为 `self._clean_optional_value(parsed.get("department"))`，用于表示`department` 在当前函数中承载的中间业务值。
            "department": self._clean_optional_value(parsed.get("department")),
            # 在字典中设置 `location` 字段为 `self._clean_optional_value(parsed.get("location"))`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": self._clean_optional_value(parsed.get("location")),
            # 在字典中设置 `salary` 字段为 `self._clean_optional_value(parsed.get("salary"))`，用于表示`salary` 在当前函数中承载的中间业务值。
            "salary": self._clean_optional_value(parsed.get("salary")),
            # 在字典中设置 `experience` 字段为 `self._clean_optional_value(parsed.get("experience"))`，用于表示`experience` 在当前函数中承载的中间业务值。
            "experience": self._clean_optional_value(parsed.get("experience")),
            # 在字典中设置 `education` 字段为 `self._clean_optional_value(parsed.get("education"))`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": self._clean_optional_value(parsed.get("education")),
            # 在字典中设置 `job_type` 字段为 `self._clean_optional_value(parsed.get("job_type"))`，用于表示`job_type` 在当前函数中承载的中间业务值。
            "job_type": self._clean_optional_value(parsed.get("job_type")),
            # 在字典中设置 `skills` 字段为 `parsed.get("skills") or []`，用于表示`skills` 在当前函数中承载的中间业务值。
            "skills": parsed.get("skills") or [],
            # 在字典中设置 `benefits` 字段为 `parsed.get("benefits") or []`，用于表示`benefits` 在当前函数中承载的中间业务值。
            "benefits": parsed.get("benefits") or [],
            # 在字典中设置 `additional_requirements` 字段为 `self._clean_optional_value(parsed.get("additional_requirements"))`，用于表示`additional_requirements` 在当前函数中承载的中间业务值。
            "additional_requirements": self._clean_optional_value(parsed.get("additional_requirements")),
        # 结束 `_AgentServiceResourcesMixin._normalize_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `if` 判断 `isinstance(normalized["skills"], str)`；条件成立时执行下面分支。
        if isinstance(normalized["skills"], str):
            # 计算表达式 `[item.strip() for item in re.split(r"[,，、/]", normalized["skills"]) if item.strip()]` 并把结果保存到 `normalized["skills"]`；该结果在后续流程中用作`normalized` 在当前函数中承载的中间业务值。
            normalized["skills"] = [item.strip() for item in re.split(r"[,，、/]", normalized["skills"]) if item.strip()]
        # 使用 `if` 判断 `isinstance(normalized["benefits"], str)`；条件成立时执行下面分支。
        if isinstance(normalized["benefits"], str):
            # 计算表达式 `[item.strip() for item in re.split(r"[,，、/]", normalized["benefits"]) if item.strip()]` 并把结果保存到 `normalized["benefits"]`；该结果在后续流程中用作`normalized` 在当前函数中承载的中间业务值。
            normalized["benefits"] = [item.strip() for item in re.split(r"[,，、/]", normalized["benefits"]) if item.strip()]
        # 从 `_AgentServiceResourcesMixin._normalize_requirements()` 返回 `normalized`，并结束当前函数。
        return normalized

    # 使用 `def` 普通函数 定义 `_normalize_exam_requirements()`。
    def _normalize_exam_requirements(
        # 声明 `self` 实例参数；方法通过它读取或修改当前 AgentService 对象的属性。
        self,
        # 声明参数 `parsed`，表示从用户自然语言中解析出的结构化招聘参数，类型标注为 `Dict[str, Any]`。
        parsed: Dict[str, Any],
        # 声明参数 `original_text`，表示当前步骤整理后的文本，供解析、匹配或模型调用，类型标注为 `Optional[str]`，默认值为 `None`。
        original_text: Optional[str] = None,
    # 结束函数参数列表，并用 `-> Dict[str, Any]` 标注该函数的返回值类型。
    ) -> Dict[str, Any]:
        # 调用 `parsed.get("question_counts") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `question_counts`；它表示`question_counts` 在当前函数中承载的中间业务值。
        question_counts = parsed.get("question_counts") or {}
        # 使用 `if` 判断 `not isinstance(question_counts, dict)`；条件成立时执行下面分支。
        if not isinstance(question_counts, dict):
            # 把 `question_counts` 初始化为 `{}`，表示当前尚无可用结果；后续分支成功后会写入`question_counts` 在当前函数中承载的中间业务值。
            question_counts = {}
        # 使用字典字面量开始构造 `normalized`；该字典用于保存`normalized` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        normalized = {
            # 在字典中设置 `title` 字段为 `self._clean_optional_value(parsed.get("title")) or (f"{self._clean_optional_value(parsed.get('sub...`，用于表示前端向用户展示的步骤或业务产物标题。
            "title": self._clean_optional_value(parsed.get("title")) or (f"{self._clean_optional_value(parsed.get('subject')) or '候选人'}能力测试"),
            # 在字典中设置 `subject` 字段为 `self._clean_optional_value(parsed.get("subject")) or self._infer_exam_subject(original_text or "")`，用于表示待发送邮件的主题。
            "subject": self._clean_optional_value(parsed.get("subject")) or self._infer_exam_subject(original_text or ""),
            # 在字典中设置 `description` 字段为 `self._clean_optional_value(parsed.get("description"))`，用于表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
            "description": self._clean_optional_value(parsed.get("description")),
            # 在字典中设置 `difficulty` 字段为 `self._clean_optional_value(parsed.get("difficulty")) or "medium"`，用于表示`difficulty` 在当前函数中承载的中间业务值。
            "difficulty": self._clean_optional_value(parsed.get("difficulty")) or "medium",
            # 在字典中设置 `duration` 字段为 `self._safe_int(parsed.get("duration"), 60)`，用于表示`duration` 在当前函数中承载的中间业务值。
            "duration": self._safe_int(parsed.get("duration"), 60),
            # 在字典中设置 `total_score` 字段为 `self._safe_int(parsed.get("total_score"), 100)`，用于表示`total_score` 在当前函数中承载的中间业务值。
            "total_score": self._safe_int(parsed.get("total_score"), 100),
            # 在字典中设置 `question_types` 字段为 `parsed.get("question_types") or ["single_choice", "multiple_choice", "short_answer"]`，用于表示`question_types` 在当前函数中承载的中间业务值。
            "question_types": parsed.get("question_types") or ["single_choice", "multiple_choice", "short_answer"],
            # 在字典中设置 `question_counts` 字段为 `{`，用于表示`question_counts` 在当前函数中承载的中间业务值。
            "question_counts": {
                # 在字典中设置 `single_choice` 字段为 `self._safe_int(question_counts.get("single_choice"), 5)`，用于表示`single_choice` 在当前函数中承载的中间业务值。
                "single_choice": self._safe_int(question_counts.get("single_choice"), 5),
                # 在字典中设置 `multiple_choice` 字段为 `self._safe_int(question_counts.get("multiple_choice"), 3)`，用于表示`multiple_choice` 在当前函数中承载的中间业务值。
                "multiple_choice": self._safe_int(question_counts.get("multiple_choice"), 3),
                # 在字典中设置 `short_answer` 字段为 `self._safe_int(question_counts.get("short_answer"), 2)`，用于表示`short_answer` 在当前函数中承载的中间业务值。
                "short_answer": self._safe_int(question_counts.get("short_answer"), 2),
            # 结束 `_AgentServiceResourcesMixin._normalize_exam_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            },
            # 在字典中设置 `knowledge_files` 字段为 `self._normalize_knowledge_files(parsed.get("knowledge_files"))`，用于表示`knowledge_files` 在当前函数中承载的中间业务值。
            "knowledge_files": self._normalize_knowledge_files(parsed.get("knowledge_files")),
            # 在字典中设置 `special_requirements` 字段为 `self._clean_optional_value(parsed.get("special_requirements")) or ""`，用于表示`special_requirements` 在当前函数中承载的中间业务值。
            "special_requirements": self._clean_optional_value(parsed.get("special_requirements")) or "",
            # 在字典中设置 `interview_plan_context` 字段为 `parsed.get("interview_plan_context") if isinstance(parsed.get("interview_plan_context"), dict) el...`，用于表示当前业务步骤需要的历史、用户或 Skill 上下文。
            "interview_plan_context": parsed.get("interview_plan_context") if isinstance(parsed.get("interview_plan_context"), dict) else None,
        # 结束 `_AgentServiceResourcesMixin._normalize_exam_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `if` 判断 `isinstance(normalized["question_types"], str)`；条件成立时执行下面分支。
        if isinstance(normalized["question_types"], str):
            # 计算表达式 `[item.strip() for item in re.split(r"[,，、/]", normalized["question_types"]) if item.strip()]` 并把结果保存到 `normalized["question_types"]`；该结果在后续流程中用作`normalized` 在当前函数中承载的中间业务值。
            normalized["question_types"] = [item.strip() for item in re.split(r"[,，、/]", normalized["question_types"]) if item.strip()]
        # 从 `_AgentServiceResourcesMixin._normalize_exam_requirements()` 返回 `normalized`，并结束当前函数。
        return normalized

    # 使用 `def` 普通函数 定义 `_safe_int()`。
    def _safe_int(self, value: Any, default: int) -> int:
        # 使用 `if` 判断 `value is None or value == ""`；条件成立时执行下面分支。
        if value is None or value == "":
            # 从 `_AgentServiceResourcesMixin._safe_int()` 返回 `default`，并结束当前函数。
            return default
        # 调用 `re.search(r"\d+", str(value))` 完成当前解析、查询或对象构造，并把返回值保存到 `match`；它表示`match` 在当前函数中承载的中间业务值。
        match = re.search(r"\d+", str(value))
        # 从 `_AgentServiceResourcesMixin._safe_int()` 返回 `int(match.group(0)) if match else default`，并结束当前函数。
        return int(match.group(0)) if match else default

    # 使用 `def` 普通函数 定义 `_normalize_knowledge_files()`。
    def _normalize_knowledge_files(self, value: Any) -> List[Dict[str, Any]]:
        # 使用 `if` 判断 `not isinstance(value, list)`；条件成立时执行下面分支。
        if not isinstance(value, list):
            # 从 `_AgentServiceResourcesMixin._normalize_knowledge_files()` 返回 `[]`，并结束当前函数。
            return []
        # 把 `files` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`files` 在当前函数中承载的中间业务值。
        files = []
        # 使用 `for` 循环 `item in value`，逐项处理集合数据。
        for item in value:
            # 使用 `if` 判断 `isinstance(item, dict) and item.get("id")`；条件成立时执行下面分支。
            if isinstance(item, dict) and item.get("id"):
                # 调用列表的 `append()`，把 `files.append({"id": item.get("id"), "fileName": item.get("fileName") or item.get("filename") or "...` 对应的新元素追加到现有结果末尾。
                files.append({"id": item.get("id"), "fileName": item.get("fileName") or item.get("filename") or "知识库文档"})
        # 从 `_AgentServiceResourcesMixin._normalize_knowledge_files()` 返回 `files`，并结束当前函数。
        return files

    # 使用 `def` 普通函数 定义 `_infer_exam_subject()`。
    def _infer_exam_subject(self, text: str) -> str:
        # 计算表达式 `["Java", "Python", "Go", "前端", "后端", "产品经理", "AI产品经理", "测试", "算法", "运营"]` 并把结果保存到 `patterns`；该结果在后续流程中用作`patterns` 在当前函数中承载的中间业务值。
        patterns = ["Java", "Python", "Go", "前端", "后端", "产品经理", "AI产品经理", "测试", "算法", "运营"]
        # 使用 `for` 循环 `pattern in patterns`，逐项处理集合数据。
        for pattern in patterns:
            # 使用 `if` 判断 `pattern.lower() in text.lower()`；条件成立时执行下面分支。
            if pattern.lower() in text.lower():
                # 从 `_AgentServiceResourcesMixin._infer_exam_subject()` 返回 `pattern`，并结束当前函数。
                return pattern
        # 从 `_AgentServiceResourcesMixin._infer_exam_subject()` 返回 `"通用岗位"`，并结束当前函数。
        return "通用岗位"

    # 使用 `def` 普通函数 定义 `_brief_exam_requirements()`。
    def _brief_exam_requirements(self, exam: Dict[str, Any]) -> str:
        # 调用 `exam.get("question_counts") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `counts`；它表示`counts` 在当前函数中承载的中间业务值。
        counts = exam.get("question_counts") or {}
        # 计算表达式 `"、".join([f"{key}:{value}" for key, value in counts.items() if value])` 并把结果保存到 `count_text`；该结果在后续流程中用作当前步骤整理后的文本，供解析、匹配或模型调用。
        count_text = "、".join([f"{key}:{value}" for key, value in counts.items() if value])
        # 从 `_AgentServiceResourcesMixin._brief_exam_requirements()` 返回 `f"{exam.get('title')} / {exam.get('subject')} / {exam.get('total_score')}分 / {exam.get('duration'...`，并结束当前函数。
        return f"{exam.get('title')} / {exam.get('subject')} / {exam.get('total_score')}分 / {exam.get('duration')}分钟 / {count_text}"

    # 使用 `def` 普通函数 定义 `_clean_optional_value()`。
    def _clean_optional_value(self, value: Any) -> Optional[str]:
        # 使用 `if` 判断 `value is None`；条件成立时执行下面分支。
        if value is None:
            # 从 `_AgentServiceResourcesMixin._clean_optional_value()` 返回 `None`，并结束当前函数。
            return None
        # 调用 `str(value).strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `text`；它表示`text` 在当前函数中承载的中间业务值。
        text = str(value).strip()
        # 使用 `if` 判断 `not text or text.lower() in {"none", "null", "n/a", "na"}`；条件成立时执行下面分支。
        if not text or text.lower() in {"none", "null", "n/a", "na"}:
            # 从 `_AgentServiceResourcesMixin._clean_optional_value()` 返回 `None`，并结束当前函数。
            return None
        # 使用 `if` 判断 `text in {"无", "暂无", "未提及", "未说明", "未知"}`；条件成立时执行下面分支。
        if text in {"无", "暂无", "未提及", "未说明", "未知"}:
            # 从 `_AgentServiceResourcesMixin._clean_optional_value()` 返回 `None`，并结束当前函数。
            return None
        # 从 `_AgentServiceResourcesMixin._clean_optional_value()` 返回 `text`，并结束当前函数。
        return text

    # 使用 `def` 普通函数 定义 `_normalize_optional_list()`。
    def _normalize_optional_list(self, value: Any) -> Optional[List[str]]:
        # 使用 `if` 判断 `value is None`；条件成立时执行下面分支。
        if value is None:
            # 从 `_AgentServiceResourcesMixin._normalize_optional_list()` 返回 `None`，并结束当前函数。
            return None
        # 使用 `if` 判断 `isinstance(value, list)`；条件成立时执行下面分支。
        if isinstance(value, list):
            # 计算表达式 `[self._clean_optional_value(item) for item in value]` 并把结果保存到 `items`；该结果在后续流程中用作`items` 在当前函数中承载的中间业务值。
            items = [self._clean_optional_value(item) for item in value]
            # 从 `_AgentServiceResourcesMixin._normalize_optional_list()` 返回 `[item for item in items if item] or None`，并结束当前函数。
            return [item for item in items if item] or None
        # 使用 `if` 判断 `isinstance(value, str)`；条件成立时执行下面分支。
        if isinstance(value, str):
            # 计算表达式 `[item.strip() for item in re.split(r"[,，、/]", value) if item.strip()]` 并把结果保存到 `items`；该结果在后续流程中用作`items` 在当前函数中承载的中间业务值。
            items = [item.strip() for item in re.split(r"[,，、/]", value) if item.strip()]
            # 从 `_AgentServiceResourcesMixin._normalize_optional_list()` 返回 `items or None`，并结束当前函数。
            return items or None
        # 从 `_AgentServiceResourcesMixin._normalize_optional_list()` 返回 `None`，并结束当前函数。
        return None

    # 使用 `def` 普通函数 定义 `_is_jd_edit_request()`。
    def _is_jd_edit_request(self, message: str) -> bool:
        # 调用 `message.lower()` 完成当前解析、查询或对象构造，并把返回值保存到 `lowered`；它表示`lowered` 在当前函数中承载的中间业务值。
        lowered = message.lower()
        # 调用 `bool(re.search(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位", lowered))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_jd_target`；它表示`has_jd_target` 在当前函数中承载的中间业务值。
        has_jd_target = bool(re.search(r"jd|职位描述|岗位说明书|招聘需求|职位|岗位", lowered))
        # 使用 `if` 判断 `(`；条件成立时执行下面分支。
        if (
            # 使用变量 `has_jd_target` 作为当前条件或位置参数；它表示当前条件或调用所需的 `has_jd_target` 业务值。
            has_jd_target
            # 使用 `and` 追加正则匹配条件 `re.search(r"删除|删掉|移除|清理", message)`；只有用户消息也命中这些业务关键词时，整个意图判断才成立。
            and re.search(r"删除|删掉|移除|清理", message)
            # 使用 `and` 追加正则匹配条件 `not re.search(r"把|将|内容|职责|要求|福利|技能|薪资|地点|经验|学历", message)`；只有用户消息也命中这些业务关键词时，整个意图判断才成立。
            and not re.search(r"把|将|内容|职责|要求|福利|技能|薪资|地点|经验|学历", message)
        # 结束多行函数参数列表或控制条件，并用冒号开始下面的缩进代码块。
        ):
            # 从 `_AgentServiceResourcesMixin._is_jd_edit_request()` 返回 `False`，并结束当前函数。
            return False
        # 调用 `bool(re.search(r"改改|修改|调整|优化|更新|编辑|润色|改成|改为|换成|加上|增加|删掉|删除|去掉", message))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_edit_action`；它表示`has_edit_action` 在当前函数中承载的中间业务值。
        has_edit_action = bool(re.search(r"改改|修改|调整|优化|更新|编辑|润色|改成|改为|换成|加上|增加|删掉|删除|去掉", message))
        # 调用 `bool(re.search(r"上次|刚才|刚刚|这个|这份|这条|那个|上一版|最近|最新|原来", message))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_context`；它表示当前业务步骤需要的历史、用户或 Skill 上下文。
        has_context = bool(re.search(r"上次|刚才|刚刚|这个|这份|这条|那个|上一版|最近|最新|原来", message))
        # 从 `_AgentServiceResourcesMixin._is_jd_edit_request()` 返回 `has_jd_target and has_edit_action and (has_context or "jd" in lowered or "职位描述" in message or "岗位...`，并结束当前函数。
        return has_jd_target and has_edit_action and (has_context or "jd" in lowered or "职位描述" in message or "岗位说明书" in message)

    # 使用 `def` 普通函数 定义 `_is_criteria_edit_request()`。
    def _is_criteria_edit_request(self, message: str) -> bool:
        # 调用 `bool(re.search(r"评分标准|评分规则|打分标准|筛选标准|简历评分标准|简历评分规则", message))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_target`；它表示`has_target` 在当前函数中承载的中间业务值。
        has_target = bool(re.search(r"评分标准|评分规则|打分标准|筛选标准|简历评分标准|简历评分规则", message))
        # 调用 `bool(re.search(r"改改|修改|调整|优化|更新|编辑|润色|改成|改为|换成|加上|增加|删掉|删除|去掉|降低|提高", message))` 完成当前解析、查询或对象构造，并把返回值保存到 `has_edit_action`；它表示`has_edit_action` 在当前函数中承载的中间业务值。
        has_edit_action = bool(re.search(r"改改|修改|调整|优化|更新|编辑|润色|改成|改为|换成|加上|增加|删掉|删除|去掉|降低|提高", message))
        # 从 `_AgentServiceResourcesMixin._is_criteria_edit_request()` 返回 `has_target and has_edit_action`，并结束当前函数。
        return has_target and has_edit_action

    # 使用 `def` 普通函数 定义 `_is_jd_edit_followup()`。
    def _is_jd_edit_followup(self, message: str, memory_context: str = "") -> bool:
        # 使用 `if` 判断 `not memory_context`；条件成立时执行下面分支。
        if not memory_context:
            # 从 `_AgentServiceResourcesMixin._is_jd_edit_followup()` 返回 `False`，并结束当前函数。
            return False
        # 调用 `bool(re.search(r"你想具体改哪些内容|等待用户说明要修改哪些内容|等待修改要求|想具体改哪些内容", memory_context))` 完成当前解析、查询或对象构造，并把返回值保存到 `waiting_for_changes`；它表示`waiting_for_changes` 在当前函数中承载的中间业务值。
        waiting_for_changes = bool(re.search(r"你想具体改哪些内容|等待用户说明要修改哪些内容|等待修改要求|想具体改哪些内容", memory_context))
        # 使用 `if` 判断 `not waiting_for_changes`；条件成立时执行下面分支。
        if not waiting_for_changes:
            # 从 `_AgentServiceResourcesMixin._is_jd_edit_followup()` 返回 `False`，并结束当前函数。
            return False
        # 从 `_AgentServiceResourcesMixin._is_jd_edit_followup()` 返回 `bool(re.search(r"改成|改为|换成|加上|增加|删掉|删除|去掉|薪资|地点|职责|技能|要求|福利|经验|学历|React|Vue|Python|Java", message,...`，并结束当前函数。
        return bool(re.search(r"改成|改为|换成|加上|增加|删掉|删除|去掉|薪资|地点|职责|技能|要求|福利|经验|学历|React|Vue|Python|Java", message, re.I))

    # 使用 `def` 普通函数 定义 `_is_criteria_edit_followup()`。
    def _is_criteria_edit_followup(self, message: str, memory_context: str = "") -> bool:
        # 使用 `if` 判断 `not memory_context`；条件成立时执行下面分支。
        if not memory_context:
            # 从 `_AgentServiceResourcesMixin._is_criteria_edit_followup()` 返回 `False`，并结束当前函数。
            return False
        # 调用 `bool(re.search(r"想具体改哪些评分规则|等待用户说明要修改哪些评分规则|等待修改要求", memory_context))` 完成当前解析、查询或对象构造，并把返回值保存到 `waiting_for_changes`；它表示`waiting_for_changes` 在当前函数中承载的中间业务值。
        waiting_for_changes = bool(re.search(r"想具体改哪些评分规则|等待用户说明要修改哪些评分规则|等待修改要求", memory_context))
        # 使用 `if` 判断 `not waiting_for_changes`；条件成立时执行下面分支。
        if not waiting_for_changes:
            # 从 `_AgentServiceResourcesMixin._is_criteria_edit_followup()` 返回 `False`，并结束当前函数。
            return False
        # 从 `_AgentServiceResourcesMixin._is_criteria_edit_followup()` 返回 `bool(re.search(r"改成|改为|换成|加上|增加|删掉|删除|去掉|降低|提高|分|权重|维度|技能|经验|学历|项目|加分|淘汰", message, re.I))`，并结束当前函数。
        return bool(re.search(r"改成|改为|换成|加上|增加|删掉|删除|去掉|降低|提高|分|权重|维度|技能|经验|学历|项目|加分|淘汰", message, re.I))

    # 使用 `def` 普通函数 定义 `_is_generic_jd_edit_request()`。
    def _is_generic_jd_edit_request(self, text: str) -> bool:
        # 调用 `re.sub(r"\s+", "", text)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized`；它表示`normalized` 在当前函数中承载的中间业务值。
        normalized = re.sub(r"\s+", "", text)
        # 调用 `re.sub(r"请|帮我|麻烦|一下|这个|那个|上次|刚才|刚刚|JD|jd|职位描述|岗位说明书", "", normalized)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized`；它表示`normalized` 在当前函数中承载的中间业务值。
        normalized = re.sub(r"请|帮我|麻烦|一下|这个|那个|上次|刚才|刚刚|JD|jd|职位描述|岗位说明书", "", normalized)
        # 从 `_AgentServiceResourcesMixin._is_generic_jd_edit_request()` 返回 `normalized in {"", "改改", "修改", "调整", "优化", "更新", "编辑", "润色", "帮改", "再改"}`，并结束当前函数。
        return normalized in {"", "改改", "修改", "调整", "优化", "更新", "编辑", "润色", "帮改", "再改"}

    # 使用 `def` 普通函数 定义 `_is_generic_criteria_edit_request()`。
    def _is_generic_criteria_edit_request(self, text: str) -> bool:
        # 调用 `re.sub(r"\s+", "", text)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized`；它表示`normalized` 在当前函数中承载的中间业务值。
        normalized = re.sub(r"\s+", "", text)
        # 调用 `re.sub(r"请|帮我|麻烦|一下|这个|那个|上次|刚才|刚刚|评分标准|评分规则|打分标准|筛选标准|简历评分", "", normalized)` 完成当前解析、查询或对象构造，并把返回值保存到 `normalized`；它表示`normalized` 在当前函数中承载的中间业务值。
        normalized = re.sub(r"请|帮我|麻烦|一下|这个|那个|上次|刚才|刚刚|评分标准|评分规则|打分标准|筛选标准|简历评分", "", normalized)
        # 从 `_AgentServiceResourcesMixin._is_generic_criteria_edit_request()` 返回 `normalized in {"", "改改", "修改", "调整", "优化", "更新", "编辑", "润色", "帮改", "再改"}`，并结束当前函数。
        return normalized in {"", "改改", "修改", "调整", "优化", "更新", "编辑", "润色", "帮改", "再改"}

    # 使用 `def` 普通函数 定义 `_fallback_parse()`。
    def _fallback_parse(self, text: str) -> Dict[str, Any]:
        # 使用字典字面量开始构造 `parsed`；该字典用于保存从用户自然语言中解析出的结构化招聘参数，下面各行会继续写入具体业务字段。
        parsed = {
            # 在字典中设置 `job_title` 字段为 `None`，用于表示`job_title` 在当前函数中承载的中间业务值。
            "job_title": None,
            # 在字典中设置 `department` 字段为 `None`，用于表示`department` 在当前函数中承载的中间业务值。
            "department": None,
            # 在字典中设置 `location` 字段为 `None`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": None,
            # 在字典中设置 `salary` 字段为 `None`，用于表示`salary` 在当前函数中承载的中间业务值。
            "salary": None,
            # 在字典中设置 `experience` 字段为 `None`，用于表示`experience` 在当前函数中承载的中间业务值。
            "experience": None,
            # 在字典中设置 `education` 字段为 `None`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": None,
            # 在字典中设置 `job_type` 字段为 `None`，用于表示`job_type` 在当前函数中承载的中间业务值。
            "job_type": None,
            # 在字典中设置 `skills` 字段为 `[]`，用于表示`skills` 在当前函数中承载的中间业务值。
            "skills": [],
            # 在字典中设置 `benefits` 字段为 `[]`，用于表示`benefits` 在当前函数中承载的中间业务值。
            "benefits": [],
            # 在字典中设置 `additional_requirements` 字段为 `None`，用于表示`additional_requirements` 在当前函数中承载的中间业务值。
            "additional_requirements": None,
        # 结束 `_AgentServiceResourcesMixin._fallback_parse` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用字典字面量开始构造 `patterns`；该字典用于保存`patterns` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        patterns = {
            # 在字典中设置 `experience` 字段为 `r"(\d+\s*-\s*\d+年|\d+年以上|\d+年\+?)"`，用于表示`experience` 在当前函数中承载的中间业务值。
            "experience": r"(\d+\s*-\s*\d+年|\d+年以上|\d+年\+?)",
            # 在字典中设置 `salary` 字段为 `r"(\d+\s*-\s*\d+\s*[kK]|\d+\s*[kK]\s*-\s*\d+\s*[kK]|\d+\s*-\s*\d+)"`，用于表示`salary` 在当前函数中承载的中间业务值。
            "salary": r"(\d+\s*-\s*\d+\s*[kK]|\d+\s*[kK]\s*-\s*\d+\s*[kK]|\d+\s*-\s*\d+)",
            # 在字典中设置 `education` 字段为 `r"本科|专科|硕士|博士|大专"`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": r"本科|专科|硕士|博士|大专",
            # 在字典中设置 `job_type` 字段为 `r"全职|兼职|实习"`，用于表示`job_type` 在当前函数中承载的中间业务值。
            "job_type": r"全职|兼职|实习",
            # 在字典中设置 `location` 字段为 `r"北京|上海|深圳|广州|杭州|南京|成都|重庆|苏州|武汉|西安|长沙"`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": r"北京|上海|深圳|广州|杭州|南京|成都|重庆|苏州|武汉|西安|长沙",
        # 结束 `_AgentServiceResourcesMixin._fallback_parse` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 调用 `re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师...` 完成当前解析、查询或对象构造，并把返回值保存到 `title_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        title_match = re.search(r"([\u4e00-\u9fa5A-Za-z0-9+#]+(?:开发|测试|算法|产品|运营|销售|人事|财务|行政)?(?:工程师|经理|专员|主管|顾问|设计师|架构师))", text)
        # 使用 `if` 判断 `title_match`；条件成立时执行下面分支。
        if title_match:
            # 调用 `title_match.group(1)` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed["job_title"]`；它表示`parsed` 在当前函数中承载的中间业务值。
            parsed["job_title"] = title_match.group(1)
        # 使用 `for` 循环 `key, pattern in patterns.items()`，逐项处理集合数据。
        for key, pattern in patterns.items():
            # 调用 `re.search(pattern, text, re.IGNORECASE)` 完成当前解析、查询或对象构造，并把返回值保存到 `match`；它表示`match` 在当前函数中承载的中间业务值。
            match = re.search(pattern, text, re.IGNORECASE)
            # 使用 `if` 判断 `match`；条件成立时执行下面分支。
            if match:
                # 调用 `match.group(0).replace(" ", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `parsed[key]`；它表示`parsed` 在当前函数中承载的中间业务值。
                parsed[key] = match.group(0).replace(" ", "")
        # 计算表达式 `["Java", "Python", "Go", "Vue", "React", "Spring", "MySQL", "Redis", "Docker", "Kubernetes", "AI"...` 并把结果保存到 `skill_candidates`；该结果在后续流程中用作`skill_candidates` 在当前函数中承载的中间业务值。
        skill_candidates = ["Java", "Python", "Go", "Vue", "React", "Spring", "MySQL", "Redis", "Docker", "Kubernetes", "AI", "大模型"]
        # 计算表达式 `[skill for skill in skill_candidates if skill.lower() in text.lower()]` 并把结果保存到 `parsed["skills"]`；该结果在后续流程中用作`parsed` 在当前函数中承载的中间业务值。
        parsed["skills"] = [skill for skill in skill_candidates if skill.lower() in text.lower()]
        # 从 `_AgentServiceResourcesMixin._fallback_parse()` 返回 `parsed`，并结束当前函数。
        return parsed

    # 使用 `def` 普通函数 定义 `_fallback_exam_parse()`。
    def _fallback_exam_parse(self, text: str) -> Dict[str, Any]:
        # 把 `title` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入前端向用户展示的步骤或业务产物标题。
        title = None
        # 把 `subject` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入待发送邮件的主题。
        subject = None
        # 调用 `text.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `clean_text`；它表示当前步骤整理后的文本，供解析、匹配或模型调用。
        clean_text = text.strip()
        # 调用 `re.search(r"(?:生成|出|创建|制作)(?:一份|一个)?(.{1,30}?)(?:试卷|考试|笔试题|题目)", clean_text)` 完成当前解析、查询或对象构造，并把返回值保存到 `title_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        title_match = re.search(r"(?:生成|出|创建|制作)(?:一份|一个)?(.{1,30}?)(?:试卷|考试|笔试题|题目)", clean_text)
        # 使用 `if` 判断 `title_match`；条件成立时执行下面分支。
        if title_match:
            # 调用 `title_match.group(1).strip(" ，,。的")` 完成当前解析、查询或对象构造，并把返回值保存到 `subject`；它表示待发送邮件的主题。
            subject = title_match.group(1).strip(" ，,。的")
            # 计算表达式 `f"{subject}试卷" if subject else None` 并把结果保存到 `title`；该结果在后续流程中用作前端向用户展示的步骤或业务产物标题。
            title = f"{subject}试卷" if subject else None

        # 调用 `re.search(` 完成当前解析、查询或对象构造，并把返回值保存到 `named_title_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        named_title_match = re.search(
            # 把该原始字符串作为正则表达式传入解析逻辑，用其中的业务关键词和捕获组从用户消息提取字段值。
            r"(?:试卷名称|试卷名|标题|名称|名字)(?:是|为|叫|设为|定为)?[:：]?\s*([^\n，,。；;]+)|(?:就叫|叫做|叫|命名为|取名为|起名为)\s*([^\n，,。；;]+?)(?:吧|。|$)",
            # 把变量 `clean_text` 作为位置参数传入当前多行函数调用；它承载当前步骤整理后的文本，供解析、匹配或模型调用。
            clean_text,
        # 结束 `_AgentServiceResourcesMixin._fallback_exam_parse` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `named_title_match`；条件成立时执行下面分支。
        if named_title_match:
            # 计算表达式 `(named_title_match.group(1) or named_title_match.group(2) or "").strip()` 并把结果保存到 `title_value`；该结果在后续流程中用作`title_value` 在当前函数中承载的中间业务值。
            title_value = (named_title_match.group(1) or named_title_match.group(2) or "").strip()
            # 调用 `re.sub(r"(?:的)?试卷$", "", title_value).strip() or title` 完成当前解析、查询或对象构造，并把返回值保存到 `title`；它表示前端向用户展示的步骤或业务产物标题。
            title = re.sub(r"(?:的)?试卷$", "", title_value).strip() or title

        # 计算表达式 `["Java", "Python", "Go", "前端", "后端", "产品经理", "AI产品经理", "测试", "算法", "运营", "大模型"]` 并把结果保存到 `subject_candidates`；该结果在后续流程中用作`subject_candidates` 在当前函数中承载的中间业务值。
        subject_candidates = ["Java", "Python", "Go", "前端", "后端", "产品经理", "AI产品经理", "测试", "算法", "运营", "大模型"]
        # 使用 `for` 循环 `candidate in subject_candidates`，逐项处理集合数据。
        for candidate in subject_candidates:
            # 使用 `if` 判断 `subject`；条件成立时执行下面分支。
            if subject:
                # 使用 `break` 结束当前循环，避免继续执行无效迭代。
                break
            # 使用 `if` 判断 `candidate.lower() in clean_text.lower()`；条件成立时执行下面分支。
            if candidate.lower() in clean_text.lower():
                # 计算表达式 `candidate` 并把结果保存到 `subject`；该结果在后续流程中用作待发送邮件的主题。
                subject = candidate
                # 计算表达式 `title or f"{candidate}试卷"` 并把结果保存到 `title`；该结果在后续流程中用作前端向用户展示的步骤或业务产物标题。
                title = title or f"{candidate}试卷"
                # 使用 `break` 结束当前循环，避免继续执行无效迭代。
                break

        # 调用 `re.search(r"(\d+)\s*分", clean_text)` 完成当前解析、查询或对象构造，并把返回值保存到 `total_score_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        total_score_match = re.search(r"(\d+)\s*分", clean_text)
        # 调用 `re.search(r"(\d+)\s*(?:分钟|分(?:钟)?|min)", clean_text, re.IGNORECASE)` 完成当前解析、查询或对象构造，并把返回值保存到 `duration_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        duration_match = re.search(r"(\d+)\s*(?:分钟|分(?:钟)?|min)", clean_text, re.IGNORECASE)
        # 使用字典字面量开始构造 `counts`；该字典用于保存`counts` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        counts = {
            # 在字典中设置 `single_choice` 字段为 `self._extract_question_count(clean_text, ["单选题", "单选", "选择题"], 5)`，用于表示`single_choice` 在当前函数中承载的中间业务值。
            "single_choice": self._extract_question_count(clean_text, ["单选题", "单选", "选择题"], 5),
            # 在字典中设置 `multiple_choice` 字段为 `self._extract_question_count(clean_text, ["多选题", "多选"], 3)`，用于表示`multiple_choice` 在当前函数中承载的中间业务值。
            "multiple_choice": self._extract_question_count(clean_text, ["多选题", "多选"], 3),
            # 在字典中设置 `short_answer` 字段为 `self._extract_question_count(clean_text, ["简答题", "简答", "问答题", "问答"], 2)`，用于表示`short_answer` 在当前函数中承载的中间业务值。
            "short_answer": self._extract_question_count(clean_text, ["简答题", "简答", "问答题", "问答"], 2),
        # 结束 `_AgentServiceResourcesMixin._fallback_exam_parse` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

        # 把 `difficulty` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入`difficulty` 在当前函数中承载的中间业务值。
        difficulty = None
        # 使用 `if` 判断 `re.search(r"简单|基础|入门", clean_text)`；条件成立时执行下面分支。
        if re.search(r"简单|基础|入门", clean_text):
            # 计算表达式 `"easy"` 并把结果保存到 `difficulty`；该结果在后续流程中用作`difficulty` 在当前函数中承载的中间业务值。
            difficulty = "easy"
        # 前一条件不成立时继续判断 `re.search(r"困难|高阶|高级", clean_text)`。
        elif re.search(r"困难|高阶|高级", clean_text):
            # 计算表达式 `"hard"` 并把结果保存到 `difficulty`；该结果在后续流程中用作`difficulty` 在当前函数中承载的中间业务值。
            difficulty = "hard"
        # 前一条件不成立时继续判断 `re.search(r"中等|中级", clean_text)`。
        elif re.search(r"中等|中级", clean_text):
            # 计算表达式 `"medium"` 并把结果保存到 `difficulty`；该结果在后续流程中用作`difficulty` 在当前函数中承载的中间业务值。
            difficulty = "medium"

        # 调用 `re.search(r"(?:要求|注意事项|其他)[:：]\s*(.+)", clean_text)` 完成当前解析、查询或对象构造，并把返回值保存到 `special_requirements_match`；它表示正则表达式或名称匹配得到的结果，用于判断和提取字段。
        special_requirements_match = re.search(r"(?:要求|注意事项|其他)[:：]\s*(.+)", clean_text)

        # 从 `_AgentServiceResourcesMixin._fallback_exam_parse()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `title` 字段为 `title`，用于表示前端向用户展示的步骤或业务产物标题。
            "title": title,
            # 在字典中设置 `subject` 字段为 `subject`，用于表示待发送邮件的主题。
            "subject": subject,
            # 在字典中设置 `difficulty` 字段为 `difficulty`，用于表示`difficulty` 在当前函数中承载的中间业务值。
            "difficulty": difficulty,
            # 在字典中设置 `duration` 字段为 `int(duration_match.group(1)) if duration_match else None`，用于表示`duration` 在当前函数中承载的中间业务值。
            "duration": int(duration_match.group(1)) if duration_match else None,
            # 在字典中设置 `total_score` 字段为 `int(total_score_match.group(1)) if total_score_match else None`，用于表示`total_score` 在当前函数中承载的中间业务值。
            "total_score": int(total_score_match.group(1)) if total_score_match else None,
            # 在字典中设置 `question_types` 字段为 `["single_choice", "multiple_choice", "short_answer"]`，用于表示`question_types` 在当前函数中承载的中间业务值。
            "question_types": ["single_choice", "multiple_choice", "short_answer"],
            # 在字典中设置 `question_counts` 字段为 `counts`，用于表示`question_counts` 在当前函数中承载的中间业务值。
            "question_counts": counts,
            # 在字典中设置 `special_requirements` 字段为 `special_requirements_match.group(1).strip() if special_requirements_match else ""`，用于表示`special_requirements` 在当前函数中承载的中间业务值。
            "special_requirements": special_requirements_match.group(1).strip() if special_requirements_match else "",
        # 结束 `_AgentServiceResourcesMixin._fallback_exam_parse` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_extract_question_count()`。
    def _extract_question_count(self, text: str, labels: List[str], default: int) -> int:
        # 使用 `for` 循环 `label in labels`，逐项处理集合数据。
        for label in labels:
            # 调用 `re.search(rf"(\d+)\s*(?:道|个)?\s*{label}|{label}\s*(\d+)\s*(?:道|个)?", text)` 完成当前解析、查询或对象构造，并把返回值保存到 `match`；它表示`match` 在当前函数中承载的中间业务值。
            match = re.search(rf"(\d+)\s*(?:道|个)?\s*{label}|{label}\s*(\d+)\s*(?:道|个)?", text)
            # 使用 `if` 判断 `match`；条件成立时执行下面分支。
            if match:
                # 从 `_AgentServiceResourcesMixin._extract_question_count()` 返回 `int(match.group(1) or match.group(2))`，并结束当前函数。
                return int(match.group(1) or match.group(2))
        # 从 `_AgentServiceResourcesMixin._extract_question_count()` 返回 `default`，并结束当前函数。
        return default

    # 使用 `def` 普通函数 定义 `_brief_requirements()`。
    def _brief_requirements(self, parsed: Dict[str, Any]) -> str:
        # 使用列表字面量开始构造 `parts`；该列表用于保存按 SKILL.md 前置元数据分隔符切开的文本片段，下面各行会继续追加有顺序的业务元素。
        parts = [
            # 调用 `parsed.get()`，执行 `_AgentServiceResourcesMixin._brief_requirements` 当前步骤所需的构造、查询或业务处理。
            parsed.get("job_title"),
            # 调用 `parsed.get()`，执行 `_AgentServiceResourcesMixin._brief_requirements` 当前步骤所需的构造、查询或业务处理。
            parsed.get("location"),
            # 调用 `parsed.get()`，执行 `_AgentServiceResourcesMixin._brief_requirements` 当前步骤所需的构造、查询或业务处理。
            parsed.get("salary"),
            # 调用 `parsed.get()`，执行 `_AgentServiceResourcesMixin._brief_requirements` 当前步骤所需的构造、查询或业务处理。
            parsed.get("experience"),
        # 结束 `_AgentServiceResourcesMixin._brief_requirements` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]
        # 从 `_AgentServiceResourcesMixin._brief_requirements()` 返回 `" / ".join([str(part) for part in parts if part]) or "已提取基础字段。"`，并结束当前函数。
        return " / ".join([str(part) for part in parts if part]) or "已提取基础字段。"

    # 使用 `def` 普通函数 定义 `_missing_required_fields()`。
    def _missing_required_fields(self, parsed: Dict[str, Any]) -> List[str]:
        # 计算表达式 `["job_title", "location", "salary", "experience", "education"]` 并把结果保存到 `required_fields`；该结果在后续流程中用作`required_fields` 在当前函数中承载的中间业务值。
        required_fields = ["job_title", "location", "salary", "experience", "education"]
        # 把 `missing` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入`missing` 在当前函数中承载的中间业务值。
        missing = []
        # 使用 `for` 循环 `field in required_fields`，逐项处理集合数据。
        for field in required_fields:
            # 调用 `parsed.get(field)` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示`value` 在当前函数中承载的中间业务值。
            value = parsed.get(field)
            # 使用 `if` 判断 `value is None or value == "" or value == []`；条件成立时执行下面分支。
            if value is None or value == "" or value == []:
                # 调用列表的 `append()`，把 `missing.append(field)` 对应的新元素追加到现有结果末尾。
                missing.append(field)
        # 从 `_AgentServiceResourcesMixin._missing_required_fields()` 返回 `missing`，并结束当前函数。
        return missing

    # 使用 `def` 普通函数 定义 `_field_label()`。
    def _field_label(self, field: str) -> str:
        # 使用字典字面量开始构造 `labels`；该字典用于保存`labels` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        labels = {
            # 在字典中设置 `job_title` 字段为 `"岗位名称"`，用于表示`job_title` 在当前函数中承载的中间业务值。
            "job_title": "岗位名称",
            # 在字典中设置 `location` 字段为 `"工作地点"`，用于表示`location` 在当前函数中承载的中间业务值。
            "location": "工作地点",
            # 在字典中设置 `salary` 字段为 `"薪资范围"`，用于表示`salary` 在当前函数中承载的中间业务值。
            "salary": "薪资范围",
            # 在字典中设置 `experience` 字段为 `"经验要求"`，用于表示`experience` 在当前函数中承载的中间业务值。
            "experience": "经验要求",
            # 在字典中设置 `education` 字段为 `"学历要求"`，用于表示`education` 在当前函数中承载的中间业务值。
            "education": "学历要求",
            # 在字典中设置 `skills` 字段为 `"核心技能"`，用于表示`skills` 在当前函数中承载的中间业务值。
            "skills": "核心技能",
        # 结束 `_AgentServiceResourcesMixin._field_label` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 从 `_AgentServiceResourcesMixin._field_label()` 返回 `labels.get(field, field)`，并结束当前函数。
        return labels.get(field, field)

    # 使用 `def` 普通函数 定义 `_fallback_message()`。
    def _fallback_message(self, intent: str, message: str = "") -> str:
        # 使用字典字面量开始构造 `mapping`；该字典用于保存`mapping` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        mapping = {
            # 在字典中设置 `interview_plan` 字段为 `"我识别到你想做面试方案。这个工具可以跳转到智能面试页继续处理。"`，用于表示`interview_plan` 在当前函数中承载的中间业务值。
            "interview_plan": "我识别到你想做面试方案。这个工具可以跳转到智能面试页继续处理。",
            # 在字典中设置 `exam_generate` 字段为 `"我识别到你想生成考试/试卷。这个工具可以跳转到试卷生成页继续处理。"`，用于表示`exam_generate` 在当前函数中承载的中间业务值。
            "exam_generate": "我识别到你想生成考试/试卷。这个工具可以跳转到试卷生成页继续处理。",
        # 结束 `_AgentServiceResourcesMixin._fallback_message` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 使用 `if` 判断 `intent == "general"`；条件成立时执行下面分支。
        if intent == "general":
            # 从 `_AgentServiceResourcesMixin._fallback_message()` 返回 `(`，并结束当前函数。
            return (
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "我是招聘场景的 HR Agent，当前主要能帮你做这些事：生成 JD、生成简历评分标准、筛选简历、生成面试计划、基于文档和面试方案生成试卷、生成邮件草稿，以及删除已生成内容。"
                # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
                "如果你想让我执行任务，可以直接说其中一个目标。"
            # 结束 `_AgentServiceResourcesMixin._fallback_message` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            )
        # 从 `_AgentServiceResourcesMixin._fallback_message()` 返回 `mapping.get(intent, "我已经识别到需求，但当前还没有可安全自动执行的招聘工具。")`，并结束当前函数。
        return mapping.get(intent, "我已经识别到需求，但当前还没有可安全自动执行的招聘工具。")

    # 使用 `def` 普通函数 定义 `_suggestions_for_intent()`。
    def _suggestions_for_intent(self, intent: str) -> List[str]:
        # 使用字典字面量开始构造 `mapping`；该字典用于保存`mapping` 在当前函数中承载的中间业务值，下面各行会继续写入具体业务字段。
        mapping = {
            # 在字典中设置 `interview_plan` 字段为 `["生成面试计划", "先筛选简历", "查看已评分候选人"]`，用于表示`interview_plan` 在当前函数中承载的中间业务值。
            "interview_plan": ["生成面试计划", "先筛选简历", "查看已评分候选人"],
            # 在字典中设置 `exam_generate` 字段为 `["生成试卷", "上传参考文档", "补充考试配置"]`，用于表示`exam_generate` 在当前函数中承载的中间业务值。
            "exam_generate": ["生成试卷", "上传参考文档", "补充考试配置"],
        # 结束 `_AgentServiceResourcesMixin._suggestions_for_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 从 `_AgentServiceResourcesMixin._suggestions_for_intent()` 返回 `mapping.get(intent, ["生成 JD", "筛选简历", "生成试卷"])`，并结束当前函数。
        return mapping.get(intent, ["生成 JD", "筛选简历", "生成试卷"])
