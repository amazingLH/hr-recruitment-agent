"""
* 文件作用
  * 这是 HR Agent 的 Pydantic 请求与响应模型，规定附件、对话请求、执行步骤、业务产物和最终响应的字段结构。它把前后端传递的数据限制在明确类型内，使 FastAPI 能自动校验请求，并让普通 JSON 与 SSE final 事件使用同一套响应协议。
* 类说明
  * `AgentAttachment`：请求附件模型，传递文件名、路径、类型和可选文本内容。
  * `AgentChatRequest`：Agent 请求模型，携带消息、会话 ID、附件、是否自动执行和人工确认参数。
  * `AgentStep`：前端步骤模型，描述规划或工具步骤的名称、状态、详情和工具名。
  * `AgentArtifact`：业务产物模型，统一承载 JD、评分结果、面试方案、试卷或邮件草稿。
  * `AgentChatResponse`：统一 Agent 响应，包含回答、意图、路由、步骤、产物、建议和确认状态。
* 主要函数与方法说明
"""

# 导入 Any, Dict, List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, Dict, List, Optional

# 导入 BaseModel, Field，用于请求与响应数据校验模型。
from pydantic import BaseModel, Field


# 使用 `class` 定义 `AgentAttachment`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：请求附件模型，传递文件名、路径、类型和可选文本内容。
class AgentAttachment(BaseModel):
    """聊天附件元信息，用于 Agent 规划前置条件"""
    # 定义 Pydantic 字段 `name`，业务作用是保存附件原始名称，供 Agent 判断文件类型并在页面展示；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    name: str = Field(..., description="附件名称")
    # 定义 Pydantic 字段 `size`，业务作用是保存附件字节数，供上传限制和页面信息展示；类型 `Optional[int]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    size: Optional[int] = Field(None, description="附件大小，单位字节")
    # 定义 Pydantic 字段 `content_type`，业务作用是保存附件 MIME 类型，供后端判断 PDF、DOCX 等文件格式；类型 `Optional[str]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    content_type: Optional[str] = Field(None, description="附件内容类型")

# 使用 `class` 定义 `AgentChatRequest`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：Agent 请求模型，携带消息、会话 ID、附件、是否自动执行和人工确认参数。
class AgentChatRequest(BaseModel):
    """HR Agent 对话请求"""
    # 定义 Pydantic 字段 `message`，业务作用是保存用户本次提交给 Agent 的自然语言需求；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误；`min_length=1` 禁止用户提交空消息。
    message: str = Field(..., min_length=1, description="用户自然语言需求")
    # 定义 Pydantic 字段 `conversation_id`，业务作用是保存当前聊天窗口 ID，用于读取和保存同一会话的历史；类型 `Optional[str]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    conversation_id: Optional[str] = Field(None, description="会话ID，用于延续已有对话")
    # 定义 Pydantic 字段 `auto_execute`，业务作用是保存是否允许信息完整后自动执行低风险生成工具；类型 `bool` 限制数据形态，默认值为 `True`，未传值时允许自动执行低风险生成操作。
    auto_execute: bool = Field(True, description="是否自动执行低风险生成类工具")
    # 定义 Pydantic 字段 `confirmed_requirements`，业务作用是保存用户补充或确认后的结构化业务参数；类型 `Optional[Dict[str, Any]]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    confirmed_requirements: Optional[Dict[str, Any]] = Field(None, description="用户确认后的结构化招聘需求")
    # 定义 Pydantic 字段 `attachments`，业务作用是保存用户随消息上传的附件列表；类型 `List[AgentAttachment]` 限制数据形态，`default_factory=list` 为每次请求创建独立空列表，避免多个对象共享同一个列表。
    attachments: List[AgentAttachment] = Field(default_factory=list, description="聊天消息中携带的附件元信息")


# 使用 `class` 定义 `AgentStep`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：前端步骤模型，描述规划或工具步骤的名称、状态、详情和工具名。
class AgentStep(BaseModel):
    """Agent 执行步骤"""
    # 定义 Pydantic 字段 `id`，业务作用是保存当前执行步骤的唯一标识，供前端定位并更新同一个步骤；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    id: str = Field(..., description="执行步骤唯一标识")
    # 定义 Pydantic 字段 `title`，业务作用是保存前端向用户展示的步骤或业务产物标题；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    title: str = Field(..., description="标题")
    # 定义 Pydantic 字段 `status`，业务作用是保存步骤当前状态，例如 pending、running、completed 或 failed；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    status: str = Field(..., description="状态")
    # 定义 Pydantic 字段 `detail`，业务作用是保存步骤的补充说明，用于告诉用户当前处理内容或失败原因；类型 `Optional[str]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    detail: Optional[str] = Field(None, description="详情说明")
    # 定义 Pydantic 字段 `tool`，业务作用是保存执行该步骤时使用的已注册工具名称；类型 `Optional[str]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    tool: Optional[str] = Field(None, description="工具名称")

# 使用 `class` 定义 `AgentArtifact`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：业务产物模型，统一承载 JD、评分结果、面试方案、试卷或邮件草稿。
class AgentArtifact(BaseModel):
    """Agent 产物"""
    # 定义 Pydantic 字段 `type`，业务作用是保存业务产物类型，供前端选择 JD、评分结果、试卷等展示组件；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    type: str = Field(..., description="产物类型")
    # 定义 Pydantic 字段 `title`，业务作用是保存前端业务产物卡片显示的标题；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    title: str = Field(..., description="产物标题")
    # 定义 Pydantic 字段 `content`，业务作用是保存业务产物的实际内容，例如完整 JD、面试方案或试卷数据；类型 `Any` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    content: Any = Field(..., description="产物内容")
    # 定义 Pydantic 字段 `metadata`，业务作用是保存产物附加信息，例如数据库 ID、候选人和保存状态；类型 `Dict[str, Any]` 限制数据形态，`default_factory=dict` 为每个响应创建独立空字典，避免共享可变默认值。
    metadata: Dict[str, Any] = Field(default_factory=dict, description="附加元数据")

# 使用 `class` 定义 `AgentChatResponse`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：统一 Agent 响应，包含回答、意图、路由、步骤、产物、建议和确认状态。
class AgentChatResponse(BaseModel):
    """HR Agent 对话响应"""
    # 定义 Pydantic 字段 `message`，业务作用是保存Agent 返回给前端和用户的最终回复文字；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    message: str = Field(..., description="Agent 返回的消息内容")
    # 定义 Pydantic 字段 `intent`，业务作用是保存识别出的业务意图标签；类型 `str` 限制数据形态，`Field(...)` 表示前端必须提供该字段，缺少时 FastAPI 会返回校验错误。
    intent: str = Field(..., description="识别出的用户意图")
    # 定义 Pydantic 字段 `route`，业务作用是保存该意图对应的前端页面路径，供前端跳转或展示下一步入口；类型 `Optional[str]` 限制数据形态，默认值为 `None`，表示该字段可以不提供。
    route: Optional[str] = Field(None, description="意图对应的处理路由")
    # 定义 Pydantic 字段 `steps`，业务作用是保存返回前端展示的 Agent 执行步骤；类型 `List[AgentStep]` 限制数据形态，`default_factory=list` 为每次请求创建独立空列表，避免多个对象共享同一个列表。
    steps: List[AgentStep] = Field(default_factory=list, description="Agent 执行步骤列表")
    # 定义 Pydantic 字段 `artifacts`，业务作用是保存本轮生成的 JD、评分、面试方案等业务产物；类型 `List[AgentArtifact]` 限制数据形态，`default_factory=list` 为每次请求创建独立空列表，避免多个对象共享同一个列表。
    artifacts: List[AgentArtifact] = Field(default_factory=list, description="Agent 生成的产物列表")
    # 定义 Pydantic 字段 `suggestions`，业务作用是保存任务结束后提供给用户的下一步操作建议；类型 `List[str]` 限制数据形态，`default_factory=list` 为每次请求创建独立空列表，避免多个对象共享同一个列表。
    suggestions: List[str] = Field(default_factory=list, description="后续操作建议列表")
    # 定义 Pydantic 字段 `requires_confirmation`，业务作用是保存后端是否暂停执行并要求用户人工确认；类型 `bool` 限制数据形态，默认值为 `False`，默认不要求人工确认。
    requires_confirmation: bool = Field(False, description="是否需要用户确认")
    # 定义 Pydantic 字段 `missing_fields`，业务作用是保存继续执行工具前仍缺少的字段名称列表；类型 `List[str]` 限制数据形态，`default_factory=list` 为每次请求创建独立空列表，避免多个对象共享同一个列表。
    missing_fields: List[str] = Field(default_factory=list, description="当前仍缺失的必要字段列表")
