"""
* 文件作用
  * 这是知识问答会话持久化的数据模型文件，定义消息角色枚举、会话状态枚举，以及 Conversation 和 Message 两个 SQLAlchemy ORM 模型。Conversation 保存用户、标题、知识库、消息数量和状态，Message 保存所属会话、角色、正文、引用来源、token 数量与反馈，并通过 relationship 和外键形成一对多关系。它把前端聊天窗口中的一次对话映射到 PostgreSQL，可用于恢复历史上下文、展示来源、统计消息和执行用户级数据隔离。
* 类说明
  * `MessageRole`：消息角色字符串枚举，限定消息发送方只能使用 user、assistant 或 system，避免数据库和接口中出现不统一的角色值。
  * `ConversationStatus`：会话状态字符串枚举，限定会话处于 active、archived 或 deleted 状态，为正常展示、归档和逻辑删除提供统一状态值。
  * `Conversation`：conversations 表对应的 ORM 模型，保存会话 id、用户 id、可选知识库 id、标题、状态、消息数量和时间字段，并通过 relationship 关联用户、知识库和消息列表。它表示一次可持续多轮的知识问答会话。
  * `Message`：messages 表对应的 ORM 模型，保存消息所属 conversation_id、发送角色、正文 content、引用 sources、token 数量和用户反馈。多条 Message 通过外键归属于一个 Conversation，用于恢复聊天历史和展示答案来源。
* 函数与方法说明
  * `Conversation.__repr__()`：这是对象的开发者字符串表示方法，返回包含模型类型和关键标识的文本，方便日志输出与调试时识别 ORM 实例。
  * `Message.__repr__()`：这是对象的开发者字符串表示方法，返回包含模型类型和关键标识的文本，方便日志输出与调试时识别 ORM 实例。
"""















"""
    定义聊天会话和消息
"""
# 从 `sqlalchemy` 导入当前模块需要的 `Column, String, Text, ForeignKey, JSON, Enum, Float, Integer`。
from sqlalchemy import Column, String, Text, ForeignKey, JSON, Enum, Float, Integer
# 从 `sqlalchemy.dialects.postgresql` 导入当前模块需要的 `UUID`。
from sqlalchemy.dialects.postgresql import UUID
# 从 `sqlalchemy.orm` 导入当前模块需要的 `relationship`。
from sqlalchemy.orm import relationship
# 导入 `enum`，供后续代码调用其功能。
import enum

# 从 `app.models.base` 导入当前模块需要的 `BaseModel`。
from app.models.base import BaseModel


# 定义 `MessageRole` 类，集中封装相关数据或业务能力。
class MessageRole(str, enum.Enum):
    # 补充当前字符串、提示词或响应正文内容。
    """消息角色枚举"""
    # 计算或设置 `USER`，用于保存后续流程需要的中间结果。
    USER = "user"
    # 计算或设置 `ASSISTANT`，用于保存后续流程需要的中间结果。
    ASSISTANT = "assistant"
    # 计算或设置 `SYSTEM`，用于保存后续流程需要的中间结果。
    SYSTEM = "system"


# 定义 `ConversationStatus` 类，集中封装相关数据或业务能力。
class ConversationStatus(str, enum.Enum):
    # 补充当前字符串、提示词或响应正文内容。
    """对话状态枚举：激活、归档、删除"""
    # 计算或设置 `ACTIVE`，用于保存后续流程需要的中间结果。
    ACTIVE = "active"
    # 计算或设置 `ARCHIVED`，用于保存后续流程需要的中间结果。
    ARCHIVED = "archived"
    # 计算或设置 `DELETED`，用于保存后续流程需要的中间结果。
    DELETED = "deleted"


# 定义 `Conversation` 类，集中封装相关数据或业务能力。
class Conversation(BaseModel):
    # 补充当前字符串、提示词或响应正文内容。
    """用于存储聊天会话的对话模型"""

    # 计算或设置 `__tablename__`，用于保存后续流程需要的中间结果。
    __tablename__ = "conversations"

    # 基本信息
    title = Column(String(255), nullable=False, comment="会话标题")
    # 声明数据库字段 `description`，并配置其类型、默认值或约束。
    description = Column(Text, nullable=True, comment="会话描述")
    # 声明数据库字段 `status`，并配置其类型、默认值或约束。
    status = Column(Enum(ConversationStatus), default=ConversationStatus.ACTIVE, nullable=False, comment="会话状态")

    # 元数据
    meta_data = Column(JSON, nullable=True, comment="会话扩展元数据")
    # 声明数据库字段 `total_messages`，并配置其类型、默认值或约束。
    total_messages = Column(Integer, default=0, nullable=False, comment="消息总数")

    # 关系：表示会话属于某个用户，
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, comment="所属用户ID")
    # 声明 ORM 关系 `user`，用于在关联模型之间导航数据。
    user = relationship("User", back_populates="conversations")

    # 一个会话可以包含多条消息
    messages = relationship("Message", back_populates="conversation", cascade="all, delete-orphan")

    # 定义 `__repr__()`，执行该函数对应的业务流程。
    def __repr__(self):
        # 结束当前函数，并把处理结果返回给调用方。
        return f"<Conversation(title='{self.title}', user_id='{self.user_id}')>"


# 定义 `Message` 类，集中封装相关数据或业务能力。
class Message(BaseModel):
    # 补充当前字符串、提示词或响应正文内容。
    """用于在对话中存储单条消息的消息模型"""

    # 计算或设置 `__tablename__`，用于保存后续流程需要的中间结果。
    __tablename__ = "messages"

    # 计算或设置 `model_config`，用于保存后续流程需要的中间结果。
    model_config = {"protected_namespaces": ()}

    # 内容
    content = Column(Text, nullable=False, comment="消息内容")
    # 声明数据库字段 `role`，并配置其类型、默认值或约束。
    role = Column(Enum(MessageRole), nullable=False, comment="消息角色")

    # AI特定字段
    model_name = Column(String(100), nullable=True, comment="AI模型名称")
    # 声明数据库字段 `tokens_used`，并配置其类型、默认值或约束。
    tokens_used = Column(Integer, nullable=True, comment="消耗的Token数量")
    # 声明数据库字段 `response_time`，并配置其类型、默认值或约束。
    response_time = Column(Float, nullable=True, comment="响应耗时，单位秒")

    # 附加上下文和元数据
    context = Column(JSON, nullable=True, comment="生成消息使用的上下文")
    # 声明数据库字段 `meta_data`，并配置其类型、默认值或约束。
    meta_data = Column(JSON, nullable=True, comment="消息扩展元数据")

    # 反馈
    rating = Column(Integer, nullable=True, comment="用户评分(1-5)")
    # 声明数据库字段 `feedback`，并配置其类型、默认值或约束。
    feedback = Column(Text, nullable=True, comment="用户反馈")

    # 关系： 消息属于哪个会话
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("conversations.id"), nullable=False, comment="所属会话ID")
    # 声明 ORM 关系 `conversation`，用于在关联模型之间导航数据。
    conversation = relationship("Conversation", back_populates="messages")

    # parent_message_id 可以用于表达消息之间的父子关系，比如回复链或上下文链。
    parent_message_id = Column(UUID(as_uuid=True), ForeignKey("messages.id"), nullable=True, comment="父消息ID")
    # 声明 ORM 关系 `parent_message`，用于在关联模型之间导航数据。
    parent_message = relationship("Message", remote_side="Message.id")

    # 定义 `__repr__()`，执行该函数对应的业务流程。
    def __repr__(self):
        # 结束当前函数，并把处理结果返回给调用方。
        return f"<Message(role='{self.role}', conversation_id='{self.conversation_id}')>"
