"""
* 文件作用
  * 这是会话和消息的数据访问与业务服务层，使用 SQLAlchemy AsyncSession 操作 conversations 与 messages 表，并集中执行“当前用户是否拥有该会话”的权限检查。它负责会话 CRUD、消息 CRUD、反馈更新、历史搜索和消息数量维护，还通过基础服务统一把数据库异常转换为业务异常。学习时应关注事务 commit/rollback/refresh、外键关联、级联删除、分页排序以及接口层为什么不能只靠前端隐藏按钮来保证数据安全。
* 类说明
  * `BaseConversationService`：会话服务的公共基础类，保存 AsyncSession，并提供带用户所有权校验的会话查询和统一异常转换。它不实现完整 CRUD，而是把“会话必须存在且属于当前用户”这一规则集中复用，供 ConversationService 的修改、删除和消息操作调用。
  * `ConversationService`：会话与消息的具体业务服务，继承 BaseConversationService，直接操作 conversations 和 messages 表。它负责会话 CRUD、用户会话分页、消息 CRUD、反馈更新、全文搜索和 message_count 维护，并在写操作中执行 commit/rollback/refresh 保证数据库事务一致性。
* 函数与方法说明
  * `BaseConversationService.__init__()`：这是 `BaseConversationService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。
  * `BaseConversationService.get_conversation_with_permission_check()`：获取对话并进行权限检查 Args: conversation_id: 对话ID current_user: 当前用户 Returns: 对话对象 Raises: HTTPException: 当对话未找到或权限不足时。内部调用 self.get_conversation()、HTTPException() 完成主要工作，使用 FastAPI 权限或参数异常响应，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `BaseConversationService.handle_conversation_error()`：统一处理对话相关错误 Args: error: 异常对象 operation: 操作描述 Returns: HTTPException: 格式化后的HTTP异常。内部调用 HTTPException() 完成主要工作，使用 FastAPI 权限或参数异常响应，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.__init__()`：这是 `ConversationService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。
  * `ConversationService.create_conversation()`：创建新的对话。内部调用 self.db.commit()、self.db.refresh()、self.db.rollback() 完成主要工作，使用 数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.get_conversation()`：通过ID获取对话。内部调用 select()、self.db.execute() 完成主要工作，使用 SQLAlchemy 查询表达式、AsyncSession 异步执行 SQL，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.get_user_conversations()`：获取用户的对话。内部调用 select()、self.db.execute() 完成主要工作，使用 SQLAlchemy 查询表达式、AsyncSession 异步执行 SQL，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.update_conversation()`：更新对话。内部调用 self.get_conversation()、self.db.execute()、self.db.commit() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.delete_conversation()`：删除对话及其消息。内部调用 self.get_conversation()、self.db.execute()、self.db.commit() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.add_message()`：向对话添加消息。内部调用 self.db.execute()、self.db.commit()、self.db.refresh() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.get_conversation_messages()`：获取对话的消息。内部调用 select()、self.db.execute() 完成主要工作，使用 SQLAlchemy 查询表达式、AsyncSession 异步执行 SQL，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.get_message()`：通过ID获取消息。内部调用 select()、self.db.execute() 完成主要工作，使用 SQLAlchemy 查询表达式、AsyncSession 异步执行 SQL，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.update_message()`：更新对话中的单条消息。内部调用 self.get_message()、self.db.execute()、self.db.commit() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.delete_message()`：删除对话中的单条消息。内部调用 self.get_message()、self.db.execute()、self.db.commit() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.update_message_feedback()`：更新消息反馈。内部调用 self.db.execute()、self.db.commit()、self.db.rollback() 完成主要工作，使用 AsyncSession 异步执行 SQL、数据库事务提交、事务失败回滚，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `ConversationService.search_conversations()`：按标题或内容搜索对话。内部调用 select()、self.db.execute() 完成主要工作，使用 SQLAlchemy 查询表达式、AsyncSession 异步执行 SQL，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
"""

"""
 会话和消息管理服务。
    负责创建、查询、更新、删除会话，添加/查询/更新/删除消息，更新消息反馈，以及搜索会话。
    BaseConversationService 做权限检查和错误封装，
    ConversationService 做实际 CRUD，并维护会话消息数量、状态等信息。
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 从 `typing` 导入当前模块需要的 `List, Optional, Dict, Any`。
from typing import List, Optional, Dict, Any
# 从 `uuid` 导入当前模块需要的 `UUID`。
from uuid import UUID
# 从 `fastapi` 导入当前模块需要的 `HTTPException, status`。
from fastapi import HTTPException, status
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy` 导入当前模块需要的 `select, update, delete, desc, func, case`。
from sqlalchemy import select, update, delete, desc, func, case
# 从 `sqlalchemy.orm` 导入当前模块需要的 `selectinload`。
from sqlalchemy.orm import selectinload

# 从 `app.models.conversation` 导入当前模块需要的 `Conversation, Message, MessageRole, ConversationStatus`。
from app.models.conversation import Conversation, Message, MessageRole, ConversationStatus
# 从 `app.schemas.conversation` 导入当前模块需要的 `ConversationCreate, ConversationUpdate, MessageCreate, MessageUpdate`。
from app.schemas.conversation import ConversationCreate, ConversationUpdate, MessageCreate, MessageUpdate
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `BaseConversationService` 类，集中封装相关数据或业务能力。
class BaseConversationService:
    # 补充当前字符串、提示词或响应正文内容。
    """对话服务基类，包含通用辅助方法"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 计算或设置 `self.db`，用于保存异步数据库会话。
        self.db = db

    # 定义 `get_conversation_with_permission_check()`，执行该函数对应的业务流程。
    async def get_conversation_with_permission_check(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: str,
        # 声明参数或字段 `current_user`，表示当前登录用户。
        current_user: UserSchema
    # 结束上方多行参数、集合或对象的定义。
    ) -> Any:
        """
        获取对话并进行权限检查
        
        Args:
            conversation_id: 对话ID
            current_user: 当前用户
            
        Returns:
            对话对象
            
        Raises:
            HTTPException: 当对话未找到或权限不足时
        """
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `conversation`。
        conversation = await self.get_conversation(conversation_id)
        
        # 判断 `not conversation` 是否成立，以选择正确的处理分支。
        if not conversation:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_404_NOT_FOUND,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="对话未找到"
            # 结束上方多行参数、集合或对象的定义。
            )
        
        # 检查用户所有权
        # 判断 `conversation.user_id != current_user.id` 是否成立，以选择正确的处理分支。
        if conversation.user_id != current_user.id:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_403_FORBIDDEN,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="权限不足"
            # 结束上方多行参数、集合或对象的定义。
            )
        
        # 结束当前函数，并把处理结果返回给调用方。
        return conversation

    # 定义 `handle_conversation_error()`，执行该函数对应的业务流程。
    def handle_conversation_error(self, error: Exception, operation: str) -> HTTPException:
        """
        统一处理对话相关错误
        
        Args:
            error: 异常对象
            operation: 操作描述
            
        Returns:
            HTTPException: 格式化后的HTTP异常
        """
        # 结束当前函数，并把处理结果返回给调用方。
        return HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"{operation}时出错: {str(error)}"
        # 结束上方多行参数、集合或对象的定义。
        )


# 定义 `ConversationService` 类，集中封装相关数据或业务能力。
class ConversationService(BaseConversationService):
    # 补充当前字符串、提示词或响应正文内容。
    """用于管理对话和消息的服务"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        super().__init__(db)

    # 定义 `create_conversation()`，执行该函数对应的业务流程。
    async def create_conversation(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `conversation_data`，表示当前业务需要的数据。
        conversation_data: ConversationCreate
    # 结束上方多行参数、集合或对象的定义。
    ) -> Conversation:
        # 补充当前字符串、提示词或响应正文内容。
        """创建新的对话"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 开始组装多行调用参数、集合或对象内容。
            conversation = Conversation(
                # 计算或设置 `user_id`，用于保存用户唯一标识。
                user_id=user_id,
                # 计算或设置 `title`，用于保存后续流程需要的中间结果。
                title=conversation_data.title,
                # 计算或设置 `description`，用于保存后续流程需要的中间结果。
                description=conversation_data.description,
                # 计算或设置 `status`，用于保存当前业务状态。
                status=ConversationStatus.ACTIVE,
                # 计算或设置 `meta_data`，用于保存后续流程需要的中间结果。
                meta_data=conversation_data.meta_data or {}
            # 结束上方多行参数、集合或对象的定义。
            )

            # 调用当前对象的方法，推进本步骤的业务处理。
            self.db.add(conversation)
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
            await self.db.refresh(conversation)

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"为用户{user_id}创建了对话{conversation.id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return conversation

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"创建对话时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_conversation()`，执行该函数对应的业务流程。
    async def get_conversation(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: Optional[UUID] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> Optional[Conversation]:
        # 补充当前字符串、提示词或响应正文内容。
        """通过ID获取对话"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            query = select(Conversation).where(Conversation.id == conversation_id)

            # 判断 `user_id` 是否成立，以选择正确的处理分支。
            if user_id:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                query = query.where(Conversation.user_id == user_id)

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalar_one_or_none()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取对话{conversation_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_user_conversations()`，执行该函数对应的业务流程。
    async def get_user_conversations(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 20,
        # 声明参数或字段 `status`，表示当前业务状态。
        status: Optional[ConversationStatus] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[Conversation]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取用户的对话"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            query = select(Conversation).where(Conversation.user_id == user_id)

            # 判断 `status` 是否成立，以选择正确的处理分支。
            if status:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                query = query.where(Conversation.status == status)

            # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
            query = query.order_by(desc(Conversation.updated_at)).offset(skip).limit(limit)

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalars().all()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取用户{user_id}的对话时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `update_conversation()`，执行该函数对应的业务流程。
    async def update_conversation(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `conversation_data`，表示当前业务需要的数据。
        conversation_data: ConversationUpdate
    # 结束上方多行参数、集合或对象的定义。
    ) -> Optional[Conversation]:
        # 补充当前字符串、提示词或响应正文内容。
        """更新对话"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 检查对话是否存在且属于用户
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `conversation`。
            conversation = await self.get_conversation(conversation_id, user_id)
            # 判断 `not conversation` 是否成立，以选择正确的处理分支。
            if not conversation:
                # 结束当前函数，并把处理结果返回给调用方。
                return None

            # 计算或设置 `update_data`，用于保存后续流程需要的中间结果。
            update_data = conversation_data.dict(exclude_unset=True)
            # 判断 `update_data` 是否成立，以选择正确的处理分支。
            if update_data:
                # 开始组装多行调用参数、集合或对象内容。
                query = (
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    update(Conversation)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .where(Conversation.id == conversation_id)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .values(**update_data)
                # 结束上方多行参数、集合或对象的定义。
                )
                # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
                await self.db.execute(query)
                # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
                await self.db.commit()
                # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
                await self.db.refresh(conversation)

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"更新了对话{conversation_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return conversation

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"更新对话{conversation_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `delete_conversation()`，执行该函数对应的业务流程。
    async def delete_conversation(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID
    # 结束上方多行参数、集合或对象的定义。
    ) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """删除对话及其消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 检查对话是否存在且属于用户
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `conversation`。
            conversation = await self.get_conversation(conversation_id, user_id)
            # 判断 `not conversation` 是否成立，以选择正确的处理分支。
            if not conversation:
                # 结束当前函数，并把处理结果返回给调用方。
                return False

            # 先删除所有消息
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                delete(Message).where(Message.conversation_id == conversation_id)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 删除对话
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                delete(Conversation).where(Conversation.id == conversation_id)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"删除了对话{conversation_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return True

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"删除对话{conversation_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `add_message()`，执行该函数对应的业务流程。
    async def add_message(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `content`，表示当前处理的正文内容。
        content: str,
        # 声明参数或字段 `role`，表示消息发送者角色。
        role: MessageRole,
        # 声明参数或字段 `model_name`，表示当前业务需要的数据。
        model_name: Optional[str] = None,
        # 声明参数或字段 `context`，表示提供给模型的上下文。
        context: Optional[Dict[str, Any]] = None,
        # 声明参数或字段 `parent_id`，表示当前业务需要的数据。
        parent_id: Optional[UUID] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> Message:
        # 补充当前字符串、提示词或响应正文内容。
        """向对话添加消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 开始组装多行调用参数、集合或对象内容。
            message = Message(
                # 计算或设置 `conversation_id`，用于保存会话唯一标识。
                conversation_id=conversation_id,
                # 计算或设置 `content`，用于保存当前处理的正文内容。
                content=content,
                # 计算或设置 `role`，用于保存消息发送者角色。
                role=role,
                # 计算或设置 `model_name`，用于保存后续流程需要的中间结果。
                model_name=model_name,
                # 计算或设置 `context`，用于保存提供给模型的上下文。
                context=context or {},
                # 计算或设置 `parent_message_id`，用于保存后续流程需要的中间结果。
                parent_message_id=parent_id
            # 结束上方多行参数、集合或对象的定义。
            )

            # 调用当前对象的方法，推进本步骤的业务处理。
            self.db.add(message)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('消息保存成功')
            # 更新对话消息计数和最后活动时间
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                update(Conversation)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(Conversation.id == conversation_id)
                # 开始组装多行调用参数、集合或对象内容。
                .values(
                    # 计算或设置 `total_messages`，用于保存后续流程需要的中间结果。
                    total_messages=Conversation.total_messages + 1,
                    # 计算或设置 `updated_at`，用于保存后续流程需要的中间结果。
                    updated_at=func.now()
                # 结束上方多行参数、集合或对象的定义。
                )
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
            await self.db.refresh(message)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('update conversation message number success')
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"向对话{conversation_id}添加了消息")
            # 结束当前函数，并把处理结果返回给调用方。
            return message

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # await self.db.rollback()
            logger.error(f"向对话{conversation_id}添加消息时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_conversation_messages()`，执行该函数对应的业务流程。
    async def get_conversation_messages(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 50
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[Message]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取对话的消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 开始组装多行调用参数、集合或对象内容。
            query = (
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                select(Message)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(Message.conversation_id == conversation_id)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .order_by(Message.created_at)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .offset(skip)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .limit(limit)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalars().all()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取对话{conversation_id}的消息时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_message()`，执行该函数对应的业务流程。
    async def get_message(self, message_id: UUID) -> Optional[Message]:
        # 补充当前字符串、提示词或响应正文内容。
        """通过ID获取消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            query = select(Message).where(Message.id == message_id)
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalar_one_or_none()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取消息{message_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `update_message()`，执行该函数对应的业务流程。
    async def update_message(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_id`，表示会话唯一标识。
        conversation_id: UUID,
        # 声明参数或字段 `message_id`，表示消息唯一标识。
        message_id: UUID,
        # 声明参数或字段 `message_update`，表示当前业务需要的数据。
        message_update: MessageUpdate
    # 结束上方多行参数、集合或对象的定义。
    ) -> Optional[Message]:
        # 补充当前字符串、提示词或响应正文内容。
        """更新对话中的单条消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `message`。
            message = await self.get_message(message_id)
            # 判断 `not message or str(message.conversation_id) != str(conversation_id)` 是否成立，以选择正确的处理分支。
            if not message or str(message.conversation_id) != str(conversation_id):
                # 结束当前函数，并把处理结果返回给调用方。
                return None

            # 计算或设置 `update_data`，用于保存后续流程需要的中间结果。
            update_data = message_update.model_dump(exclude_unset=True)
            # 判断 `"user_feedback" in update_data` 是否成立，以选择正确的处理分支。
            if "user_feedback" in update_data:
                # 计算或设置 `update_data["feedback"]`，用于保存后续流程需要的中间结果。
                update_data["feedback"] = update_data.pop("user_feedback")
            # 判断 `update_data` 是否成立，以选择正确的处理分支。
            if update_data:
                # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
                await self.db.execute(
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    update(Message)
                    # 开始组装多行调用参数、集合或对象内容。
                    .where(
                        # 设置调用参数 `Message.id`，把对应业务数据传给下游方法。
                        Message.id == message_id,
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        Message.conversation_id == conversation_id
                    # 结束上方多行参数、集合或对象的定义。
                    )
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .values(**update_data, updated_at=func.now())
                # 结束上方多行参数、集合或对象的定义。
                )
                # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
                await self.db.execute(
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    update(Conversation)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .where(Conversation.id == conversation_id)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .values(updated_at=func.now())
                # 结束上方多行参数、集合或对象的定义。
                )
                # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
                await self.db.commit()
                # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
                await self.db.refresh(message)

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"更新了对话{conversation_id}中的消息{message_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return message

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"更新消息{message_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `delete_message()`，执行该函数对应的业务流程。
    async def delete_message(self, conversation_id: UUID, message_id: UUID) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """删除对话中的单条消息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `message`。
            message = await self.get_message(message_id)
            # 判断 `not message or str(message.conversation_id) != str(conversation_id)` 是否成立，以选择正确的处理分支。
            if not message or str(message.conversation_id) != str(conversation_id):
                # 结束当前函数，并把处理结果返回给调用方。
                return False

            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 开始组装多行调用参数、集合或对象内容。
                delete(Message).where(
                    # 设置调用参数 `Message.id`，把对应业务数据传给下游方法。
                    Message.id == message_id,
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    Message.conversation_id == conversation_id
                # 结束上方多行参数、集合或对象的定义。
                )
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                update(Conversation)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(Conversation.id == conversation_id)
                # 开始组装多行调用参数、集合或对象内容。
                .values(
                    # 开始组装多行调用参数、集合或对象内容。
                    total_messages=case(
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        (Conversation.total_messages > 0, Conversation.total_messages - 1),
                        # 计算或设置 `else_`，用于保存后续流程需要的中间结果。
                        else_=0
                    # 结束上方多行参数、集合或对象的定义。
                    ),
                    # 计算或设置 `updated_at`，用于保存后续流程需要的中间结果。
                    updated_at=func.now()
                # 结束上方多行参数、集合或对象的定义。
                )
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"删除了对话{conversation_id}中的消息{message_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return True

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"删除消息{message_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `update_message_feedback()`，执行该函数对应的业务流程。
    async def update_message_feedback(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `message_id`，表示消息唯一标识。
        message_id: str,
        # 声明参数或字段 `rating`，表示当前业务需要的数据。
        rating: int,
        # 声明参数或字段 `feedback`，表示当前业务需要的数据。
        feedback: str = ""
    # 结束上方多行参数、集合或对象的定义。
    ) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """更新消息反馈"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `message_uuid`，用于保存后续流程需要的中间结果。
            message_uuid = UUID(message_id)
            # 开始组装多行调用参数、集合或对象内容。
            query = (
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                update(Message)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(Message.id == message_uuid)
                # 开始组装多行调用参数、集合或对象内容。
                .values(
                    # 开始组装多行调用参数、集合或对象内容。
                    user_feedback={
                        # 补充当前字符串、提示词或响应正文内容。
                        "rating": rating,
                        # 补充当前字符串、提示词或响应正文内容。
                        "feedback": feedback
                    # 结束上方多行参数、集合或对象的定义。
                    }
                # 结束上方多行参数、集合或对象的定义。
                )
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()

            # 结束当前函数，并把处理结果返回给调用方。
            return result.rowcount > 0

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"更新消息反馈{message_id}时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `search_conversations()`，执行该函数对应的业务流程。
    async def search_conversations(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `query`，表示查询文本或数据库查询对象。
        query: str,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 10
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[Conversation]:
        # 补充当前字符串、提示词或响应正文内容。
        """按标题或内容搜索对话"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 简单文本搜索 - 在生产环境中，您可能希望使用全文搜索
            # 开始组装多行调用参数、集合或对象内容。
            search_query = (
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                select(Conversation)
                # 开始组装多行调用参数、集合或对象内容。
                .where(
                    # 设置调用参数 `Conversation.user_id`，把对应业务数据传给下游方法。
                    Conversation.user_id == user_id,
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    Conversation.title.ilike(f"%{query}%")
                # 结束上方多行参数、集合或对象的定义。
                )
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .order_by(desc(Conversation.updated_at))
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .limit(limit)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(search_query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalars().all()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"搜索用户{user_id}的对话时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise
