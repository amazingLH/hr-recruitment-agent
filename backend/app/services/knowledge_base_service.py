"""
* 文件作用
  * 这是知识库管理模块的核心业务服务层，位于 FastAPI 接口层与 SQLAlchemy ORM 模型之间，集中完成知识库 CRUD、可访问范围过滤、名称搜索、文档数量统计和管理员权限复核。代码使用 AsyncSession、select/update/delete、异步 await 和 ORM 条件表达式访问 PostgreSQL，并通过 KnowledgeBaseEndpointService 再封装一层适合接口调用的权限逻辑。学习时要重点理解 knowledge_bases.id 是知识库主键、documents.knowledge_base_id 是文档指向知识库的外键，以及公开知识库、本人创建的知识库、管理员操作和删除后解除文档归属之间的业务关系。
* 类说明
  * `KnowledgeBaseService`：知识库的数据访问服务，构造时保存 AsyncSession 数据库会话，直接使用 SQLAlchemy 操作 knowledge_bases 和 documents 表。它负责知识库创建、单条/列表查询、公开或本人知识库过滤、更新、删除、名称搜索和文档数量统计；这里只处理数据与基础业务，不接收 FastAPI 的 current_user，也不决定当前请求是否具有管理员权限。
  * `KnowledgeBaseEndpointService`：知识库接口专用的业务编排与权限服务，构造时组合 KnowledgeBaseService。它接收接口层传来的 current_user，先执行管理员或知识库访问权限检查，再调用 KnowledgeBaseService 完成实际数据库操作；因此它与 KnowledgeBaseService 的区别是：前者负责请求级鉴权和流程编排，后者负责通用 CRUD 与数据库查询。
* 函数与方法说明
  * `KnowledgeBaseService.__init__()`：KnowledgeBaseService 的构造方法，接收 FastAPI 依赖注入产生的 AsyncSession 并保存到 self.db，后续知识库 CRUD、文档统计和搜索方法都复用这个异步数据库会话执行 SQLAlchemy 查询与事务操作。
  * `KnowledgeBaseService.create_knowledge_base()`：创建一条知识库记录：把 KnowledgeBaseCreate 中的名称、描述和公开开关转换成 KnowledgeBase ORM 对象，同时写入创建者 user_id；随后通过 AsyncSession 提交事务并刷新对象，使返回结果包含数据库生成的知识库 UUID 和时间字段，提交失败时回滚事务。
  * `KnowledgeBaseService.get_knowledge_base()`：按照知识库主键 kb_id 查询 knowledge_bases 表，使用 SQLAlchemy select() 和 KnowledgeBase.id == kb_id 精确过滤，再通过 scalar_one_or_none() 返回唯一对象；ID 不存在时返回 None，而不是在这一层直接生成 HTTP 错误。
  * `KnowledgeBaseService.get_knowledge_bases()`：分页查询全部知识库，使用 SQLAlchemy select() 构造查询，通过 offset(skip) 跳过前面的记录、limit(limit) 限制返回数量，并以 created_at 倒序排列，使新创建的知识库优先显示。
  * `KnowledgeBaseService.get_accessible_knowledge_bases()`：查询指定用户允许查看的知识库：SQL 条件同时保留 is_public=True 的公开知识库和 created_by=user_id 的本人知识库，再应用分页与创建时间倒序；因此普通用户看不到其他用户创建的私有知识库。
  * `KnowledgeBaseService.update_knowledge_base()`：根据 kb_id 读取知识库，把 KnowledgeBaseUpdate 中实际传入且不为未设置状态的字段逐项写回 ORM 对象，再提交并刷新数据库记录；知识库不存在时返回 None，数据库写入失败时回滚，权限判断不在该基础数据方法中完成。
  * `KnowledgeBaseService.delete_knowledge_base()`：删除指定知识库前先确认记录存在，然后把 documents 表中引用该知识库的 knowledge_base_id 更新为 NULL，解除文档与知识库的归属关系，再删除 knowledge_bases 记录并提交事务；这样保留原文档和向量资料，避免删除知识库时级联丢失文件，任一步失败都会回滚。
  * `KnowledgeBaseService.search_knowledge_base()`：在用户可访问范围内按关键词搜索知识库，使用 SQLAlchemy 对名称和描述执行不区分大小写的 ILIKE 模糊匹配，同时叠加“公开知识库或本人创建”权限条件、分页和创建时间倒序，返回既匹配关键词又有权访问的知识库列表。
  * `KnowledgeBaseService.get_knowledge_base_stats()`：统计指定知识库的文档情况，先确认 knowledge_bases.id 存在，再查询 documents.knowledge_base_id == kb_id 的记录数量及处理状态，整理为 document_count 等统计结果供接口层补入知识库响应；知识库不存在时返回空统计或对应的失败结果。
  * `KnowledgeBaseEndpointService.__init__()`：KnowledgeBaseEndpointService 的构造方法，保存 AsyncSession，并基于同一数据库会话创建 KnowledgeBaseService；该组合让端点服务负责用户权限和调用编排，而基础服务负责实际 SQLAlchemy 数据访问。
  * `KnowledgeBaseEndpointService.get_accessible_knowledge_bases()`：接收接口层传入的当前用户 ID、skip 和 limit，把查询委托给 KnowledgeBaseService.get_accessible_knowledge_bases()，返回公开知识库与该用户本人创建的私有知识库，作为知识库列表接口的数据来源。
  * `KnowledgeBaseEndpointService.create_knowledge_base()`：接收已经通过身份认证的 current_user 和创建数据，把 current_user.id 作为 created_by 传给基础服务，确保新知识库归属于当前登录用户；数据库创建、提交和刷新由 KnowledgeBaseService.create_knowledge_base() 完成。
  * `KnowledgeBaseEndpointService._check_admin_permission()`：执行服务层管理员权限复核：读取 current_user.is_superuser；若不是超级管理员则立即抛出 HTTP 403，阻止知识库修改或删除继续执行。
  * `KnowledgeBaseEndpointService.update_knowledge_base_with_permission_check()`：更新知识库的端点业务流程：先调用 _check_admin_permission() 检查 current_user.is_superuser，非管理员立即返回 HTTP 403；通过后再按 kb_id 调用 KnowledgeBaseService.update_knowledge_base()，目标不存在时转换为 HTTP 404，从而同时保证管理员权限和资源存在性。
  * `KnowledgeBaseEndpointService.delete_knowledge_base_with_permission_check()`：删除知识库的端点业务流程：先进行超级管理员权限复核，再调用基础服务解除 documents.knowledge_base_id 并删除 knowledge_bases 记录；非管理员得到 HTTP 403、目标不存在得到 HTTP 404，其他数据库异常转换为服务器错误。
  * `KnowledgeBaseEndpointService._check_knowledge_base_access()`：检查当前用户能否访问指定知识库：先按 kb_id 读取记录，记录不存在时返回 HTTP 404；is_public=True 表示任何已登录用户可访问，私有知识库仅创建者或超级管理员可以访问，越权时返回 HTTP 403。
"""

"""

知识库管理服务：用于管理知识库的服务
    KnowledgeBaseService 做知识库 CRUD、可访问知识库列表、知识库搜索、统计。
    KnowledgeBaseEndpointService 是面向 API endpoint 的包装层，额外处理管理员权限、知识库访问权限和用户角色校验。
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 从 `typing` 导入当前模块需要的 `List, Optional, Dict, Any, Union`。
from typing import List, Optional, Dict, Any, Union
# 从 `uuid` 导入当前模块需要的 `UUID`。
from uuid import UUID
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy` 导入当前模块需要的 `select, update, delete, desc, func`。
from sqlalchemy import select, update, delete, desc, func
# 从 `fastapi` 导入当前模块需要的 `HTTPException, status`。
from fastapi import HTTPException, status

# 从 `app.models.knowledge_base` 导入当前模块需要的 `KnowledgeBase`。
from app.models.knowledge_base import KnowledgeBase
# 从 `app.models.document` 导入当前模块需要的 `Document`。
from app.models.document import Document
# 从 `app.models.user` 导入当前模块需要的 `Role, UserRoleAssociation`。
from app.models.user import Role, UserRoleAssociation
# 从 `app.schemas.knowledge_base` 导入当前模块需要的 `KnowledgeBaseCreate, KnowledgeBaseUpdate`。
from app.schemas.knowledge_base import KnowledgeBaseCreate, KnowledgeBaseUpdate
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `KnowledgeBaseService` 类，集中封装相关数据或业务能力。
class KnowledgeBaseService:
    # 补充当前字符串、提示词或响应正文内容。
    """用于管理知识库的服务"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 计算或设置 `self.db`，用于保存异步数据库会话。
        self.db = db

    # 定义 `create_knowledge_base()`，执行该函数对应的业务流程。
    async def create_knowledge_base(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_data`，表示当前业务需要的数据。
        kb_data: KnowledgeBaseCreate
    # 结束上方多行参数、集合或对象的定义。
    ) -> KnowledgeBase:
        # 补充当前字符串、提示词或响应正文内容。
        """创建新的知识库"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 开始组装多行调用参数、集合或对象内容。
            knowledge_base = KnowledgeBase(
                # 计算或设置 `name`，用于保存后续流程需要的中间结果。
                name=kb_data.name,
                # 计算或设置 `description`，用于保存后续流程需要的中间结果。
                description=kb_data.description,
                # 计算或设置 `is_public`，用于保存后续流程需要的中间结果。
                is_public=kb_data.is_public,
                # 计算或设置 `is_searchable`，用于保存后续流程需要的中间结果。
                is_searchable=kb_data.is_searchable,
                # 计算或设置 `category`，用于保存文档或知识库分类。
                category=kb_data.category,
                # 计算或设置 `tags`，用于保存标签列表。
                tags=kb_data.tags or [],
                # 计算或设置 `meta_data`，用于保存后续流程需要的中间结果。
                meta_data=kb_data.meta_data or {}
            # 结束上方多行参数、集合或对象的定义。
            )

            # 调用当前对象的方法，推进本步骤的业务处理。
            self.db.add(knowledge_base)
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
            await self.db.refresh(knowledge_base)

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"创建了知识库 {knowledge_base.id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return knowledge_base

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"创建知识库时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_knowledge_base()`，执行该函数对应的业务流程。
    async def get_knowledge_base(self, kb_id: Union[UUID, str]) -> Optional[KnowledgeBase]:
        # 补充当前字符串、提示词或响应正文内容。
        """通过ID获取知识库"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 如果是字符串，转换为UUID
            # 判断 `isinstance(kb_id, str)` 是否成立，以选择正确的处理分支。
            if isinstance(kb_id, str):
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 计算或设置 `kb_uuid`，用于保存后续流程需要的中间结果。
                    kb_uuid = UUID(kb_id)
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except ValueError:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.error(f"无效的UUID格式: {kb_id}")
                    # 结束当前函数，并把处理结果返回给调用方。
                    return None
            # 前面的判断条件均不成立时，执行当前备用分支。
            else:
                # 计算或设置 `kb_uuid`，用于保存后续流程需要的中间结果。
                kb_uuid = kb_id
                
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            query = select(KnowledgeBase).where(KnowledgeBase.id == kb_uuid)
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalar_one_or_none()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取知识库 {kb_id} 时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_knowledge_bases()`，执行该函数对应的业务流程。
    async def get_knowledge_bases(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 20,
        # 声明参数或字段 `is_public`，表示当前业务需要的数据。
        is_public: Optional[bool] = None,
        # 声明参数或字段 `category`，表示文档或知识库分类。
        category: Optional[str] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[KnowledgeBase]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取知识库并可选择过滤"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
            query = select(KnowledgeBase)

            # 判断 `is_public is not None` 是否成立，以选择正确的处理分支。
            if is_public is not None:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                query = query.where(KnowledgeBase.is_public == is_public)

            # 判断 `category` 是否成立，以选择正确的处理分支。
            if category:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                query = query.where(KnowledgeBase.category == category)

            # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
            query = query.order_by(desc(KnowledgeBase.created_at)).offset(skip).limit(limit)

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalars().all()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取知识库时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_accessible_knowledge_bases()`，执行该函数对应的业务流程。
    async def get_accessible_knowledge_bases(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 20
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[KnowledgeBase]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取用户可访问的知识库"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 目前，返回所有公共知识库
            # 未来，这可能包括用户特定的访问控制
            # 开始组装多行调用参数、集合或对象内容。
            query = select(KnowledgeBase).where(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                KnowledgeBase.is_public == True
            # 结束上方多行参数、集合或对象的定义。
            ).            order_by(desc(KnowledgeBase.created_at)).offset(skip).limit(limit)

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            return result.scalars().all()

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取用户 {user_id} 可访问的知识库时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise



    # 定义 `update_knowledge_base()`，执行该函数对应的业务流程。
    async def update_knowledge_base(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_id`，表示知识库唯一标识。
        kb_id: UUID,
        # 声明参数或字段 `kb_data`，表示当前业务需要的数据。
        kb_data: KnowledgeBaseUpdate
    # 结束上方多行参数、集合或对象的定义。
    ) -> Optional[KnowledgeBase]:
        # 补充当前字符串、提示词或响应正文内容。
        """更新知识库"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 检查知识库是否存在
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `kb`。
            kb = await self.get_knowledge_base(kb_id)
            # 判断 `not kb` 是否成立，以选择正确的处理分支。
            if not kb:
                # 结束当前函数，并把处理结果返回给调用方。
                return None

            # 计算或设置 `update_data`，用于保存后续流程需要的中间结果。
            update_data = kb_data.dict(exclude_unset=True)
            # 判断 `update_data` 是否成立，以选择正确的处理分支。
            if update_data:
                # 开始组装多行调用参数、集合或对象内容。
                query = (
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    update(KnowledgeBase)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .where(KnowledgeBase.id == kb_id)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    .values(**update_data)
                # 结束上方多行参数、集合或对象的定义。
                )
                # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
                await self.db.execute(query)
                # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
                await self.db.commit()
                # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
                await self.db.refresh(kb)

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"更新了知识库 {kb_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return kb

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"更新知识库 {kb_id} 时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `delete_knowledge_base()`，执行该函数对应的业务流程。
    async def delete_knowledge_base(self, kb_id: UUID) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """删除知识库并更新相关文档"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 检查知识库是否存在
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `kb`。
            kb = await self.get_knowledge_base(kb_id)
            # 判断 `not kb` 是否成立，以选择正确的处理分支。
            if not kb:
                # 结束当前函数，并把处理结果返回给调用方。
                return False

            # 更新文档以移除知识库引用
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                update(Document)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(Document.knowledge_base_id == kb_id)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .values(knowledge_base_id=None)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 删除知识库
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                delete(KnowledgeBase).where(KnowledgeBase.id == kb_id)
            # 结束上方多行参数、集合或对象的定义。
            )

            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"删除了知识库 {kb_id}")
            # 结束当前函数，并把处理结果返回给调用方。
            return True

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"删除知识库 {kb_id} 时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `search_knowledge_base()`，执行该函数对应的业务流程。
    async def search_knowledge_base(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_id`，表示知识库唯一标识。
        kb_id: UUID,
        # 声明参数或字段 `query`，表示查询文本或数据库查询对象。
        query: str,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 10
    # 结束上方多行参数、集合或对象的定义。
    ) -> Dict[str, Any]:
        # 补充当前字符串、提示词或响应正文内容。
        """在特定知识库中搜索"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 获取知识库
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `kb`。
            kb = await self.get_knowledge_base(kb_id)
            # 判断 `not kb` 是否成立，以选择正确的处理分支。
            if not kb:
                # 结束当前函数，并把处理结果返回给调用方。
                return {"documents": []}

            # 搜索文档
            # 开始组装多行调用参数、集合或对象内容。
            doc_query = (
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                select(Document)
                # 开始组装多行调用参数、集合或对象内容。
                .where(
                    # 设置调用参数 `Document.knowledge_base_id`，把对应业务数据传给下游方法。
                    Document.knowledge_base_id == kb_id,
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    Document.extracted_content.ilike(f"%{query}%")
                # 结束上方多行参数、集合或对象的定义。
                )
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .limit(limit)
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `doc_result`。
            doc_result = await self.db.execute(doc_query)
            # 计算或设置 `documents`，用于保存候选文档或文档列表。
            documents = doc_result.scalars().all()

            # 结束当前函数，并把处理结果返回给调用方。
            return {
                # 开始组装多行调用参数、集合或对象内容。
                "knowledge_base": {
                    # 补充当前字符串、提示词或响应正文内容。
                    "id": str(kb.id),
                    # 补充当前字符串、提示词或响应正文内容。
                    "name": kb.name,
                    # 补充当前字符串、提示词或响应正文内容。
                    "description": kb.description
                # 结束上方多行参数、集合或对象的定义。
                },
                # 开始组装多行调用参数、集合或对象内容。
                "documents": [
                    # 开始组装多行调用参数、集合或对象内容。
                    {
                        # 补充当前字符串、提示词或响应正文内容。
                        "id": str(doc.id),
                        # 补充当前字符串、提示词或响应正文内容。
                        "filename": doc.filename,
                        # 补充当前字符串、提示词或响应正文内容。
                        "content": doc.extracted_content[:300]
                    # 结束上方多行参数、集合或对象的定义。
                    }
                    # 遍历当前集合中的每一项，逐项完成相同处理。
                    for doc in documents
                # 结束上方多行参数、集合或对象的定义。
                ]
            # 结束上方多行参数、集合或对象的定义。
            }

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"搜索知识库 {kb_id} 时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_knowledge_base_stats()`，执行该函数对应的业务流程。
    async def get_knowledge_base_stats(self, kb_id: UUID) -> Dict[str, Any]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取知识库统计信息"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 获取文档数量
            # 开始组装多行调用参数、集合或对象内容。
            doc_count_query = select(func.count(Document.id)).where(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                Document.knowledge_base_id == kb_id
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `doc_count_result`。
            doc_count_result = await self.db.execute(doc_count_query)
            # 计算或设置 `doc_count`，用于保存后续流程需要的中间结果。
            doc_count = doc_count_result.scalar()

            # 更新知识库文档数量
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            await self.db.execute(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                update(KnowledgeBase)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .where(KnowledgeBase.id == kb_id)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                .values(document_count=doc_count)
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            await self.db.commit()

            # 结束当前函数，并把处理结果返回给调用方。
            return {"document_count": doc_count}

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取知识库统计信息 {kb_id} 时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

# 定义 `KnowledgeBaseEndpointService` 类，集中封装相关数据或业务能力。
class KnowledgeBaseEndpointService:
    # 补充当前字符串、提示词或响应正文内容。
    """知识库端点服务类，处理所有知识库相关的业务逻辑"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 计算或设置 `self.db`，用于保存异步数据库会话。
        self.db = db
        # 计算或设置 `self.kb_service`，用于保存后续流程需要的中间结果。
        self.kb_service = KnowledgeBaseService(db)

    # 定义 `get_accessible_knowledge_bases()`，执行该函数对应的业务流程。
    async def get_accessible_knowledge_bases(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: str,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 100
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[KnowledgeBase]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取用户可访问的知识库列表"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `user_uuid`，用于保存后续流程需要的中间结果。
            user_uuid = UUID(user_id)
            # 开始组装多行调用参数、集合或对象内容。
            knowledge_bases = await self.kb_service.get_accessible_knowledge_bases(
                # 计算或设置 `user_id`，用于保存用户唯一标识。
                user_id=user_uuid,
                # 计算或设置 `skip`，用于保存分页跳过记录数。
                skip=skip,
                # 计算或设置 `limit`，用于保存本次最大返回数量。
                limit=limit
            # 结束上方多行参数、集合或对象的定义。
            )
            # 结束当前函数，并把处理结果返回给调用方。
            return knowledge_bases
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 计算或设置 `error_msg`，用于保存后续流程需要的中间结果。
            error_msg = f"获取知识库列表错误: {e}"
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(error_msg)
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise Exception(error_msg)

    # 定义 `create_knowledge_base()`，执行该函数对应的业务流程。
    async def create_knowledge_base(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_data`，表示当前业务需要的数据。
        kb_data: KnowledgeBaseCreate
    # 结束上方多行参数、集合或对象的定义。
    ) -> KnowledgeBase:
        # 补充当前字符串、提示词或响应正文内容。
        """创建知识库"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `knowledge_base`。
            knowledge_base = await self.kb_service.create_knowledge_base(kb_data)
            # 结束当前函数，并把处理结果返回给调用方。
            return knowledge_base
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 计算或设置 `error_msg`，用于保存后续流程需要的中间结果。
            error_msg = f"创建知识库错误: {e}"
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(error_msg)
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise Exception(error_msg)

    # 定义 `_check_admin_permission()`，执行该函数对应的业务流程。
    async def _check_admin_permission(self, current_user: UserSchema) -> None:
        # 补充当前字符串、提示词或响应正文内容。
        """检查管理员权限，无权限则抛出异常"""
        # 判断 `not current_user.is_superuser` 是否成立，以选择正确的处理分支。
        if not current_user.is_superuser:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_403_FORBIDDEN,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="权限不足，只有管理员可以执行此操作"
            # 结束上方多行参数、集合或对象的定义。
            )

    # async def get_knowledge_base_with_permission_check(
    #     self,
    #     kb_id: str,
    #     current_user: UserSchema
    # ) -> KnowledgeBase:
    #     """获取知识库并检查权限"""
    #     try:
    #         knowledge_base = await self.kb_service.get_knowledge_base(kb_id)
    #         if not knowledge_base:
    #             raise HTTPException(
    #                 status_code=status.HTTP_404_NOT_FOUND,
    #                 detail="知识库未找到"
    #             )
    #         await self._check_knowledge_base_access(knowledge_base, current_user)
    #         return knowledge_base
    #     except HTTPException:
    #         raise
    #     except Exception as e:
    #         logger.error(f"获取知识库错误: {e}")
    #         raise HTTPException(
    #             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    #             detail="获取知识库失败"
    #         )

    # 定义 `update_knowledge_base_with_permission_check()`，执行该函数对应的业务流程。
    async def update_knowledge_base_with_permission_check(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_id`，表示知识库唯一标识。
        kb_id: str,
        # 声明参数或字段 `kb_update`，表示当前业务需要的数据。
        kb_update: KnowledgeBaseUpdate,
        # 声明参数或字段 `current_user`，表示当前登录用户。
        current_user: UserSchema
    # 结束上方多行参数、集合或对象的定义。
    ) -> KnowledgeBase:
        # 补充当前字符串、提示词或响应正文内容。
        """更新知识库并检查权限"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `knowledge_base`。
            knowledge_base = await self.kb_service.get_knowledge_base(kb_id)
            # 判断 `not knowledge_base` 是否成立，以选择正确的处理分支。
            if not knowledge_base:
                # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                raise HTTPException(
                    # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                    status_code=status.HTTP_404_NOT_FOUND,
                    # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                    detail="知识库未找到"
                # 结束上方多行参数、集合或对象的定义。
                )
            # 异步等待 `self._check_admin_permission` 完成，避免阻塞其他并发请求。
            await self._check_admin_permission(current_user)
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `updated_kb`。
            updated_kb = await self.kb_service.update_knowledge_base(knowledge_base.id, kb_update)
            # 结束当前函数，并把处理结果返回给调用方。
            return updated_kb
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except HTTPException:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"更新知识库错误: {e}")
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="更新知识库失败"
            # 结束上方多行参数、集合或对象的定义。
            )

    # 定义 `delete_knowledge_base_with_permission_check()`，执行该函数对应的业务流程。
    async def delete_knowledge_base_with_permission_check(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `kb_id`，表示知识库唯一标识。
        kb_id: str,
        # 声明参数或字段 `current_user`，表示当前登录用户。
        current_user: UserSchema
    # 结束上方多行参数、集合或对象的定义。
    ) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """删除知识库并检查权限"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `knowledge_base`。
            knowledge_base = await self.kb_service.get_knowledge_base(kb_id)
            # 判断 `not knowledge_base` 是否成立，以选择正确的处理分支。
            if not knowledge_base:
                # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                raise HTTPException(
                    # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                    status_code=status.HTTP_404_NOT_FOUND,
                    # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                    detail="知识库未找到"
                # 结束上方多行参数、集合或对象的定义。
                )
            # 异步等待 `self._check_admin_permission` 完成，避免阻塞其他并发请求。
            await self._check_admin_permission(current_user)
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `success`。
            success = await self.kb_service.delete_knowledge_base(knowledge_base.id)
            # 结束当前函数，并把处理结果返回给调用方。
            return {"message": "知识库删除成功"}
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except HTTPException:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"删除知识库错误: {e}")
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="删除知识库失败"
            # 结束上方多行参数、集合或对象的定义。
            )


    # 定义 `_check_knowledge_base_access()`，执行该函数对应的业务流程。
    async def _check_knowledge_base_access(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `knowledge_base`，表示当前业务需要的数据。
        knowledge_base: KnowledgeBase,
        # 声明参数或字段 `current_user`，表示当前登录用户。
        current_user: UserSchema
    # 结束上方多行参数、集合或对象的定义。
    ) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """检查用户对知识库的访问权限"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 如果是公开知识库，允许访问
            # 判断 `knowledge_base.is_public` 是否成立，以选择正确的处理分支。
            if knowledge_base.is_public:
                # 结束当前函数，并把处理结果返回给调用方。
                return True

            # 如果是超级管理员，允许访问
            # 判断 `current_user.is_superuser` 是否成立，以选择正确的处理分支。
            if current_user.is_superuser:
                # 结束当前函数，并把处理结果返回给调用方。
                return True

            # 检查用户是否有访问权限
            # 开始组装多行调用参数、集合或对象内容。
            query = select(Role).join(UserRoleAssociation).where(
                # 设置调用参数 `UserRoleAssociation.user_id`，把对应业务数据传给下游方法。
                UserRoleAssociation.user_id == str(current_user.id),
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                Role.is_active == True
            # 结束上方多行参数、集合或对象的定义。
            )
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 计算或设置 `roles`，用于保存后续流程需要的中间结果。
            roles = result.scalars().all()

            # 如果用户有管理员角色，允许访问
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for role in roles:
                # 判断 `role.name in ["超级管理员", "系统管理员"]` 是否成立，以选择正确的处理分支。
                if role.name in ["超级管理员", "系统管理员"]:
                    # 结束当前函数，并把处理结果返回给调用方。
                    return True

            # 私有知识库，权限不足
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_403_FORBIDDEN,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="权限不足，无法访问此知识库"
            # 结束上方多行参数、集合或对象的定义。
            )
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except HTTPException:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"检查知识库访问权限错误: {e}")
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise HTTPException(
                # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                detail="检查访问权限失败"
            # 结束上方多行参数、集合或对象的定义。
            )
