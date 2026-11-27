"""
* 文件作用
  * 这是知识库管理功能的 FastAPI 接口层文件，负责把前端发来的查询、创建、修改和删除请求转换为后端可执行的业务调用。它使用 APIRouter 声明 HTTP 路由，使用 Depends 注入数据库会话和当前登录用户，并通过 Pydantic Schema 约束请求、响应的数据结构；真正的数据查询和权限判断继续委托给 KnowledgeBaseEndpointService 与 KnowledgeBaseService。理解本文件时应重点观察“请求进入接口—解析参数—鉴权—调用 Service—转换异常—返回 JSON”的完整调用链，以及普通用户访问范围和管理员修改权限是怎样从接口层传递到服务层的。
* 类说明
  * 本文件没有定义业务类，而是以模块级函数组织接口或工具能力；这样可以直接被 FastAPI 路由注册或被其他模块按函数导入调用。
* 函数与方法说明
  * `get_knowledge_bases()`：知识库列表接口，接收分页参数和当前登录用户，通过 Depends 注入鉴权用户与 AsyncSession，再调用 KnowledgeBaseEndpointService 查询公开知识库和该用户本人创建的私有知识库；随后逐个调用 get_knowledge_base_stats() 统计 documents.knowledge_base_id 对应的文档数量并写入 document_count，最终返回前端可直接展示的知识库列表。
  * `create_knowledge_base()`：知识库创建接口，接收 KnowledgeBaseCreate 请求数据和当前登录用户，把 current_user.id 作为创建者传给 KnowledgeBaseEndpointService.create_knowledge_base()；服务层将名称、描述、公开状态和创建者写入 knowledge_bases 表，成功返回新知识库对象，数据库异常转换为 HTTP 500。
  * `update_knowledge_base()`：知识库更新接口，路由层先通过 get_current_admin_by_role 校验当前用户具有管理员角色，再把知识库 kb_id、KnowledgeBaseUpdate 更新数据和管理员用户交给 KnowledgeBaseEndpointService；端点服务继续检查 is_superuser、确认知识库存在，然后只更新请求中实际提供的字段并返回更新后的记录。
  * `delete_knowledge_base()`：知识库删除接口，只允许通过 get_current_admin_by_role 鉴权的管理员调用；接口把 kb_id 和 current_user 交给 KnowledgeBaseEndpointService，再次校验 is_superuser 和知识库存在性，随后将该知识库下文档的 documents.knowledge_base_id 置为 NULL、删除 knowledge_bases 记录并提交事务，成功时返回删除结果，越权和不存在分别返回 HTTP 403 与 404。
"""

"""
知识库管理端点
"""
# 从 `typing` 导入当前模块需要的 `Any, List`。
from typing import Any, List
# 从 `fastapi` 导入当前模块需要的 `APIRouter, Depends, HTTPException, status`。
from fastapi import APIRouter, Depends, HTTPException, status
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession

# 从 `app.core.database` 导入当前模块需要的 `get_db`。
from app.core.database import get_db
# 从 `app.schemas.knowledge_base` 导入当前模块需要的 `KnowledgeBase as KnowledgeBaseSchema, KnowledgeBaseCreate, KnowledgeBaseUpdate`。
from app.schemas.knowledge_base import KnowledgeBase as KnowledgeBaseSchema, KnowledgeBaseCreate, KnowledgeBaseUpdate
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema
# 从 `app.services.knowledge_base_service` 导入当前模块需要的 `KnowledgeBaseEndpointService`。
from app.services.knowledge_base_service import KnowledgeBaseEndpointService
# 从 `app.api.deps` 导入当前模块需要的 `get_current_user, get_current_admin_by_role`。
from app.api.deps import get_current_user, get_current_admin_by_role
# 导入 `logging`，供后续代码调用其功能。
import logging 

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)

# 计算或设置 `router`，用于保存后续流程需要的中间结果。
router = APIRouter()

# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/", response_model=List[KnowledgeBaseSchema])
# 定义 `get_knowledge_bases()`，执行该函数对应的业务流程。
async def get_knowledge_bases(
    # 声明参数或字段 `skip`，表示分页跳过记录数。
    skip: int = 0,
    # 声明参数或字段 `limit`，表示本次最大返回数量。
    limit: int = 100,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取知识库列表
        GET 返回当前用户可访问的知识库，同时会补充知识库统计信息。
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeBaseEndpointService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        knowledge_bases = await service.get_accessible_knowledge_bases(
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=str(current_user.id),
            # 计算或设置 `skip`，用于保存分页跳过记录数。
            skip=skip,
            # 计算或设置 `limit`，用于保存本次最大返回数量。
            limit=limit
        # 结束上方多行参数、集合或对象的定义。
        )
        
        # 为每个知识库更新文档数量
        # 从 `app.services.knowledge_base_service` 导入当前模块需要的 `KnowledgeBaseService`。
        from app.services.knowledge_base_service import KnowledgeBaseService
        # 计算或设置 `kb_service`，用于保存后续流程需要的中间结果。
        kb_service = KnowledgeBaseService(db)
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for kb in knowledge_bases:
            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            try:
                # 获取并更新知识库统计信息
                # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `stats`。
                stats = await kb_service.get_knowledge_base_stats(kb.id)
                # 更新知识库对象的文档数量
                kb.document_count = stats.get("document_count", 0)
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except Exception as e:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.warning(f"更新知识库 {kb.id} 文档数量时出错: {e}")
                # 如果更新失败，保持原值
        
        # 结束当前函数，并把处理结果返回给调用方。
        return knowledge_bases
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"获取知识库列表错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/", response_model=KnowledgeBaseSchema)
# 定义 `create_knowledge_base()`，执行该函数对应的业务流程。
async def create_knowledge_base(
    # 声明参数或字段 `kb_data`，表示当前业务需要的数据。
    kb_data: KnowledgeBaseCreate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    创建新的知识库
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeBaseEndpointService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `knowledge_base`。
        knowledge_base = await service.create_knowledge_base(kb_data)
        # 结束当前函数，并把处理结果返回给调用方。
        return knowledge_base
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"创建知识库错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )



# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.put("/{kb_id}", response_model=KnowledgeBaseSchema)
# 定义 `update_knowledge_base()`，执行该函数对应的业务流程。
async def update_knowledge_base(
    # 声明参数或字段 `kb_id`，表示知识库唯一标识。
    kb_id: str,
    # 声明参数或字段 `kb_update`，表示当前业务需要的数据。
    kb_update: KnowledgeBaseUpdate,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    更新知识库（仅管理员）
            需要管理员权限，使用 get_current_admin_by_role。
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeBaseEndpointService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        updated_kb = await service.update_knowledge_base_with_permission_check(
            # 计算或设置 `kb_id`，用于保存知识库唯一标识。
            kb_id=kb_id,
            # 计算或设置 `kb_update`，用于保存后续流程需要的中间结果。
            kb_update=kb_update,
            # 计算或设置 `current_user`，用于保存当前登录用户。
            current_user=current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return updated_kb
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise  # 重新抛出HTTP异常
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"更新知识库错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.delete("/{kb_id}")
# 定义 `delete_knowledge_base()`，执行该函数对应的业务流程。
async def delete_knowledge_base(
    # 声明参数或字段 `kb_id`，表示知识库唯一标识。
    kb_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_admin_by_role),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    删除知识库（仅管理员）
    """
    # 计算或设置 `service`，用于保存后续流程需要的中间结果。
    service = KnowledgeBaseEndpointService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        result = await service.delete_knowledge_base_with_permission_check(
            # 计算或设置 `kb_id`，用于保存知识库唯一标识。
            kb_id=kb_id,
            # 计算或设置 `current_user`，用于保存当前登录用户。
            current_user=current_user
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return result
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except HTTPException:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise  # 重新抛出HTTP异常
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise HTTPException(
            # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
            detail=f"删除知识库错误: {str(e)}"
        # 结束上方多行参数、集合或对象的定义。
        )
