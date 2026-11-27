"""
* 文件作用
  * 这是知识库文档管理的 FastAPI 接口层文件，对外提供文档分页查询、上传、触发解析与向量化、查看详情、查看分块和删除文档的 HTTP API。它使用 UploadFile 接收 multipart/form-data 文件，使用 Depends 获取当前用户与数据库会话，再把文件校验、保存、解析、分块、Embedding 和清理工作交给 EnhancedDocumentService。该文件是理解“前端点击上传文档以后，文件怎样进入后端并最终成为可检索向量”的入口，应结合状态码、异常转换、用户 ID 与 knowledge_base_id 的传递过程一起阅读。
* 类说明
  * 本文件没有定义业务类，而是以模块级函数组织接口或工具能力；这样可以直接被 FastAPI 路由注册或被其他模块按函数导入调用。
* 函数与方法说明
  * `get_documents()`：文档列表接口，通过当前用户 ID、knowledge_base_id、分页和筛选参数调用轻量文档服务查询 documents 表；它只读取文档元数据而不初始化 LLM 与 Embedding，减少普通列表请求的资源开销。
  * `upload_document()`：文档上传接口，接收 multipart/form-data 文件、知识库 ID、分类和标签，先校验文件类型与大小，再由 EnhancedDocumentService 计算 SHA-256 防重复、保存文件到 uploads 目录并在 documents 表创建待处理记录；此步骤不执行正文解析和向量化。
  * `process_document()`：文档处理接口，根据 document_id 读取已上传文件并验证用户权限，调用 EnhancedDocumentService.process_document() 提取和清洗正文、执行语义与长度混合分块、生成 Embedding 并写入 PGVector，同时更新 documents 表的处理状态与错误信息。
  * `get_document()`：文档详情接口，按 document_id 和 current_user.id 查询 documents 表，返回文件名、类型、大小、知识库归属与处理状态；通过用户过滤保证不能读取其他用户上传的文档。
  * `get_document_chunks()`：文档分块预览接口，先校验 document_id 对应文档属于当前用户，再从 langchain_pg_embedding 向量表按 metadata 中的文档 ID 读取各个 chunk，依据 chunk_index 排序后返回正文和元数据。
  * `delete_document()`：文档删除接口，按 document_id 校验文档存在且属于当前用户，然后同时删除 PGVector 中的相关分块向量、documents 元数据记录和 uploads 目录中的本地文件，避免留下无法追踪的脏向量或孤立文件。
"""

"""
    负责文档上传和管理，挂载前缀是 /documents。
    主要接口：
        GET    /documents/
        POST   /documents/upload
        POST   /documents/{document_id}/process
        GET    /documents/{document_id}
        GET    /documents/{document_id}/chunks
        DELETE /documents/{document_id}
"""
# 从 `typing` 导入当前模块需要的 `Any, List`。
from typing import Any, List
# 从 `fastapi` 导入当前模块需要的 `APIRouter, Depends, HTTPException, status, UploadFile, File, Form`。
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession

# 从 `app.core.database` 导入当前模块需要的 `get_db`。
from app.core.database import get_db
# 从 `app.schemas.document` 导入当前模块需要的 `Document as DocumentSchema, DocumentCreate`。
from app.schemas.document import Document as DocumentSchema, DocumentCreate
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema
# 从 `app.api.deps` 导入当前模块需要的 `get_current_user`。
from app.api.deps import get_current_user
# 从 `app.services.enhanced_document_service` 导入当前模块需要的 `EnhancedDocumentService`。
from app.services.enhanced_document_service import EnhancedDocumentService
# 从 `app.services.lightweight_document_service` 导入当前模块需要的 `LightweightDocumentService`。
from app.services.lightweight_document_service import LightweightDocumentService

# 计算或设置 `router`，用于保存后续流程需要的中间结果。
router = APIRouter()


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/", response_model=List[DocumentSchema])
# 定义 `get_documents()`，执行该函数对应的业务流程。
async def get_documents(
    # 声明参数或字段 `skip`，表示分页跳过记录数。
    skip: int = 0,
    # 声明参数或字段 `limit`，表示本次最大返回数量。
    limit: int = 100,
    # 声明参数或字段 `category`，表示文档或知识库分类。
    category: str = None,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取文档 - 性能优化版本
        查询类接口优先使用 LightweightDocumentService，避免初始化LLM或embedding，提高性能。
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = LightweightDocumentService(db)
    # 开始组装多行调用参数、集合或对象内容。
    documents = await document_service.get_user_documents(
        # 计算或设置 `user_id`，用于保存用户唯一标识。
        user_id=current_user.id,
        # 计算或设置 `skip`，用于保存分页跳过记录数。
        skip=skip,
        # 计算或设置 `limit`，用于保存本次最大返回数量。
        limit=limit,
        # 计算或设置 `category`，用于保存文档或知识库分类。
        category=category
    # 结束上方多行参数、集合或对象的定义。
    )
    # 结束当前函数，并把处理结果返回给调用方。
    return documents


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/upload", response_model=DocumentSchema)
# 定义 `upload_document()`，执行该函数对应的业务流程。
async def upload_document(
    # 声明参数或字段 `file`，表示上传文件对象。
    file: UploadFile = File(...),
    # 声明参数或字段 `category`，表示文档或知识库分类。
    category: str = Form(None),
    # 声明参数或字段 `tags`，表示标签列表。
    tags: List[str] = Form(None),
    # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
    knowledge_base_id: str = Form(None),
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    上传新文档：
        upload_document 接收文件、分类、标签、知识库 ID。
        上传、处理、查看分块、删除使用 EnhancedDocumentService。
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = EnhancedDocumentService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 开始组装多行调用参数、集合或对象内容。
        document = await document_service.upload_document(
            # 计算或设置 `file`，用于保存上传文件对象。
            file=file,
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=current_user.id,
            # 计算或设置 `category`，用于保存文档或知识库分类。
            category=category,
            # 计算或设置 `tags`，用于保存标签列表。
            tags=tags or [],
            # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
            knowledge_base_id=knowledge_base_id
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return document
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 异步等待 `document_service.handle_document_service_error` 完成，避免阻塞其他并发请求。
        await document_service.handle_document_service_error(e, "上传文档")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.post("/{document_id}/process", response_model=DocumentSchema)
# 定义 `process_document()`，执行该函数对应的业务流程。
async def process_document(
    # 声明参数或字段 `document_id`，表示文档唯一标识。
    document_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    处理已上传的文档：提取文本、分块、向量化
        process_document 会触发文本提取、分块、向量化。
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = EnhancedDocumentService(db)

    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 从 `uuid` 导入当前模块需要的 `UUID`。
        from uuid import UUID
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `document`。
        document = await document_service.process_document(UUID(document_id))
        # 结束当前函数，并把处理结果返回给调用方。
        return document

    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 异步等待 `document_service.handle_document_service_error` 完成，避免阻塞其他并发请求。
        await document_service.handle_document_service_error(e, "处理文档")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/{document_id}", response_model=DocumentSchema)
# 定义 `get_document()`，执行该函数对应的业务流程。
async def get_document(
    # 声明参数或字段 `document_id`，表示文档唯一标识。
    document_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    根据ID获取文档 - 性能优化版本
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = LightweightDocumentService(db)
    # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `document`。
    document = await document_service.get_document_with_permission_check(document_id, current_user)
    # 结束当前函数，并把处理结果返回给调用方。
    return document


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.get("/{document_id}/chunks")
# 定义 `get_document_chunks()`，执行该函数对应的业务流程。
async def get_document_chunks(
    # 声明参数或字段 `document_id`，表示文档唯一标识。
    document_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    获取文档分块用于预览
        chunks 用于预览文档分块内容。
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = EnhancedDocumentService(db)
    
    # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
    try:
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `chunks`。
        chunks = await document_service.get_document_chunks(document_id, current_user.id)
        # 结束当前函数，并把处理结果返回给调用方。
        return {"chunks": chunks}
        
    # 捕获指定异常，避免底层错误未经处理直接中断请求。
    except Exception as e:
        # 异步等待 `document_service.handle_document_service_error` 完成，避免阻塞其他并发请求。
        await document_service.handle_document_service_error(e, "获取文档分块")


# 注册 FastAPI 路由，并声明请求路径、HTTP 方法或响应结构。
@router.delete("/{document_id}")
# 定义 `delete_document()`，执行该函数对应的业务流程。
async def delete_document(
    # 声明参数或字段 `document_id`，表示文档唯一标识。
    document_id: str,
    # 声明参数或字段 `current_user`，表示当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 声明参数或字段 `db`，表示异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数、集合或对象的定义。
) -> Any:
    """
    删除文档
    """
    # 计算或设置 `document_service`，用于保存后续流程需要的中间结果。
    document_service = EnhancedDocumentService(db)
    # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `document`。
    document = await document_service.get_document_with_permission_check(document_id, current_user)
    
    # 异步等待 `document_service.delete` 完成，避免阻塞其他并发请求。
    await document_service.delete(document)
    # 结束当前函数，并把处理结果返回给调用方。
    return {"message": "文档删除成功"}
