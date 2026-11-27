"""
* 文件作用
  * 这是知识助理接口与底层 RAG 引擎之间的门面服务，负责组合 EnhancedDocumentService、RAGService 和数据库会话，为上层提供配置读取、流式问答与知识库文档查询三个稳定入口。它本身不重复实现检索算法，而是整理 question、knowledge_base_id、conversation_history 等参数并委托给 RAGService，再把异步生成结果原样向上游转发。该设计体现服务编排、依赖组合和职责分离，使接口层不用了解向量检索、重排与 Prompt 构造的内部细节。
* 类说明
  * `KnowledgeAssistantService`：知识助理接口的门面服务，构造时组合 EnhancedDocumentService 与 RAGService。它向接口层提供配置查询、流式提问和知识库文档查询三个入口；自身不实现检索算法，而是整理用户、知识库、问题与会话历史参数，再将问答委托给 RAGService。
* 函数与方法说明
  * `KnowledgeAssistantService.__init__()`：这是 `KnowledgeAssistantService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。内部调用 EnhancedDocumentService()、LightweightDocumentService()、RAGService() 完成主要工作，最终主要通过修改对象状态、提交事务或调用下游服务产生效果。
  * `KnowledgeAssistantService.get_config()`：获取知识助手配置信息 Returns: Dict: 配置信息字典。
  * `KnowledgeAssistantService.ask_question_stream()`：作为知识助理门面层的流式转发方法，把 question、user_id、knowledge_base_id、conversation_history 和 context_limit 原样交给 RAGService.ask_question_stream，并使用 async for/yield 将底层产生的每个事件继续返回给 FastAPI 接口。
  * `KnowledgeAssistantService.get_documents()`：获取知识库中的文档列表（性能优化版本） Args: user_id: 用户ID knowledge_base_id: 知识库ID（可选） skip: 跳过的文档数量 limit: 返回的文档数量限制 Returns: Dict: 包含文档列表和总数的字典 Raises: HTTPException: 知识库ID格式无效时抛出异常 Exception: 获取文档失败时抛出异常。
"""

"""
知识助手业务服务：处理知识库相关的业务逻辑
    组合 EnhancedDocumentService、LightweightDocumentService、RAGService，
    对外提供知识助手配置、流式问答、文档列表。
    核心方法 ask_question_stream 会把用户问题交给 RAG 服务检索并生成回答
"""
# 导入 `json`，供后续代码调用其功能。
import json
# 导入 `logging`，供后续代码调用其功能。
import logging
# 从 `typing` 导入当前模块需要的 `Any, List, Dict, Optional`。
from typing import Any, List, Dict, Optional
# 从 `uuid` 导入当前模块需要的 `UUID`。
from uuid import UUID

# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `fastapi` 导入当前模块需要的 `HTTPException, status`。
from fastapi import HTTPException, status

# 从 `app.services.enhanced_document_service` 导入当前模块需要的 `EnhancedDocumentService`。
from app.services.enhanced_document_service import EnhancedDocumentService
# 从 `app.services.lightweight_document_service` 导入当前模块需要的 `LightweightDocumentService`。
from app.services.lightweight_document_service import LightweightDocumentService
# 从 `app.services.rag_service` 导入当前模块需要的 `RAGService`。
from app.services.rag_service import RAGService
# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings
# 从 `app.schemas.user` 导入当前模块需要的 `User as UserSchema`。
from app.schemas.user import User as UserSchema

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `KnowledgeAssistantService` 类，集中封装相关数据或业务能力。
class KnowledgeAssistantService:
    # 补充当前字符串、提示词或响应正文内容。
    """知识助手服务类，处理所有知识库相关的业务逻辑"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 计算或设置 `self.db`，用于保存异步数据库会话。
        self.db = db
        # 计算或设置 `self.enhanced_document_service`，用于保存后续流程需要的中间结果。
        self.enhanced_document_service = EnhancedDocumentService(db)
        # 计算或设置 `self.lightweight_document_service`，用于保存后续流程需要的中间结果。
        self.lightweight_document_service = LightweightDocumentService(db)
        # 计算或设置 `self.rag_service`，用于保存后续流程需要的中间结果。
        self.rag_service = RAGService(db)

    # 定义 `get_config()`，执行该函数对应的业务流程。
    async def get_config(self) -> Dict[str, Any]:
        """
        获取知识助手配置信息
        
        Returns:
            Dict: 配置信息字典
        """
        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info("获取知识助手配置")
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 补充当前字符串、提示词或响应正文内容。
            "context_limit": settings.CONTEXT_LIMIT
        # 结束上方多行参数、集合或对象的定义。
        }


    # 定义 `ask_question_stream()`，执行该函数对应的业务流程。
    async def ask_question_stream(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `question`，表示用户当前问题。
        question: str,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: str,
        # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
        knowledge_base_id: Optional[str] = None,
        # 声明参数或字段 `conversation_history`，表示此前的多轮对话历史。
        conversation_history: Optional[List[Dict]] = None
    # 结束上方多行参数、集合或对象的定义。
    ):
        """
        向知识助手提问，使用RAG工作流生成流式响应
        
        Args:
            question: 问题
            user_id: 用户ID
            knowledge_base_id: 知识库ID（可选）
            conversation_history: 对话历史（可选）
            
        Yields:
            Dict: 流式响应数据块
            
        Raises:
            Exception: 生成答案时发生错误
        """
        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info(f"用户 {user_id} 提问: {question}")
        
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 转换知识库ID从字符串到UUID（如果提供）
            # 计算或设置 `kb_id`，用于保存知识库唯一标识。
            kb_id = None
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            if knowledge_base_id:
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 计算或设置 `kb_id`，用于保存知识库唯一标识。
                    kb_id = UUID(knowledge_base_id)
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except (ValueError, TypeError):
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.warning(f"无效的知识库ID格式: {knowledge_base_id}")
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    pass  # 如果UUID无效则使用None
            
            # 解析对话历史
            # 计算或设置 `conv_history`，用于保存后续流程需要的中间结果。
            conv_history = conversation_history or []
            
            # 使用RAG服务生成流式答案
            # 开始组装多行调用参数、集合或对象内容。
            async for chunk in self.rag_service.ask_question_stream(
                # 计算或设置 `question`，用于保存用户当前问题。
                question=question,
                # 计算或设置 `user_id`，用于保存用户唯一标识。
                user_id=user_id,
                # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
                knowledge_base_id=kb_id,
                # 计算或设置 `conversation_history`，用于保存此前的多轮对话历史。
                conversation_history=conv_history,
                # 计算或设置 `context_limit`，用于保存后续流程需要的中间结果。
                context_limit=settings.CONTEXT_LIMIT
            # 结束上方多行参数、集合或对象的定义。
            ):
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield chunk
                
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"生成答案时发生错误: {str(e)}")
            # 开始组装多行调用参数、集合或对象内容。
            error_data = {
                # 补充当前字符串、提示词或响应正文内容。
                "type": "error",
                # 补充当前字符串、提示词或响应正文内容。
                "error": str(e)
            # 结束上方多行参数、集合或对象的定义。
            }
            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
            yield error_data

    # 定义 `get_documents()`，执行该函数对应的业务流程。
    async def get_documents(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: str,
        # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
        knowledge_base_id: Optional[str] = None,
        # 声明参数或字段 `skip`，表示分页跳过记录数。
        skip: int = 0,
        # 声明参数或字段 `limit`，表示本次最大返回数量。
        limit: int = 20
    # 结束上方多行参数、集合或对象的定义。
    ) -> Dict[str, Any]:
        """
        获取知识库中的文档列表（性能优化版本）
        
        Args:
            user_id: 用户ID
            knowledge_base_id: 知识库ID（可选）
            skip: 跳过的文档数量
            limit: 返回的文档数量限制
            
        Returns:
            Dict: 包含文档列表和总数的字典
            
        Raises:
            HTTPException: 知识库ID格式无效时抛出异常
            Exception: 获取文档失败时抛出异常
        """
        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info(f"用户 {user_id} 获取文档列表")
        
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 转换知识库ID从字符串到UUID（如果提供）
            # 计算或设置 `kb_id`，用于保存知识库唯一标识。
            kb_id = None
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            if knowledge_base_id:
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 计算或设置 `kb_id`，用于保存知识库唯一标识。
                    kb_id = UUID(knowledge_base_id)
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except (ValueError, TypeError):
                    # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                    raise HTTPException(
                        # 计算或设置 `status_code`，用于保存后续流程需要的中间结果。
                        status_code=status.HTTP_400_BAD_REQUEST,
                        # 计算或设置 `detail`，用于保存后续流程需要的中间结果。
                        detail="无效的知识库ID格式"
                    # 结束上方多行参数、集合或对象的定义。
                    )
            
            # 使用轻量级服务获取文档列表（无需LLM初始化）
            # 开始组装多行调用参数、集合或对象内容。
            documents = await self.lightweight_document_service.get_user_documents(
                # 计算或设置 `user_id`，用于保存用户唯一标识。
                user_id=user_id,
                # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
                knowledge_base_id=kb_id,
                # 计算或设置 `skip`，用于保存分页跳过记录数。
                skip=skip,
                # 计算或设置 `limit`，用于保存本次最大返回数量。
                limit=limit
            # 结束上方多行参数、集合或对象的定义。
            )
            
            # 将SQLAlchemy模型转换为字典以进行序列化
            # 计算或设置 `documents_data`，用于保存后续流程需要的中间结果。
            documents_data = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for doc in documents:
                # 开始组装多行调用参数、集合或对象内容。
                doc_dict = {
                    # 补充当前字符串、提示词或响应正文内容。
                    "id": str(doc.id),
                    # 补充当前字符串、提示词或响应正文内容。
                    "filename": doc.filename,
                    # 补充当前字符串、提示词或响应正文内容。
                    "original_filename": doc.original_filename,
                    # 补充当前字符串、提示词或响应正文内容。
                    "file_path": doc.file_path,
                    # 补充当前字符串、提示词或响应正文内容。
                    "file_size": doc.file_size,
                    # 补充当前字符串、提示词或响应正文内容。
                    "file_hash": doc.file_hash,
                    # 补充当前字符串、提示词或响应正文内容。
                    "mime_type": doc.mime_type,
                    # 补充当前字符串、提示词或响应正文内容。
                    "extracted_content": doc.extracted_content,
                    # 补充当前字符串、提示词或响应正文内容。
                    "category": doc.category,
                    # 补充当前字符串、提示词或响应正文内容。
                    "tags": doc.tags or [],
                    # 补充当前字符串、提示词或响应正文内容。
                    "knowledge_base_id": str(doc.knowledge_base_id) if doc.knowledge_base_id else None,
                    # 补充当前字符串、提示词或响应正文内容。
                    "user_id": str(doc.user_id),
                    # 补充当前字符串、提示词或响应正文内容。
                    "meta_data": doc.meta_data or {},
                    # 补充当前字符串、提示词或响应正文内容。
                    "created_at": doc.created_at.isoformat() if doc.created_at else None,
                    # 补充当前字符串、提示词或响应正文内容。
                    "updated_at": doc.updated_at.isoformat() if doc.updated_at else None
                # 结束上方多行参数、集合或对象的定义。
                }
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                documents_data.append(doc_dict)
            
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"获取到 {len(documents_data)} 个文档")
            # 结束当前函数，并把处理结果返回给调用方。
            return {
                # 补充当前字符串、提示词或响应正文内容。
                "documents": documents_data,
                # 补充当前字符串、提示词或响应正文内容。
                "total": len(documents_data)
            # 结束上方多行参数、集合或对象的定义。
            }
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except HTTPException:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise  # 重新抛出HTTP异常
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"获取文档列表时发生错误: {str(e)}")
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            raise Exception(f"获取文档列表错误: {str(e)}")
