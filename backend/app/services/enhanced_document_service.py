"""
* 文件作用
  * 这是文档摄取与检索服务，智能培训主要调用 get_by_id() 按文档 ID 和用户 ID 读取 documents.extracted_content，作为试卷生成依据。完整服务还负责上传文件、提取正文、LLM 语义分块、长度约束、Embedding、PGVector 入库、语义检索和删除清理，因此它是知识库与试卷生成之间的文档数据桥梁。
* 类说明
  * `EnhancedDocumentService`：文档全生命周期服务类；智能培训使用它校验用户权限并读取文档完整正文，知识库模块还使用它完成分块、向量化和检索。
* 函数与方法说明
  * `EnhancedDocumentService.__init__()`：保存 AsyncSession，取得单例 Embedding 服务和递归文本切分器，初始化 LLM 服务、PGVector 连接地址与上传目录，为文档读取、分块、向量化和检索复用这些依赖。
  * `EnhancedDocumentService._split_by_semantic_points()`：按照 LLM 返回的原文分割点在 content 中依次定位并切片，只保留非空片段；最后补入末尾剩余正文，保证分块顺序与原文一致且不丢内容。
  * `EnhancedDocumentService.get_semantic_split_points()`：调用千问 Chat Completions 分析全文并返回语义分割点，temperature=0.2、max_tokens=1000；随后去重、在原文定位并处理重复片段，最终按原文索引排序。
  * `EnhancedDocumentService.get_semantic_split_points.ensure_unique()`：按原顺序去除 LLM 返回的空分割点和重复分割点，避免相同位置被重复切分。
  * `EnhancedDocumentService._force_split_long_chunk()`：将超过 1500 字符的块优先按段落、换行和句子边界拆分，仍找不到边界时按长度强制切开，保证块大小不超过限制。
  * `EnhancedDocumentService._merge_short_chunks()`：把短于 200 字符的片段与相邻片段合并，但合并后不超过 1500 字符，减少过碎上下文。
  * `EnhancedDocumentService._split_text()`：执行实际混合分块流程：LLM 找语义分割点、按分割点切块、强制拆分超过 1500 字符的块，最后按 200/1500 阈值合并短块。
  * `EnhancedDocumentService.upload_document()`：兼容旧调用方式，将上传文件、用户和知识库参数转交 upload_and_process_document() 创建文档记录。
  * `EnhancedDocumentService.upload_and_process_document()`：读取上传文件并计算 SHA-256，在同一用户和知识库内检查重复；保存原文件并写入 documents 元数据，但不立即解析和向量化。
  * `EnhancedDocumentService.process_document()`：读取已保存文件，提取并清洗完整正文，调用 _split_text() 分块并写入 PGVector，最后更新 documents.extracted_content 和处理状态；失败时记录错误状态。
  * `EnhancedDocumentService._create_document_chunks_with_pgvector()`：把每个正文块包装为 LangChain Document，写入 document_id、knowledge_base_id、user_id、chunk_index 和 filename 元数据，再由 Embedding 服务生成向量并保存到 langchain_pg_embedding。
  * `EnhancedDocumentService.semantic_search()`：连接用户专属 PGVector collection，通过查询向量执行相似度检索，并按 knowledge_base_id 等 metadata 过滤，返回相关正文块和分数。
  * `EnhancedDocumentService._fallback_text_search()`：向量检索失败时使用 PostgreSQL ILIKE 对 documents.extracted_content 做关键词匹配，作为降级搜索结果。
  * `EnhancedDocumentService.get_user_documents()`：按 user_id 查询 documents 表，可叠加 knowledge_base_id、分类和分页条件，返回当前用户有权查看的文档列表。
  * `EnhancedDocumentService.get_by_id()`：按 document_id 查询 documents 表；传入 user_id 时同时校验文档所有权，智能培训用它读取所选文档的 extracted_content。
  * `EnhancedDocumentService.get_document_chunks()`：先验证文档存在和用户权限，再从 langchain_pg_embedding 按 metadata.document_id 查询分块，并按 chunk_index 排序用于预览。
  * `EnhancedDocumentService.delete()`：删除文档时同时清理 langchain_pg_embedding 中的分块向量、documents 表记录和本地文件，失败时回滚事务，避免脏向量。
  * `EnhancedDocumentService._get_document_by_hash()`：根据 user_id、knowledge_base_id 和 SHA-256 文件哈希查询重复文档，防止同一知识库重复上传相同文件。
  * `EnhancedDocumentService._resolve_file_path()`：依次检查数据库路径、上传目录和文件名组合，返回当前机器实际存在的文件路径，兼容相对路径和运行目录变化。
  * `EnhancedDocumentService._get_mime_type()`：根据文件扩展名映射 PDF、DOCX、TXT、Markdown 的 MIME 类型，未知类型使用通用二进制类型。
  * `EnhancedDocumentService._save_temp_file()`：把上传流写入临时文件，供需要真实文件路径的 PDF 或 DOCX 解析库读取，并在后续流程中清理。
  * `EnhancedDocumentService._save_file()`：在 uploads 目录中生成安全且唯一的文件名，将上传内容永久写入磁盘并返回保存路径。
"""

"""
增强文档服务，集成LangChain进行文档处理和向量化：
    面向RAG和pgvector。支持语义切分、强制切分过长 chunk、合并过短 chunk、上传并处理文档、
    生成 chunk embedding、写入 LangChain/pgvector 表、语义搜索和文本 fallback 搜索。
    相比 document_service.py，它更偏知识库问答的文档索引流程
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 导入 `os`，供后续代码调用其功能。
import os
# 导入 `hashlib`，供后续代码调用其功能。
import hashlib
# 从 `pathlib` 导入当前模块需要的 `Path`。
from pathlib import Path
# 从 `typing` 导入当前模块需要的 `List, Optional, Dict, Any, BinaryIO`。
from typing import List, Optional, Dict, Any, BinaryIO
# 从 `uuid` 导入当前模块需要的 `UUID`。
from uuid import UUID
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy` 导入当前模块需要的 `select, update, delete, desc, func, text`。
from sqlalchemy import select, update, delete, desc, func, text
# 从 `sqlalchemy.orm` 导入当前模块需要的 `selectinload`。
from sqlalchemy.orm import selectinload

# LangChain imports
# 从 `langchain_core.documents` 导入当前模块需要的 `Document as LangChainDocument`。
from langchain_core.documents import Document as LangChainDocument
# 从 `langchain_postgres` 导入当前模块需要的 `PGVector`。
from langchain_postgres import PGVector

# Document processing imports
# 导入 `PyPDF2`，供后续代码调用其功能。
import PyPDF2
# 从 `docx` 导入当前模块需要的 `Document as DocxDocument`。
from docx import Document as DocxDocument
# 导入 `tempfile`，供后续代码调用其功能。
import tempfile

# 从 `app.models.document` 导入当前模块需要的 `Document`。
from app.models.document import Document
# 从 `app.models.knowledge_base` 导入当前模块需要的 `KnowledgeBase`。
from app.models.knowledge_base import KnowledgeBase
# 从 `app.services.llm_service` 导入当前模块需要的 `LLMService`。
from app.services.llm_service import LLMService
# 从 `app.services.embedding_service` 导入当前模块需要的 `get_embedding_service`。
from app.services.embedding_service import get_embedding_service
# 从 `app.schemas.document` 导入当前模块需要的 `DocumentCreate, DocumentUpdate`。
from app.schemas.document import DocumentCreate, DocumentUpdate
# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings
# 从 `app.utils.text_utils` 导入当前模块需要的 `extract_text_content`。
from app.utils.text_utils import extract_text_content
# 从 `app.services.lightweight_document_service` 导入当前模块需要的 `BaseDocumentService`。
from app.services.lightweight_document_service import BaseDocumentService

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `EnhancedDocumentService` 类，集中封装相关数据或业务能力。
class EnhancedDocumentService(BaseDocumentService):
    # 补充当前字符串、提示词或响应正文内容。
    """增强文档管理服务，集成LangChain功能"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 调用 `super()`，完成当前函数调用并取得其返回结果。
        super().__init__(db)
        # 计算或设置 `self.llm_service`，用于保存后续流程需要的中间结果。
        # 将 `LLMService()` 的结果保存到 `self.llm_service`，供后续流程使用。
        # 将 `LLMService()` 的结果保存到 `self.llm_service`，供后续流程使用。
        # 将 `LLMService()` 的结果保存到 `self.llm_service`，供后续流程使用。
        self.llm_service = LLMService()

        # 使用共享嵌入服务避免重复初始化
        # 计算或设置 `self.embedding_service`，用于保存后续流程需要的中间结果。
        # 将 `get_embedding_service()` 的结果保存到 `self.embedding_service`，供后续流程使用。
        # 将 `get_embedding_service()` 的结果保存到 `self.embedding_service`，供后续流程使用。
        # 将 `get_embedding_service()` 的结果保存到 `self.embedding_service`，供后续流程使用。
        self.embedding_service = get_embedding_service()
        # 计算或设置 `self.embeddings`，用于保存后续流程需要的中间结果。
        # 将 `self.embedding_service.get_embeddings()` 的结果保存到 `self.embeddings`，供后续流程使用。
        # 将 `self.embedding_service.get_embeddings()` 的结果保存到 `self.embeddings`，供后续流程使用。
        # 将 `self.embedding_service.get_embeddings()` 的结果保存到 `self.embeddings`，供后续流程使用。
        self.embeddings = self.embedding_service.get_embeddings()
        # 计算或设置 `self.text_splitter`，用于保存后续流程需要的中间结果。
        # 将 `self.embedding_service.get_text_splitter()` 的结果保存到 `self.text_splitter`，供后续流程使用。
        # 将 `self.embedding_service.get_text_splitter()` 的结果保存到 `self.text_splitter`，供后续流程使用。
        # 将 `self.embedding_service.get_text_splitter()` 的结果保存到 `self.text_splitter`，供后续流程使用。
        self.text_splitter = self.embedding_service.get_text_splitter()

        # PGVector数据库连接字符串（使用psycopg2进行同步连接）
        # 计算或设置 `self.connection_string`，用于保存后续流程需要的中间结果。
        # 将 `settings.DATABASE_URL` 的结果保存到 `self.connection_string`，供后续流程使用。
        # 将 `settings.DATABASE_URL` 的结果保存到 `self.connection_string`，供后续流程使用。
        # 将 `settings.DATABASE_URL` 的结果保存到 `self.connection_string`，供后续流程使用。
        self.connection_string = settings.DATABASE_URL

        # 记录当前执行状态或异常信息，便于开发和运行排查。
        # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
        logger.info("增强文档服务已使用共享嵌入服务初始化")

    # 定义 `_split_by_semantic_points()`，执行该函数对应的业务流程。
    def _split_by_semantic_points(self, text: str, split_points: List[str]) -> List[str]:
        # 补充当前字符串、提示词或响应正文内容。
        """根据语义分割点切分文本"""
        # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
        chunks = []
        # 计算或设置 `current_pos`，用于保存后续流程需要的中间结果。
        current_pos = 0

        # 按顺序查找每个分割点并切分文本
        # 遍历当前集合中的每一项，逐项完成相同处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for point in split_points:
            # 计算或设置 `pos`，用于保存后续流程需要的中间结果。
            pos = text.find(point, current_pos)
            # 判断 `pos != -1` 是否成立，以选择正确的处理分支。
            # 判断 `if pos != -1`，决定是否执行当前分支。
            # 判断 `if pos != -1`，决定是否执行当前分支。
            # 判断 `if pos != -1`，决定是否执行当前分支。
            if pos != -1:
                # 添加当前位置到分割点位置的文本块
                # 判断 `pos > current_pos` 是否成立，以选择正确的处理分支。
                # 判断 `if pos > current_pos`，决定是否执行当前分支。
                # 判断 `if pos > current_pos`，决定是否执行当前分支。
                # 判断 `if pos > current_pos`，决定是否执行当前分支。
                if pos > current_pos:
                    # 计算或设置 `chunk`，用于保存后续流程需要的中间结果。
                    chunk = text[current_pos:pos].strip()
                    # 判断 `chunk` 是否成立，以选择正确的处理分支。
                    # 判断 `if chunk`，决定是否执行当前分支。
                    # 判断 `if chunk`，决定是否执行当前分支。
                    # 判断 `if chunk`，决定是否执行当前分支。
                    if chunk:
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 调用 `chunks.append()`，保存或创建当前数据。
                        chunks.append(chunk)
                # 计算或设置 `current_pos`，用于保存后续流程需要的中间结果。
                current_pos = pos

        # 添加最后一个文本块
        # 判断 `current_pos < len(text)` 是否成立，以选择正确的处理分支。
        # 判断 `if current_pos < len(text)`，决定是否执行当前分支。
        # 判断 `if current_pos < len(text)`，决定是否执行当前分支。
        # 判断 `if current_pos < len(text)`，决定是否执行当前分支。
        if current_pos < len(text):
            # 计算或设置 `chunk`，用于保存后续流程需要的中间结果。
            chunk = text[current_pos:].strip()
            # 判断 `chunk` 是否成立，以选择正确的处理分支。
            # 判断 `if chunk`，决定是否执行当前分支。
            # 判断 `if chunk`，决定是否执行当前分支。
            # 判断 `if chunk`，决定是否执行当前分支。
            if chunk:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `chunks.append()`，保存或创建当前数据。
                chunks.append(chunk)

        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return chunks

    # 定义 `get_semantic_split_points()`，执行该函数对应的业务流程。
    async def get_semantic_split_points(self, content: str) -> List[str]:
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 通过聊天模型生成分割点，仅返回以 `~~` 分隔的分割点字符串
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `(` 的结果保存到 `system_prompt`，供后续流程使用。
            system_prompt = (
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "你是一个文档结构分析助手。只输出用于 split 的分割点字符串，"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "用`~~`分隔，不要输出任何其他文字。确保每个分割点在原文中唯一，"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "如果遇到重复标题或目录项，需要在分割点后追加少量后续字符形成唯一片段。"
            # 结束上方多行参数或数据结构。
            )
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `(` 的结果保存到 `user_prompt`，供后续流程使用。
            user_prompt = (
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "# 任务\n请分析文档，识别适合作为分割点的文本片段。\n\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "# 规则\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "1) 分割点应位于章节、各级标题、句子或段落的开头；\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "2) 分割后每段尽量保持在500至1000字，严禁>1500字；\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "3) 若存在重复片段（例如目录与正文相同标题），需在分割点后追加少量后续内容以确保唯一；\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "4) 仅输出分割点字符串，使用`~~`分隔，不要解释或添加其他文本。\n\n"
                # 补充当前字符串、提示词或响应正文内容。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                f"# 文档（截断）\n{content[:10000]}"
            # 结束上方多行参数或数据结构。
            )

            # 开始组装多行调用参数、集合或对象内容。
            # 将 `await self.llm_service.client.chat.completions.create(` 的结果保存到 `response`，供后续流程使用。
            response = await self.llm_service.client.chat.completions.create(
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=self.llm_service.llm_model,
                # 开始组装多行调用参数、集合或对象内容。
                # 将 `[` 的结果保存到 `messages`，供后续流程使用。
                messages=[
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 执行表达式 `{"role": "system", "content": system_prompt},`，完成本行明确的数据运算或控制操作。
                    {"role": "system", "content": system_prompt},
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 执行表达式 `{"role": "user", "content": user_prompt},`，完成本行明确的数据运算或控制操作。
                    {"role": "user", "content": user_prompt},
                # 结束上方多行参数或数据结构。
                ],
                # 计算或设置 `temperature`，用于保存后续流程需要的中间结果。
                temperature=0.2,
                # 计算或设置 `max_tokens`，用于保存后续流程需要的中间结果。
                max_tokens=1000,
            # 结束上方多行参数或数据结构。
            )
            # 计算或设置 `raw`，用于保存后续流程需要的中间结果。
            raw = response.choices[0].message.content or ""
            # 计算或设置 `points`，用于保存后续流程需要的中间结果。
            points = [p.strip() for p in raw.split("~~") if p.strip()]
            # 判断 `not points` 是否成立，以选择正确的处理分支。
            # 判断 `if not points`，决定是否执行当前分支。
            # 判断 `if not points`，决定是否执行当前分支。
            # 判断 `if not points`，决定是否执行当前分支。
            if not points:
                # 结束当前函数，并把处理结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return []

            # 去重并保持顺序
            # 计算或设置 `seen`，用于保存后续流程需要的中间结果。
            seen = set()
            # 计算或设置 `unique_points`，用于保存后续流程需要的中间结果。
            unique_points = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for p in points:
                # 判断 `p not in seen` 是否成立，以选择正确的处理分支。
                # 判断 `if p not in seen`，决定是否执行当前分支。
                # 判断 `if p not in seen`，决定是否执行当前分支。
                # 判断 `if p not in seen`，决定是否执行当前分支。
                if p not in seen:
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 调用 `seen.add()`，保存或创建当前数据。
                    seen.add(p)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 调用 `unique_points.append()`，保存或创建当前数据。
                    unique_points.append(p)

            # 保证每个分割点在正文中唯一：必要时追加后续字符
            # 定义 `ensure_unique()`，执行该函数对应的业务流程。
            def ensure_unique(point: str) -> str:
                # 计算或设置 `start`，用于保存后续流程需要的中间结果。
                start = content.find(point)
                # 判断 `start == -1` 是否成立，以选择正确的处理分支。
                # 判断 `if start == -1`，决定是否执行当前分支。
                # 判断 `if start == -1`，决定是否执行当前分支。
                # 判断 `if start == -1`，决定是否执行当前分支。
                if start == -1:
                    # 结束当前函数，并把处理结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return ""  # 模型输出不在原文中，丢弃
                
                # 统计出现次数
                # 计算或设置 `count`，用于保存后续流程需要的中间结果。
                count = 0
                # 计算或设置 `search_pos`，用于保存后续流程需要的中间结果。
                search_pos = 0
                # 在条件成立期间重复执行当前处理逻辑。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                while True:
                    # 计算或设置 `idx`，用于保存后续流程需要的中间结果。
                    idx = content.find(point, search_pos)
                    # 判断 `idx == -1` 是否成立，以选择正确的处理分支。
                    # 判断 `if idx == -1`，决定是否执行当前分支。
                    # 判断 `if idx == -1`，决定是否执行当前分支。
                    # 判断 `if idx == -1`，决定是否执行当前分支。
                    if idx == -1:
                        # 结束当前循环。
                        # 执行表达式 `break`，完成本行明确的数据运算或控制操作。
                        break
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 执行表达式 `count += 1`，完成本行明确的数据运算或控制操作。
                    count += 1
                    # 计算或设置 `search_pos`，用于保存后续流程需要的中间结果。
                    search_pos = idx + 1
                
                # 判断 `count <= 1` 是否成立，以选择正确的处理分支。
                # 判断 `if count <= 1`，决定是否执行当前分支。
                # 判断 `if count <= 1`，决定是否执行当前分支。
                # 判断 `if count <= 1`，决定是否执行当前分支。
                if count <= 1:
                    # 结束当前函数，并把处理结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return point
                
                # 重复：逐步扩展片段直到唯一或达到限制
                # 最多追加 100 个字符，步长 10
                # 计算或设置 `max_extra`，用于保存后续流程需要的中间结果。
                max_extra = 100
                # 计算或设置 `step`，用于保存后续流程需要的中间结果。
                step = 10
                # 计算或设置 `extra`，用于保存后续流程需要的中间结果。
                extra = 0
                # 在条件成立期间重复执行当前处理逻辑。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                while extra <= max_extra:
                    # 计算或设置 `candidate`，用于保存后续流程需要的中间结果。
                    candidate = content[start:start + len(point) + extra]
                    # 判断 `len(candidate) <= len(point):  # 避免空候选` 是否成立，以选择正确的处理分支。
                    # 判断 `if len(candidate) <= len(point):  # 避免空候选`，决定是否执行当前分支。
                    # 判断 `if len(candidate) <= len(point):  # 避免空候选`，决定是否执行当前分支。
                    # 判断 `if len(candidate) <= len(point):  # 避免空候选`，决定是否执行当前分支。
                    if len(candidate) <= len(point):  # 避免空候选
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 执行表达式 `extra += step`，完成本行明确的数据运算或控制操作。
                        extra += step
                        # 跳过当前项并继续处理下一项。
                        # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                        continue
                    
                    # 重新统计候选字符串的出现次数
                    # 计算或设置 `c`，用于保存后续流程需要的中间结果。
                    c = 0
                    # 计算或设置 `sp`，用于保存后续流程需要的中间结果。
                    sp = 0
                    # 在条件成立期间重复执行当前处理逻辑。
                    # 在条件成立时循环执行，直到达到结束条件。
                    # 在条件成立时循环执行，直到达到结束条件。
                    # 在条件成立时循环执行，直到达到结束条件。
                    while True:
                        # 计算或设置 `j`，用于保存后续流程需要的中间结果。
                        j = content.find(candidate, sp)
                        # 判断 `j == -1` 是否成立，以选择正确的处理分支。
                        # 判断 `if j == -1`，决定是否执行当前分支。
                        # 判断 `if j == -1`，决定是否执行当前分支。
                        # 判断 `if j == -1`，决定是否执行当前分支。
                        if j == -1:
                            # 结束当前循环。
                            # 执行表达式 `break`，完成本行明确的数据运算或控制操作。
                            break
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 执行表达式 `c += 1`，完成本行明确的数据运算或控制操作。
                        c += 1
                        # 计算或设置 `sp`，用于保存后续流程需要的中间结果。
                        sp = j + 1
                    
                    # 判断 `c <= 1` 是否成立，以选择正确的处理分支。
                    # 判断 `if c <= 1`，决定是否执行当前分支。
                    # 判断 `if c <= 1`，决定是否执行当前分支。
                    # 判断 `if c <= 1`，决定是否执行当前分支。
                    if c <= 1:
                        # 结束当前函数，并把处理结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        return candidate
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 执行表达式 `extra += step`，完成本行明确的数据运算或控制操作。
                    extra += step
                
                # 仍不唯一则返回原始（极少数情况），后续切分时按位置处理
                # 结束当前函数，并把处理结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return point

            # 声明 `adjusted_points_with_index` 参数（当前步骤使用的数据），类型为 `List[tuple[int`。
            adjusted_points_with_index: List[tuple[int, str]] = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for p in unique_points:
                # 计算或设置 `adj`，用于保存后续流程需要的中间结果。
                adj = ensure_unique(p)
                # 判断 `not adj` 是否成立，以选择正确的处理分支。
                # 判断 `if not adj`，决定是否执行当前分支。
                # 判断 `if not adj`，决定是否执行当前分支。
                # 判断 `if not adj`，决定是否执行当前分支。
                if not adj:
                    # 跳过当前项并继续处理下一项。
                    # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                    continue
                # 计算或设置 `idx`，用于保存后续流程需要的中间结果。
                idx = content.find(adj)
                # 判断 `idx != -1` 是否成立，以选择正确的处理分支。
                # 判断 `if idx != -1`，决定是否执行当前分支。
                # 判断 `if idx != -1`，决定是否执行当前分支。
                # 判断 `if idx != -1`，决定是否执行当前分支。
                if idx != -1:
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 调用 `adjusted_points_with_index.append()`，保存或创建当前数据。
                    adjusted_points_with_index.append((idx, adj))

            # 按在正文中的出现位置排序
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `adjusted_points_with_index.sort()`，完成当前函数调用并取得其返回结果。
            adjusted_points_with_index.sort(key=lambda x: x[0])
            # 计算或设置 `final_points`，用于保存后续流程需要的中间结果。
            final_points = [pt for _, pt in adjusted_points_with_index]
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return final_points
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"增强文档服务获取语义分割点时出错: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return []

    # 定义 `_force_split_long_chunk()`，执行该函数对应的业务流程。
    def _force_split_long_chunk(self, chunk: str) -> List[str]:
        # 补充当前字符串、提示词或响应正文内容。
        """强制分割超长段落（超过1500字符）"""
        # 计算或设置 `max_length`，用于保存后续流程需要的中间结果。
        max_length = 1500
        # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
        chunks = []

        # 先尝试按换行符分割
        # 判断 `if '\n' in chunk`，决定是否执行当前分支。
        # 判断 `if '\n' in chunk`，决定是否执行当前分支。
        # 判断 `if '\n' in chunk`，决定是否执行当前分支。
        if '\n' in chunk:
            # 计算或设置 `lines`，用于保存后续流程需要的中间结果。
            lines = chunk.split('\n')
            # 计算或设置 `current_chunk`，用于保存后续流程需要的中间结果。
            current_chunk = ""
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for line in lines:
                # 判断 `len(current_chunk) + len(line) + 1 > max_length` 是否成立，以选择正确的处理分支。
                # 判断 `if len(current_chunk) + len(line) + 1 > max_length`，决定是否执行当前分支。
                # 判断 `if len(current_chunk) + len(line) + 1 > max_length`，决定是否执行当前分支。
                # 判断 `if len(current_chunk) + len(line) + 1 > max_length`，决定是否执行当前分支。
                if len(current_chunk) + len(line) + 1 > max_length:
                    # 判断 `current_chunk` 是否成立，以选择正确的处理分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    if current_chunk:
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 调用 `chunks.append()`，保存或创建当前数据。
                        chunks.append(current_chunk)
                        # 计算或设置 `current_chunk`，用于保存后续流程需要的中间结果。
                        current_chunk = line
                    # 前面的判断条件均不成立时，执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    else:
                        # 单行就超过最大长度，需要递归分割
                        # 计算或设置 `line_chunks`，用于保存后续流程需要的中间结果。
                        line_chunks = self._force_split_long_chunk(line)
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 调用 `chunks.extend()`，完成当前函数调用并取得其返回结果。
                        chunks.extend(line_chunks)
                        # 计算或设置 `current_chunk`，用于保存后续流程需要的中间结果。
                        current_chunk = ""
                # 前面的判断条件均不成立时，执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                else:
                    # 判断 `current_chunk` 是否成立，以选择正确的处理分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    # 判断 `if current_chunk`，决定是否执行当前分支。
                    if current_chunk:
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 执行表达式 `current_chunk += "\n" + line`，完成本行明确的数据运算或控制操作。
                        current_chunk += "\n" + line
                    # 前面的判断条件均不成立时，执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    else:
                        # 计算或设置 `current_chunk`，用于保存后续流程需要的中间结果。
                        current_chunk = line
            # 判断 `current_chunk` 是否成立，以选择正确的处理分支。
            # 判断 `if current_chunk`，决定是否执行当前分支。
            # 判断 `if current_chunk`，决定是否执行当前分支。
            # 判断 `if current_chunk`，决定是否执行当前分支。
            if current_chunk:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `chunks.append()`，保存或创建当前数据。
                chunks.append(current_chunk)
        # 前面的判断条件均不成立时，执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 没有换行符则直接按长度分割
            # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
            chunks = [chunk[i:i + max_length] for i in range(0, len(chunk), max_length)]

        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return chunks

    # 定义 `_merge_short_chunks()`，执行该函数对应的业务流程。
    def _merge_short_chunks(self, chunks: List[str], min_length: int = 200, max_length: int = 1500) -> List[str]:
        # 补充当前字符串、提示词或响应正文内容。
        """合并过短片段，避免产生极短段落，同时不超过最大长度"""
        # 判断 `not chunks` 是否成立，以选择正确的处理分支。
        # 判断 `if not chunks`，决定是否执行当前分支。
        # 判断 `if not chunks`，决定是否执行当前分支。
        # 判断 `if not chunks`，决定是否执行当前分支。
        if not chunks:
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return []
        # 声明 `merged` 参数（当前步骤使用的数据），类型为 `List[str]`，默认值为 `[]`。
        merged: List[str] = []
        # 计算或设置 `i`，用于保存后续流程需要的中间结果。
        i = 0
        # 在条件成立期间重复执行当前处理逻辑。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        while i < len(chunks):
            # 计算或设置 `cur`，用于保存后续流程需要的中间结果。
            cur = chunks[i]
            
            # 如果当前片段过短且不是最后一个，尝试合并
            # 判断 `len(cur) < min_length and i + 1 < len(chunks)` 是否成立，以选择正确的处理分支。
            # 判断 `if len(cur) < min_length and i + 1 < len(chunks)`，决定是否执行当前分支。
            # 判断 `if len(cur) < min_length and i + 1 < len(chunks)`，决定是否执行当前分支。
            # 判断 `if len(cur) < min_length and i + 1 < len(chunks)`，决定是否执行当前分支。
            if len(cur) < min_length and i + 1 < len(chunks):
                # 尝试合并多个连续的短片段
                # 计算或设置 `merged_chunk`，用于保存后续流程需要的中间结果。
                merged_chunk = cur
                # 计算或设置 `j`，用于保存后续流程需要的中间结果。
                j = i + 1
                
                # 在条件成立期间重复执行当前处理逻辑。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                # 在条件成立时循环执行，直到达到结束条件。
                while j < len(chunks) and len(merged_chunk) < min_length:
                    # 计算或设置 `nxt`，用于保存后续流程需要的中间结果。
                    nxt = chunks[j]
                    # 添加适当的分隔符
                    # 将 `"\n" if not merged_chunk.endswith("\n") else ""` 的结果保存到 `separator`，供后续流程使用。
                    separator = "\n" if not merged_chunk.endswith("\n") else ""
                    # 计算或设置 `potential_chunk`，用于保存后续流程需要的中间结果。
                    potential_chunk = merged_chunk + separator + nxt
                    
                    # 判断 `len(potential_chunk) <= max_length` 是否成立，以选择正确的处理分支。
                    # 判断 `if len(potential_chunk) <= max_length`，决定是否执行当前分支。
                    # 判断 `if len(potential_chunk) <= max_length`，决定是否执行当前分支。
                    # 判断 `if len(potential_chunk) <= max_length`，决定是否执行当前分支。
                    if len(potential_chunk) <= max_length:
                        # 计算或设置 `merged_chunk`，用于保存后续流程需要的中间结果。
                        merged_chunk = potential_chunk
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 执行表达式 `j += 1`，完成本行明确的数据运算或控制操作。
                        j += 1
                    # 前面的判断条件均不成立时，执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    # 前面的条件不成立时执行当前备用分支。
                    else:
                        # 结束当前循环。
                        # 执行表达式 `break`，完成本行明确的数据运算或控制操作。
                        break
                
                # 如果成功合并了至少一个片段
                # 判断 `j > i + 1` 是否成立，以选择正确的处理分支。
                # 判断 `if j > i + 1`，决定是否执行当前分支。
                # 判断 `if j > i + 1`，决定是否执行当前分支。
                # 判断 `if j > i + 1`，决定是否执行当前分支。
                if j > i + 1:
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 调用 `merged.append()`，保存或创建当前数据。
                    merged.append(merged_chunk)
                    # 计算或设置 `i`，用于保存后续流程需要的中间结果。
                    i = j
                    # 跳过当前项并继续处理下一项。
                    # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                    continue
                # 前面的判断条件均不成立时，执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                else:
                    # 只合并下一个片段
                    # 计算或设置 `nxt`，用于保存后续流程需要的中间结果。
                    nxt = chunks[i + 1]
                    # 判断 `len(cur) + len(nxt) + 1 <= max_length` 是否成立，以选择正确的处理分支。
                    # 判断 `if len(cur) + len(nxt) + 1 <= max_length`，决定是否执行当前分支。
                    # 判断 `if len(cur) + len(nxt) + 1 <= max_length`，决定是否执行当前分支。
                    # 判断 `if len(cur) + len(nxt) + 1 <= max_length`，决定是否执行当前分支。
                    if len(cur) + len(nxt) + 1 <= max_length:
                        # 计算或设置 `separator`，用于保存后续流程需要的中间结果。
                        separator = "\n" if not cur.endswith("\n") else ""
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 调用 `merged.append()`，保存或创建当前数据。
                        merged.append(cur + separator + nxt)
                        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                        # 执行表达式 `i += 2`，完成本行明确的数据运算或控制操作。
                        i += 2
                        # 跳过当前项并继续处理下一项。
                        # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                        continue
            
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `merged.append()`，保存或创建当前数据。
            merged.append(cur)
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `i += 1`，完成本行明确的数据运算或控制操作。
            i += 1
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return merged

    # 定义 `_split_text()`，执行该函数对应的业务流程。
    async def _split_text(self, content: str) -> List[str]:
        # 补充当前字符串、提示词或响应正文内容。
        """主分割流程：使用 LLM 分割点 + 切分 + 长度约束"""
        # 1) 获取分割点（可能为空）
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `points`。
        points = await self.get_semantic_split_points(content)
        # 2) 根据分割点切分；若为空则整体作为一个片段
        # 判断 `points` 是否成立，以选择正确的处理分支。
        # 判断 `if points`，决定是否执行当前分支。
        # 判断 `if points`，决定是否执行当前分支。
        # 判断 `if points`，决定是否执行当前分支。
        if points:
            # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
            chunks = self._split_by_semantic_points(content, points)
        # 前面的判断条件均不成立时，执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 计算或设置 `chunks`，用于保存后续流程需要的中间结果。
            chunks = [content]
        # 3) 对超长片段进行强制分割（>1500）
        # 声明 `normalized` 参数（当前步骤使用的数据），类型为 `List[str]`，默认值为 `[]`。
        normalized: List[str] = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for ch in chunks:
            # 判断 `len(ch) > 1500` 是否成立，以选择正确的处理分支。
            # 判断 `if len(ch) > 1500`，决定是否执行当前分支。
            # 判断 `if len(ch) > 1500`，决定是否执行当前分支。
            # 判断 `if len(ch) > 1500`，决定是否执行当前分支。
            if len(ch) > 1500:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `normalized.extend()`，完成当前函数调用并取得其返回结果。
                normalized.extend(self._force_split_long_chunk(ch))
            # 前面的判断条件均不成立时，执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `normalized.append()`，保存或创建当前数据。
                normalized.append(ch)
        # 4) 合并过短片段（<200）
        # 计算或设置 `normalized`，用于保存后续流程需要的中间结果。
        normalized = self._merge_short_chunks(normalized, min_length=200, max_length=1500)
        # 5) 去除空白
        # 计算或设置 `normalized`，用于保存后续流程需要的中间结果。
        normalized = [c.strip() for c in normalized if c and c.strip()]
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return normalized

    # 定义 `upload_document()`，执行该函数对应的业务流程。
    async def upload_document(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `file,  # 上传文档对象`，完成本行明确的数据运算或控制操作。
        file,  # 上传文档对象
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `category` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        category: Optional[str] = None,
        # 声明 `tags` 参数（当前步骤使用的数据），类型为 `Optional[List[str]]`，默认值为 `None`。
        tags: Optional[List[str]] = None,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        knowledge_base_id: Optional[str] = None
    # 结束上方多行参数或数据结构。
    ) -> Document:
        # 补充当前字符串、提示词或响应正文内容。
        """上传并处理文档"""
        # 如果提供，将knowledge_base_id从字符串转换为UUID
        # 计算或设置 `kb_id`，用于保存知识库唯一标识。
        kb_id = None
        # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        if knowledge_base_id:
            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            try:
                # 计算或设置 `kb_id`，用于保存知识库唯一标识。
                kb_id = UUID(knowledge_base_id)
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            except (ValueError, TypeError):
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                logger.warning(f"无效的知识库ID格式: {knowledge_base_id}")
                # 计算或设置 `kb_id`，用于保存知识库唯一标识。
                kb_id = None

        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return await self.upload_and_process_document(
            # 计算或设置 `file`，用于保存上传文件对象。
            file=file.file,
            # 计算或设置 `filename`，用于保存文件名称。
            filename=file.filename,
            # 计算或设置 `user_id`，用于保存用户唯一标识。
            user_id=user_id,
            # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
            knowledge_base_id=kb_id,
            # 计算或设置 `category`，用于保存文档或知识库分类。
            category=category,
            # 计算或设置 `tags`，用于保存标签列表。
            tags=tags
        # 结束上方多行参数或数据结构。
        )

    # 定义 `upload_and_process_document()`，执行该函数对应的业务流程。
    async def upload_and_process_document(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `file` 参数（当前步骤使用的数据），类型为 `BinaryIO`。
        # 声明 `file` 参数（当前步骤使用的数据），类型为 `BinaryIO`。
        # 声明 `file` 参数（当前步骤使用的数据），类型为 `BinaryIO`。
        # 声明 `file` 参数（当前步骤使用的数据），类型为 `BinaryIO`。
        file: BinaryIO,
        # 声明 `filename` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `filename` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `filename` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `filename` 参数（当前步骤使用的数据），类型为 `str`。
        filename: str,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[UUID]`，默认值为 `None`。
        knowledge_base_id: Optional[UUID] = None,
        # 声明 `category` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        category: Optional[str] = None,
        # 声明 `tags` 参数（当前步骤使用的数据），类型为 `Optional[List[str]]`，默认值为 `None`。
        tags: Optional[List[str]] = None
    # 结束上方多行参数或数据结构。
    ) -> Document:
        # 补充当前字符串、提示词或响应正文内容。
        """上传文档（仅保存文件，不进行解析和向量化）"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 读取文件内容
            # 计算或设置 `file_content`，用于保存后续流程需要的中间结果。
            file_content = file.read()
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `file.seek()`，完成当前函数调用并取得其返回结果。
            file.seek(0)  # 重置文件指针

            # 生成文件哈希
            # 计算或设置 `file_hash`，用于保存后续流程需要的中间结果。
            file_hash = hashlib.sha256(file_content).hexdigest()

            # 检查文档是否已存在
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `await self._get_document_by_hash(` 的结果保存到 `existing_doc`，供后续流程使用。
            existing_doc = await self._get_document_by_hash(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 执行表达式 `file_hash,`，完成本行明确的数据运算或控制操作。
                file_hash,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 执行表达式 `user_id,`，完成本行明确的数据运算或控制操作。
                user_id,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 执行表达式 `knowledge_base_id,`，完成本行明确的数据运算或控制操作。
                knowledge_base_id,
            # 结束上方多行参数或数据结构。
            )
            # 判断 `existing_doc` 是否成立，以选择正确的处理分支。
            # 判断 `if existing_doc`，决定是否执行当前分支。
            # 判断 `if existing_doc`，决定是否执行当前分支。
            # 判断 `if existing_doc`，决定是否执行当前分支。
            if existing_doc:
                # 计算或设置 `existing_path`，用于保存后续流程需要的中间结果。
                existing_path = self._resolve_file_path(existing_doc.file_path)
                # 判断 `not existing_path.exists()` 是否成立，以选择正确的处理分支。
                # 判断 `if not existing_path.exists()`，决定是否执行当前分支。
                # 判断 `if not existing_path.exists()`，决定是否执行当前分支。
                # 判断 `if not existing_path.exists()`，决定是否执行当前分支。
                if not existing_path.exists():
                    # 异步调用下游方法，并把得到的文件在磁盘中的路径保存到 `existing_doc.file_path`。
                    # 将 `await self._save_file(file_content, filename, user_id)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    # 将 `await self._save_file(file_content, filename, user_id)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    # 将 `await self._save_file(file_content, filename, user_id)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    existing_doc.file_path = await self._save_file(file_content, filename, user_id)
                    # 计算或设置 `existing_doc.filename`，用于保存文件名称。
                    # 将 `filename` 的结果保存到 `existing_doc.filename`，供后续流程使用。
                    # 将 `filename` 的结果保存到 `existing_doc.filename`，供后续流程使用。
                    # 将 `filename` 的结果保存到 `existing_doc.filename`，供后续流程使用。
                    existing_doc.filename = filename
                    # 计算或设置 `existing_doc.original_filename`，用于保存后续流程需要的中间结果。
                    # 将 `filename` 的结果保存到 `existing_doc.original_filename`，供后续流程使用。
                    # 将 `filename` 的结果保存到 `existing_doc.original_filename`，供后续流程使用。
                    # 将 `filename` 的结果保存到 `existing_doc.original_filename`，供后续流程使用。
                    existing_doc.original_filename = filename
                    # 计算或设置 `existing_doc.file_size`，用于保存后续流程需要的中间结果。
                    # 将 `len(file_content)` 的结果保存到 `existing_doc.file_size`，供后续流程使用。
                    # 将 `len(file_content)` 的结果保存到 `existing_doc.file_size`，供后续流程使用。
                    # 将 `len(file_content)` 的结果保存到 `existing_doc.file_size`，供后续流程使用。
                    existing_doc.file_size = len(file_content)
                    # 计算或设置 `existing_doc.mime_type`，用于保存文件媒体类型。
                    # 将 `self._get_mime_type(filename)` 的结果保存到 `existing_doc.mime_type`，供后续流程使用。
                    # 将 `self._get_mime_type(filename)` 的结果保存到 `existing_doc.mime_type`，供后续流程使用。
                    # 将 `self._get_mime_type(filename)` 的结果保存到 `existing_doc.mime_type`，供后续流程使用。
                    existing_doc.mime_type = self._get_mime_type(filename)
                    # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    await self.db.commit()
                    # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    await self.db.refresh(existing_doc)
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
                    logger.info(f"已恢复缺失的文档文件: {existing_doc.id}")
                # 判断 `existing_doc.file_path != str(existing_path)` 是否成立，以选择正确的处理分支。
                # 判断 `elif existing_doc.file_path != str(existing_path)`，决定是否执行当前分支。
                # 判断 `elif existing_doc.file_path != str(existing_path)`，决定是否执行当前分支。
                # 判断 `elif existing_doc.file_path != str(existing_path)`，决定是否执行当前分支。
                elif existing_doc.file_path != str(existing_path):
                    # 计算或设置 `existing_doc.file_path`，用于保存文件在磁盘中的路径。
                    # 将 `str(existing_path)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    # 将 `str(existing_path)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    # 将 `str(existing_path)` 的结果保存到 `existing_doc.file_path`，供后续流程使用。
                    existing_doc.file_path = str(existing_path)
                    # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                    await self.db.commit()
                    # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
                    await self.db.refresh(existing_doc)
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
                logger.info(f"哈希为 {file_hash} 的文档在当前知识库中已存在")
                # 结束当前函数，并把处理结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return existing_doc

            # 确定MIME类型
            # 计算或设置 `mime_type`，用于保存文件媒体类型。
            mime_type = self._get_mime_type(filename)

            # 永久保存文件
            # 异步调用下游方法，并把得到的文件在磁盘中的路径保存到 `file_path`。
            file_path = await self._save_file(file_content, filename, user_id)

            # 创建文档记录（暂不提取内容和向量化）
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `Document(` 的结果保存到 `document`，供后续流程使用。
            document = Document(
                # 计算或设置 `filename`，用于保存文件名称。
                filename=filename,
                # 计算或设置 `original_filename`，用于保存后续流程需要的中间结果。
                original_filename=filename,
                # 计算或设置 `file_path`，用于保存文件在磁盘中的路径。
                file_path=file_path,
                # 计算或设置 `file_size`，用于保存后续流程需要的中间结果。
                file_size=len(file_content),
                # 计算或设置 `file_hash`，用于保存后续流程需要的中间结果。
                file_hash=file_hash,
                # 计算或设置 `mime_type`，用于保存文件媒体类型。
                mime_type=mime_type,
                # 计算或设置 `extracted_content`，用于保存后续流程需要的中间结果。
                extracted_content=None,
                # 计算或设置 `embedding`，用于保存文本对应的浮点向量。
                embedding=None,
                # 计算或设置 `category`，用于保存文档或知识库分类。
                category=category,
                # 计算或设置 `tags`，用于保存标签列表。
                tags=tags,
                # 计算或设置 `user_id`，用于保存用户唯一标识。
                user_id=user_id,
                # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
                knowledge_base_id=knowledge_base_id
            # 结束上方多行参数或数据结构。
            )

            # 调用当前对象的方法，推进本步骤的业务处理。
            # 调用 `self.db.add()`，保存或创建当前数据。
            self.db.add(document)
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()
            # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            await self.db.refresh(document)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"文档上传成功: {document.id}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return document

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"上传文档时出错: {e}")
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `process_document()`，执行该函数对应的业务流程。
    async def process_document(self, document_id: UUID) -> Document:
        # 补充当前字符串、提示词或响应正文内容。
        """处理已上传的文档：提取文本、分块、向量化"""
        # 开始组装多行调用参数、集合或对象内容。
        # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
        result = await self.db.execute(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `select()`，获取所需数据。
            select(Document).where(Document.id == document_id)
        # 结束上方多行参数或数据结构。
        )
        # 计算或设置 `document`，用于保存后续流程需要的中间结果。
        document = result.scalar_one_or_none()
        # 判断 `not document` 是否成立，以选择正确的处理分支。
        # 判断 `if not document`，决定是否执行当前分支。
        # 判断 `if not document`，决定是否执行当前分支。
        # 判断 `if not document`，决定是否执行当前分支。
        if not document:
            # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise ValueError(f"文档不存在: {document_id}")

        # 计算或设置 `temp_file_path`，用于保存后续流程需要的中间结果。
        temp_file_path = None
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 临时保存文件以进行文本提取
            # 计算或设置 `document_path`，用于保存后续流程需要的中间结果。
            document_path = self._resolve_file_path(document.file_path)
            # 判断 `not document_path.exists()` 是否成立，以选择正确的处理分支。
            # 判断 `if not document_path.exists()`，决定是否执行当前分支。
            # 判断 `if not document_path.exists()`，决定是否执行当前分支。
            # 判断 `if not document_path.exists()`，决定是否执行当前分支。
            if not document_path.exists():
                # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise FileNotFoundError(f"文档文件不存在: {document_path}")
            # 通过上下文管理器安全取得并在结束时释放当前资源。
            # 调用 `document_path.open()`，完成当前函数调用并取得其返回结果。
            with document_path.open('rb') as f:
                # 计算或设置 `file_content`，用于保存后续流程需要的中间结果。
                file_content = f.read()
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `temp_file_path`。
            temp_file_path = await self._save_temp_file(file_content, document.filename)

            # 提取文本内容
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `extracted_content`。
            extracted_content = await extract_text_content(temp_file_path, document.mime_type)
            # 计算或设置 `document.extracted_content`，用于保存后续流程需要的中间结果。
            # 将 `extracted_content` 的结果保存到 `document.extracted_content`，供后续流程使用。
            # 将 `extracted_content` 的结果保存到 `document.extracted_content`，供后续流程使用。
            # 将 `extracted_content` 的结果保存到 `document.extracted_content`，供后续流程使用。
            document.extracted_content = extracted_content

            # 判断 `extracted_content` 是否成立，以选择正确的处理分支。
            # 判断 `if extracted_content`，决定是否执行当前分支。
            # 判断 `if extracted_content`，决定是否执行当前分支。
            # 判断 `if extracted_content`，决定是否执行当前分支。
            if extracted_content:
                # 异步等待 `self._create_document_chunks_with_pgvector` 完成，避免阻塞其他并发请求。
                # 异步等待 `self._create_document_chunks_with_pgvector()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self._create_document_chunks_with_pgvector()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self._create_document_chunks_with_pgvector()` 完成，取得数据库或外部服务处理结果。
                await self._create_document_chunks_with_pgvector(document, extracted_content)

            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()
            # 异步等待 `self.db.refresh` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            await self.db.refresh(document)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"文档处理成功: {document.id}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return document

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"处理文档时出错: {e}")
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise
        # 进入最终清理阶段，无论前面成功或失败都会执行。
        # 执行表达式 `finally:`，完成本行明确的数据运算或控制操作。
        finally:
            # 判断 `temp_file_path and os.path.exists(temp_file_path)` 是否成立，以选择正确的处理分支。
            # 判断 `if temp_file_path and os.path.exists(temp_file_path)`，决定是否执行当前分支。
            # 判断 `if temp_file_path and os.path.exists(temp_file_path)`，决定是否执行当前分支。
            # 判断 `if temp_file_path and os.path.exists(temp_file_path)`，决定是否执行当前分支。
            if temp_file_path and os.path.exists(temp_file_path):
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `os.unlink()`，完成当前函数调用并取得其返回结果。
                os.unlink(temp_file_path)

    # 定义 `_create_document_chunks_with_pgvector()`，执行该函数对应的业务流程。
    async def _create_document_chunks_with_pgvector(self, document: Document, content: str) -> None:
        # 补充当前字符串、提示词或响应正文内容。
        """使用PGVector为嵌入向量创建文档块"""
        # text_chunks = self.get_semantic_split_points(content)
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `text_chunks`。
        text_chunks = await self._split_text(content)
        # 判断 `not text_chunks` 是否成立，以选择正确的处理分支。
        # 判断 `if not text_chunks`，决定是否执行当前分支。
        # 判断 `if not text_chunks`，决定是否执行当前分支。
        # 判断 `if not text_chunks`，决定是否执行当前分支。
        if not text_chunks:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
            logger.warning(f"未为文档 {document.id} 提供文本块")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return

        # 创建带有元数据的LangChain文档（内容块）
        # 将 `f"document_chunks_{document.user_id}".replace("-", "_")` 的结果保存到 `chunks_collection`，供后续流程使用。
        chunks_collection = f"document_chunks_{document.user_id}".replace("-", "_")
        
        # 计算或设置 `langchain_docs`，用于保存后续流程需要的中间结果。
        langchain_docs = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for i, chunk_text in enumerate(text_chunks):
            # 直接在创建时包含所有必要的元数据
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `LangChainDocument(` 的结果保存到 `doc`，供后续流程使用。
            doc = LangChainDocument(
                # 计算或设置 `page_content`，用于保存后续流程需要的中间结果。
                page_content=chunk_text,
                # 开始组装多行调用参数、集合或对象内容。
                # 将 `{` 的结果保存到 `metadata`，供后续流程使用。
                metadata={
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `document_id` 字段，值来自 `str(document.id)`。
                    "document_id": str(document.id),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `knowledge_base_id` 字段，值来自 `str(document.knowledge_base_id)`。
                    "knowledge_base_id": str(document.knowledge_base_id),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `chunk_index` 字段，值来自 `i`。
                    "chunk_index": i,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `chunk_size` 字段，值来自 `len(chunk_text)`。
                    "chunk_size": len(chunk_text),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `filename` 字段，值来自 `document.filename`。
                    "filename": document.filename,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `category` 字段，值来自 `document.category or "general"`。
                    "category": document.category or "general",
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `file_path` 字段，值来自 `document.file_path`。
                    "file_path": document.file_path,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `mime_type` 字段，值来自 `document.mime_type`。
                    "mime_type": document.mime_type,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `source_type` 字段，值来自 `"content"`。
                    "source_type": "content",
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `collection_name` 字段，值来自 `chunks_collection`。
                    "collection_name": chunks_collection
                # 结束上方多行参数或数据结构。
                }
            # 结束上方多行参数或数据结构。
            )
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `langchain_docs.append()`，保存或创建当前数据。
            langchain_docs.append(doc)

        # 获取向量存储
        # 开始组装多行调用参数、集合或对象内容。
        # 将 `PGVector(` 的结果保存到 `vector_store`，供后续流程使用。
        vector_store = PGVector(
            # 计算或设置 `connection`，用于保存后续流程需要的中间结果。
            connection=self.connection_string,
            # 计算或设置 `embeddings`，用于保存后续流程需要的中间结果。
            embeddings=self.embeddings,
            # 计算或设置 `collection_name`，用于保存后续流程需要的中间结果。
            collection_name=chunks_collection,
            # 计算或设置 `use_jsonb`，用于保存后续流程需要的中间结果。
            use_jsonb=True
        # 结束上方多行参数或数据结构。
        )

        existing_result = await self.db.execute(text("""
            SELECT id
            FROM langchain_pg_embedding
            WHERE cmetadata->>'document_id' = :document_id
        """), {"document_id": str(document.id)})
        # 计算或设置 `existing_ids`，用于保存后续流程需要的中间结果。
        existing_ids = [str(row.id) for row in existing_result.fetchall()]
        # 判断 `existing_ids` 是否成立，以选择正确的处理分支。
        # 判断 `if existing_ids`，决定是否执行当前分支。
        # 判断 `if existing_ids`，决定是否执行当前分支。
        # 判断 `if existing_ids`，决定是否执行当前分支。
        if existing_ids:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `vector_store.delete()`，删除目标数据。
            vector_store.delete(ids=existing_ids)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info("已清理文档 %s 的 %s 个旧分块", document.id, len(existing_ids))

        # 开始组装多行调用参数、集合或对象内容。
        # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
        logger.info(
            # 补充当前字符串、提示词或响应正文内容。
            # 调用 `len()`，完成当前函数调用并取得其返回结果。
            f"正在向PGVector集合 {chunks_collection} 添加 {len(langchain_docs)} 个文档块"
        # 结束上方多行参数或数据结构。
        )

        # 将文档添加到向量存储（同步操作）
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `vector_store.add_documents()`，保存或创建当前数据。
            vector_store.add_documents(langchain_docs)
            # 开始组装多行调用参数、集合或对象内容。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(
                # 补充当前字符串、提示词或响应正文内容。
                # 调用 `len()`，完成当前函数调用并取得其返回结果。
                f"成功向PGVector集合 {chunks_collection} 添加了 {len(langchain_docs)} 个文档块"
            # 结束上方多行参数或数据结构。
            )
            # 开始组装多行调用参数、集合或对象内容。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(
                # 补充当前字符串、提示词或响应正文内容。
                # 调用 `len()`，完成当前函数调用并取得其返回结果。
                f"为文档 {document.id} 创建了 {len(text_chunks)} 个块"
            # 结束上方多行参数或数据结构。
            )
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"向PGVector添加文档时出错: {e}")
            # 不在这里处理数据库事务，让调用方处理
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `semantic_search()`，执行该函数对应的业务流程。
    async def semantic_search(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        query: str,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `limit` 参数（分页返回上限），类型为 `int`，默认值为 `10`。
        limit: int = 10,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[UUID]`，默认值为 `None`。
        knowledge_base_id: Optional[UUID] = None,
        # 声明 `category` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        category: Optional[str] = None,
        # 声明 `similarity_threshold` 参数（当前步骤使用的数据），类型为 `float`，默认值为 `0.8`。
        similarity_threshold: float = 0.8
    # 结束上方多行参数或数据结构。
    ) -> List[Dict[str, Any]]:
        # 补充当前字符串、提示词或响应正文内容。
        """使用PGVector执行语义搜索"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 计算或设置 `collection_name`，用于保存后续流程需要的中间结果。
            collection_name = f"document_chunks_{user_id}".replace("-", "_")

            # 开始组装多行调用参数、集合或对象内容。
            # 将 `PGVector(` 的结果保存到 `vector_store`，供后续流程使用。
            vector_store = PGVector(
                # 计算或设置 `connection`，用于保存后续流程需要的中间结果。
                connection=self.connection_string,
                # 计算或设置 `embeddings`，用于保存后续流程需要的中间结果。
                embeddings=self.embeddings,
                # 计算或设置 `collection_name`，用于保存后续流程需要的中间结果。
                collection_name=collection_name,
                # 计算或设置 `use_jsonb`，用于保存后续流程需要的中间结果。
                use_jsonb=True
            # 结束上方多行参数或数据结构。
            )

            # 计算或设置 `filter_conditions`，用于保存后续流程需要的中间结果。
            filter_conditions = {}
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            if knowledge_base_id:
                # 计算或设置 `filter_conditions["knowledge_base_id"]`，用于保存后续流程需要的中间结果。
                # 将 `str(knowledge_base_id)` 的结果保存到 `filter_conditions["knowledge_base_id"]`，供后续流程使用。
                # 将 `str(knowledge_base_id)` 的结果保存到 `filter_conditions["knowledge_base_id"]`，供后续流程使用。
                # 将 `str(knowledge_base_id)` 的结果保存到 `filter_conditions["knowledge_base_id"]`，供后续流程使用。
                filter_conditions["knowledge_base_id"] = str(knowledge_base_id)
            # 判断 `category` 是否成立，以选择正确的处理分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            if category:
                # 计算或设置 `filter_conditions["category"]`，用于保存后续流程需要的中间结果。
                # 将 `category` 的结果保存到 `filter_conditions["category"]`，供后续流程使用。
                # 将 `category` 的结果保存到 `filter_conditions["category"]`，供后续流程使用。
                # 将 `category` 的结果保存到 `filter_conditions["category"]`，供后续流程使用。
                filter_conditions["category"] = category

            # 开始组装多行调用参数、集合或对象内容。
            # 将 `vector_store.similarity_search_with_score(` 的结果保存到 `results`，供后续流程使用。
            results = vector_store.similarity_search_with_score(
                # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
                query=query,
                # 计算或设置 `k`，用于保存后续流程需要的中间结果。
                k=limit,
                # 计算或设置 `filter`，用于保存后续流程需要的中间结果。
                filter=filter_conditions if filter_conditions else None
            # 结束上方多行参数或数据结构。
            )
            
            # 格式化结果
            # 计算或设置 `search_results`，用于保存后续流程需要的中间结果。
            search_results = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for doc, score in results:
                # 判断 `score <= similarity_threshold` 是否成立，以选择正确的处理分支。
                # 判断 `if score <= similarity_threshold`，决定是否执行当前分支。
                # 判断 `if score <= similarity_threshold`，决定是否执行当前分支。
                # 判断 `if score <= similarity_threshold`，决定是否执行当前分支。
                if score <= similarity_threshold:
                    # 开始组装多行调用参数、集合或对象内容。
                    # 调用 `search_results.append()`，保存或创建当前数据。
                    search_results.append({
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `document_id` 字段，值来自 `doc.metadata.get("document_id")`。
                        "document_id": doc.metadata.get("document_id"),
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `filename` 字段，值来自 `doc.metadata.get("filename")`。
                        "filename": doc.metadata.get("filename"),
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `content` 字段，值来自 `doc.page_content`。
                        "content": doc.page_content,
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `chunk_index` 字段，值来自 `doc.metadata.get("chunk_index")`。
                        "chunk_index": doc.metadata.get("chunk_index"),
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `category` 字段，值来自 `doc.metadata.get("category")`。
                        "category": doc.metadata.get("category"),
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `similarity` 字段，值来自 `float(score)`。
                        "similarity": float(score),
                        # 补充当前字符串、提示词或响应正文内容。
                        # 设置 `metadata` 字段，值来自 `doc.metadata`。
                        "metadata": doc.metadata
                    # 结束上方多行参数或数据结构。
                    })
            
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return search_results
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"语义搜索时出错: {e}")
            # 回退到文本搜索
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return await self._fallback_text_search(query, user_id, limit, knowledge_base_id, category)

    # 定义 `_fallback_text_search()`，执行该函数对应的业务流程。
    async def _fallback_text_search(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        query: str,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `limit` 参数（分页返回上限），类型为 `int`。
        # 声明 `limit` 参数（分页返回上限），类型为 `int`。
        # 声明 `limit` 参数（分页返回上限），类型为 `int`。
        # 声明 `limit` 参数（分页返回上限），类型为 `int`。
        limit: int,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[UUID]`，默认值为 `None`。
        knowledge_base_id: Optional[UUID] = None,
        # 声明 `category` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        category: Optional[str] = None
    # 结束上方多行参数或数据结构。
    ) -> List[Dict[str, Any]]:
        # 补充当前字符串、提示词或响应正文内容。
        """向量搜索失败时的回退文本搜索"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `select()`，获取所需数据。
            base_query = select(Document).where(Document.user_id == user_id)
            
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            if knowledge_base_id:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `base_query.where()`，完成当前函数调用并取得其返回结果。
                base_query = base_query.where(Document.knowledge_base_id == knowledge_base_id)
            
            # 判断 `category` 是否成立，以选择正确的处理分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            if category:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `base_query.where()`，完成当前函数调用并取得其返回结果。
                base_query = base_query.where(Document.category == category)
            
            # 简单文本搜索
            # 开始组装多行调用参数、集合或对象内容。
            # 将 `base_query.where(` 的结果保存到 `text_query`，供后续流程使用。
            text_query = base_query.where(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `Document.extracted_content.ilike()`，完成当前函数调用并取得其返回结果。
                Document.extracted_content.ilike(f"%{query}%")
            # 调用 `limit()`，完成当前函数调用并取得其返回结果。
            ).limit(limit)
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(text_query)
            # 计算或设置 `documents`，用于保存候选文档或文档列表。
            documents = result.scalars().all()
            
            # 格式化结果
            # 计算或设置 `search_results`，用于保存后续流程需要的中间结果。
            search_results = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for doc in documents:
                # 开始组装多行调用参数、集合或对象内容。
                # 调用 `search_results.append()`，保存或创建当前数据。
                search_results.append({
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `document_id` 字段，值来自 `str(doc.id)`。
                    "document_id": str(doc.id),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `filename` 字段，值来自 `doc.filename`。
                    "filename": doc.filename,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `content` 字段，值来自 `doc.extracted_content[:500] if doc.extracted_content else ""`。
                    "content": doc.extracted_content[:500] if doc.extracted_content else "",
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `category` 字段，值来自 `doc.category`。
                    "category": doc.category,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `similarity` 字段，值来自 `0.5,  # 文本搜索的默认相似度`。
                    "similarity": 0.5,  # 文本搜索的默认相似度
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `created_at` 字段，值来自 `doc.created_at.isoformat()`。
                    "created_at": doc.created_at.isoformat()
                # 结束上方多行参数或数据结构。
                })
            
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return search_results
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"回退文本搜索时出错: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return []

    # 定义 `get_user_documents()`，执行该函数对应的业务流程。
    async def get_user_documents(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `skip` 参数（分页跳过数量），类型为 `int`，默认值为 `0`。
        skip: int = 0,
        # 声明 `limit` 参数（分页返回上限），类型为 `int`，默认值为 `100`。
        limit: int = 100,
        # 声明 `category` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        category: Optional[str] = None,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[UUID]`，默认值为 `None`。
        knowledge_base_id: Optional[UUID] = None
    # 结束上方多行参数或数据结构。
    ) -> List[Document]:
        # 补充当前字符串、提示词或响应正文内容。
        """获取用户文档，支持可选过滤"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `select()`，获取所需数据。
            query = select(Document).where(Document.user_id == user_id)
            
            # 判断 `category` 是否成立，以选择正确的处理分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            # 判断 `if category`，决定是否执行当前分支。
            if category:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
                query = query.where(Document.category == category)
            
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            # 判断 `if knowledge_base_id`，决定是否执行当前分支。
            if knowledge_base_id:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
                query = query.where(Document.knowledge_base_id == knowledge_base_id)
            
            # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
            query = query.offset(skip).limit(limit).order_by(desc(Document.created_at))
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return result.scalars().all()
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取用户文档时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `get_by_id()`，执行该函数对应的业务流程。
    async def get_by_id(self, document_id: str, user_id: Optional[UUID] = None) -> Optional[Document]:
        # 补充当前字符串、提示词或响应正文内容。
        """根据ID获取文档，可选择验证用户权限"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 将字符串ID转换为UUID
            # 计算或设置 `doc_uuid`，用于保存后续流程需要的中间结果。
            doc_uuid = UUID(document_id)
            
            # 构建查询
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `select()`，获取所需数据。
            query = select(Document).where(Document.id == doc_uuid)
            # 判断 `user_id` 是否成立，以选择正确的处理分支。
            # 判断 `if user_id`，决定是否执行当前分支。
            # 判断 `if user_id`，决定是否执行当前分支。
            # 判断 `if user_id`，决定是否执行当前分支。
            if user_id:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
                query = query.where(Document.user_id == user_id)
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query)
            # 计算或设置 `document`，用于保存后续流程需要的中间结果。
            document = result.scalar_one_or_none()
            
            # 添加日志记录文档状态
            # 判断 `document` 是否成立，以选择正确的处理分支。
            # 判断 `if document`，决定是否执行当前分支。
            # 判断 `if document`，决定是否执行当前分支。
            # 判断 `if document`，决定是否执行当前分支。
            if document:
                # 计算或设置 `content_length`，用于保存后续流程需要的中间结果。
                content_length = len(document.extracted_content) if document.extracted_content else 0
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
                logger.info(f"找到文档 {document_id}，内容长度: {content_length}")
            # 前面的判断条件均不成立时，执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 判断 `user_id` 是否成立，以选择正确的处理分支。
                # 判断 `if user_id`，决定是否执行当前分支。
                # 判断 `if user_id`，决定是否执行当前分支。
                # 判断 `if user_id`，决定是否执行当前分支。
                if user_id:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                    logger.warning(f"为用户 {user_id} 未找到文档 {document_id}")
                # 前面的判断条件均不成立时，执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                else:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                    logger.warning(f"未找到文档 {document_id}")

            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return document

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except (ValueError, TypeError) as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"无效的文档ID格式 {document_id}: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取文档 {document_id} 时出错: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

    # 定义 `get_document_chunks()`，执行该函数对应的业务流程。
    async def get_document_chunks(self, document_id: str, user_id: UUID) -> List[Dict[str, Any]]:
        # 补充当前字符串、提示词或响应正文内容。
        """从langchain_pg_embedding表获取文档块用于预览"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 首先检查文档是否存在且用户有权限
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `document`。
            document = await self.get_by_id(document_id)
            # 判断 `not document` 是否成立，以选择正确的处理分支。
            # 判断 `if not document`，决定是否执行当前分支。
            # 判断 `if not document`，决定是否执行当前分支。
            # 判断 `if not document`，决定是否执行当前分支。
            if not document:
                # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("文档未找到")
            
            # 判断 `document.user_id != user_id` 是否成立，以选择正确的处理分支。
            # 判断 `if document.user_id != user_id`，决定是否执行当前分支。
            # 判断 `if document.user_id != user_id`，决定是否执行当前分支。
            # 判断 `if document.user_id != user_id`，决定是否执行当前分支。
            if document.user_id != user_id:
                # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("权限被拒绝")
            
            # 直接查询langchain_pg_embedding表
            query = text("""
                SELECT 
                    id,
                    document,
                    cmetadata
                FROM langchain_pg_embedding 
                WHERE cmetadata->>'document_id' = :document_id
                ORDER BY CAST(cmetadata->>'chunk_index' AS INTEGER)
            """)
            
            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
            result = await self.db.execute(query, {"document_id": str(document_id)})
            # 计算或设置 `rows`，用于保存后续流程需要的中间结果。
            rows = result.fetchall()
            
            # 格式化块用于响应
            # 计算或设置 `formatted_chunks`，用于保存后续流程需要的中间结果。
            formatted_chunks = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for row in rows:
                # 计算或设置 `metadata`，用于保存后续流程需要的中间结果。
                metadata = row.cmetadata if row.cmetadata else {}
                # 开始组装多行调用参数、集合或对象内容。
                # 调用 `formatted_chunks.append()`，保存或创建当前数据。
                formatted_chunks.append({
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `id` 字段，值来自 `str(row.id)`。
                    "id": str(row.id),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `content` 字段，值来自 `row.document`。
                    "content": row.document,
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `chunk_index` 字段，值来自 `metadata.get("chunk_index", 0)`。
                    "chunk_index": metadata.get("chunk_index", 0),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `chunk_size` 字段，值来自 `metadata.get("chunk_size", len(row.document) if row.document else 0)`。
                    "chunk_size": metadata.get("chunk_size", len(row.document) if row.document else 0),
                    # 补充当前字符串、提示词或响应正文内容。
                    # 设置 `metadata` 字段，值来自 `metadata`。
                    "metadata": metadata
                # 结束上方多行参数或数据结构。
                })
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return formatted_chunks
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取文档块时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `delete()`，执行该函数对应的业务流程。
    async def delete(self, document: Document) -> None:
        # 补充当前字符串、提示词或响应正文内容。
        """从langchain_pg_embedding删除文档及其块"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 从langchain_pg_embedding表删除文档块
            delete_query = text("""
                DELETE FROM langchain_pg_embedding 
                WHERE cmetadata->>'document_id' = :document_id
            """)
            # 异步等待 `self.db.execute` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.execute()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.execute()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.execute()` 完成，取得数据库或外部服务处理结果。
            await self.db.execute(delete_query, {"document_id": str(document.id)})
            
            # 删除文档
            # 异步等待 `self.db.delete` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            await self.db.delete(document)
            # 异步等待 `self.db.commit` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()
            
            # 如果文件存在则清理
            # 判断 `document.file_path` 是否成立，以选择正确的处理分支。
            # 判断 `if document.file_path`，决定是否执行当前分支。
            # 判断 `if document.file_path`，决定是否执行当前分支。
            # 判断 `if document.file_path`，决定是否执行当前分支。
            if document.file_path:
                # 计算或设置 `document_path`，用于保存后续流程需要的中间结果。
                document_path = self._resolve_file_path(document.file_path)
                # 判断 `document_path.exists()` 是否成立，以选择正确的处理分支。
                # 判断 `if document_path.exists()`，决定是否执行当前分支。
                # 判断 `if document_path.exists()`，决定是否执行当前分支。
                # 判断 `if document_path.exists()`，决定是否执行当前分支。
                if document_path.exists():
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    # 调用 `document_path.unlink()`，完成当前函数调用并取得其返回结果。
                    document_path.unlink()
                
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"已删除文档 {document.id} 及其嵌入向量")
            
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"删除文档时出错: {e}")
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `_get_document_by_hash()`，执行该函数对应的业务流程。
    async def _get_document_by_hash(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `file_hash` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `file_hash` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `file_hash` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `file_hash` 参数（当前步骤使用的数据），类型为 `str`。
        file_hash: str,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `UUID`。
        user_id: UUID,
        # 声明 `knowledge_base_id` 参数（当前步骤使用的数据），类型为 `Optional[UUID]`，默认值为 `None`。
        knowledge_base_id: Optional[UUID] = None,
    # 结束上方多行参数或数据结构。
    ) -> Optional[Document]:
        # 补充当前字符串、提示词或响应正文内容。
        """根据用户、知识库和文件哈希获取文档。"""
        # 开始组装多行调用参数、集合或对象内容。
        # 将 `select(Document).where(` 的结果保存到 `query`，供后续流程使用。
        query = select(Document).where(
            # 设置调用参数 `Document.file_hash`，把对应业务数据传给下游方法。
            # 执行表达式 `Document.file_hash == file_hash,`，完成本行明确的数据运算或控制操作。
            Document.file_hash == file_hash,
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `Document.user_id == user_id`，完成本行明确的数据运算或控制操作。
            Document.user_id == user_id
        # 结束上方多行参数或数据结构。
        )
        # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        # 判断 `if knowledge_base_id`，决定是否执行当前分支。
        if knowledge_base_id:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
            query = query.where(Document.knowledge_base_id == knowledge_base_id)
        # 前面的判断条件均不成立时，执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
            query = query.where(Document.knowledge_base_id.is_(None))
        # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `result`。
        result = await self.db.execute(query)
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result.scalar_one_or_none()

    # 定义 `_resolve_file_path()`，执行该函数对应的业务流程。
    def _resolve_file_path(self, file_path: str) -> Path:
        # 计算或设置 `path`，用于保存后续流程需要的中间结果。
        path = Path(file_path)
        # 判断 `path.is_absolute()` 是否成立，以选择正确的处理分支。
        # 判断 `if path.is_absolute()`，决定是否执行当前分支。
        # 判断 `if path.is_absolute()`，决定是否执行当前分支。
        # 判断 `if path.is_absolute()`，决定是否执行当前分支。
        if path.is_absolute():
            # 结束当前函数，并把处理结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return path
        # 计算或设置 `backend_root`，用于保存后续流程需要的中间结果。
        backend_root = Path(__file__).resolve().parents[2]
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return (backend_root / path).resolve()

    # 定义 `_get_mime_type()`，执行该函数对应的业务流程。
    def _get_mime_type(self, filename: str) -> str:
        # 补充当前字符串、提示词或响应正文内容。
        """根据文件名确定MIME类型"""
        # 计算或设置 `extension`，用于保存后续流程需要的中间结果。
        extension = filename.lower().split('.')[-1] if '.' in filename else ''
        # 开始组装多行调用参数、集合或对象内容。
        # 将 `{` 的结果保存到 `mime_types`，供后续流程使用。
        mime_types = {
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `pdf` 字段，值来自 `'application/pdf'`。
            'pdf': 'application/pdf',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `doc` 字段，值来自 `'application/msword'`。
            'doc': 'application/msword',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `docx` 字段，值来自 `'application/vnd.openxmlformats-officedocument.wordprocessingml.document'`。
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `txt` 字段，值来自 `'text/plain'`。
            'txt': 'text/plain',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `md` 字段，值来自 `'text/markdown'`。
            'md': 'text/markdown',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `xlsx` 字段，值来自 `'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'`。
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            # 补充当前字符串、提示词或响应正文内容。
            # 设置 `xls` 字段，值来自 `'application/vnd.ms-excel'`。
            'xls': 'application/vnd.ms-excel'
        # 结束上方多行参数或数据结构。
        }
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return mime_types.get(extension, 'application/octet-stream')

    # 定义 `_save_temp_file()`，执行该函数对应的业务流程。
    async def _save_temp_file(self, content: bytes, filename: str) -> str:
        # 补充当前字符串、提示词或响应正文内容。
        """临时保存文件以进行处理"""
        # 计算或设置 `temp_dir`，用于保存后续流程需要的中间结果。
        temp_dir = tempfile.gettempdir()
        # 计算或设置 `safe_filename`，用于保存后续流程需要的中间结果。
        safe_filename = Path(filename).name
        # 计算或设置 `temp_file_path`，用于保存后续流程需要的中间结果。
        temp_file_path = os.path.join(temp_dir, f"temp_{safe_filename}")
        
        # 通过上下文管理器安全取得并在结束时释放当前资源。
        # 调用 `open()`，完成当前函数调用并取得其返回结果。
        with open(temp_file_path, 'wb') as f:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `f.write()`，完成当前函数调用并取得其返回结果。
            f.write(content)
        
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return temp_file_path

    # 定义 `_save_file()`，执行该函数对应的业务流程。
    async def _save_file(self, content: bytes, filename: str, user_id: UUID) -> str:
        # 补充当前字符串、提示词或响应正文内容。
        """永久保存文件"""
        # 创建用户特定的上传目录
        # 计算或设置 `configured_upload_dir`，用于保存后续流程需要的中间结果。
        configured_upload_dir = Path(settings.UPLOAD_DIR)
        # 判断 `not configured_upload_dir.is_absolute()` 是否成立，以选择正确的处理分支。
        # 判断 `if not configured_upload_dir.is_absolute()`，决定是否执行当前分支。
        # 判断 `if not configured_upload_dir.is_absolute()`，决定是否执行当前分支。
        # 判断 `if not configured_upload_dir.is_absolute()`，决定是否执行当前分支。
        if not configured_upload_dir.is_absolute():
            # 计算或设置 `configured_upload_dir`，用于保存后续流程需要的中间结果。
            configured_upload_dir = Path(__file__).resolve().parents[2] / configured_upload_dir
        # 计算或设置 `upload_dir`，用于保存后续流程需要的中间结果。
        upload_dir = configured_upload_dir.resolve() / str(user_id)
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 调用 `upload_dir.mkdir()`，完成当前函数调用并取得其返回结果。
        upload_dir.mkdir(parents=True, exist_ok=True)
        # 计算或设置 `safe_filename`，用于保存后续流程需要的中间结果。
        safe_filename = Path(filename).name
        
        # 生成唯一文件名
        # 计算或设置 `file_path`，用于保存文件在磁盘中的路径。
        file_path = upload_dir / safe_filename
        # 计算或设置 `counter`，用于保存后续流程需要的中间结果。
        counter = 1
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        # 调用 `os.path.splitext()`，完成当前函数调用并取得其返回结果。
        base_name, extension = os.path.splitext(safe_filename)
        
        # 在条件成立期间重复执行当前处理逻辑。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        while file_path.exists():
            # 计算或设置 `new_filename`，用于保存后续流程需要的中间结果。
            new_filename = f"{base_name}_{counter}{extension}"
            # 计算或设置 `file_path`，用于保存文件在磁盘中的路径。
            file_path = upload_dir / new_filename
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 执行表达式 `counter += 1`，完成本行明确的数据运算或控制操作。
            counter += 1
        
        # 保存文件
        # 调用 `file_path.open()`，完成当前函数调用并取得其返回结果。
        with file_path.open('wb') as f:
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            # 调用 `f.write()`，完成当前函数调用并取得其返回结果。
            f.write(content)
        
        # 结束当前函数，并把处理结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return str(file_path)
