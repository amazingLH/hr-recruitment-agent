"""
* 文件作用
  * 这是文档元数据的 SQLAlchemy ORM 模型文件，对应 PostgreSQL 的 documents 表，而文档正文分块及向量主要存放在 PGVector 使用的向量表中。该模型保存文档 id、文件名、磁盘路径、大小、MIME 类型、哈希值、处理状态、所属用户和 knowledge_base_id，并通过 relationship 连接 User 与 KnowledgeBase。它让业务层能够追踪上传文件的身份、归属和处理进度，也是删除文件、权限隔离、避免重复上传以及把检索结果还原为来源文档的重要依据。
* 类说明
  * `Document`：documents 表对应的 SQLAlchemy ORM 模型，保存文档 id、文件名、路径、大小、MIME 类型、哈希、处理状态、所属用户和 knowledge_base_id。它保存的是文档管理元数据；可检索的正文分块、Embedding 向量和 chunk metadata 由 PGVector 相关表保存。
* 函数与方法说明
  * `Document.__repr__()`：这是对象的开发者字符串表示方法，返回包含模型类型和关键标识的文本，方便日志输出与调试时识别 ORM 实例。
"""















"""
    用于文件管理和向量存储的文档模型
        用于存储上传文件的信息、解析内容、向量 embedding 和知识库归属
"""
# 从 `sqlalchemy` 导入当前模块需要的 `Column, String, Text, Integer, ForeignKey, JSON`。
from sqlalchemy import Column, String, Text, Integer, ForeignKey, JSON
# 从 `sqlalchemy.dialects.postgresql` 导入当前模块需要的 `UUID`。
from sqlalchemy.dialects.postgresql import UUID
# 从 `sqlalchemy.orm` 导入当前模块需要的 `relationship`。
from sqlalchemy.orm import relationship
# 从 `pgvector.sqlalchemy` 导入当前模块需要的 `Vector`。
from pgvector.sqlalchemy import Vector

# 从 `app.models.base` 导入当前模块需要的 `BaseModel`。
from app.models.base import BaseModel
# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings


# 定义 `Document` 类，集中封装相关数据或业务能力。
class Document(BaseModel):
    # 补充当前字符串、提示词或响应正文内容。
    """ 用于存储文件及其元数据的文档模型 """

    # 计算或设置 `__tablename__`，用于保存后续流程需要的中间结果。
    __tablename__ = "documents"

    # 基本信息
    filename = Column(String(255), nullable=False, comment="存储文件名")
    # 声明数据库字段 `original_filename`，并配置其类型、默认值或约束。
    original_filename = Column(String(255), nullable=False, comment="原始文件名")
    # 声明数据库字段 `file_path`，并配置其类型、默认值或约束。
    file_path = Column(String(500), nullable=False, comment="文件存储路径")
    # 声明数据库字段 `file_size`，并配置其类型、默认值或约束。
    file_size = Column(Integer, nullable=False, comment="文件大小，单位字节")
    # 声明数据库字段 `file_hash`，并配置其类型、默认值或约束。
    file_hash = Column(String(64), nullable=False, comment="文件SHA256哈希值")
    # 声明数据库字段 `mime_type`，并配置其类型、默认值或约束。
    mime_type = Column(String(100), nullable=False, comment="文件MIME类型")

    # 内容和元数据
    extracted_content = Column(Text, nullable=True, comment="文件解析后的文本内容")
    # 声明数据库字段 `meta_data`，并配置其类型、默认值或约束。
    meta_data = Column(JSON, nullable=True, comment="文档扩展元数据")

    # 用于语义搜索的向量嵌入
    #     表示文档的向量，用于语义搜索，依赖 PostgreSQL 的 pgvector。
    embedding = Column(Vector(settings.VECTOR_DIMENSION), nullable=True, comment="文档语义向量")

    # 分类
    category = Column(String(100), nullable=True, comment="文档分类")
    # 声明数据库字段 `tags`，并配置其类型、默认值或约束。
    tags = Column(JSON, nullable=True, comment="文档标签列表")

    # 关系： 文档由哪个用户上传
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, comment="上传用户ID")
    # 声明 ORM 关系 `uploaded_by`，用于在关联模型之间导航数据。
    uploaded_by = relationship("User", back_populates="documents")

    # 文档属于哪个知识库
    knowledge_base = relationship("KnowledgeBase", back_populates="documents")
    # 声明数据库字段 `knowledge_base_id`，并配置其类型、默认值或约束。
    knowledge_base_id = Column(UUID(as_uuid=True), ForeignKey("knowledge_bases.id"), nullable=True, comment="所属知识库ID")

    # 定义 `__repr__()`，执行该函数对应的业务流程。
    def __repr__(self):
        # 结束当前函数，并把处理结果返回给调用方。
        return f"<Document at {hex(id(self))}>"


