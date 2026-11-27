"""
* 文件作用
  * 这是知识库的 SQLAlchemy ORM 模型文件，用 Python 类把 PostgreSQL 中 knowledge_bases 表映射为可操作对象。模型声明知识库主键 id、名称、描述、公开状态、创建者外键和文档数量字段，并通过 relationship 建立创建者与文档的对象关系；接口和 Service 查询到的 KnowledgeBase 实例最终都来源于这里。学习重点是区分 knowledge_bases.id（知识库自身 ID）、documents.id（文档自身 ID）与 documents.knowledge_base_id（文档保存的知识库外键），并理解 Column、ForeignKey、relationship 和继承 BaseModel 的作用。
* 类说明
  * `KnowledgeBase`：knowledge_bases 表对应的 SQLAlchemy ORM 模型，定义知识库自身的 id、名称、描述、公开状态、创建者和文档数量，并声明与 User、Document 的对象关系。knowledge_bases.id 是知识库主键，documents.knowledge_base_id 通过外键引用它，用于表示每个文档属于哪个知识库。
* 函数与方法说明
  * `KnowledgeBase.__repr__()`：这是对象的开发者字符串表示方法，返回包含模型类型和关键标识的文本，方便日志输出与调试时识别 ORM 实例。
"""















"""
    知识库：
        用于组织文档和信息的知识库模型
"""
# 从 `sqlalchemy` 导入当前模块需要的 `Column, String, Text, JSON, Integer, Boolean`。
from sqlalchemy import Column, String, Text, JSON, Integer, Boolean
# 从 `sqlalchemy.orm` 导入当前模块需要的 `relationship`。
from sqlalchemy.orm import relationship

# 从 `app.models.base` 导入当前模块需要的 `BaseModel`。
from app.models.base import BaseModel


# 定义 `KnowledgeBase` 类，集中封装相关数据或业务能力。
class KnowledgeBase(BaseModel):
    # 补充当前字符串、提示词或响应正文内容。
    """ 知识库模型 ： 对应 knowledge_bases 表，用于组织文档"""

    # 计算或设置 `__tablename__`，用于保存后续流程需要的中间结果。
    __tablename__ = "knowledge_bases"

    # 基本信息
    name = Column(String(255), nullable=False, unique=True, comment="知识库名称")
    # 声明数据库字段 `description`，并配置其类型、默认值或约束。
    description = Column(Text, nullable=True, comment="知识库描述")

    # 配置
    is_public = Column(Boolean, default=False, nullable=False, comment="是否公开")
    # 声明数据库字段 `is_searchable`，并配置其类型、默认值或约束。
    is_searchable = Column(Boolean, default=True, nullable=False, comment="是否允许搜索")

    # 元数据
    meta_data = Column(JSON, nullable=True, comment="知识库扩展元数据")
    # 声明数据库字段 `document_count`，并配置其类型、默认值或约束。
    document_count = Column(Integer, default=0, nullable=False, comment="文档数量")

    # 分类
    category = Column(String(100), nullable=True, comment="知识库分类")
    # 声明数据库字段 `tags`，并配置其类型、默认值或约束。
    tags = Column(JSON, nullable=True, comment="知识库标签列表")

    # 关系：表示一个知识库下可以有多个文档
    documents = relationship("Document", back_populates="knowledge_base")

    # 定义 `__repr__()`，执行该函数对应的业务流程。
    def __repr__(self):
        # 结束当前函数，并把处理结果返回给调用方。
        return f"<KnowledgeBase(name='{self.name}', category='{self.category}')>"
