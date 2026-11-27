"""
* 文件作用
  * 这是所有 SQLAlchemy 业务模型共享的基础模型文件，通过声明式基类统一提供主键、创建时间、更新时间、启用状态和软删除标记，避免每张表重复定义公共字段。它还利用 declared_attr 按类名自动生成表名，并提供模型转字典、按字典批量更新等通用方法。理解本文件可以掌握 ORM 继承、Column 字段声明、类级动态属性和反射式字段遍历这些 Python 与 SQLAlchemy 技术。
* 类说明
  * `BaseModel`：所有业务 ORM 模型的公共父类，统一声明 UUID 主键、创建时间、更新时间、启用状态和软删除标记，并根据类名自动生成表名。to_dict() 用于把 ORM 字段转换为字典，update_from_dict() 只更新模型确实存在的属性，减少各模型重复代码。
* 函数与方法说明
  * `BaseModel.__tablename__()`：SQLAlchemy 的 declared_attr 动态表名方法，在每个 ORM 子类完成声明时读取类名，把 CamelCase 类名转换为小写复数表名并返回给 __tablename__；因此子模型可以继承 BaseModel 自动获得表名，也可以像 Document、Conversation 一样显式覆盖。
  * `BaseModel.to_dict()`：将模型实例转换为字典。
  * `BaseModel.update_from_dict()`：根据字典批量更新模型字段，并刷新 updated_at。
"""

"""
  这是所有数据库模型的公共基类。
"""
# 导入 `uuid`，供后续代码调用其功能。
import uuid
# 从 `datetime` 导入当前模块需要的 `datetime`。
from datetime import datetime
# 从 `typing` 导入当前模块需要的 `Any, Dict`。
from typing import Any, Dict
# 从 `sqlalchemy` 导入当前模块需要的 `Column, DateTime, String, Boolean`。
from sqlalchemy import Column, DateTime, String, Boolean
# 从 `sqlalchemy.dialects.postgresql` 导入当前模块需要的 `UUID`。
from sqlalchemy.dialects.postgresql import UUID
# 从 `sqlalchemy.ext.declarative` 导入当前模块需要的 `declared_attr`。
from sqlalchemy.ext.declarative import declared_attr

# 从 `app.core.database` 导入当前模块需要的 `Base`。
from app.core.database import Base


# 定义 `BaseModel` 类，集中封装相关数据或业务能力。
class BaseModel(Base):
    # 补充当前字符串、提示词或响应正文内容。
    """Base model 包括公共字段"""

    # 计算或设置 `__abstract__`，用于保存后续流程需要的中间结果。
    __abstract__ = True

    # 自动生成tablename
    # 应用装饰器，为下方类或函数增加框架行为。
    @declared_attr
    # 定义 `__tablename__()`，执行该函数对应的业务流程。
    def __tablename__(cls) -> str:
        # 结束当前函数，并把处理结果返回给调用方。
        return cls.__name__.lower()

    # 分别是主键、创建时间、更新时间、是否启用、创建人、更新人
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True, comment="主键ID")
    # 声明数据库字段 `created_at`，并配置其类型、默认值或约束。
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, comment="创建时间")
    # 声明数据库字段 `updated_at`，并配置其类型、默认值或约束。
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, comment="更新时间")
    # 声明数据库字段 `is_active`，并配置其类型、默认值或约束。
    is_active = Column(Boolean, default=True, nullable=False, comment="是否启用")
    # 声明数据库字段 `created_by`，并配置其类型、默认值或约束。
    created_by = Column(UUID(as_uuid=True), nullable=True, comment="创建人用户ID")
    # 声明数据库字段 `updated_by`，并配置其类型、默认值或约束。
    updated_by = Column(UUID(as_uuid=True), nullable=True, comment="更新人用户ID")

    # 定义 `to_dict()`，执行该函数对应的业务流程。
    def to_dict(self) -> Dict[str, Any]:
        # 补充当前字符串、提示词或响应正文内容。
        """将模型实例转换为字典"""
        # 结束当前函数，并把处理结果返回给调用方。
        return {
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            column.name: getattr(self, column.name)
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for column in self.__table__.columns
        # 结束上方多行参数、集合或对象的定义。
        }

    # 定义 `update_from_dict()`，执行该函数对应的业务流程。
    def update_from_dict(self, data: Dict[str, Any]) -> None:
        # 补充当前字符串、提示词或响应正文内容。
        """根据字典批量更新模型字段，并刷新 updated_at。"""
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for key, value in data.items():
            # 判断 `hasattr(self, key)` 是否成立，以选择正确的处理分支。
            if hasattr(self, key):
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                setattr(self, key, value)
        # 计算或设置 `self.updated_at`，用于保存后续流程需要的中间结果。
        self.updated_at = datetime.utcnow()
