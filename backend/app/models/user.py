"""
* 文件作用
  * 这是系统用户、角色和用户角色关联表的 SQLAlchemy ORM 模型。User 保存账号、密码哈希、基础角色和超级管理员标志；Role 保存可分配的角色名称；UserRoleAssociation 通过 users.id 与 role.id 建立多对多关系，并用联合唯一约束阻止重复分配。
* 类说明
  * `UserRole`：基础角色枚举，把管理员、HR 经理、HR 专员和员工限制为固定字符串值。
  * `User`：users 表 ORM 模型，保存登录账号、密码哈希、资料、基础角色和超级管理员状态，并关联会话、文档及多个 Role。
  * `Role`：role 表 ORM 模型，保存可分配的角色名称、说明和内置标志，并通过 user_role 关联多个用户。
  * `UserRoleAssociation`：user_role 多对多中间表，用 user_id 和 role_id 连接用户与角色，并禁止重复组合。
* 主要函数与方法说明
  * `User.__repr__()`：返回便于日志调试的用户摘要，只显示用户名和邮箱，不用于 API 响应。
"""

# 导入 Column, String, Boolean, DateTime, Text, Enum, ForeignKey, UniqueConstraint，用于SQLAlchemy 查询表达式。
from sqlalchemy import Column, String, Boolean, DateTime, Text, Enum, ForeignKey, UniqueConstraint
# 导入 relationship，用于使用 relationship 提供的类型或函数。
from sqlalchemy.orm import relationship
# 导入 enum，用于提供当前实现所需的标准库能力。
import enum

# 导入 BaseModel，用于查询或保存 BaseModel 对应的 ORM 数据。
from app.models.base import BaseModel


# 使用 `class` 定义 `UserRole`，并继承 `str, enum.Enum` 中的数据模型或 Mixin 能力；该类的业务作用是：基础角色枚举，把管理员、HR 经理、HR 专员和员工限制为固定字符串值。
class UserRole(str, enum.Enum):
    """用户角色枚举"""
    # 定义 `ADMIN` 枚举值 `"admin"`，表示管理员角色。
    ADMIN = "admin"
    # 定义 `HR_MANAGER` 枚举值 `"hr_manager"`，表示HR 经理角色。
    HR_MANAGER = "hr_manager"
    # 定义 `HR_SPECIALIST` 枚举值 `"hr_specialist"`，表示HR 专员角色。
    HR_SPECIALIST = "hr_specialist"
    # 定义 `EMPLOYEE` 枚举值 `"employee"`，表示普通员工角色。
    EMPLOYEE = "employee"


# 使用 `class` 定义 `User`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：users 表 ORM 模型，保存登录账号、密码哈希、资料、基础角色和超级管理员状态，并关联会话、文档及多个 Role。
class User(BaseModel):
    """用户模型：对应 users 表，存储用户账号信息"""

    # 设置 SQLAlchemy 表名为 "users"，该 ORM 类的查询和写入都映射到这张表。
    __tablename__ = "users"

    # 定义 SQLAlchemy ORM 字段 `username`，用于保存唯一登录用户名；unique=True 禁止重复值，nullable=False 禁止空值。
    username = Column(String(50), unique=True, index=True, nullable=False, comment="用户名")
    # 定义 SQLAlchemy ORM 字段 `email`，用于保存唯一邮箱地址；unique=True 禁止重复值，nullable=False 禁止空值。
    email = Column(String(255), unique=True, index=True, nullable=False, comment="邮箱地址")
    # 定义 SQLAlchemy ORM 字段 `full_name`，用于保存用户姓名。
    full_name = Column(String(255), nullable=True, comment="用户全名")
    # 定义 SQLAlchemy ORM 字段 `hashed_password`，用于保存密码哈希，登录时用于校验而不是还原明文；nullable=False 禁止空值。
    hashed_password = Column(String(255), nullable=False, comment="加密后的密码")

    # 定义 SQLAlchemy ORM 字段 `phone`，用于保存用户手机号。
    phone = Column(String(20), nullable=True, comment="手机号")
    # 定义 SQLAlchemy ORM 字段 `department`，用于保存用户所属部门。
    department = Column(String(100), nullable=True, comment="所属部门")
    # 定义 SQLAlchemy ORM 字段 `position`，用于保存用户岗位。
    position = Column(String(100), nullable=True, comment="职位")
    # 定义 SQLAlchemy ORM 字段 `employee_id`，用于保存企业员工编号；unique=True 禁止重复值。
    employee_id = Column(String(50), unique=True, nullable=True, comment="员工编号")

    # 定义 SQLAlchemy ORM 字段 `role`，用于保存用户基础角色枚举；nullable=False 禁止空值。
    role = Column(Enum(UserRole), default=UserRole.EMPLOYEE, nullable=False, comment="用户角色")
    # 定义 SQLAlchemy ORM 字段 `is_superuser`，用于保存超级管理员标志，系统管理接口据此放行；nullable=False 禁止空值。
    is_superuser = Column(Boolean, default=False, nullable=False, comment="是否为超级管理员")
    # 定义 SQLAlchemy ORM 字段 `is_verified`，用于保存账号验证状态；nullable=False 禁止空值。
    is_verified = Column(Boolean, default=False, nullable=False, comment="是否已验证")

    # 定义 SQLAlchemy ORM 字段 `bio`，用于保存用户个人简介。
    bio = Column(Text, nullable=True, comment="个人简介")
    # 定义 SQLAlchemy ORM 字段 `avatar_url`，用于保存用户头像地址。
    avatar_url = Column(String(500), nullable=True, comment="头像地址")
    # 定义 SQLAlchemy ORM 字段 `last_login`，用于保存最后登录时间。
    last_login = Column(DateTime, nullable=True, comment="最后登录时间")

    # 定义 SQLAlchemy `relationship()` 关系 `conversations`：一个用户拥有多条 Conversation；删除用户时 ORM 级联删除这些会话；cascade=all, delete-orphan 表示删除父记录时一并删除失去归属的子记录。
    conversations = relationship("Conversation", back_populates="user", cascade="all, delete-orphan")
    # 定义 SQLAlchemy `relationship()` 关系 `documents`：一个用户上传多份 Document；删除用户时 ORM 级联删除这些文档；cascade=all, delete-orphan 表示删除父记录时一并删除失去归属的子记录。
    documents = relationship("Document", back_populates="uploaded_by", cascade="all, delete-orphan")
    # 定义 SQLAlchemy `relationship()` 关系 `roles`：User 通过 user_role 中间表关联多个 Role。
    roles = relationship("Role", secondary="user_role", back_populates="users")

    # 使用 `def` 普通函数 定义 `__repr__()`。返回便于日志调试的用户摘要，只显示用户名和邮箱，不用于 API 响应。
    def __repr__(self):
        # 从 `User.__repr__()` 返回 `f"<User(username='{self.username}', email='{self.email}')>"`，并结束当前函数。
        return f"<User(username='{self.username}', email='{self.email}')>"

# 使用 `class` 定义 `Role`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：role 表 ORM 模型，保存可分配的角色名称、说明和内置标志，并通过 user_role 关联多个用户。
class Role(BaseModel):
    """Role 对应 role 表，表示系统角色"""

    # 设置 SQLAlchemy 表名为 "role"，该 ORM 类的查询和写入都映射到这张表。
    __tablename__ = "role"

    # 定义 SQLAlchemy ORM 字段 `name`，用于保存配置或角色名称；unique=True 禁止重复值，nullable=False 禁止空值。
    name = Column(String(50), unique=True, nullable=False, index=True, comment="角色名称")
    # 定义 SQLAlchemy ORM 字段 `description`，用于保存角色说明。
    description = Column(String(255), nullable=True, comment="角色描述")
    # 定义 SQLAlchemy ORM 字段 `is_builtin`，用于保存是否为系统内置、不可随意删除的角色；nullable=False 禁止空值。
    is_builtin = Column(Boolean, default=False, nullable=False, comment="是否为系统内置角色")

    # 定义 SQLAlchemy `relationship()` 关系 `users`：Role 通过 user_role 中间表反向关联多个 User。
    users = relationship("User", secondary="user_role", back_populates="roles")


# 使用 `class` 定义 `UserRoleAssociation`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：user_role 多对多中间表，用 user_id 和 role_id 连接用户与角色，并禁止重复组合。
class UserRoleAssociation(BaseModel):
    """用户User 和 角色Role的多对多中间表"""
    # 设置 SQLAlchemy 表名为 "user_role"，该 ORM 类的查询和写入都映射到这张表。
    __tablename__ = "user_role"

    # 定义 SQLAlchemy ORM 字段 `user_id`，用于保存关联 users.id 的用户外键；ForeignKey 关联 `users.id`，nullable=False 禁止空值。
    user_id = Column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True, comment="用户ID")
    # 定义 SQLAlchemy ORM 字段 `role_id`，用于保存关联 role.id 的角色外键；ForeignKey 关联 `role.id`，nullable=False 禁止空值。
    role_id = Column(ForeignKey("role.id", ondelete="CASCADE"), nullable=False, index=True, comment="角色ID")

    # 开始声明当前 ORM 表的表级约束；下面的联合唯一约束用于防止重复用户角色关系。
    __table_args__ = (
        # 使用 SQLAlchemy 联合唯一约束，禁止同一个 user_id 与 role_id 组合重复写入 user_role。
        UniqueConstraint("user_id", "role_id", name="uq_user_role_user_id_role_id"),
    # 结束 `UserRoleAssociation` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )
