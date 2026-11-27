"""
* 文件作用
  * 这是招聘邮箱配置与抓取日志的 SQLAlchemy ORM 模型。EmailConfig 保存 IMAP/SMTP、授权码、抓取频率、关键词和连接状态；EmailFetchLog 记录每次抓取的开始结束时间、发现邮件数、提取简历数及错误原因，两表通过 email_config_id 建立一对多关系。
* 类说明
  * `EmailConfig`：email_configs 表 ORM 模型，保存邮箱账号、IMAP/SMTP 服务器、授权码、抓取关键词、自动抓取设置、连接状态和创建者，并通过 logs 关系访问抓取日志。
  * `EmailFetchLog`：email_fetch_logs 表 ORM 模型，记录一次邮箱抓取的状态、数量、时间和错误。
  * `FetchedEmail`：fetched_emails 表 ORM 模型，保存命中关键词邮件的主题、发件人、收件人、正文、附件信息和处理状态。
* 主要函数与方法说明
  * `EmailConfig.__repr__()`：返回便于日志调试的邮箱配置摘要，内容包含邮箱地址和当前启用状态，不用于 API 响应或密码展示。
  * `EmailFetchLog.__repr__()`：返回抓取日志的简短调试文本，包含配置 ID、状态和发现邮件数。
"""

# 导入 Column, String, Integer, Boolean, Text, ForeignKey, DateTime，用于SQLAlchemy 查询表达式。
from sqlalchemy import Column, String, Integer, Boolean, Text, ForeignKey, DateTime, JSON, UniqueConstraint
# 导入 UUID，用于使用 UUID 提供的类型或函数。
from sqlalchemy.dialects.postgresql import UUID
# 导入 relationship，用于使用 relationship 提供的类型或函数。
from sqlalchemy.orm import relationship
# 导入 datetime，用于使用 datetime 提供的类型或函数。
from datetime import datetime

# 导入 BaseModel，用于查询或保存 BaseModel 对应的 ORM 数据。
from app.models.base import BaseModel


# 使用 `class` 定义 `EmailConfig`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：email_configs 表 ORM 模型，保存邮箱账号、IMAP/SMTP 服务器、授权码、抓取关键词、自动抓取设置、连接状态和创建者，并通过 logs 关系访问抓取日志。
class EmailConfig(BaseModel):
    """ 招聘邮箱配置 ： 用于配置系统从招聘邮箱抓取简历邮件"""

    # 设置 SQLAlchemy 表名为 "email_configs"，该 ORM 类的查询和写入都映射到这张表。
    __tablename__ = "email_configs"

    # 定义 SQLAlchemy ORM 字段 `name`，用于保存配置或角色名称；nullable=False 禁止空值。
    name = Column(String(255), nullable=False, comment="邮箱配置名称")
    # 定义 SQLAlchemy ORM 字段 `email`，用于保存唯一邮箱地址；unique=True 禁止重复值，nullable=False 禁止空值。
    email = Column(String(255), nullable=False, unique=True, comment="邮箱地址")

    # 定义 SQLAlchemy ORM 字段 `imap_server`，用于保存IMAP 收件服务器地址；nullable=False 禁止空值。
    imap_server = Column(String(255), nullable=False, comment="IMAP服务器地址")
    # 定义 SQLAlchemy ORM 字段 `imap_port`，用于保存IMAP 端口；nullable=False 禁止空值。
    imap_port = Column(Integer, nullable=False, default=993, comment="IMAP端口")
    # 定义 SQLAlchemy ORM 字段 `imap_ssl`，用于保存是否使用 IMAP SSL；nullable=False 禁止空值。
    imap_ssl = Column(Boolean, nullable=False, default=True, comment="IMAP是否启用SSL")

    # 定义 SQLAlchemy ORM 字段 `smtp_server`，用于保存SMTP 发件服务器地址。
    smtp_server = Column(String(255), nullable=True, comment="SMTP服务器地址")
    # 定义 SQLAlchemy ORM 字段 `smtp_port`，用于保存SMTP 端口。
    smtp_port = Column(Integer, nullable=True, default=587, comment="SMTP端口")
    # 定义 SQLAlchemy ORM 字段 `smtp_ssl`，用于保存是否使用 SMTP SSL；nullable=False 禁止空值。
    smtp_ssl = Column(Boolean, nullable=False, default=True, comment="SMTP是否启用SSL")

    # 定义 SQLAlchemy ORM 字段 `password`，用于保存邮箱客户端授权码；当前代码直接存储，生产环境应加密。
    password = Column(String(512), nullable=True, comment="邮箱授权码或密码")

    # 定义 SQLAlchemy ORM 字段 `fetch_interval`，用于保存自动抓取时间间隔（分钟）；nullable=False 禁止空值。
    fetch_interval = Column(Integer, nullable=False, default=30, comment="自动抓取间隔，单位分钟")
    # 定义 SQLAlchemy ORM 字段 `auto_fetch`，用于保存是否开启后台自动抓取；nullable=False 禁止空值。
    auto_fetch = Column(Boolean, nullable=False, default=False, comment="是否启用自动抓取")
    # 定义 SQLAlchemy ORM 字段 `subject_keywords`，用于保存逗号分隔的招聘邮件主题关键词。
    subject_keywords = Column(Text, nullable=True, comment="简历邮件主题过滤关键词")

    # 定义 SQLAlchemy ORM 字段 `status`，用于保存配置启用状态或抓取执行状态；nullable=False 禁止空值。
    status = Column(String(50), nullable=False, default="active", comment="邮箱配置状态")
    # 定义 SQLAlchemy ORM 字段 `connection_status`，用于保存最近一次邮箱连接状态；nullable=False 禁止空值。
    connection_status = Column(String(50), nullable=False, default="unknown", comment="邮箱连接状态")
    # 定义 SQLAlchemy ORM 字段 `last_fetch_at`，用于保存最近成功抓取时间。
    last_fetch_at = Column(DateTime, nullable=True, comment="最后抓取时间")

    # 定义 SQLAlchemy `relationship()` 关系 `logs`：一个邮箱配置拥有多条 EmailFetchLog；删除配置时 ORM 级联删除日志；cascade=all, delete-orphan 表示删除父记录时一并删除失去归属的子记录。
    logs = relationship("EmailFetchLog", back_populates="email_config", cascade="all, delete-orphan")
    # 一个邮箱配置可以抓取多封邮件；删除邮箱配置时，同时删除这些邮件展示记录。
    fetched_messages = relationship("FetchedEmail", back_populates="email_config", cascade="all, delete-orphan")

    # 使用 `def` 普通函数 定义 `__repr__()`。返回便于日志调试的邮箱配置摘要，内容包含邮箱地址和当前启用状态，不用于 API 响应或密码展示。
    def __repr__(self):
        # 从 `EmailConfig.__repr__()` 返回 `f"<EmailConfig(email='{self.email}', status='{self.status}', connection='{self.connection_status}...`，并结束当前函数。
        return f"<EmailConfig(email='{self.email}', status='{self.status}', connection='{self.connection_status}')>"


# 使用 `class` 定义 `EmailFetchLog`，并继承 `BaseModel` 中的数据模型或 Mixin 能力；该类的业务作用是：email_fetch_logs 表 ORM 模型，记录一次邮箱抓取的状态、数量、时间和错误。
class EmailFetchLog(BaseModel):
    """邮件抓取操作的日志： """

    # 设置 SQLAlchemy 表名为 "email_fetch_logs"，该 ORM 类的查询和写入都映射到这张表。
    __tablename__ = "email_fetch_logs"

    # 定义 SQLAlchemy ORM 字段 `status`，用于保存配置启用状态或抓取执行状态；nullable=False 禁止空值。
    status = Column(String(50), nullable=False, default="running", comment="抓取任务状态")
    # 定义 SQLAlchemy ORM 字段 `emails_found`，用于保存本次搜索发现的邮件数量；nullable=False 禁止空值。
    emails_found = Column(Integer, nullable=False, default=0, comment="发现的邮件数量")
    # 定义 SQLAlchemy ORM 字段 `resumes_extracted`，用于保存本次识别或保存的简历附件数量；nullable=False 禁止空值。
    resumes_extracted = Column(Integer, nullable=False, default=0, comment="提取的简历数量")
    # 定义 SQLAlchemy ORM 字段 `error_message`，用于保存抓取失败时记录的异常原因。
    error_message = Column(Text, nullable=True, comment="错误信息")

    # 定义 SQLAlchemy ORM 字段 `email_config_id`，用于保存关联 email_configs.id 的外键；ForeignKey 关联 `email_configs.id`，nullable=False 禁止空值。
    email_config_id = Column(UUID(as_uuid=True), ForeignKey("email_configs.id"), nullable=False, index=True, comment="邮箱配置ID")
    # 定义 SQLAlchemy `relationship()` 关系 `email_config`：每条抓取日志反向关联其所属 EmailConfig。
    email_config = relationship("EmailConfig", back_populates="logs")

    # 使用 `def` 普通函数 定义 `__repr__()`。返回抓取日志的简短调试文本，包含配置 ID、状态和发现邮件数。
    def __repr__(self):
        # 从 `EmailFetchLog.__repr__()` 返回 `f"<EmailFetchLog(config='{self.email_config_id}', status='{self.status}', found={self.emails_foun...`，并结束当前函数。
        return f"<EmailFetchLog(config='{self.email_config_id}', status='{self.status}', found={self.emails_found})>"


class FetchedEmail(BaseModel):
    """保存通过招聘邮箱抓取并命中主题关键词的邮件，供系统管理页面查询。"""

    __tablename__ = "fetched_emails"
    # 同一个邮箱配置中的同一个 IMAP 邮件编号只能保存一次，避免每分钟重复抓取产生重复行。
    __table_args__ = (
        UniqueConstraint("email_config_id", "mailbox_message_id", name="uq_fetched_email_config_message"),
    )

    # 邮箱服务器返回的邮件编号；它只在当前邮箱配置范围内唯一。
    mailbox_message_id = Column(String(255), nullable=False, comment="邮箱服务器中的邮件编号")
    # 保存用户需要在列表中查看的邮件头与正文信息。
    subject = Column(Text, nullable=True, comment="邮件主题")
    sender = Column(String(500), nullable=True, comment="发件人")
    recipients = Column(JSON, nullable=False, default=list, comment="收件人列表")
    sent_at = Column(DateTime, nullable=True, comment="邮件发送时间")
    body = Column(Text, nullable=True, comment="纯文本正文")
    html_body = Column(Text, nullable=True, comment="HTML正文")
    # 只保存附件名称和本地保存路径，不把附件二进制内容写进数据库。
    attachment_names = Column(JSON, nullable=False, default=list, comment="附件名称列表")
    attachment_paths = Column(JSON, nullable=False, default=list, comment="附件本地保存路径列表")
    attachment_count = Column(Integer, nullable=False, default=0, comment="附件数量")
    # matched 表示命中关键词，processed 表示附件已保存，partial/failed 表示处理未完全成功。
    processing_status = Column(String(50), nullable=False, default="matched", comment="邮件处理状态")
    error_message = Column(Text, nullable=True, comment="邮件处理失败原因")

    email_config_id = Column(
        UUID(as_uuid=True),
        ForeignKey("email_configs.id"),
        nullable=False,
        index=True,
        comment="所属邮箱配置ID",
    )
    email_config = relationship("EmailConfig", back_populates="fetched_messages")
