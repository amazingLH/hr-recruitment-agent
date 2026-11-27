"""
* 文件作用
  * 这是邮箱配置、收件抓取和 Agent 发信的业务服务层。EmailConfigService 用 SQLAlchemy 管理 email_configs 并测试 IMAP；EmailFetchService 读取收件箱、按主题筛选并保存简历附件，随后触发自动评价；EmailSendService 从邮件 Skill 的 config.txt 读取发件配置，经人工确认后使用 SMTP 发送。
* 类说明
  * `SkillMailConfig`：Agent 邮件 Skill 的 SMTP 发件配置数据对象，保存发件地址、授权码、服务器、端口和 SSL 标志。
  * `EmailConfigService`：邮箱配置服务，使用 AsyncSession 对 email_configs 执行增删改查，并调用 EmailReader 测试 IMAP 登录。
  * `EmailFetchService`：收件抓取服务，连接 IMAP、过滤邮件、保存简历附件、触发简历自动评价并维护抓取日志。
  * `EmailSendService`：Agent 邮件发送服务，从 Skill 配置文件读取 SMTP 参数并提交已由用户确认的邮件。
* 主要函数与方法说明
  * `_email_config_model()`：延迟导入 EmailConfig ORM 模型，避免服务模块加载时产生循环依赖。
  * `_email_fetch_log_model()`：延迟导入 EmailFetchLog ORM 模型，避免循环依赖。
  * `_resume_evaluation_model()`：延迟导入 ResumeEvaluation ORM 模型，用于抓取时检查邮件是否已经评价。
  * `_resume_evaluation_service()`：延迟导入 ResumeEvaluationService，用于下载简历后触发自动评价。
  * `EmailConfigService.__init__()`：保存请求范围内的 AsyncSession，后续配置 CRUD 和状态更新共用该数据库事务。
  * `EmailConfigService.list()`：按创建时间倒序分页查询 email_configs。
  * `EmailConfigService.get()`：按配置 UUID 查询单条 email_configs 记录。
  * `EmailConfigService.create()`：把 Pydantic 创建模型转换为 EmailConfig ORM 对象，写入连接参数、授权码和抓取设置后提交事务。
  * `EmailConfigService.update()`：只更新请求实际提供的字段，忽略空密码以保护旧授权码，然后提交并刷新 ORM 对象。
  * `EmailConfigService.delete()`：通过 ORM 删除邮箱配置并提交；relationship 级联会同时删除其抓取日志。
  * `EmailConfigService.test_connection()`：使用配置中的 IMAP 地址、账号及临时或已保存授权码创建 EmailReader，登录后更新 connection_status。
  * `EmailFetchService.__init__()`：保存 AsyncSession，用于写入抓取日志、更新邮箱状态和查询简历评价。
  * `EmailFetchService.manual_fetch()`：连接 IMAP，扫描最近最多 50 封邮件并统计 PDF/DOC/DOCX 附件；该方法只统计，不保存附件或自动评价。
  * `EmailFetchService.list_logs()`：按邮箱配置 ID 分页查询抓取日志并按创建时间倒序返回。
  * `EmailFetchService.list_fetched_emails()`：分页查询已经命中关键词并持久化的邮件，供邮件管理页面显示。
  * `EmailFetchService._upsert_fetched_email()`：按邮箱配置 ID 和 IMAP 邮件编号新增或更新邮件，避免定时任务重复写入。
  * `EmailFetchService.fetch_recent_attachments()`：读取最近 limit 封邮件，按主题关键词过滤，去重后把附件保存到用户目录，并调用 ResumeEvaluationService 自动评价。
  * `EmailSendService.__init__()`：保存数据库会话并定位 backend/skills/hr-agent-email/config.txt；当前发信配置不读取 email_configs 表。
  * `EmailSendService.send_agent_email()`：检查收件人、主题和正文，读取 Skill SMTP 配置，提交邮件并把发件人和投递状态返回给 Agent。
  * `EmailSendService._load_skill_mail_config()`：逐行解析 config.txt，校验必填 SMTP 字段、端口和 SSL 标志，构造 SkillMailConfig。
  * `EmailSendService._send_via_smtp()`：构造标准 EmailMessage；SSL 模式使用 SMTP_SSL，非 SSL 模式尝试 STARTTLS，登录后调用 send_message。
"""

# 导入 annotations，用于启用延迟解析类型注解，避免前向类型引用在导入阶段报错。
from __future__ import annotations

# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
邮件配置、抓取和发送服务：
    EmailConfigService 管理招聘邮箱配置的增删改查和连接测试；
    EmailFetchService 抓取最近邮件附件、记录抓取日志，并可触发简历评价；
    EmailSendService 用 SMTP 发送 Agent 生成的邮件。
    文件顶部的 _email_config_model() 等函数是延迟导入，减少循环依赖。
"""
# 导入 smtplib，用于提供当前实现所需的标准库能力。
import smtplib
# 导入 logging，用于记录 Agent 规划、工具执行和降级异常。
import logging
# 导入 dataclass，用于定义只保存数据、不承担业务执行的内部结构。
from dataclasses import dataclass
# 导入 datetime，用于使用 datetime 提供的类型或函数。
from datetime import datetime
# 导入 TYPE_CHECKING, List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import TYPE_CHECKING, List, Optional
# 导入 EmailMessage as SMTPMessage，用于使用 EmailMessage as SMTPMessage 提供的类型或函数。
from email.message import EmailMessage as SMTPMessage
# 导入 AsyncSession，用于异步数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession
# 导入 select, UUID，用于SQLAlchemy 查询表达式。
from sqlalchemy import select, UUID, func

# 导入 EmailConfig as ReaderConfig, EmailReader，用于使用 EmailConfig as ReaderConfig, EmailReader 提供的类型或函数。
from app.utils.email_utils import EmailConfig as ReaderConfig, EmailReader
# 导入 Path，用于以跨平台 Path 对象定位 Skill 目录和脚本文件。
from pathlib import Path
# 导入 os，用于读取环境或处理文件系统路径。
import os

# 调用 `logging.getLogger(__name__)` 完成当前解析、查询或对象构造，并把返回值保存到 `logger`；它表示当前模块的日志记录器，用于记录执行状态和异常。
logger = logging.getLogger(__name__)

# 使用 `if` 判断 `TYPE_CHECKING`；条件成立时执行下面分支。
if TYPE_CHECKING:
    # 导入 EmailConfig, EmailFetchLog，用于查询或保存 EmailConfig, EmailFetchLog 对应的 ORM 数据。
    from app.models.email_config import EmailConfig, EmailFetchLog
    # 导入 EmailConfigCreate, EmailConfigUpdate，用于校验 EmailConfigCreate, EmailConfigUpdate 对应的请求或响应数据。
    from app.schemas.email_config import EmailConfigCreate, EmailConfigUpdate


# 使用 `def` 普通函数 定义 `_email_config_model()`。延迟导入 EmailConfig ORM 模型，避免服务模块加载时产生循环依赖。
def _email_config_model():
    # 导入 EmailConfig，用于查询或保存 EmailConfig 对应的 ORM 数据。
    from app.models.email_config import EmailConfig
    # 从 `_email_config_model()` 返回 `EmailConfig`，并结束当前函数。
    return EmailConfig


# 使用 `def` 普通函数 定义 `_email_fetch_log_model()`。延迟导入 EmailFetchLog ORM 模型，避免循环依赖。
def _email_fetch_log_model():
    # 导入 EmailFetchLog，用于查询或保存 EmailFetchLog 对应的 ORM 数据。
    from app.models.email_config import EmailFetchLog
    # 从 `_email_fetch_log_model()` 返回 `EmailFetchLog`，并结束当前函数。
    return EmailFetchLog


def _fetched_email_model():
    """延迟导入已抓取邮件 ORM，避免服务层与模型层产生循环导入。"""
    from app.models.email_config import FetchedEmail
    return FetchedEmail


# 使用 `def` 普通函数 定义 `_resume_evaluation_model()`。延迟导入 ResumeEvaluation ORM 模型，用于抓取时检查邮件是否已经评价。
def _resume_evaluation_model():
    # 导入 ResumeEvaluation，用于查询或保存 ResumeEvaluation 对应的 ORM 数据。
    from app.models.resume_evaluation import ResumeEvaluation
    # 从 `_resume_evaluation_model()` 返回 `ResumeEvaluation`，并结束当前函数。
    return ResumeEvaluation


# 使用 `def` 普通函数 定义 `_resume_evaluation_service()`。延迟导入 ResumeEvaluationService，用于下载简历后触发自动评价。
def _resume_evaluation_service():
    # 导入 ResumeEvaluationService，用于解析并评分简历以及查询候选人结果。
    from app.services.resume_evaluation_service import ResumeEvaluationService
    # 从 `_resume_evaluation_service()` 返回 `ResumeEvaluationService`，并结束当前函数。
    return ResumeEvaluationService


# 使用 `@dataclass` 把下方类声明为轻量数据对象，并禁止实例创建后修改字段；业务上用于稳定保存工具或 Skill 配置。
@dataclass(frozen=True)
# 使用 `class` 定义 `SkillMailConfig`；该类的业务作用是：Agent 邮件 Skill 的 SMTP 发件配置数据对象，保存发件地址、授权码、服务器、端口和 SSL 标志。
class SkillMailConfig:
    """Skill bundle 中的发送邮箱配置。"""

    # 声明参数 `email`，表示邮箱账号或用户联系邮箱，类型标注为 `str`。
    email: str
    # 声明参数 `password`，表示邮箱客户端授权码或用户密码，类型标注为 `str`。
    password: str
    # 声明参数 `smtp_server`，表示用于发送邮件的 SMTP 服务器地址，类型标注为 `str`。
    smtp_server: str
    # 声明参数 `smtp_port`，表示SMTP 服务端口，类型标注为 `int`。
    smtp_port: int
    # 声明参数 `use_ssl`，表示是否使用隐式 SSL 建立邮件连接，类型标注为 `bool`。
    use_ssl: bool


# 使用 `class` 定义 `EmailConfigService`；该类的业务作用是：邮箱配置服务，使用 AsyncSession 对 email_configs 执行增删改查，并调用 EmailReader 测试 IMAP 登录。
class EmailConfigService:
    # 使用 `def` 普通函数 定义 `__init__()`。保存请求范围内的 AsyncSession，后续配置 CRUD 和状态更新共用该数据库事务。
    def __init__(self, db: AsyncSession):
        # 把 `db` 保存为实例属性 `self.db`，供同一 Agent 服务的其他方法复用；该属性表示当前请求使用的异步数据库会话。
        self.db = db

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `list()`。按创建时间倒序分页查询 email_configs。
    async def list(self, skip: int = 0, limit: int = 100) -> List[EmailConfig]:
        # 调用 `_email_config_model()` 延迟取得 `EmailConfig` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_configs` 表；延迟导入用于避免模块循环依赖。
        EmailConfig = _email_config_model()
        # 调用 `select(EmailConfig).offset(skip).limit(limit).order_by(EmailConfig.created_at.desc())` 完成当前解析、查询或对象构造，并把返回值保存到 `stmt`；它表示准备交给 AsyncSession 执行的 SQLAlchemy 查询语句。
        stmt = select(EmailConfig).offset(skip).limit(limit).order_by(EmailConfig.created_at.desc())
        # 使用 `await` 异步执行当前 SQLAlchemy 查询语句，并保存数据库返回结果，后续再提取 ORM 对象。
        result = await self.db.execute(stmt)
        # 从 `EmailConfigService.list()` 返回 `[row[0] for row in result.all()]`，并结束当前函数。
        return [row[0] for row in result.all()]

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get()`。按配置 UUID 查询单条 email_configs 记录。
    async def get(self, config_id: str) -> Optional[EmailConfig]:
        # 调用 `_email_config_model()` 延迟取得 `EmailConfig` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_configs` 表；延迟导入用于避免模块循环依赖。
        EmailConfig = _email_config_model()
        # 使用关键字参数语法把 `select(EmailConfig).where(EmailConfig.id == config_id)` 传给 `stmt`；该参数表示准备交给 AsyncSession 执行的 SQLAlchemy 查询语句。
        stmt = select(EmailConfig).where(EmailConfig.id == config_id)
        # 使用 `await` 异步执行当前 SQLAlchemy 查询语句，并保存数据库返回结果，后续再提取 ORM 对象。
        result = await self.db.execute(stmt)
        # 从 `EmailConfigService.get()` 返回 `result.scalar_one_or_none()`，并结束当前函数。
        return result.scalar_one_or_none()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `create()`。把 Pydantic 创建模型转换为 EmailConfig ORM 对象，写入连接参数、授权码和抓取设置后提交事务。
    async def create(self, data: EmailConfigCreate,create_by: UUID) -> EmailConfig:
        # 调用 `_email_config_model()` 延迟取得 `EmailConfig` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_configs` 表；延迟导入用于避免模块循环依赖。
        EmailConfig = _email_config_model()
        # 调用 `EmailConfig(` 完成当前解析、查询或对象构造，并把返回值保存到 `config`；它表示当前数据库邮箱配置或邮件协议连接配置。
        config = EmailConfig(
            # 把创建请求中的配置名称写入 `EmailConfig.name`，作为系统管理页面显示的邮箱配置名称。
            name=data.name,
            # 使用关键字参数语法，把 `data.email` 传入 `email` 参数；它表示邮箱账号或用户联系邮箱。
            email=data.email,
            # 使用关键字参数语法，把 `data.imap_server` 传入 `imap_server` 参数；它表示用于读取收件箱的 IMAP 服务器地址。
            imap_server=data.imap_server,
            # 使用关键字参数语法，把 `data.imap_port` 传入 `imap_port` 参数；它表示IMAP 服务端口。
            imap_port=data.imap_port,
            # 使用关键字参数语法，把 `data.imap_ssl` 传入 `imap_ssl` 参数；它表示IMAP 是否使用隐式 SSL 加密连接。
            imap_ssl=data.imap_ssl,
            # 使用关键字参数语法，把 `data.smtp_server` 传入 `smtp_server` 参数；它表示用于发送邮件的 SMTP 服务器地址。
            smtp_server=data.smtp_server,
            # 使用关键字参数语法，把 `data.smtp_port` 传入 `smtp_port` 参数；它表示SMTP 服务端口。
            smtp_port=data.smtp_port,
            # 使用关键字参数语法，把 `data.smtp_ssl` 传入 `smtp_ssl` 参数；它表示SMTP 是否使用隐式 SSL 加密连接。
            smtp_ssl=data.smtp_ssl,
            # 使用关键字参数语法，把 `data.password` 传入 `password` 参数；它表示邮箱客户端授权码或用户密码。
            password=data.password,
            # 使用关键字参数语法，把 `data.fetch_interval` 传入 `fetch_interval` 参数；它表示自动抓取的分钟间隔。
            fetch_interval=data.fetch_interval,
            # 使用关键字参数语法，把 `data.auto_fetch` 传入 `auto_fetch` 参数；它表示是否为该邮箱启动周期抓取任务。
            auto_fetch=data.auto_fetch,
            # 使用关键字参数语法，把 `data.status` 传入 `status` 参数；它表示步骤当前状态，例如 pending、running、completed 或 failed。
            status=data.status,
            # 使用关键字参数语法，把 `data.subject_keywords` 传入 `subject_keywords` 参数；它表示用于筛选招聘邮件主题的关键词字符串。
            subject_keywords = data.subject_keywords,
            # 使用关键字参数语法，把 `"unknown"` 传入 `connection_status` 参数；它表示邮箱最近一次连接测试或抓取的状态。
            connection_status="unknown",
            # 使用关键字参数语法，把 `create_by` 传入 `created_by` 参数；它表示创建当前记录的用户 ID。
            created_by = create_by,
            # 计算表达式 `create_by` 并把结果保存到 `updated_by`；该结果在后续流程中用作最后修改邮箱配置的管理员用户 ID。
            updated_by = create_by
        # 结束 `EmailConfigService.create` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `self.db.add()`，把新 ORM 对象加入当前 SQLAlchemy 会话，等待 flush/commit 写入数据库。
        self.db.add(config)
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()
        # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
        await self.db.commit()
        # 使用 `await` 等待异步操作 `self.db.refresh(config)` 完成，期间不会阻塞事件循环。
        await self.db.refresh(config)
        # 从 `EmailConfigService.create()` 返回 `config`，并结束当前函数。
        return config

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `update()`。只更新请求实际提供的字段，忽略空密码以保护旧授权码，然后提交并刷新 ORM 对象。
    async def update(self, config: EmailConfig, data: EmailConfigUpdate) -> EmailConfig:
        # 调用 `data.model_dump(exclude_unset=True)` 完成当前解析、查询或对象构造，并把返回值保存到 `update_data`；它表示只包含前端实际提交字段的更新字典或 Pydantic 更新模型。
        update_data = data.model_dump(exclude_unset=True)
        # 使用 `if` 判断 `"password" in update_data and not update_data.get("password")`；条件成立时执行下面分支。
        if "password" in update_data and not update_data.get("password"):
            # 调用 `update_data.pop()`，移除空 password，避免用户编辑其他字段时清空原邮箱授权码。
            update_data.pop("password")
        # 使用 `for` 循环 `k, v in update_data.items()`，逐项处理集合数据。
        for k, v in update_data.items():
            # 调用 `setattr()`，把更新字典中的字段逐项写回 EmailConfig ORM 对象。
            setattr(config, k, v)
        # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
        await self.db.commit()
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()
        # 使用 `await` 等待异步操作 `self.db.refresh(config)` 完成，期间不会阻塞事件循环。
        await self.db.refresh(config)
        # 从 `EmailConfigService.update()` 返回 `config`，并结束当前函数。
        return config

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `delete()`。通过 ORM 删除邮箱配置并提交；relationship 级联会同时删除其抓取日志。
    async def delete(self, config: EmailConfig) -> None:
        # 使用 `await` 等待异步操作 `self.db.delete(config)` 完成，期间不会阻塞事件循环。
        await self.db.delete(config)
        # 提交当前数据库事务，使本次资源新增、修改或删除正式生效。
        await self.db.commit()
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `test_connection()`。使用配置中的 IMAP 地址、账号及临时或已保存授权码创建 EmailReader，登录后更新 connection_status。
    async def test_connection(self, config: EmailConfig, password: Optional[str] = None) -> bool:
        # 调用 `ReaderConfig(` 完成当前解析、查询或对象构造，并把返回值保存到 `reader_cfg`；它表示根据邮箱记录构造的底层读取连接参数。
        reader_cfg = ReaderConfig(
            # 使用关键字参数语法，把 `config.imap_server` 传入 `host` 参数；它表示IMAP 或 POP3 邮件服务器地址。
            host=config.imap_server,
            # 使用关键字参数语法，把 `config.imap_port` 传入 `port` 参数；它表示邮件协议服务器端口。
            port=config.imap_port,
            # 使用关键字参数语法，把 `config.email` 传入 `username` 参数；它表示登录用户名。
            username=config.email,
            # 使用关键字参数语法，把 `password or config.password or ""` 传入 `password` 参数；它表示邮箱客户端授权码或用户密码。
            password=password or config.password or "",
            # 使用关键字参数语法，把 `config.imap_ssl` 传入 `use_ssl` 参数；它表示是否使用隐式 SSL 建立邮件连接。
            use_ssl=config.imap_ssl,
            # 使用关键字参数语法，把 `"IMAP"` 传入 `protocol` 参数；它表示当前读取协议名称，限定为 IMAP 或 POP3。
            protocol="IMAP",
        # 结束 `EmailConfigService.test_connection` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `EmailReader(reader_cfg)` 完成当前解析、查询或对象构造，并把返回值保存到 `reader`；它表示负责 IMAP/POP3 登录、搜索和解析的 EmailReader。
        reader = EmailReader(reader_cfg)
        # 调用 `reader.connect()` 完成当前解析、查询或对象构造，并把返回值保存到 `ok`；它表示当前连接、删除或业务操作是否成功的布尔结果。
        ok = reader.connect()
        # 调用 `reader.disconnect()`，退出 IMAP/POP3 并释放网络连接，避免请求结束后连接泄漏。
        reader.disconnect()
        # 计算表达式 `"connected" if ok else "error"` 并把结果保存到 `config.connection_status`；该结果在后续流程中用作邮箱最近一次连接测试或抓取的状态。
        config.connection_status = "connected" if ok else "error"
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()
        # 从 `EmailConfigService.test_connection()` 返回 `ok`，并结束当前函数。
        return ok


# 使用 `class` 定义 `EmailFetchService`；该类的业务作用是：收件抓取服务，连接 IMAP、过滤邮件、保存简历附件、触发简历自动评价并维护抓取日志。
class EmailFetchService:
    # 使用 `def` 普通函数 定义 `__init__()`。保存 AsyncSession，用于写入抓取日志、更新邮箱状态和查询简历评价。
    def __init__(self, db: AsyncSession):
        # 把 `db` 保存为实例属性 `self.db`，供同一 Agent 服务的其他方法复用；该属性表示当前请求使用的异步数据库会话。
        self.db = db

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `manual_fetch()`。连接 IMAP，扫描最近最多 50 封邮件并统计 PDF/DOC/DOCX 附件；该方法只统计，不保存附件或自动评价。
    async def manual_fetch(self, config: EmailConfig) -> EmailFetchLog:
        # 调用 `_email_fetch_log_model()` 延迟取得 `EmailFetchLog` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_fetch_logs` 表；延迟导入用于避免模块循环依赖。
        EmailFetchLog = _email_fetch_log_model()
        # 调用 `EmailFetchLog(email_config_id=config.id, status="running")` 完成当前解析、查询或对象构造，并把返回值保存到 `log`；它表示本次邮件抓取对应的 EmailFetchLog ORM 记录。
        log = EmailFetchLog(email_config_id=config.id, status="running")
        # 调用 `self.db.add()`，把新 ORM 对象加入当前 SQLAlchemy 会话，等待 flush/commit 写入数据库。
        self.db.add(log)
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()

        # 调用 `ReaderConfig(` 完成当前解析、查询或对象构造，并把返回值保存到 `reader_cfg`；它表示根据邮箱记录构造的底层读取连接参数。
        reader_cfg = ReaderConfig(
            # 使用关键字参数语法，把 `config.imap_server` 传入 `host` 参数；它表示IMAP 或 POP3 邮件服务器地址。
            host=config.imap_server,
            # 使用关键字参数语法，把 `config.imap_port` 传入 `port` 参数；它表示邮件协议服务器端口。
            port=config.imap_port,
            # 使用关键字参数语法，把 `config.email` 传入 `username` 参数；它表示登录用户名。
            username=config.email,
            # 使用关键字参数语法，把 `config.password or ""` 传入 `password` 参数；它表示邮箱客户端授权码或用户密码。
            password=config.password or "",
            # 使用关键字参数语法，把 `config.imap_ssl` 传入 `use_ssl` 参数；它表示是否使用隐式 SSL 建立邮件连接。
            use_ssl=config.imap_ssl,
            # 使用关键字参数语法，把 `"IMAP"` 传入 `protocol` 参数；它表示当前读取协议名称，限定为 IMAP 或 POP3。
            protocol="IMAP",
        # 结束 `EmailFetchService.manual_fetch` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `EmailReader(reader_cfg)` 完成当前解析、查询或对象构造，并把返回值保存到 `reader`；它表示负责 IMAP/POP3 登录、搜索和解析的 EmailReader。
        reader = EmailReader(reader_cfg)
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `not reader.connect()`；条件成立时执行下面分支。
            if not reader.connect():
                # 计算表达式 `"failed"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
                log.status = "failed"
                # 计算表达式 `"无法连接到邮箱服务器"` 并把结果保存到 `log.error_message`；该结果在后续流程中用作本次抓取或连接失败的具体原因。
                log.error_message = "无法连接到邮箱服务器"
                # 从 `EmailFetchService.manual_fetch()` 返回 `log`，并结束当前函数。
                return log

            # 调用 `reader.select_folder()`，让 IMAP 连接选择 INBOX 收件箱，后续 search/fetch 都在该文件夹执行。
            reader.select_folder("INBOX")
            # 调用 `reader.search_emails(["ALL"]) or []` 完成当前解析、查询或对象构造，并把返回值保存到 `ids`；它表示IMAP 搜索返回的邮件编号列表。
            ids = reader.search_emails(["ALL"]) or []
            # 调用 `len(ids)` 完成当前解析、查询或对象构造，并把返回值保存到 `log.emails_found`；它表示本次搜索到的邮件数量。
            log.emails_found = len(ids)

            # 计算表达式 `0` 并把结果保存到 `resumes`；该结果在后续流程中用作本次抓取识别或保存的简历附件数量。
            resumes = 0
            # 计算表达式 `ids[-50:] if len(ids) > 50 else ids` 并把结果保存到 `take`；该结果在后续流程中用作从搜索结果中截取后准备处理的最近邮件编号。
            take = ids[-50:] if len(ids) > 50 else ids
            # 使用 `for` 循环 `msg_id in take`，逐项处理集合数据。
            for msg_id in take:
                # 调用 `reader.get_email(msg_id)` 完成当前解析、查询或对象构造，并把返回值保存到 `msg`；它表示解析后的单封 EmailMessage。
                msg = reader.get_email(msg_id)
                # 使用 `if` 判断 `not msg`；条件成立时执行下面分支。
                if not msg:
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 使用 `for` 循环 `att in msg.attachments`，逐项处理集合数据。
                for att in msg.attachments:
                    # 计算表达式 `(att.get("filename") or "").lower()` 并把结果保存到 `filename`；该结果在后续流程中用作邮件附件原始文件名。
                    filename = (att.get("filename") or "").lower()
                    # 使用 `if` 判断 `filename.endswith((".pdf", ".doc", ".docx"))`；条件成立时执行下面分支。
                    if filename.endswith((".pdf", ".doc", ".docx")):
                        # 使用 `+=` 将 `1` 累加到 `resumes`；业务作用是识别到一个有效简历附件后把本次简历计数增加。
                        resumes += 1

            # 计算表达式 `resumes` 并把结果保存到 `log.resumes_extracted`；该结果在后续流程中用作本次识别、保存或统计到的简历附件数。
            log.resumes_extracted = resumes
            # 计算表达式 `"success"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
            log.status = "success"
            # 调用 `datetime.utcnow()` 完成当前解析、查询或对象构造，并把返回值保存到 `config.last_fetch_at`；它表示邮箱最近成功抓取的时间。
            config.last_fetch_at = datetime.utcnow()
            # 计算表达式 `"connected"` 并把结果保存到 `config.connection_status`；该结果在后续流程中用作邮箱最近一次连接测试或抓取的状态。
            config.connection_status = "connected"
            # 从 `EmailFetchService.manual_fetch()` 返回 `log`，并结束当前函数。
            return log
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 计算表达式 `"failed"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
            log.status = "failed"
            # 调用 `str(e)` 完成当前解析、查询或对象构造，并把返回值保存到 `log.error_message`；它表示本次抓取或连接失败的具体原因。
            log.error_message = str(e)
            # 计算表达式 `"error"` 并把结果保存到 `config.connection_status`；该结果在后续流程中用作邮箱最近一次连接测试或抓取的状态。
            config.connection_status = "error"
            # 从 `EmailFetchService.manual_fetch()` 返回 `log`，并结束当前函数。
            return log
        # 使用 `finally` 保证无论成功或失败都执行资源清理和状态恢复。
        finally:
            # 调用 `reader.disconnect()`，退出 IMAP/POP3 并释放网络连接，避免请求结束后连接泄漏。
            reader.disconnect()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `list_logs()`。按邮箱配置 ID 分页查询抓取日志并按创建时间倒序返回。
    async def list_logs(self, config_id: str, skip: int = 0, limit: int = 100) -> List[EmailFetchLog]:
        # 调用 `_email_fetch_log_model()` 延迟取得 `EmailFetchLog` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_fetch_logs` 表；延迟导入用于避免模块循环依赖。
        EmailFetchLog = _email_fetch_log_model()
        # 计算表达式 `(` 并把结果保存到 `stmt`；该结果在后续流程中用作准备交给 AsyncSession 执行的 SQLAlchemy 查询语句。
        stmt = (
            # 调用 `select()`，创建 SQLAlchemy SELECT 查询，后续继续追加过滤、分页和排序条件。
            select(EmailFetchLog)
            # 在 SQLAlchemy 查询链中调用 `where()`，应用 `EmailFetchLog.email_config_id == config_id` 中的过滤条件，只查询当前用户和当前业务目标允许访问的记录。
            .where(EmailFetchLog.email_config_id == config_id)
            # 在 SQLAlchemy 查询链中调用 `offset(skip)`，跳过前 `skip` 条抓取日志，实现分页。
            .offset(skip)
            # 在 SQLAlchemy 查询链中调用 `limit()`，最多读取 `limit` 条记录，避免会话记忆或候选资源查询无限增长。
            .limit(limit)
            # 在 SQLAlchemy 查询链中调用 `order_by()`，按 `EmailFetchLog.created_at.desc()` 排序，使最近或最匹配的记录优先返回。
            .order_by(EmailFetchLog.created_at.desc())
        # 结束 `EmailFetchService.list_logs` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `await` 异步执行当前 SQLAlchemy 查询语句，并保存数据库返回结果，后续再提取 ORM 对象。
        result = await self.db.execute(stmt)
        # 从 `EmailFetchService.list_logs()` 返回 `[row[0] for row in result.all()]`，并结束当前函数。
        return [row[0] for row in result.all()]

    async def list_fetched_emails(self, config_id: str, skip: int = 0, limit: int = 100):
        """分页返回指定邮箱配置已经抓取并保存的邮件。"""
        FetchedEmail = _fetched_email_model()
        # 邮件按照实际发送时间倒序排列；没有发送时间时再按抓取时间排列。
        stmt = (
            select(FetchedEmail)
            .where(FetchedEmail.email_config_id == config_id)
            .order_by(FetchedEmail.sent_at.desc().nullslast(), FetchedEmail.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        count_stmt = select(func.count(FetchedEmail.id)).where(FetchedEmail.email_config_id == config_id)
        result = await self.db.execute(stmt)
        total = (await self.db.execute(count_stmt)).scalar_one()
        return list(result.scalars().all()), total

    async def _upsert_fetched_email(self, config, msg_id, msg, status: str = "matched"):
        """按邮箱配置和 IMAP 邮件编号新增或更新邮件，防止定时任务重复写入。"""
        FetchedEmail = _fetched_email_model()
        stable_message_id = str(getattr(msg, "message_id", None) or msg_id)[:255]
        query = select(FetchedEmail).where(
            FetchedEmail.email_config_id == config.id,
            FetchedEmail.mailbox_message_id == stable_message_id,
        )
        record = (await self.db.execute(query)).scalar_one_or_none()
        if record is None:
            record = FetchedEmail(
                email_config_id=config.id,
                mailbox_message_id=stable_message_id,
            )
            self.db.add(record)

        # 每次重新抓取时同步最新解析结果，但保留已经保存的附件路径。
        record.subject = msg.subject or ""
        record.sender = msg.sender or ""
        record.recipients = list(msg.recipients or [])
        record.sent_at = msg.date
        record.body = msg.body or ""
        record.html_body = msg.html_body or ""
        record.attachment_names = [
            str(item.get("filename") or "attachment")
            for item in (msg.attachments or [])
        ]
        record.attachment_count = len(msg.attachments or [])
        record.processing_status = status
        record.error_message = None
        await self.db.flush()
        return record

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `fetch_recent_attachments()`。读取最近 limit 封邮件，按主题关键词过滤，去重后把附件保存到用户目录，并调用 ResumeEvaluationService 自动评价。
    async def fetch_recent_attachments(
        # 声明 `self` 实例参数；当前方法通过它访问所属 Service、邮件工具、调度器或 Skill 对象的属性和其他方法。
        self,
        # 声明参数 `config`，表示当前数据库邮箱配置或邮件协议连接配置，类型标注为 `EmailConfig`。
        config: EmailConfig,
        # 声明参数 `create_by`，表示创建邮箱配置或附件记录的当前管理员 ID，类型标注为 `UUID`。
        create_by: UUID,
        # 声明局部变量 `limit` 的类型为 `int`，并用 `10,` 初始化；该变量用于本次查询或抓取允许返回的最大数量。
        limit: int = 10,
        # 声明局部变量 `subject_keyword` 的类型为 `list`，并用 `None,` 初始化；该变量用于用于过滤邮件主题的一个或多个关键词。
        subject_keyword: list = None,
        # 声明局部变量 `output_dir` 的类型为 `Optional[Path]`，并用 `None,` 初始化；该变量用于调用方可选指定的简历附件输出目录。
        output_dir: Optional[Path] = None,
    # 结束函数参数列表，并用 `-> EmailFetchLog` 标注该函数的返回值类型。
    ) -> EmailFetchLog:
        """
        获取最近的邮件并在主题包含关键词时下载附件。
        """
        # 调用 `_email_fetch_log_model()` 延迟取得 `EmailFetchLog` ORM 类，随后通过 SQLAlchemy 查询或写入 `email_fetch_logs` 表；延迟导入用于避免模块循环依赖。
        EmailFetchLog = _email_fetch_log_model()
        # 调用 `EmailFetchLog(email_config_id=config.id, status="running")` 完成当前解析、查询或对象构造，并把返回值保存到 `log`；它表示本次邮件抓取对应的 EmailFetchLog ORM 记录。
        log = EmailFetchLog(email_config_id=config.id, status="running")
        # 调用 `self.db.add()`，把新 ORM 对象加入当前 SQLAlchemy 会话，等待 flush/commit 写入数据库。
        self.db.add(log)
        # 使用 `await` 等待异步操作 `self.db.flush()` 完成，期间不会阻塞事件循环。
        await self.db.flush()

        # 计算表达式 `output_dir or (Path(__file__).resolve().parent.parent.parent / "uploads" / "email_attachments" / ...` 并把结果保存到 `base_dir`；该结果在后续流程中用作最终使用的用户级邮件附件保存目录。
        base_dir = output_dir or (Path(__file__).resolve().parent.parent.parent / "uploads" / "email_attachments" / str(create_by))
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 调用 `os.makedirs()` 创建邮件附件保存目录；`exist_ok=True` 表示目录已存在时不报错。
            os.makedirs(base_dir, exist_ok=True)
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception:
            # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
            pass

        # 调用 `ReaderConfig(` 完成当前解析、查询或对象构造，并把返回值保存到 `reader_cfg`；它表示根据邮箱记录构造的底层读取连接参数。
        reader_cfg = ReaderConfig(
            # 使用关键字参数语法，把 `config.imap_server` 传入 `host` 参数；它表示IMAP 或 POP3 邮件服务器地址。
            host=config.imap_server,
            # 使用关键字参数语法，把 `config.imap_port` 传入 `port` 参数；它表示邮件协议服务器端口。
            port=config.imap_port,
            # 使用关键字参数语法，把 `config.email` 传入 `username` 参数；它表示登录用户名。
            username=config.email,
            # 使用关键字参数语法，把 `config.password or ""` 传入 `password` 参数；它表示邮箱客户端授权码或用户密码。
            password=config.password or "",
            # 使用关键字参数语法，把 `config.imap_ssl` 传入 `use_ssl` 参数；它表示是否使用隐式 SSL 建立邮件连接。
            use_ssl=config.imap_ssl,
            # 使用关键字参数语法，把 `"IMAP"` 传入 `protocol` 参数；它表示当前读取协议名称，限定为 IMAP 或 POP3。
            protocol="IMAP",
        # 结束 `EmailFetchService.fetch_recent_attachments` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 调用 `EmailReader(reader_cfg)` 完成当前解析、查询或对象构造，并把返回值保存到 `reader`；它表示负责 IMAP/POP3 登录、搜索和解析的 EmailReader。
        reader = EmailReader(reader_cfg)
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `not reader.connect()`；条件成立时执行下面分支。
            if not reader.connect():
                # 计算表达式 `"failed"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
                log.status = "failed"
                # 计算表达式 `"无法连接到邮箱服务器"` 并把结果保存到 `log.error_message`；该结果在后续流程中用作本次抓取或连接失败的具体原因。
                log.error_message = "无法连接到邮箱服务器"
                # 从 `EmailFetchService.fetch_recent_attachments()` 返回 `log`，并结束当前函数。
                return log
            # 通过模块日志记录器执行 `logger.debug("邮箱连接状态: %s", getattr(reader.connection, "state", "unknown"))`，用于保留运行状态或异常原因。
            logger.debug("邮箱连接状态: %s", getattr(reader.connection, "state", "unknown"))
            # 调用 `reader.select_folder("INBOX")` 完成当前解析、查询或对象构造，并把返回值保存到 `select_result`；它表示当前业务调用完成后返回的单个处理结果。
            select_result = reader.select_folder("INBOX")
            # 使用 `if` 判断 `select_result`；条件成立时执行下面分支。
            if select_result:
                # 通过模块日志记录器执行 `logger.debug("成功选择邮箱: INBOX")`，用于保留运行状态或异常原因。
                logger.debug("成功选择邮箱: INBOX")

            # 调用 `reader.search_emails(["ALL"]) or []` 完成当前解析、查询或对象构造，并把返回值保存到 `ids`；它表示IMAP 搜索返回的邮件编号列表。
            ids = reader.search_emails(["ALL"]) or []
            # 使用 `if` 判断 `not ids`；条件成立时执行下面分支。
            if not ids:
                # 计算表达式 `"success"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
                log.status = "success"
                # 计算表达式 `0` 并把结果保存到 `log.emails_found`；该结果在后续流程中用作本次搜索到的邮件数量。
                log.emails_found = 0
                # 计算表达式 `0` 并把结果保存到 `log.resumes_extracted`；该结果在后续流程中用作本次识别、保存或统计到的简历附件数。
                log.resumes_extracted = 0
                # 从 `EmailFetchService.fetch_recent_attachments()` 返回 `log`，并结束当前函数。
                return log
            # 通过模块日志记录器执行 `logger.debug("找到邮件 ID: %s", ids)`，用于保留运行状态或异常原因。
            logger.debug("找到邮件 ID: %s", ids)
            # 计算表达式 `ids[-limit:] if len(ids) > limit else ids` 并把结果保存到 `take`；该结果在后续流程中用作从搜索结果中截取后准备处理的最近邮件编号。
            take = ids[-limit:] if len(ids) > limit else ids
            # emails_found 改为记录真正命中主题关键词的邮件数量，不再固定显示本轮扫描的 10 封。
            matched_count = 0
            log.emails_found = 0
            # 计算表达式 `0` 并把结果保存到 `resumes`；该结果在后续流程中用作本次抓取识别或保存的简历附件数量。
            resumes = 0

            # 使用 `for` 循环 `msg_id in reversed(take)`，逐项处理集合数据。
            for msg_id in reversed(take):
                # 调用 `reader.get_email(msg_id)` 完成当前解析、查询或对象构造，并把返回值保存到 `msg`；它表示解析后的单封 EmailMessage。
                msg = reader.get_email(msg_id)
                # 使用 `if` 判断 `not msg`；条件成立时执行下面分支。
                if not msg:
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue
                # 计算表达式 `(msg.subject or "").lower()` 并把结果保存到 `subject`；该结果在后续流程中用作待发送邮件的主题。
                subject = (msg.subject or "").lower()

                # 使用 `if` 判断 `subject_keyword and not any(keyword.lower() in subject for keyword in subject_keyword)`；条件成立时执行下面分支。
                if subject_keyword and not any(keyword.lower() in subject for keyword in subject_keyword):
                    # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                    continue

                matched_count += 1
                # 通过模块日志记录器执行 `logger.info("发现符合条件的邮件: %s (ID: %s)", subject, msg_id)`，用于保留运行状态或异常原因。
                logger.info("发现符合条件的邮件: %s (ID: %s)", subject, msg_id)

                # 先把邮件头、正文和附件名称落库；后续附件或 AI 评价失败时，用户仍能看到邮件和失败原因。
                mail_record = await self._upsert_fetched_email(config, msg_id, msg)
                saved_paths = list(mail_record.attachment_paths or [])
                message_errors = []

                # 使用 `for` 循环 `att in (msg.attachments or [])`，逐项处理集合数据。
                for att in (msg.attachments or []):
                    # 调用 `att.get("filename") or "attachment"` 完成当前解析、查询或对象构造，并把返回值保存到 `fname`；它表示当前附件文件名。
                    fname = att.get("filename") or "attachment"
                    # 调用 `att.get("content")` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
                    content = att.get("content")
                    # 使用 `if` 判断 `not content`；条件成立时执行下面分支。
                    if not content:
                        message_errors.append(f"附件 {fname} 内容为空")
                        # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                        continue
                    # 通过模块日志记录器执行 `logger.debug("处理附件: %s", fname)`，用于保留运行状态或异常原因。
                    logger.debug("处理附件: %s", fname)
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        # 调用 `_resume_evaluation_model()` 完成当前解析、查询或对象构造，并把返回值保存到 `ResumeEvaluation`；它表示用于检查邮件是否已经评价的简历评价 ORM 类。
                        ResumeEvaluation = _resume_evaluation_model()
                        # 使用 `await` 等待 `self.db.execute(select(ResumeEvaluation).where(ResumeEvaluation.email_id == str(msg_id)))` 的异步数据库、邮箱或业务操作完成，把结果保存到 `existing`；该结果用于按邮件 ID 查询既有简历评价的数据库结果。
                        existing = await self.db.execute(select(ResumeEvaluation).where(ResumeEvaluation.email_id == str(msg_id)))
                        # 调用 `existing.scalars().first()` 完成当前解析、查询或对象构造，并把返回值保存到 `records`；它表示当前邮件已经存在的第一条简历评价记录。
                        records = existing.scalars().first()
                        # 使用 `if` 判断 `records`；条件成立时执行下面分支。
                        if records:
                            # 通过模块日志记录器执行 `logger.info("邮件 ID %s 已存在评价记录，跳过", msg_id)`，用于保留运行状态或异常原因。
                            logger.info("邮件 ID %s 已存在评价记录，跳过", msg_id)
                            # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                            continue
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception as e:
                        # 通过模块日志记录器执行 `logger.warning("检查邮件 ID %s 是否已存在评价记录时出错: %s", msg_id, e)`，用于保留运行状态或异常原因。
                        logger.warning("检查邮件 ID %s 是否已存在评价记录时出错: %s", msg_id, e)
                        # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
                        pass
                    # 如果该附件以前已经保存，复用原路径，避免每分钟产生一份重复文件。
                    existing_path = next((item for item in saved_paths if Path(item).name == fname), None)
                    target_path = Path(existing_path) if existing_path else base_dir / fname
                    # 计算表达式 `1` 并把结果保存到 `idx`；该结果在后续流程中用作文件重名时追加到文件名后的递增序号。
                    idx = 1
                    # 使用 `while` 在 `target_path.exists()` 成立期间重复执行下面逻辑。
                    while not existing_path and target_path.exists():
                        # 计算表达式 `target_path.stem` 并把结果保存到 `stem`；该结果在后续流程中用作不包含扩展名的附件基础文件名。
                        stem = target_path.stem
                        # 计算表达式 `target_path.suffix` 并把结果保存到 `suffix`；该结果在后续流程中用作附件扩展名，例如 .pdf 或 .docx。
                        suffix = target_path.suffix
                        # 计算表达式 `base_dir / f"{stem}_{idx}{suffix}"` 并把结果保存到 `target_path`；该结果在后续流程中用作当前简历附件准备写入的本地完整路径。
                        target_path = base_dir / f"{stem}_{idx}{suffix}"
                        # 使用 `+=` 将 `1` 累加到 `idx`；业务作用是附件重名后增加序号，继续尝试新的不冲突文件名。
                        idx += 1
                    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                    try:
                        if not existing_path:
                            # 使用二进制模式保存附件，并记录最终路径供前端展示。
                            with open(target_path, "wb") as f:
                                f.write(content)
                            saved_paths.append(str(target_path))
                            resumes += 1
                            logger.info("保存附件成功: %s", target_path)
                        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
                        try:
                            # 调用 `_resume_evaluation_service()` 完成当前解析、查询或对象构造，并把返回值保存到 `ResumeEvaluationService`；它表示负责解析并自动评分邮件简历的业务服务类。
                            ResumeEvaluationService = _resume_evaluation_service()
                            # 调用 `ResumeEvaluationService(self.db)` 完成当前解析、查询或对象构造，并把返回值保存到 `ev_svc`；它表示当前抓取流程使用的 ResumeEvaluationService 实例。
                            ev_svc = ResumeEvaluationService(self.db)
                            # 计算表达式 `create_by` 并把结果保存到 `user_id`；该结果在后续流程中用作当前登录用户的唯一 ID，用于权限和数据隔离。
                            user_id = create_by
                            # 使用 `if` 判断 `user_id`；条件成立时执行下面分支。
                            if user_id:
                                # 通过模块日志记录器执行 `logger.info("开始评价简历: %s", fname)`，用于保留运行状态或异常原因。
                                logger.info("开始评价简历: %s", fname)
                                # 异步调用简历自动评价服务，把邮件主题、附件字节、文件名和邮件 ID 交给大模型评分流程。
                                await ev_svc.evaluate_resume_auto(user_id=user_id, subject=subject, file_content=content, filename=fname, email_id=str(msg_id))
                                # 通过模块日志记录器执行 `logger.info("成功评价简历: %s", fname)`，用于保留运行状态或异常原因。
                                logger.info("成功评价简历: %s", fname)
                        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                        except Exception as e:
                            # 通过模块日志记录器执行 `logger.warning("评价简历 %s 时出错: %s", fname, e)`，用于保留运行状态或异常原因。
                            logger.warning("评价简历 %s 时出错: %s", fname, e)
                            message_errors.append(f"简历评价失败：{e}")
                    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
                    except Exception as e:
                        # 通过模块日志记录器执行 `logger.warning("保存附件 %s 时出错: %s", fname, e)`，用于保留运行状态或异常原因。
                        logger.warning("保存附件 %s 时出错: %s", fname, e)
                        message_errors.append(f"附件 {fname} 保存失败：{e}")

                # 把附件路径、最终状态和错误信息写回本封邮件记录，供前端列表直接展示。
                mail_record.attachment_paths = saved_paths
                if message_errors:
                    mail_record.processing_status = "partial" if saved_paths else "failed"
                    mail_record.error_message = "；".join(message_errors)
                elif saved_paths:
                    mail_record.processing_status = "processed"
                    mail_record.error_message = None
                else:
                    mail_record.processing_status = "no_attachment"
                    mail_record.error_message = "邮件命中关键词，但没有解析到可保存的附件"

            log.emails_found = matched_count
            # 计算表达式 `resumes` 并把结果保存到 `log.resumes_extracted`；该结果在后续流程中用作本次识别、保存或统计到的简历附件数。
            log.resumes_extracted = resumes
            # 把 `log.error_message` 初始化为 `None`，表示当前尚无可用结果；后续分支成功后会写入本次抓取或连接失败的具体原因。
            log.error_message = None
            # 计算表达式 `"success"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
            log.status = "success"
            # 调用 `datetime.utcnow()` 完成当前解析、查询或对象构造，并把返回值保存到 `config.last_fetch_at`；它表示邮箱最近成功抓取的时间。
            config.last_fetch_at = datetime.utcnow()
            # 计算表达式 `"connected"` 并把结果保存到 `config.connection_status`；该结果在后续流程中用作邮箱最近一次连接测试或抓取的状态。
            config.connection_status = "connected"
            # 从 `EmailFetchService.fetch_recent_attachments()` 返回 `log`，并结束当前函数。
            return log
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception as e:
            # 计算表达式 `"failed"` 并把结果保存到 `log.status`；该结果在后续流程中用作步骤当前状态，例如 pending、running、completed 或 failed。
            log.status = "failed"
            # 调用 `str(e)` 完成当前解析、查询或对象构造，并把返回值保存到 `log.error_message`；它表示本次抓取或连接失败的具体原因。
            log.error_message = str(e)
            # 计算表达式 `"error"` 并把结果保存到 `config.connection_status`；该结果在后续流程中用作邮箱最近一次连接测试或抓取的状态。
            config.connection_status = "error"
            # 从 `EmailFetchService.fetch_recent_attachments()` 返回 `log`，并结束当前函数。
            return log
        # 使用 `finally` 保证无论成功或失败都执行资源清理和状态恢复。
        finally:
            # 调用 `reader.disconnect()`，退出 IMAP/POP3 并释放网络连接，避免请求结束后连接泄漏。
            reader.disconnect()


# 使用 `class` 定义 `EmailSendService`；该类的业务作用是：Agent 邮件发送服务，从 Skill 配置文件读取 SMTP 参数并提交已由用户确认的邮件。
class EmailSendService:
    # 使用 `def` 普通函数 定义 `__init__()`。保存数据库会话并定位 backend/skills/hr-agent-email/config.txt；当前发信配置不读取 email_configs 表。
    def __init__(self, db: AsyncSession):
        # 把 `db` 保存为实例属性 `self.db`，供同一 Agent 服务的其他方法复用；该属性表示当前请求使用的异步数据库会话。
        self.db = db
        # 把 `Path(__file__).resolve().parents[2] / "skills" / "hr-agent-email" / "config.txt"` 保存为实例属性 `self.config_path`，供同一 Agent 服务的其他方法复用；该属性表示当前需要读取、加载或写入的文件路径。
        self.config_path = Path(__file__).resolve().parents[2] / "skills" / "hr-agent-email" / "config.txt"

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `send_agent_email()`。检查收件人、主题和正文，读取 Skill SMTP 配置，提交邮件并把发件人和投递状态返回给 Agent。
    async def send_agent_email(
        # 声明 `self` 实例参数；当前方法通过它访问所属 Service、邮件工具、调度器或 Skill 对象的属性和其他方法。
        self,
        # 声明参数 `user_id`，表示当前登录用户的唯一 ID，用于权限和数据隔离，类型标注为 `UUID`。
        user_id: UUID,
        # 声明参数 `recipient_email`，表示邮件收件人地址，类型标注为 `str`。
        recipient_email: str,
        # 声明参数 `subject`，表示待发送邮件的主题，类型标注为 `str`。
        subject: str,
        # 声明参数 `body`，表示待发送邮件的正文，类型标注为 `str`。
        body: str,
    # 结束函数参数列表，并用 `-> dict` 标注该函数的返回值类型。
    ) -> dict:
        # 调用 `self._load_skill_mail_config()` 完成当前解析、查询或对象构造，并把返回值保存到 `config`；它表示当前数据库邮箱配置或邮件协议连接配置。
        config = self._load_skill_mail_config()
        # 使用 `if` 判断 `not recipient_email`；条件成立时执行下面分支。
        if not recipient_email:
            # 使用 `raise` 抛出 `ValueError("缺少收件人邮箱地址。")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("缺少收件人邮箱地址。")
        # 使用 `if` 判断 `not subject`；条件成立时执行下面分支。
        if not subject:
            # 使用 `raise` 抛出 `ValueError("缺少邮件主题。")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("缺少邮件主题。")
        # 使用 `if` 判断 `not body`；条件成立时执行下面分支。
        if not body:
            # 使用 `raise` 抛出 `ValueError("缺少邮件正文。")`，立即终止当前失败流程并交给上层处理。
            raise ValueError("缺少邮件正文。")

        # 调用 `self._send_via_smtp(` 完成当前解析、查询或对象构造，并把返回值保存到 `rejected_recipients`；它表示SMTP 服务器拒绝接收的地址及原因映射。
        rejected_recipients = self._send_via_smtp(
            # 使用关键字参数语法，把 `config` 传入 `config` 参数；它表示当前数据库邮箱配置或邮件协议连接配置。
            config=config,
            # 使用关键字参数语法，把 `recipient_email` 传入 `recipient_email` 参数；它表示邮件收件人地址。
            recipient_email=recipient_email,
            # 使用关键字参数语法，把 `subject` 传入 `subject` 参数；它表示待发送邮件的主题。
            subject=subject,
            # 使用关键字参数语法，把 `body` 传入 `body` 参数；它表示待发送邮件的正文。
            body=body,
        # 结束 `EmailSendService.send_agent_email` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
        # 使用 `if` 判断 `rejected_recipients`；条件成立时执行下面分支。
        if rejected_recipients:
            # 调用 `rejected_recipients.get(recipient_email) or next(iter(rejected_recipients.values()))` 完成当前解析、查询或对象构造，并把返回值保存到 `reason`；它表示SMTP 拒收当前邮件的原因。
            reason = rejected_recipients.get(recipient_email) or next(iter(rejected_recipients.values()))
            # 使用 `raise` 抛出 `ValueError(f"SMTP 服务器拒收该邮件：{reason}")`，立即终止当前失败流程并交给上层处理。
            raise ValueError(f"SMTP 服务器拒收该邮件：{reason}")

        # 从 `EmailSendService.send_agent_email()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `recipient_email` 字段为 `recipient_email`，用于表示邮件收件人地址。
            "recipient_email": recipient_email,
            # 在字典中设置 `subject` 字段为 `subject`，用于表示待发送邮件的主题。
            "subject": subject,
            # 在字典中设置 `sender_email` 字段为 `config.email`，用于表示实际提交邮件的发件邮箱。
            "sender_email": config.email,
            # 在字典中设置 `config_path` 字段为 `str(self.config_path)`，用于表示当前需要读取、加载或写入的文件路径。
            "config_path": str(self.config_path),
            # 在字典中设置 `status` 字段为 `"submitted"`，用于表示步骤当前状态，例如 pending、running、completed 或 failed。
            "status": "submitted",
            # 在字典中设置 `delivery_note` 字段为 `"SMTP 服务器已接受邮件，但最终投递结果以收件方服务器返回为准。"`，用于表示说明 SMTP 已接收但最终投递仍由收件服务器决定的提示。
            "delivery_note": "SMTP 服务器已接受邮件，但最终投递结果以收件方服务器返回为准。",
        # 结束 `EmailSendService.send_agent_email` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 使用 `def` 普通函数 定义 `_load_skill_mail_config()`。逐行解析 config.txt，校验必填 SMTP 字段、端口和 SSL 标志，构造 SkillMailConfig。
    def _load_skill_mail_config(self) -> SkillMailConfig:
        # 使用 `if` 判断 `not self.config_path.exists()`；条件成立时执行下面分支。
        if not self.config_path.exists():
            # 使用 `raise` 抛出 `ValueError(f"未找到邮箱配置文件：{self.config_path}")`，立即终止当前失败流程并交给上层处理。
            raise ValueError(f"未找到邮箱配置文件：{self.config_path}")

        # 声明局部变量 `raw` 的类型为 `dict[str, str]`，并用 `{}` 初始化；该变量用于从配置文件或邮件服务器读取的原始数据。
        raw: dict[str, str] = {}
        # 使用 `for` 循环 `line in self.config_path.read_text(encoding="utf-8").splitlines()`，逐项处理集合数据。
        for line in self.config_path.read_text(encoding="utf-8").splitlines():
            # 调用 `line.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `stripped`；它表示去除首尾空白后的当前文本行，便于判断标题、正文或配置字段。
            stripped = line.strip()
            # 使用 `if` 判断 `not stripped or stripped.startswith("#") or "=" not in stripped`；条件成立时执行下面分支。
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue
            # 使用 Python 序列解包，把 `stripped.split("=", 1)` 返回的多个结果分别赋给 `key, value`，供后续状态判断和数据解析。
            key, value = stripped.split("=", 1)
            # 执行字典或映射赋值 `raw[key.strip()] = value.strip()`；业务作用是把解析后的配置键和值写入字典，供后续构造 Skill 元数据或邮箱发送配置。
            raw[key.strip()] = value.strip()

        # 使用 `def` 普通函数 定义 `require()`。
        def require(key: str) -> str:
            # 调用 `raw.get(key, "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示当前配置项或邮件头片段的原始值。
            value = raw.get(key, "").strip()
            # 使用 `if` 判断 `not value or value.startswith("your_") or value.endswith("example.com")`；条件成立时执行下面分支。
            if not value or value.startswith("your_") or value.endswith("example.com"):
                # 使用 `raise` 抛出 `ValueError(f"邮箱配置缺少有效字段：{key}，请先填写 {self.config_path}")`，立即终止当前失败流程并交给上层处理。
                raise ValueError(f"邮箱配置缺少有效字段：{key}，请先填写 {self.config_path}")
            # 从 `EmailSendService._load_skill_mail_config.require()` 返回 `value`，并结束当前函数。
            return value

        # 使用 `def` 普通函数 定义 `optional_int()`。
        def optional_int(key: str, default: int) -> int:
            # 调用 `raw.get(key, "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `value`；它表示当前配置项或邮件头片段的原始值。
            value = raw.get(key, "").strip()
            # 使用 `if` 判断 `not value`；条件成立时执行下面分支。
            if not value:
                # 从 `EmailSendService._load_skill_mail_config.optional_int()` 返回 `default`，并结束当前函数。
                return default
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 从 `EmailSendService._load_skill_mail_config.optional_int()` 返回 `int(value)`，并结束当前函数。
                return int(value)
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except ValueError as exc:
                # 使用 `raise` 抛出 `ValueError(f"邮箱配置字段 {key} 必须是整数") from exc`，立即终止当前失败流程并交给上层处理。
                raise ValueError(f"邮箱配置字段 {key} 必须是整数") from exc

        # 调用 `raw.get("MAIL_ACCOUNT_1_USE_SSL", "true").strip().lower() in {"1", "true", "yes", "on"}` 完成当前解析、查询或对象构造，并把返回值保存到 `use_ssl`；它表示是否使用隐式 SSL 建立邮件连接。
        use_ssl = raw.get("MAIL_ACCOUNT_1_USE_SSL", "true").strip().lower() in {"1", "true", "yes", "on"}

        # 从 `EmailSendService._load_skill_mail_config()` 返回 `SkillMailConfig(`，并结束当前函数。
        return SkillMailConfig(
            # 使用关键字参数语法，把 `require("MAIL_ACCOUNT_1_EMAIL")` 传入 `email` 参数；它表示邮箱账号或用户联系邮箱。
            email=require("MAIL_ACCOUNT_1_EMAIL"),
            # 使用关键字参数语法，把 `require("MAIL_ACCOUNT_1_PASSWORD")` 传入 `password` 参数；它表示邮箱客户端授权码或用户密码。
            password=require("MAIL_ACCOUNT_1_PASSWORD"),
            # 使用关键字参数语法，把 `require("MAIL_ACCOUNT_1_SMTP_SERVER")` 传入 `smtp_server` 参数；它表示用于发送邮件的 SMTP 服务器地址。
            smtp_server=require("MAIL_ACCOUNT_1_SMTP_SERVER"),
            # 使用关键字参数语法，把 `optional_int("MAIL_ACCOUNT_1_SMTP_PORT", 465 if use_ssl else 587)` 传入 `smtp_port` 参数；它表示SMTP 服务端口。
            smtp_port=optional_int("MAIL_ACCOUNT_1_SMTP_PORT", 465 if use_ssl else 587),
            # 使用关键字参数语法，把 `use_ssl` 传入 `use_ssl` 参数；它表示是否使用隐式 SSL 建立邮件连接。
            use_ssl=use_ssl,
        # 结束 `EmailSendService._load_skill_mail_config` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

    # 使用 `def` 普通函数 定义 `_send_via_smtp()`。构造标准 EmailMessage；SSL 模式使用 SMTP_SSL，非 SSL 模式尝试 STARTTLS，登录后调用 send_message。
    def _send_via_smtp(
        # 声明 `self` 实例参数；当前方法通过它访问所属 Service、邮件工具、调度器或 Skill 对象的属性和其他方法。
        self,
        # 声明参数 `config`，表示当前数据库邮箱配置或邮件协议连接配置，类型标注为 `SkillMailConfig`。
        config: SkillMailConfig,
        # 声明参数 `recipient_email`，表示邮件收件人地址，类型标注为 `str`。
        recipient_email: str,
        # 声明参数 `subject`，表示待发送邮件的主题，类型标注为 `str`。
        subject: str,
        # 声明参数 `body`，表示待发送邮件的正文，类型标注为 `str`。
        body: str,
    # 结束函数参数列表，并用 `-> dict` 标注该函数的返回值类型。
    ) -> dict:
        # 调用 `SMTPMessage()` 完成当前解析、查询或对象构造，并把返回值保存到 `message`；它表示用户本次输入的自然语言消息。
        message = SMTPMessage()
        # 执行字典或映射赋值 `message["From"] = config.email`；业务作用是设置 EmailMessage 的发件人字段，供 SMTP 服务器生成并投递标准邮件。
        message["From"] = config.email
        # 执行字典或映射赋值 `message["To"] = recipient_email`；业务作用是设置 EmailMessage 的收件人字段，供 SMTP 服务器生成并投递标准邮件。
        message["To"] = recipient_email
        # 执行字典或映射赋值 `message["Subject"] = subject`；业务作用是设置 EmailMessage 的邮件主题字段，供 SMTP 服务器生成并投递标准邮件。
        message["Subject"] = subject
        # 调用 `message.set_content()`，把已确认的纯文本正文写入标准库 SMTP 邮件对象。
        message.set_content(body)

        # 使用 `if` 判断 `config.use_ssl`；条件成立时执行下面分支。
        if config.use_ssl:
            # 使用 `SMTP_SSL` 建立隐式加密的发信连接，随后登录发件邮箱并提交邮件。
            with smtplib.SMTP_SSL(config.smtp_server, config.smtp_port or 465, timeout=20) as server:
                # 使用发件邮箱与客户端授权码登录 SMTP 服务器，认证通过后才允许发送邮件。
                server.login(config.email, config.password or "")
                # 从 `EmailSendService._send_via_smtp()` 返回 `server.send_message(message) or {}`，并结束当前函数。
                return server.send_message(message) or {}

        # 使用普通 SMTP 连接；后续调用 `starttls()` 将连接升级为 TLS 后再登录发信。
        with smtplib.SMTP(config.smtp_server, config.smtp_port or 587, timeout=20) as server:
            # 调用 `server.ehlo()`，向 SMTP 服务器声明客户端并获取其支持的扩展能力。
            server.ehlo()
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用 `server.starttls()`，把普通 SMTP 连接升级为 TLS 加密连接。
                server.starttls()
                # 调用 `server.ehlo()`，向 SMTP 服务器声明客户端并获取其支持的扩展能力。
                server.ehlo()
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
                pass
            # 使用发件邮箱与客户端授权码登录 SMTP 服务器，认证通过后才允许发送邮件。
            server.login(config.email, config.password or "")
            # 从 `EmailSendService._send_via_smtp()` 返回 `server.send_message(message) or {}`，并结束当前函数。
            return server.send_message(message) or {}
