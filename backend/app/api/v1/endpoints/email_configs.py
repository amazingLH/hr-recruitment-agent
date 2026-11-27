"""
* 文件作用
  * 这是系统邮箱管理的 FastAPI 接口层，且所有接口仅允许超级管理员访问。它提供邮箱配置 CRUD、IMAP 连接测试、手动抓取、抓取日志和最近邮件读取；接口把数据库会话交给 EmailConfigService/EmailFetchService，并在配置变化后同步刷新 EmailScheduler。
* 类说明
  * 本文件没有定义业务类，使用模块级函数实现 FastAPI 接口或 Skill 阶段。
* 主要函数与方法说明
  * `list_email_configs()`：分页查询 email_configs 并返回邮箱配置列表，仅超级管理员可调用。
  * `create_email_config()`：校验 EmailConfigCreate 后写入邮箱配置，并依据 auto_fetch 状态创建或跳过后台抓取任务。
  * `get_email_config()`：按 config_id 读取一条邮箱配置，不存在时返回 404。
  * `update_email_config()`：更新实际提交的邮箱字段，空密码不会覆盖旧授权码；随后刷新该配置对应的自动抓取任务。
  * `delete_email_config()`：删除邮箱配置及其抓取日志，并停止该配置对应的后台抓取任务。
  * `test_email_connection()`：读取已保存的邮箱配置，用请求中的临时授权码执行 IMAP 登录测试；当前不会测试 SMTP。
  * `manual_fetch_emails()`：立即运行一次 IMAP 抓取，提交 EmailFetchLog，并向前端返回抓取数量和错误信息。
  * `list_fetch_logs()`：按 config_id 分页读取抓取日志，供前端展示历史执行结果。
  * `list_fetched_emails()`：分页返回指定邮箱已经持久化的邮件主题、正文、附件和处理状态。
  * `get_latest_email()`：连接 IMAP 收件箱，按可选发件人筛选并返回最新邮件的主题、发件人、时间和正文摘要。
"""

# 导入 Any, List，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, List
import re
# 导入 APIRouter, Depends, HTTPException, status, Request，用于FastAPI 路由、依赖注入和 HTTP 异常能力。
from fastapi import APIRouter, Depends, HTTPException, status, Request
# 导入 AsyncSession，用于异步数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession

# 导入 get_db，用于取得 FastAPI 请求范围内的异步数据库会话。
from app.core.database import get_db
# 导入 get_current_user, get_current_superuser，用于解析 JWT 并取得当前登录用户。
from app.api.deps import get_current_user, get_current_superuser
# 导入 User as UserSchema，用于当前登录用户响应模型。
from app.schemas.user import User as UserSchema
# 导入 (，用于校验 ( 对应的请求或响应数据。
from app.schemas.email_config import (
    # 从当前多行 import 中导入 `EmailConfig`，并在本文件使用别名 `EmailConfigSchema` 避免与同名 ORM 类冲突。
    EmailConfig as EmailConfigSchema,
    # 从当前多行 import 中导入 `EmailConfigCreate`（邮箱配置创建请求模型），供下方接口进行参数校验或响应序列化。
    EmailConfigCreate,
    # 从当前多行 import 中导入 `EmailConfigUpdate`（邮箱配置修改请求模型），供下方接口进行参数校验或响应序列化。
    EmailConfigUpdate,
    # 从当前多行 import 中导入 `EmailFetchLog`（单条邮件抓取日志响应模型），供下方接口进行参数校验或响应序列化。
    EmailFetchLog,
    # 从当前多行 import 中导入 `EmailFetchLogList`（邮件抓取日志分页响应模型），供下方接口进行参数校验或响应序列化。
    EmailFetchLogList,
    # 从当前多行 import 中导入 `EmailConnectionTest`（邮箱连接测试请求模型），供下方接口进行参数校验或响应序列化。
    EmailConnectionTest,
    FetchedEmailListResponse,
# 结束 `模块初始化` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
)
# 导入 `EmailConfigService`（邮箱配置 CRUD 与连接测试）和 `EmailFetchService`（收件抓取、附件保存与抓取日志），供接口层或调度器复用。
from app.services.email_service import EmailConfigService, EmailFetchService
# 导入 EmailConfig as ReaderConfig, EmailReader，用于使用 EmailConfig as ReaderConfig, EmailReader 提供的类型或函数。
from app.utils.email_utils import EmailConfig as ReaderConfig, EmailReader


# 创建本模块的 FastAPI `APIRouter`，下方装饰器会把用户或邮箱管理接口注册到该路由器。
router = APIRouter()


# 将 GET / 注册到下方处理函数。
@router.get("/", response_model=List[EmailConfigSchema])
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `list_email_configs()`。分页查询 email_configs 并返回邮箱配置列表，仅超级管理员可调用。
async def list_email_configs(
    # 声明局部变量 `skip` 的类型为 `int`，并用 `0,` 初始化；该变量用于分页时跳过的记录数量。
    skip: int = 0,
    # 声明局部变量 `limit` 的类型为 `int`，并用 `100,` 初始化；该变量用于本次查询或抓取允许返回的最大数量。
    limit: int = 100,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.list(skip=skip, limit=limit)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `configs`；该结果用于分页查询得到的邮箱配置列表。
    configs = await svc.list(skip=skip, limit=limit)
    # 从 `list_email_configs()` 返回 `configs`，并结束当前函数。
    return configs


# 将 POST / 注册到下方处理函数。
@router.post("/", response_model=EmailConfigSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `create_email_config()`。校验 EmailConfigCreate 后写入邮箱配置，并依据 auto_fetch 状态创建或跳过后台抓取任务。
async def create_email_config(
    # 声明参数 `config_data`，表示前端提交并通过 Pydantic 校验的新邮箱配置，类型标注为 `EmailConfigCreate`。
    config_data: EmailConfigCreate,
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `Request`。
    request: Request,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.create(config_data,create_by=current_user.id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await svc.create(config_data,create_by=current_user.id)
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `getattr(request.app.state, "email_scheduler", None)` 完成当前解析、查询或对象构造，并把返回值保存到 `scheduler`；它表示挂载在 FastAPI app.state 上的 EmailScheduler。
        scheduler = getattr(request.app.state, "email_scheduler", None)
        # 使用 `if` 判断 `scheduler`；条件成立时执行下面分支。
        if scheduler:
            # 使用 `await` 等待异步操作 `scheduler.refresh_for_config(config)` 完成，期间不会阻塞事件循环。
            await scheduler.refresh_for_config(config)
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except Exception:
        # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
        pass
    # 从 `create_email_config()` 返回 `config`，并结束当前函数。
    return config


# 将 GET /{config_id} 注册到下方处理函数。
@router.get("/{config_id}", response_model=EmailConfigSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get_email_config()`。按 config_id 读取一条邮箱配置，不存在时返回 404。
async def get_email_config(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 从 `get_email_config()` 返回 `config`，并结束当前函数。
    return config


# 将 PUT /{config_id} 注册到下方处理函数。
@router.put("/{config_id}", response_model=EmailConfigSchema)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `update_email_config()`。更新实际提交的邮箱字段，空密码不会覆盖旧授权码；随后刷新该配置对应的自动抓取任务。
async def update_email_config(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明参数 `update_data`，表示只包含前端实际提交字段的更新字典或 Pydantic 更新模型，类型标注为 `EmailConfigUpdate`。
    update_data: EmailConfigUpdate,
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `Request`。
    request: Request,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 使用 `await` 等待 `svc.update(config, update_data)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `updated`；该结果用于数据库提交后刷新得到的最新邮箱配置或业务对象。
    updated = await svc.update(config, update_data)
    # 计算表达式 `current_user.id` 并把结果保存到 `updated.updated_by`；该结果在后续流程中用作最后修改邮箱配置的管理员用户 ID。
    updated.updated_by = current_user.id
    # 使用 `await` 等待异步操作 `db.flush()` 完成，期间不会阻塞事件循环。
    await db.flush()
    # 使用 `await` 等待异步操作 `db.refresh(updated)` 完成，期间不会阻塞事件循环。
    await db.refresh(updated)
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `getattr(request.app.state, "email_scheduler", None)` 完成当前解析、查询或对象构造，并把返回值保存到 `scheduler`；它表示挂载在 FastAPI app.state 上的 EmailScheduler。
        scheduler = getattr(request.app.state, "email_scheduler", None)
        # 使用 `if` 判断 `scheduler`；条件成立时执行下面分支。
        if scheduler:
            # 使用 `await` 等待异步操作 `scheduler.refresh_for_config(updated)` 完成，期间不会阻塞事件循环。
            await scheduler.refresh_for_config(updated)
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except Exception:
        # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
        pass
    # 从 `update_email_config()` 返回 `updated`，并结束当前函数。
    return updated


# 将 DELETE /{config_id} 注册到下方处理函数。
@router.delete("/{config_id}")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `delete_email_config()`。删除邮箱配置及其抓取日志，并停止该配置对应的后台抓取任务。
async def delete_email_config(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明参数 `request`，表示FastAPI 已按 Pydantic 模型校验的请求数据，类型标注为 `Request`。
    request: Request,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 使用 `await` 等待异步操作 `svc.delete(config)` 完成，期间不会阻塞事件循环。
    await svc.delete(config)
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `getattr(request.app.state, "email_scheduler", None)` 完成当前解析、查询或对象构造，并把返回值保存到 `scheduler`；它表示挂载在 FastAPI app.state 上的 EmailScheduler。
        scheduler = getattr(request.app.state, "email_scheduler", None)
        # 使用 `if` 判断 `scheduler`；条件成立时执行下面分支。
        if scheduler:
            # 使用 `await` 等待异步操作 `scheduler.stop_task_for_config(config_id)` 完成，期间不会阻塞事件循环。
            await scheduler.stop_task_for_config(config_id)
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except Exception:
        # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
        pass
    # 从 `delete_email_config()` 返回 `{"message": "邮箱配置已删除"}`，并结束当前函数。
    return {"message": "邮箱配置已删除"}


# 将 POST /{config_id}/test 注册到下方处理函数。
@router.post("/{config_id}/test")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `test_email_connection()`。读取已保存的邮箱配置，用请求中的临时授权码执行 IMAP 登录测试；当前不会测试 SMTP。
async def test_email_connection(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明参数 `test_data`，表示连接测试请求中的临时邮箱参数，当前主要使用 password，类型标注为 `EmailConnectionTest`。
    test_data: EmailConnectionTest,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
    svc = EmailConfigService(db)
    # 使用 `await` 等待 `svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 使用 `await` 等待 `svc.test_connection(config, password=test_data.password)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `ok`；该结果用于当前连接、删除或业务操作是否成功的布尔结果。
    ok = await svc.test_connection(config, password=test_data.password)
    # 从 `test_email_connection()` 返回 `{"success": ok}`，并结束当前函数。
    return {"success": ok}


# 将 POST /{config_id}/fetch 注册到下方处理函数。
@router.post("/{config_id}/fetch")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `manual_fetch_emails()`。立即运行一次 IMAP 抓取，提交 EmailFetchLog，并向前端返回抓取数量和错误信息。
async def manual_fetch_emails(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `cfg_svc`；它表示负责邮箱配置查询和状态维护的 EmailConfigService。
    cfg_svc = EmailConfigService(db)
    # 调用 `EmailFetchService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `fetch_svc`；它表示负责收件扫描、附件处理和日志记录的 EmailFetchService。
    fetch_svc = EmailFetchService(db)
    # 使用 `await` 等待 `cfg_svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await cfg_svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 手动抓取与自动抓取使用同一套关键词过滤、邮件落库、附件保存和简历评价流程。
    raw_keywords = (config.subject_keywords or "").strip()
    keywords = [item.strip() for item in re.split(r"[,，]", raw_keywords) if item.strip()]
    if not keywords:
        keywords = ["简历", "招聘", "岗位", "职位", "求职", "应聘"]
    log = await fetch_svc.fetch_recent_attachments(
        config,
        config.created_by or current_user.id,
        limit=50,
        subject_keyword=keywords,
    )
    # 使用 `await` 等待异步操作 `db.commit()` 完成，期间不会阻塞事件循环。
    await db.commit()
    # 从 `manual_fetch_emails()` 返回 `{"message": "邮件抓取完成", "log": log.to_dict()}`，并结束当前函数。
    return {"message": "邮件抓取完成", "log": log.to_dict()}


# 将 GET /{config_id}/logs 注册到下方处理函数。
@router.get("/{config_id}/logs", response_model=EmailFetchLogList)
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `list_fetch_logs()`。按 config_id 分页读取抓取日志，供前端展示历史执行结果。
async def list_fetch_logs(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明局部变量 `skip` 的类型为 `int`，并用 `0,` 初始化；该变量用于分页时跳过的记录数量。
    skip: int = 0,
    # 声明局部变量 `limit` 的类型为 `int`，并用 `50,` 初始化；该变量用于本次查询或抓取允许返回的最大数量。
    limit: int = 50,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `cfg_svc`；它表示负责邮箱配置查询和状态维护的 EmailConfigService。
    cfg_svc = EmailConfigService(db)
    # 使用 `await` 等待 `cfg_svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await cfg_svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    # 调用 `EmailFetchService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `fetch_svc`；它表示负责收件扫描、附件处理和日志记录的 EmailFetchService。
    fetch_svc = EmailFetchService(db)
    # 使用 `await` 等待 `fetch_svc.list_logs(config_id, skip=skip, limit=limit)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `logs`；该结果用于指定邮箱配置的历史抓取日志列表。
    logs = await fetch_svc.list_logs(config_id, skip=skip, limit=limit)
    # 从 `list_fetch_logs()` 返回 `{"items": [log.to_dict() for log in logs]}`，并结束当前函数。
    return {"items": [log.to_dict() for log in logs]}


@router.get("/{config_id}/messages", response_model=FetchedEmailListResponse)
async def list_fetched_emails(
    config_id: str,
    skip: int = 0,
    limit: int = 100,
    current_user: UserSchema = Depends(get_current_superuser),
    db: AsyncSession = Depends(get_db),
) -> Any:
    """分页读取已经命中关键词并写入数据库的邮件。"""
    cfg_svc = EmailConfigService(db)
    if not await cfg_svc.get(config_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
    fetch_svc = EmailFetchService(db)
    items, total = await fetch_svc.list_fetched_emails(config_id, skip=skip, limit=limit)
    return {"items": items, "total": total, "skip": skip, "limit": limit}


# 将 GET /{config_id}/latest-email 注册到下方处理函数。
@router.get("/{config_id}/latest-email")
# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `get_latest_email()`。连接 IMAP 收件箱，按可选发件人筛选并返回最新邮件的主题、发件人、时间和正文摘要。
async def get_latest_email(
    # 声明参数 `config_id`，表示目标邮箱配置的 UUID，类型标注为 `str`。
    config_id: str,
    # 声明局部变量 `sender` 的类型为 `str | None`，并用 `None,` 初始化；该变量用于用于筛选最近邮件的发件人地址。
    sender: str | None = None,
    # 声明局部变量 `current_user` 的类型为 `UserSchema`，并用 `Depends(get_current_superuser),` 初始化；该变量用于JWT 鉴权后得到的当前登录用户。
    current_user: UserSchema = Depends(get_current_superuser),
    # 声明局部变量 `db` 的类型为 `AsyncSession`，并用 `Depends(get_db),` 初始化；该变量用于当前请求使用的异步数据库会话。
    db: AsyncSession = Depends(get_db),
# 结束函数参数列表，并用 `-> Any` 标注该函数的返回值类型。
) -> Any:
    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `cfg_svc`；它表示负责邮箱配置查询和状态维护的 EmailConfigService。
    cfg_svc = EmailConfigService(db)
    # 使用 `await` 等待 `cfg_svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `config`；该结果用于当前数据库邮箱配置或邮件协议连接配置。
    config = await cfg_svc.get(config_id)
    # 使用 `if` 判断 `not config`；条件成立时执行下面分支。
    if not config:
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="邮箱配置未找到")
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
    # 结束 `get_latest_email` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )
    # 调用 `EmailReader(reader_cfg)` 完成当前解析、查询或对象构造，并把返回值保存到 `reader`；它表示负责 IMAP/POP3 登录、搜索和解析的 EmailReader。
    reader = EmailReader(reader_cfg)
    # 使用 `if` 判断 `not reader.connect()`；条件成立时执行下面分支。
    if not reader.connect():
        # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无法连接到邮箱服务器")`，立即终止当前失败流程并交给上层处理。
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="无法连接到邮箱服务器")
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `reader.select_folder()`，让 IMAP 连接选择 INBOX 收件箱，后续 search/fetch 都在该文件夹执行。
        reader.select_folder("INBOX")
        # 调用 `reader.search_emails(["FROM", f'"{sender}"']) if sender else reader.search_emails(["ALL"]) or []` 完成当前解析、查询或对象构造，并把返回值保存到 `ids`；它表示IMAP 搜索返回的邮件编号列表。
        ids = reader.search_emails(["FROM", f'"{sender}"']) if sender else reader.search_emails(["ALL"]) or []
        # 使用 `if` 判断 `not ids`；条件成立时执行下面分支。
        if not ids:
            # 从 `get_latest_email()` 返回 `{"message": "邮箱中没有邮件"}`，并结束当前函数。
            return {"message": "邮箱中没有邮件"}
        # 计算表达式 `ids[-1]` 并把结果保存到 `msg_id`；该结果在后续流程中用作邮箱服务器中的单封邮件编号。
        msg_id = ids[-1]
        # 调用 `reader.get_email(msg_id)` 完成当前解析、查询或对象构造，并把返回值保存到 `msg`；它表示解析后的单封 EmailMessage。
        msg = reader.get_email(msg_id)
        # 使用 `if` 判断 `not msg`；条件成立时执行下面分支。
        if not msg:
            # 使用 `raise` 抛出 `HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="读取邮件失败")`，立即终止当前失败流程并交给上层处理。
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="读取邮件失败")
        # 计算表达式 `msg.body or msg.html_body or ""` 并把结果保存到 `body`；该结果在后续流程中用作待发送邮件的正文。
        body = msg.body or msg.html_body or ""
        # 计算表达式 `body[:2000]` 并把结果保存到 `snippet`；该结果在后续流程中用作限制为前 2000 字符的最新邮件正文摘要。
        snippet = body[:2000]
        # 从 `get_latest_email()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `subject` 字段为 `msg.subject`，用于表示待发送邮件的主题。
            "subject": msg.subject,
            # 在字典中设置 `sender` 字段为 `msg.sender`，用于表示用于筛选最近邮件的发件人地址。
            "sender": msg.sender,
            # 在字典中设置 `date` 字段为 `msg.date.isoformat()`，用于表示邮件发送时间。
            "date": msg.date.isoformat(),
            # 在字典中设置 `snippet` 字段为 `snippet`，用于表示限制为前 2000 字符的最新邮件正文摘要。
            "snippet": snippet,
        # 结束 `get_latest_email` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
    # 使用 `finally` 保证无论成功或失败都执行资源清理和状态恢复。
    finally:
        # 调用 `reader.disconnect()`，退出 IMAP/POP3 并释放网络连接，避免请求结束后连接泄漏。
        reader.disconnect()
