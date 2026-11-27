"""
* 文件作用
  * 这是邮箱自动抓取调度器。它使用 asyncio 为每个启用自动抓取的邮箱创建独立后台任务，按 fetch_interval 周期调用 EmailFetchService，支持配置新增/修改后的刷新、单任务停止以及应用关闭时统一清理。
* 类说明
  * `EmailScheduler`：基于 asyncio.Task 管理多个邮箱周期抓取循环的轻量调度器。
* 主要函数与方法说明
  * `EmailScheduler.__init__()`：初始化 config_id 到 asyncio.Task、asyncio.Event 的两个映射，用于管理任务与停止信号。
  * `EmailScheduler.start()`：应用启动时读取最多 1000 个邮箱配置，为 auto_fetch 且 active 的配置创建周期任务。
  * `EmailScheduler._run_fetch_loop()`：在停止信号出现前循环读取最新配置、解析主题关键词、抓取并自动评价附件，然后按分钟间隔等待下一次执行。
  * `EmailScheduler.shutdown()`：设置全部停止事件并取消任务，清空映射，供应用关闭时释放后台任务。
  * `EmailScheduler.stop_task_for_config()`：停止并移除指定 config_id 的抓取任务和停止事件。
  * `EmailScheduler.start_task_for_config()`：先清理旧任务，再为指定配置创建新的周期抓取 asyncio.Task。
  * `EmailScheduler.refresh_for_config()`：配置新增或修改后重建任务；只有 auto_fetch 开启且状态 active 才重新启动。
"""

# 导入 asyncio，用于组织异步流式任务和并发操作。
import asyncio
# 导入 re，用于通过正则识别意图、字段、候选人和邮件地址。
import re
# 导入 Dict，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Dict
# 导入 AsyncSessionLocal，用于取得 FastAPI 请求范围内的异步数据库会话。
from app.core.database import AsyncSessionLocal
# 导入 EmailConfig，用于查询或保存 EmailConfig 对应的 ORM 数据。
from app.models.email_config import EmailConfig
# 导入 `EmailConfigService`（邮箱配置 CRUD 与连接测试）和 `EmailFetchService`（收件抓取、附件保存与抓取日志），供接口层或调度器复用。
from app.services.email_service import EmailConfigService, EmailFetchService


# 使用 `class` 定义 `EmailScheduler`；该类的业务作用是：基于 asyncio.Task 管理多个邮箱周期抓取循环的轻量调度器。
class EmailScheduler:
    # 使用 `def` 普通函数 定义 `__init__()`。初始化 config_id 到 asyncio.Task、asyncio.Event 的两个映射，用于管理任务与停止信号。
    def __init__(self):
        # 通过类型注解声明实例字典 `self.tasks`，并初始化为空字典；键是邮箱配置 ID，值用于保存各邮箱配置对应的 asyncio 后台抓取任务。
        self.tasks: Dict[str, asyncio.Task] = {}
        # 通过类型注解声明实例字典 `self.stoppers`，并初始化为空字典；键是邮箱配置 ID，值用于保存各邮箱配置对应的停止事件。
        self.stoppers: Dict[str, asyncio.Event] = {}

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `start()`。应用启动时读取最多 1000 个邮箱配置，为 auto_fetch 且 active 的配置创建周期任务。
    async def start(self):
        # 使用上下文管理器 `async with AsyncSessionLocal() as db`，结束后自动释放文件或网络资源。
        async with AsyncSessionLocal() as db:
            # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `svc`；它表示当前接口使用的领域业务服务对象。
            svc = EmailConfigService(db)
            # 使用 `await` 等待 `svc.list(skip=0, limit=1000)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `configs`；该结果用于分页查询得到的邮箱配置列表。
            configs = await svc.list(skip=0, limit=1000)
        # 使用 `for` 循环 `cfg in configs`，逐项处理集合数据。
        for cfg in configs:
            # 使用 `if` 判断 `getattr(cfg, "auto_fetch", False) and getattr(cfg, "status", "active") == "active"`；条件成立时执行下面分支。
            if getattr(cfg, "auto_fetch", False) and getattr(cfg, "status", "active") == "active":
                # 调用 `max(1, int(getattr(cfg, "fetch_interval", 30) or 30))` 完成当前解析、查询或对象构造，并把返回值保存到 `interval_min`；它表示当前邮箱自动抓取的分钟间隔。
                interval_min = max(1, int(getattr(cfg, "fetch_interval", 30) or 30))
                # 调用 `asyncio.Event()` 完成当前解析、查询或对象构造，并把返回值保存到 `stopper`；它表示控制单个自动抓取循环退出的 asyncio.Event。
                stopper = asyncio.Event()
                # 执行字典或映射赋值 `self.stoppers[str(cfg.id)] = stopper`；业务作用是按邮箱配置 ID 保存停止事件，之后可精确终止该邮箱的自动抓取循环。
                self.stoppers[str(cfg.id)] = stopper
                # 使用 `asyncio.create_task()` 启动独立后台抓取协程，使接口无需等待整个周期任务。
                task = asyncio.create_task(self._run_fetch_loop(str(cfg.id), interval_min, stopper))
                # 执行字典或映射赋值 `self.tasks[str(cfg.id)] = task`；业务作用是按邮箱配置 ID 保存 asyncio 后台任务，之后可取消、重建或关闭该任务。
                self.tasks[str(cfg.id)] = task

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `_run_fetch_loop()`。在停止信号出现前循环读取最新配置、解析主题关键词、抓取并自动评价附件，然后按分钟间隔等待下一次执行。
    async def _run_fetch_loop(self, config_id: str, interval_min: int, stopper: asyncio.Event):
        # 使用 `while` 在 `not stopper.is_set()` 成立期间重复执行下面逻辑。
        while not stopper.is_set():
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 使用上下文管理器 `async with AsyncSessionLocal() as db`，结束后自动释放文件或网络资源。
                async with AsyncSessionLocal() as db:
                    # 调用 `EmailConfigService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `cfg_svc`；它表示负责邮箱配置查询和状态维护的 EmailConfigService。
                    cfg_svc = EmailConfigService(db)
                    # 调用 `EmailFetchService(db)` 完成当前解析、查询或对象构造，并把返回值保存到 `fetch_svc`；它表示负责收件扫描、附件处理和日志记录的 EmailFetchService。
                    fetch_svc = EmailFetchService(db)
                    # 使用 `await` 等待 `cfg_svc.get(config_id)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `cfg`；该结果用于当前自动抓取循环重新查询到的 EmailConfig ORM 记录。
                    cfg = await cfg_svc.get(config_id)
                    # 使用 `if` 判断 `cfg and getattr(cfg, "auto_fetch", False) and getattr(cfg, "status", "active") == "active"`；条件成立时执行下面分支。
                    if cfg and getattr(cfg, "auto_fetch", False) and getattr(cfg, "status", "active") == "active":
                        # 计算表达式 `(getattr(cfg, 'subject_keywords', '') or '').strip()` 并把结果保存到 `raw`；该结果在后续流程中用作从配置文件或邮件服务器读取的原始数据。
                        raw = (getattr(cfg, 'subject_keywords', '') or '').strip()
                        # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
                        print('subject_keywords:',raw)
                        # 使用 `if` 判断 `raw`；条件成立时执行下面分支。
                        if raw:
                            # 计算表达式 `[p.strip() for p in re.split(r"[,，]", raw) if p.strip()]` 并把结果保存到 `parts`；该结果在后续流程中用作按 SKILL.md 前置元数据分隔符切开的文本片段。
                            parts = [p.strip() for p in re.split(r"[,，]", raw) if p.strip()]
                            # 计算表达式 `parts if parts else ['简历']` 并把结果保存到 `kws`；该结果在后续流程中用作解析后的主题关键词列表。
                            kws = parts if parts else ['简历']
                        # 前面的 `if/elif` 条件均不成立时进入该备用分支。
                        else:
                            # 计算表达式 `['简历','招聘','岗位','职位']` 并把结果保存到 `kws`；该结果在后续流程中用作解析后的主题关键词列表。
                            kws = ['简历','招聘','岗位','职位']
                        # 使用 `await` 等待 `fetch_svc.fetch_recent_attachments(cfg, cfg.created_by, limit=10, subject_keyword=kws)` 的异步数据库、邮箱或业务操作完成，把结果保存到 `log`；该结果用于本次邮件抓取对应的 EmailFetchLog ORM 记录。
                        log = await fetch_svc.fetch_recent_attachments(cfg, cfg.created_by, limit=10, subject_keyword=kws)
                        # 使用 `await` 等待异步操作 `db.commit()` 完成，期间不会阻塞事件循环。
                        await db.commit()
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
                pass
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 等待停止事件；超过抓取间隔仍未停止时抛出 TimeoutError，从而进入下一轮邮件抓取。
                await asyncio.wait_for(stopper.wait(), timeout=interval_min * 60)
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except asyncio.TimeoutError:
                # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
                continue

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `shutdown()`。设置全部停止事件并取消任务，清空映射，供应用关闭时释放后台任务。
    async def shutdown(self):
        # 使用 `for` 循环 `stopper in self.stoppers.values()`，逐项处理集合数据。
        for stopper in self.stoppers.values():
            # 调用 `stopper.set()`，设置停止事件，通知对应自动抓取循环退出。
            stopper.set()
        # 使用 `for` 循环 `task in self.tasks.values()`，逐项处理集合数据。
        for task in self.tasks.values():
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用 `task.cancel()`，取消仍在运行或等待中的 asyncio 抓取任务。
                task.cancel()
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
                pass
        # 调用 `self.tasks.clear()`，清空全部邮箱任务引用。
        self.tasks.clear()
        # 调用 `self.stoppers.clear()`，清空全部任务停止事件引用。
        self.stoppers.clear()

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `stop_task_for_config()`。停止并移除指定 config_id 的抓取任务和停止事件。
    async def stop_task_for_config(self, config_id: str):
        # 调用 `self.stoppers.get(config_id)` 完成当前解析、查询或对象构造，并把返回值保存到 `stopper`；它表示控制单个自动抓取循环退出的 asyncio.Event。
        stopper = self.stoppers.get(config_id)
        # 使用 `if` 判断 `stopper`；条件成立时执行下面分支。
        if stopper:
            # 调用 `stopper.set()`，设置停止事件，通知对应自动抓取循环退出。
            stopper.set()
        # 调用 `self.tasks.get(config_id)` 完成当前解析、查询或对象构造，并把返回值保存到 `task`；它表示单个邮箱配置对应的 asyncio 后台抓取任务。
        task = self.tasks.get(config_id)
        # 使用 `if` 判断 `task`；条件成立时执行下面分支。
        if task:
            # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
            try:
                # 调用 `task.cancel()`，取消仍在运行或等待中的 asyncio 抓取任务。
                task.cancel()
            # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
            except Exception:
                # 使用 `pass` 保留当前语法分支，本行不执行额外操作。
                pass
        # 调用 `self.tasks.pop()`，从任务映射中移除指定邮箱配置。
        self.tasks.pop(config_id, None)
        # 调用 `self.stoppers.pop()`，从停止事件映射中移除指定邮箱配置。
        self.stoppers.pop(config_id, None)

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `start_task_for_config()`。先清理旧任务，再为指定配置创建新的周期抓取 asyncio.Task。
    async def start_task_for_config(self, config_id: str, interval_min: int):
        # 使用 `await` 等待异步操作 `self.stop_task_for_config(config_id)` 完成，期间不会阻塞事件循环。
        await self.stop_task_for_config(config_id)
        # 调用 `asyncio.Event()` 完成当前解析、查询或对象构造，并把返回值保存到 `stopper`；它表示控制单个自动抓取循环退出的 asyncio.Event。
        stopper = asyncio.Event()
        # 执行字典或映射赋值 `self.stoppers[config_id] = stopper`；业务作用是按邮箱配置 ID 保存停止事件，之后可精确终止该邮箱的自动抓取循环。
        self.stoppers[config_id] = stopper
        # 使用 `asyncio.create_task()` 启动独立后台抓取协程，使接口无需等待整个周期任务。
        task = asyncio.create_task(self._run_fetch_loop(config_id, interval_min, stopper))
        # 执行字典或映射赋值 `self.tasks[config_id] = task`；业务作用是按邮箱配置 ID 保存 asyncio 后台任务，之后可取消、重建或关闭该任务。
        self.tasks[config_id] = task

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `refresh_for_config()`。配置新增或修改后重建任务；只有 auto_fetch 开启且状态 active 才重新启动。
    async def refresh_for_config(self, config: EmailConfig):
        # 调用 `str(config.id)` 完成当前解析、查询或对象构造，并把返回值保存到 `cfg_id`；它表示用于关联对应数据库记录或业务资源的唯一标识。
        cfg_id = str(config.id)
        # 使用 `await` 等待异步操作 `self.stop_task_for_config(cfg_id)` 完成，期间不会阻塞事件循环。
        await self.stop_task_for_config(cfg_id)
        # 使用 `if` 判断 `getattr(config, "auto_fetch", False) and getattr(config, "status", "active") == "active"`；条件成立时执行下面分支。
        if getattr(config, "auto_fetch", False) and getattr(config, "status", "active") == "active":
            # 调用 `max(1, int(getattr(config, "fetch_interval", 30) or 30))` 完成当前解析、查询或对象构造，并把返回值保存到 `interval_min`；它表示当前邮箱自动抓取的分钟间隔。
            interval_min = max(1, int(getattr(config, "fetch_interval", 30) or 30))
            # 调用 `print()`，输出当前调试信息；生产环境更适合改用带级别的 logger。
            print('interval_min:%s' % interval_min)
            # 使用 `await` 等待异步操作 `self.start_task_for_config(cfg_id, interval_min)` 完成，期间不会阻塞事件循环。
            await self.start_task_for_config(cfg_id, interval_min)
