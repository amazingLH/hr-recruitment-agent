"""
* 文件作用
  * 这是 Agent 的可插拔 Skill 加载与分发器。它扫描 backend/skills 下的 Skill 包，读取 SKILL.md 和 skill.json，把 draft、send 等阶段映射到 Python 脚本函数，并在运行时动态加载执行。SkillMetadata 描述能力，AgentSkillBundle 聚合阶段，AgentSkillDispatcher 根据 intent、phase 或确认动作选择实际脚本。
* 类说明
  * `SkillMetadata`：Skill 清单元数据，描述意图、路由、前置条件、阶段和确认动作。
  * `SkillPhaseDefinition`：Skill 单阶段定义，记录脚本文件与需要调用的函数。
  * `ScriptedSkillPhase`：动态加载并执行某个 Skill 阶段脚本，同时注入 SKILL.md 指令和上下文。
  * `AgentSkillBundle`：将同一 Skill 的元数据及多个阶段组合为一个可调度能力包。
  * `AgentSkillDispatcher`：按意图、阶段或 confirmation_action 查找并分发 Skill。
* 主要函数与方法说明
  * `ScriptedSkillPhase.skill_markdown_path()`：返回当前 Skill 包内 SKILL.md 的路径，供阶段执行前读取能力规则。
  * `ScriptedSkillPhase.load_skill_instructions()`：读取 SKILL.md 全文并交给阶段脚本；文件缺失时记录警告并返回空字符串，避免整个 Agent 启动失败。
  * `ScriptedSkillPhase.run()`：按照 skill.json 中的 script_path 和 function_name 动态加载 Python 脚本，注入业务上下文、Skill 目录与 SKILL.md 规则，调用阶段函数并兼容同步或异步返回值。
  * `AgentSkillBundle.get_phase()`：按阶段名称从当前 Skill 包取得 ScriptedSkillPhase；找不到时返回 None，由上层决定报错或降级。
  * `AgentSkillBundle.resolve_phase()`：根据用户 confirmation_action 选择 send 等确认阶段；未匹配确认动作时返回 manifest 声明的 default_phase。
  * `AgentSkillDispatcher.get_bundle()`：按业务意图从已加载映射中查找对应 Skill 包，使 Agent 可以把 email_notification 等意图交给插件处理。
  * `AgentSkillDispatcher.dispatch()`：取得意图对应的 Skill 包和目标阶段，执行该阶段脚本并返回结构化结果；意图或阶段不存在时抛出明确错误。
  * `AgentSkillDispatcher.match_confirmation_action()`：遍历所有 Skill 的 confirmation_actions，判断用户提交的 action 属于哪个意图和阶段，用于把确认后的请求接回原 Skill 流程。
  * `parse_skill_metadata()`：解析 SKILL.md 顶部 frontmatter，提取名称和描述；正文规则仍保留给阶段脚本读取。
  * `parse_skill_manifest()`：读取并校验 skill.json，将意图、阶段、路由和前置条件转换为 SkillMetadata。
  * `build_skill_bundle_from_directory()`：组合 Skill 元数据与每个 phase 的脚本定义，形成运行时可分发的 AgentSkillBundle。
  * `parse_phase_script_spec()`：读取 skill.json 中指定阶段的脚本相对路径和函数名，校验脚本存在后生成 SkillPhaseDefinition。
  * `build_default_skill_dispatcher()`：扫描 backend/skills 子目录，加载全部有效 Skill 并创建调度器。
"""

# 导入 annotations，用于启用延迟解析类型注解，避免前向类型引用在导入阶段报错。
from __future__ import annotations

# 导入 importlib.util，用于从 Skill 配置指定的 Python 文件动态加载阶段函数。
import importlib.util
# 导入 inspect，用于判断 Skill 阶段返回值是否需要异步等待。
import inspect
# 导入 logging，用于记录 Agent 规划、工具执行和降级异常。
import logging
# 导入 json，用于编码和解析 ReAct JSON、SSE 事件及模型结构化结果。
import json
# 导入 dataclass, field，用于定义只保存数据、不承担业务执行的内部结构。
from dataclasses import dataclass, field
# 导入 Path，用于以跨平台 Path 对象定位 Skill 目录和脚本文件。
from pathlib import Path
# 导入 Any, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import Any, Optional

# 调用 `logging.getLogger(__name__)` 完成当前解析、查询或对象构造，并把返回值保存到 `logger`；它表示当前模块的日志记录器，用于记录执行状态和异常。
logger = logging.getLogger(__name__)


# 使用 `@dataclass` 把下方类声明为轻量数据对象，并禁止实例创建后修改字段；业务上用于稳定保存工具或 Skill 配置。
@dataclass(frozen=True)
# 使用 `class` 定义 `SkillMetadata`；该类的业务作用是：Skill 清单元数据，描述意图、路由、前置条件、阶段和确认动作。
class SkillMetadata:
    # 声明参数 `name`，表示Skill 能力包名称，用于注册、日志和前端步骤展示，类型标注为 `str`。
    name: str
    # 声明参数 `description`，表示提供给 Agent 或 LLM 的 Skill 能力说明，类型标注为 `str`。
    description: str
    # 声明参数 `intent`，表示该 Skill 响应的 Agent 意图标签，类型标注为 `str`。
    intent: str
    # 声明参数 `route`，表示执行该 Skill 后建议前端跳转的页面路径，类型标注为 `Optional[str]`。
    route: Optional[str]
    # 声明参数 `prerequisites`，表示运行该 Skill 前必须满足的附件或业务数据条件，类型标注为 `tuple[str, ...]`。
    prerequisites: tuple[str, ...]
    # 声明参数 `phases`，表示该 Skill 声明的 draft、send 等可执行阶段，类型标注为 `tuple[str, ...]`。
    phases: tuple[str, ...]
    # 声明参数 `default_phase`，表示没有人工确认动作时默认运行的阶段，类型标注为 `str`。
    default_phase: str
    # 声明局部变量 `confirmation_action` 的类型为 `Optional[str]`，并用 `None` 初始化；该变量用于触发高风险操作继续执行的人工确认动作。
    confirmation_action: Optional[str] = None


# 使用 `@dataclass` 把下方类声明为轻量数据对象，并禁止实例创建后修改字段；业务上用于稳定保存工具或 Skill 配置。
@dataclass(frozen=True)
# 使用 `class` 定义 `SkillPhaseDefinition`；该类的业务作用是：Skill 单阶段定义，记录脚本文件与需要调用的函数。
class SkillPhaseDefinition:
    # 声明参数 `phase_name`，表示当前阶段名称，例如 draft 或 send，类型标注为 `str`。
    phase_name: str
    # 声明参数 `script_path`，表示该阶段实际执行的 Python 脚本路径，类型标注为 `Path`。
    script_path: Path
    # 声明参数 `function_name`，表示需要从脚本中加载并调用的入口函数名，类型标注为 `str`。
    function_name: str


# 使用 `class` 定义 `ScriptedSkillPhase`；该类的业务作用是：动态加载并执行某个 Skill 阶段脚本，同时注入 SKILL.md 指令和上下文。
class ScriptedSkillPhase:
    """技能阶段由技能包中的Python脚本执行."""

    # 使用 `def` 普通函数 定义 `__init__()`。
    def __init__(self, skill_dir: Path, definition: SkillPhaseDefinition):
        # 把 `skill_dir` 保存为实例属性 `self.skill_dir`，供同一 Agent 服务的其他方法复用；该属性表示当前 Skill 包所在目录。
        self.skill_dir = skill_dir
        # 把 `definition` 保存为实例属性 `self.definition`，供同一 Agent 服务的其他方法复用；该属性表示当前 Skill 阶段的名称、脚本路径和入口函数配置。
        self.definition = definition

    # 使用 `@property` 把下方无参方法暴露成只读属性，使调用方可像读取字段一样取得计算后的 Skill 文件路径。
    @property
    # 使用 `def` 普通函数 定义 `skill_markdown_path()`。返回当前 Skill 包内 SKILL.md 的路径，供阶段执行前读取能力规则。
    def skill_markdown_path(self) -> Path:
        # 从 `ScriptedSkillPhase.skill_markdown_path()` 返回 `self.skill_dir / "SKILL.md"`，并结束当前函数。
        return self.skill_dir / "SKILL.md"

    # 使用 `def` 普通函数 定义 `load_skill_instructions()`。读取 SKILL.md 全文并交给阶段脚本；文件缺失时记录警告并返回空字符串，避免整个 Agent 启动失败。
    def load_skill_instructions(self) -> str:
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 从 `ScriptedSkillPhase.load_skill_instructions()` 返回 `self.skill_markdown_path.read_text(encoding="utf-8").strip()`，并结束当前函数。
            return self.skill_markdown_path.read_text(encoding="utf-8").strip()
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except FileNotFoundError:
            # 通过模块日志记录器执行 `logger.warning("Skill 文件不存在: %s", self.skill_markdown_path)`，用于保留运行状态或异常原因。
            logger.warning("Skill 文件不存在: %s", self.skill_markdown_path)
            # 从 `ScriptedSkillPhase.load_skill_instructions()` 返回 `""`，并结束当前函数。
            return ""

    # 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `run()`。按照 skill.json 中的 script_path 和 function_name 动态加载 Python 脚本，注入业务上下文、Skill 目录与 SKILL.md 规则，调用阶段函数并兼容同步或异步返回值。
    async def run(self, context: dict[str, Any]) -> dict[str, Any]:
        # 计算表达式 `f"skill_{self.skill_dir.name}_{self.definition.phase_name}".replace("-", "_")` 并把结果保存到 `module_name`；该结果在后续流程中用作为动态加载 Skill 脚本生成的唯一 Python 模块名。
        module_name = f"skill_{self.skill_dir.name}_{self.definition.phase_name}".replace("-", "_")
        # 调用 `importlib.util.spec_from_file_location(module_name, self.definition.script_path)` 完成当前解析、查询或对象构造，并把返回值保存到 `spec`；它表示Python 根据脚本路径创建的动态模块加载规格。
        spec = importlib.util.spec_from_file_location(module_name, self.definition.script_path)
        # 使用 `if` 判断 `spec is None or spec.loader is None`；条件成立时执行下面分支。
        if spec is None or spec.loader is None:
            # 使用 `raise` 抛出 `RuntimeError(f"无法加载 skill 脚本: {self.definition.script_path}")`，立即终止当前失败流程并交给上层处理。
            raise RuntimeError(f"无法加载 skill 脚本: {self.definition.script_path}")
        # 调用 `importlib.util.module_from_spec(spec)` 完成当前解析、查询或对象构造，并把返回值保存到 `module`；它表示已经根据加载规格创建、准备执行的 Skill Python 模块对象。
        module = importlib.util.module_from_spec(spec)
        # 让 Python 加载器执行目标 Skill 脚本，把脚本中定义的 run_draft_phase() 或 run_send_phase() 等函数注册到动态模块对象中。
        spec.loader.exec_module(module)
        # 调用 `getattr(module, self.definition.function_name, None)` 完成当前解析、查询或对象构造，并把返回值保存到 `fn`；它表示从 Skill 脚本中按配置取得的阶段入口函数。
        fn = getattr(module, self.definition.function_name, None)
        # 使用 `if` 判断 `fn is None`；条件成立时执行下面分支。
        if fn is None:
            # 使用 `raise` 抛出 `RuntimeError(f"skill 脚本缺少函数 {self.definition.function_name}: {self.definition.script_path}")`，立即终止当前失败流程并交给上层处理。
            raise RuntimeError(f"skill 脚本缺少函数 {self.definition.function_name}: {self.definition.script_path}")

        # 使用字典字面量开始构造 `enriched_context`；该字典用于保存在原始上下文基础上补充 Skill 目录和规则文本后的执行参数，下面各行会继续写入具体业务字段。
        enriched_context = {
            # 使用 `**` 字典解包，把 `context` 中的全部键值作为关键字参数传给当前函数，避免逐个重复传参。
            **context,
            # 在字典中设置 `skill_dir` 字段为 `self.skill_dir`，用于表示当前 Skill 包所在目录。
            "skill_dir": self.skill_dir,
            # 在字典中设置 `skill_markdown` 字段为 `self.load_skill_instructions()`，用于表示SKILL.md 的完整规则文本。
            "skill_markdown": self.load_skill_instructions(),
            # 在字典中设置 `phase_name` 字段为 `self.definition.phase_name`，用于表示本次准备执行的 Skill 阶段名称。
            "phase_name": self.definition.phase_name,
        # 结束 `ScriptedSkillPhase.run` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }
        # 调用 `fn(enriched_context)` 完成当前解析、查询或对象构造，并把返回值保存到 `result`；它表示当前查询、解析或业务调用产生的结果。
        result = fn(enriched_context)
        # 使用 `if` 判断 `inspect.isawaitable(result)`；条件成立时执行下面分支。
        if inspect.isawaitable(result):
            # 检测到 Skill 阶段函数返回了 awaitable 后，通过 `await` 等待其异步执行完成，并用真实阶段结果替换原对象。
            result = await result
        # 使用 `if` 判断 `not isinstance(result, dict)`；条件成立时执行下面分支。
        if not isinstance(result, dict):
            # 使用 `raise` 抛出 `RuntimeError(f"skill phase 必须返回 dict: {self.definition.script_path}#{self.definition.function_nam...`，立即终止当前失败流程并交给上层处理。
            raise RuntimeError(f"skill phase 必须返回 dict: {self.definition.script_path}#{self.definition.function_name}")
        # 从 `ScriptedSkillPhase.run()` 返回 `result`，并结束当前函数。
        return result


# 使用 `@dataclass` 把下方类声明为轻量数据对象，并自动生成初始化和比较方法；业务上用于稳定保存工具或 Skill 配置。
@dataclass
# 使用 `class` 定义 `AgentSkillBundle`；该类的业务作用是：将同一 Skill 的元数据及多个阶段组合为一个可调度能力包。
class AgentSkillBundle:
    # 声明参数 `intent`，表示识别出的业务意图标签，类型标注为 `str`。
    intent: str
    # 声明参数 `bundle_name`，表示Skill 能力包名称，用于日志和执行步骤展示，类型标注为 `str`。
    bundle_name: str
    # 声明参数 `skill_dir`，表示当前 Skill 包所在目录，类型标注为 `Path`。
    skill_dir: Path
    # 声明参数 `metadata`，表示产物附加信息，例如数据库 ID、候选人和保存状态，类型标注为 `SkillMetadata`。
    metadata: SkillMetadata
    # 声明局部变量 `phases` 的类型为 `dict[str, ScriptedSkillPhase]`，并用 `field(default_factory=dict)` 初始化；该变量用于Skill 支持的 draft、send 等阶段集合或阶段对象映射。
    phases: dict[str, ScriptedSkillPhase] = field(default_factory=dict)

    # 使用 `def` 普通函数 定义 `get_phase()`。按阶段名称从当前 Skill 包取得 ScriptedSkillPhase；找不到时返回 None，由上层决定报错或降级。
    def get_phase(self, phase: str) -> ScriptedSkillPhase:
        # 使用 `if` 判断 `phase not in self.phases`；条件成立时执行下面分支。
        if phase not in self.phases:
            # 使用 `raise` 抛出 `KeyError(f"Skill bundle {self.bundle_name} 未注册 phase: {phase}")`，立即终止当前失败流程并交给上层处理。
            raise KeyError(f"Skill bundle {self.bundle_name} 未注册 phase: {phase}")
        # 从 `AgentSkillBundle.get_phase()` 返回 `self.phases[phase]`，并结束当前函数。
        return self.phases[phase]

    # 使用 `def` 普通函数 定义 `resolve_phase()`。根据用户 confirmation_action 选择 send 等确认阶段；未匹配确认动作时返回 manifest 声明的 default_phase。
    def resolve_phase(self, confirmed_requirements: Optional[dict[str, Any]] = None) -> str:
        # 调用 `str((confirmed_requirements or {}).get("action") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `action`；它表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
        action = str((confirmed_requirements or {}).get("action") or "").strip()
        # 使用 `if` 判断 `self.metadata.confirmation_action and action == self.metadata.confirmation_action and "send" in s...`；条件成立时执行下面分支。
        if self.metadata.confirmation_action and action == self.metadata.confirmation_action and "send" in self.phases:
            # 从 `AgentSkillBundle.resolve_phase()` 返回 `"send"`，并结束当前函数。
            return "send"
        # 从 `AgentSkillBundle.resolve_phase()` 返回 `self.metadata.default_phase`，并结束当前函数。
        return self.metadata.default_phase


# 使用 `class` 定义 `AgentSkillDispatcher`；该类的业务作用是：按意图、阶段或 confirmation_action 查找并分发 Skill。
class AgentSkillDispatcher:
    """根据 intent 和 phase 分发到 skill bundle。"""

    # 使用 `def` 普通函数 定义 `__init__()`。
    def __init__(self, bundles: dict[str, AgentSkillBundle]):
        # 把 `bundles` 保存为实例属性 `self.bundles`，供同一 Agent 服务的其他方法复用；该属性表示系统扫描并加载完成的全部 Skill 能力包映射。
        self.bundles = bundles

    # 使用 `def` 普通函数 定义 `get_bundle()`。按业务意图从已加载映射中查找对应 Skill 包，使 Agent 可以把 email_notification 等意图交给插件处理。
    def get_bundle(self, intent: str) -> AgentSkillBundle:
        # 使用 `if` 判断 `intent not in self.bundles`；条件成立时执行下面分支。
        if intent not in self.bundles:
            # 使用 `raise` 抛出 `KeyError(f"未找到 intent 对应的 skill bundle: {intent}")`，立即终止当前失败流程并交给上层处理。
            raise KeyError(f"未找到 intent 对应的 skill bundle: {intent}")
        # 从 `AgentSkillDispatcher.get_bundle()` 返回 `self.bundles[intent]`，并结束当前函数。
        return self.bundles[intent]

    # 使用 `def` 普通函数 定义 `dispatch()`。取得意图对应的 Skill 包和目标阶段，执行该阶段脚本并返回结构化结果；意图或阶段不存在时抛出明确错误。
    def dispatch(self, intent: str, phase: str) -> ScriptedSkillPhase:
        # 从 `AgentSkillDispatcher.dispatch()` 返回 `self.get_bundle(intent).get_phase(phase)`，并结束当前函数。
        return self.get_bundle(intent).get_phase(phase)

    # 使用 `def` 普通函数 定义 `match_confirmation_action()`。遍历所有 Skill 的 confirmation_actions，判断用户提交的 action 属于哪个意图和阶段，用于把确认后的请求接回原 Skill 流程。
    def match_confirmation_action(self, action: str) -> Optional[AgentSkillBundle]:
        # 使用 `for` 循环 `bundle in self.bundles.values()`，逐项处理集合数据。
        for bundle in self.bundles.values():
            # 使用 `if` 判断 `bundle.metadata.confirmation_action == action`；条件成立时执行下面分支。
            if bundle.metadata.confirmation_action == action:
                # 从 `AgentSkillDispatcher.match_confirmation_action()` 返回 `bundle`，并结束当前函数。
                return bundle
        # 从 `AgentSkillDispatcher.match_confirmation_action()` 返回 `None`，并结束当前函数。
        return None


# 使用 `def` 普通函数 定义 `parse_skill_metadata()`。解析 SKILL.md 顶部 frontmatter，提取名称和描述；正文规则仍保留给阶段脚本读取。
def parse_skill_metadata(skill_markdown_path: Path) -> Optional[dict[str, str]]:
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `skill_markdown_path.read_text(encoding="utf-8")` 完成当前解析、查询或对象构造，并把返回值保存到 `content`；它表示业务产物的实际内容，例如完整 JD、面试方案或试卷数据。
        content = skill_markdown_path.read_text(encoding="utf-8")
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except FileNotFoundError:
        # 从 `parse_skill_metadata()` 返回 `None`，并结束当前函数。
        return None

    # 使用 `if` 判断 `not content.startswith("---\n")`；条件成立时执行下面分支。
    if not content.startswith("---\n"):
        # 从 `parse_skill_metadata()` 返回 `None`，并结束当前函数。
        return None
    # 调用 `content.split("\n---\n", 1)` 完成当前解析、查询或对象构造，并把返回值保存到 `parts`；它表示按 SKILL.md 前置元数据分隔符切开的文本片段。
    parts = content.split("\n---\n", 1)
    # 使用 `if` 判断 `len(parts) != 2`；条件成立时执行下面分支。
    if len(parts) != 2:
        # 从 `parse_skill_metadata()` 返回 `None`，并结束当前函数。
        return None
    # 计算表达式 `parts[0].removeprefix("---\n")` 并把结果保存到 `frontmatter`；该结果在后续流程中用作SKILL.md 顶部保存名称和说明的元数据文本。
    frontmatter = parts[0].removeprefix("---\n")

    # 声明局部变量 `data` 的类型为 `dict[str, str]`，并用 `{}` 初始化；该变量用于当前 JSON、SKILL.md 元数据或业务对象解析得到的字典。
    data: dict[str, str] = {}
    # 使用 `for` 循环 `line in frontmatter.splitlines()`，逐项处理集合数据。
    for line in frontmatter.splitlines():
        # 调用 `line.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `stripped`；它表示去除首尾空白后的当前文本行，便于判断标题、正文或配置字段。
        stripped = line.strip()
        # 使用 `if` 判断 `not stripped or ":" not in stripped`；条件成立时执行下面分支。
        if not stripped or ":" not in stripped:
            # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
            continue
        # 使用 Python 序列解包，把 `stripped.split(":", 1)` 返回的多个结果分别赋给 `key, value`，供后续状态判断和数据解析。
        key, value = stripped.split(":", 1)
        # 执行字典或映射赋值 `data[key.strip()] = value.strip()`；业务作用是把解析后的配置键和值写入字典，供后续构造 Skill 元数据或邮箱发送配置。
        data[key.strip()] = value.strip()

    # 读取 SKILL.md frontmatter 中的 `name` 并去除首尾空白，得到 Skill 的注册名称。
    name = data.get("name", "").strip()
    # 调用 `data.get("description", "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `description`；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
    description = data.get("description", "").strip()
    # 使用 `if` 判断 `not name or not description`；条件成立时执行下面分支。
    if not name or not description:
        # 从 `parse_skill_metadata()` 返回 `None`，并结束当前函数。
        return None
    # 从 `parse_skill_metadata()` 返回 `{"name": name, "description": description}`，并结束当前函数。
    return {"name": name, "description": description}


# 使用 `def` 普通函数 定义 `parse_skill_manifest()`。读取并校验 skill.json，将意图、阶段、路由和前置条件转换为 SkillMetadata。
def parse_skill_manifest(skill_dir: Path) -> Optional[SkillMetadata]:
    # 计算表达式 `skill_dir / "skill.json"` 并把结果保存到 `manifest_path`；该结果在后续流程中用作当前 Skill 包中 skill.json 配置文件的路径。
    manifest_path = skill_dir / "skill.json"
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `json.loads(manifest_path.read_text(encoding="utf-8"))` 完成当前解析、查询或对象构造，并把返回值保存到 `data`；它表示当前 JSON、SKILL.md 元数据或业务对象解析得到的字典。
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except FileNotFoundError:
        # 从 `parse_skill_manifest()` 返回 `None`，并结束当前函数。
        return None
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except json.JSONDecodeError as exc:
        # 通过模块日志记录器执行 `logger.warning("Skill manifest 解析失败: %s", exc)`，用于保留运行状态或异常原因。
        logger.warning("Skill manifest 解析失败: %s", exc)
        # 从 `parse_skill_manifest()` 返回 `None`，并结束当前函数。
        return None

    # 优先读取 skill.json 的 `name`，缺失时使用 Skill 目录名，统一转成去除首尾空白的注册名称。
    name = str(data.get("name") or skill_dir.name).strip()
    # 调用 `str(data.get("intent") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `intent`；它表示识别出的业务意图标签。
    intent = str(data.get("intent") or "").strip()
    # 调用 `str(data.get("route") or "").strip() or None` 完成当前解析、查询或对象构造，并把返回值保存到 `route`；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
    route = str(data.get("route") or "").strip() or None
    # 调用 `tuple(str(item).strip() for item in data.get("phases") or [] if str(item).strip())` 完成当前解析、查询或对象构造，并把返回值保存到 `phases`；它表示Skill 支持的 draft、send 等阶段集合或阶段对象映射。
    phases = tuple(str(item).strip() for item in data.get("phases") or [] if str(item).strip())
    # 调用 `tuple(str(item).strip() for item in data.get("prerequisites") or [] if str(item).strip())` 完成当前解析、查询或对象构造，并把返回值保存到 `prerequisites`；它表示执行 Skill 前必须满足的前置条件列表。
    prerequisites = tuple(str(item).strip() for item in data.get("prerequisites") or [] if str(item).strip())
    # 调用 `str(data.get("default_phase") or "").strip() or (phases[0] if phases else "")` 完成当前解析、查询或对象构造，并把返回值保存到 `default_phase`；它表示未明确指定确认动作时默认执行的 Skill 阶段。
    default_phase = str(data.get("default_phase") or "").strip() or (phases[0] if phases else "")
    # 调用 `str(data.get("confirmation_action") or "").strip() or None` 完成当前解析、查询或对象构造，并把返回值保存到 `confirmation_action`；它表示触发高风险操作继续执行的人工确认动作。
    confirmation_action = str(data.get("confirmation_action") or "").strip() or None
    # 调用 `data.get("phase_scripts") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `phase_scripts_raw`；它表示skill.json 中尚未转换成阶段对象的脚本映射配置。
    phase_scripts_raw = data.get("phase_scripts") or {}
    # 使用 `if` 判断 `not name or not intent or not phases or not default_phase or not isinstance(phase_scripts_raw, dict)`；条件成立时执行下面分支。
    if not name or not intent or not phases or not default_phase or not isinstance(phase_scripts_raw, dict):
        # 从 `parse_skill_manifest()` 返回 `None`，并结束当前函数。
        return None

    # 把 `description` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入Skill 或工具的功能说明，供注册、匹配和模型决策使用。
    description = ""
    # 调用 `parse_skill_metadata(skill_dir / "SKILL.md")` 完成当前解析、查询或对象构造，并把返回值保存到 `skill_md`；它表示从 SKILL.md 读取并解析出的能力名称、说明和执行规则。
    skill_md = parse_skill_metadata(skill_dir / "SKILL.md")
    # 使用 `if` 判断 `skill_md`；条件成立时执行下面分支。
    if skill_md:
        # 计算表达式 `skill_md["description"]` 并把结果保存到 `description`；该结果在后续流程中用作Skill 或工具的功能说明，供注册、匹配和模型决策使用。
        description = skill_md["description"]
    # 从 `parse_skill_manifest()` 返回 `SkillMetadata(`，并结束当前函数。
    return SkillMetadata(
        # 把解析完成的 Skill 名称传入 `SkillMetadata.name`，供日志、注册和调试展示。
        name=name,
        # 使用关键字参数语法，把 `description` 传入 `description` 参数；它表示Skill 或工具的功能说明，供注册、匹配和模型决策使用。
        description=description,
        # 使用关键字参数语法，把 `intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
        intent=intent,
        # 使用关键字参数语法，把 `route` 传入 `route` 参数；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
        route=route,
        # 使用关键字参数语法，把 `prerequisites` 传入 `prerequisites` 参数；它表示执行 Skill 前必须满足的前置条件列表。
        prerequisites=prerequisites,
        # 使用关键字参数语法，把 `phases` 传入 `phases` 参数；它表示Skill 支持的 draft、send 等阶段集合或阶段对象映射。
        phases=phases,
        # 使用关键字参数语法，把 `default_phase` 传入 `default_phase` 参数；它表示未明确指定确认动作时默认执行的 Skill 阶段。
        default_phase=default_phase,
        # 使用关键字参数语法，把 `confirmation_action` 传入 `confirmation_action` 参数；它表示触发高风险操作继续执行的人工确认动作。
        confirmation_action=confirmation_action,
    # 结束 `parse_skill_manifest` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 使用 `def` 普通函数 定义 `build_skill_bundle_from_directory()`。组合 Skill 元数据与每个 phase 的脚本定义，形成运行时可分发的 AgentSkillBundle。
def build_skill_bundle_from_directory(skill_dir: Path) -> Optional[AgentSkillBundle]:
    # 调用 `parse_skill_manifest(skill_dir)` 完成当前解析、查询或对象构造，并把返回值保存到 `metadata`；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
    metadata = parse_skill_manifest(skill_dir)
    # 使用 `if` 判断 `not metadata`；条件成立时执行下面分支。
    if not metadata:
        # 从 `build_skill_bundle_from_directory()` 返回 `None`，并结束当前函数。
        return None

    # 声明局部变量 `phases` 的类型为 `dict[str, ScriptedSkillPhase]`，并用 `{}` 初始化；该变量用于Skill 支持的 draft、send 等阶段集合或阶段对象映射。
    phases: dict[str, ScriptedSkillPhase] = {}
    # 使用 `for` 循环 `phase in metadata.phases`，逐项处理集合数据。
    for phase in metadata.phases:
        # 调用 `parse_phase_script_spec(skill_dir, phase)` 完成当前解析、查询或对象构造，并把返回值保存到 `phase_spec`；它表示当前阶段解析出的脚本路径和入口函数配置。
        phase_spec = parse_phase_script_spec(skill_dir, phase)
        # 使用 `if` 判断 `not phase_spec`；条件成立时执行下面分支。
        if not phase_spec:
            # 通过模块日志记录器执行 `logger.warning("Skill %s 缺少 phase 脚本定义: %s", metadata.name, phase)`，用于保留运行状态或异常原因。
            logger.warning("Skill %s 缺少 phase 脚本定义: %s", metadata.name, phase)
            # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
            continue
        # 执行字典或映射赋值 `phases[phase] = ScriptedSkillPhase(skill_dir=skill_dir, definition=phase_spec)`；业务作用是把当前阶段名映射到动态加载的执行对象，供 Dispatcher 按 draft/send 阶段调用。
        phases[phase] = ScriptedSkillPhase(skill_dir=skill_dir, definition=phase_spec)

    # 使用 `if` 判断 `not phases`；条件成立时执行下面分支。
    if not phases:
        # 从 `build_skill_bundle_from_directory()` 返回 `None`，并结束当前函数。
        return None
    # 从 `build_skill_bundle_from_directory()` 返回 `AgentSkillBundle(`，并结束当前函数。
    return AgentSkillBundle(
        # 使用关键字参数语法，把 `metadata.intent` 传入 `intent` 参数；它表示识别出的业务意图标签。
        intent=metadata.intent,
        # 使用关键字参数语法，把 `metadata.name` 传入 `bundle_name` 参数；它表示Skill 能力包名称，用于日志和执行步骤展示。
        bundle_name=metadata.name,
        # 使用关键字参数语法，把 `skill_dir` 传入 `skill_dir` 参数；它表示当前 Skill 包所在目录。
        skill_dir=skill_dir,
        # 使用关键字参数语法，把 `metadata` 传入 `metadata` 参数；它表示产物附加信息，例如数据库 ID、候选人和保存状态。
        metadata=metadata,
        # 使用关键字参数语法，把 `phases` 传入 `phases` 参数；它表示Skill 支持的 draft、send 等阶段集合或阶段对象映射。
        phases=phases,
    # 结束 `build_skill_bundle_from_directory` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 使用 `def` 普通函数 定义 `parse_phase_script_spec()`。读取 skill.json 中指定阶段的脚本相对路径和函数名，校验脚本存在后生成 SkillPhaseDefinition。
def parse_phase_script_spec(skill_dir: Path, phase: str) -> Optional[SkillPhaseDefinition]:
    # 计算表达式 `skill_dir / "skill.json"` 并把结果保存到 `manifest_path`；该结果在后续流程中用作当前 Skill 包中 skill.json 配置文件的路径。
    manifest_path = skill_dir / "skill.json"
    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 调用 `json.loads(manifest_path.read_text(encoding="utf-8"))` 完成当前解析、查询或对象构造，并把返回值保存到 `data`；它表示当前 JSON、SKILL.md 元数据或业务对象解析得到的字典。
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except FileNotFoundError:
        # 从 `parse_phase_script_spec()` 返回 `None`，并结束当前函数。
        return None
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except json.JSONDecodeError:
        # 从 `parse_phase_script_spec()` 返回 `None`，并结束当前函数。
        return None

    # 调用 `data.get("phase_scripts") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `phase_scripts`；它表示Skill 每个阶段到脚本规格的映射。
    phase_scripts = data.get("phase_scripts") or {}
    # 调用 `str(phase_scripts.get(phase) or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `raw`；它表示从配置文件或邮件服务器读取的原始数据。
    raw = str(phase_scripts.get(phase) or "").strip()
    # 使用 `if` 判断 `not raw or ":" not in raw`；条件成立时执行下面分支。
    if not raw or ":" not in raw:
        # 从 `parse_phase_script_spec()` 返回 `None`，并结束当前函数。
        return None
    # 使用 Python 序列解包，把 `raw.split(":", 1)` 返回的多个结果分别赋给 `rel_path, function_name`，供后续状态判断和数据解析。
    rel_path, function_name = raw.split(":", 1)
    # 从 `parse_phase_script_spec()` 返回 `SkillPhaseDefinition(`，并结束当前函数。
    return SkillPhaseDefinition(
        # 使用关键字参数语法，把 `phase` 传入 `phase_name` 参数；它表示本次准备执行的 Skill 阶段名称。
        phase_name=phase,
        # 使用关键字参数语法，把 `skill_dir / rel_path.strip()` 传入 `script_path` 参数；它表示skill.json 指定的 Python 阶段脚本绝对路径。
        script_path=skill_dir / rel_path.strip(),
        # 使用关键字参数语法，把 `function_name.strip()` 传入 `function_name` 参数；它表示skill.json 指定的阶段入口函数名称。
        function_name=function_name.strip(),
    # 结束 `parse_phase_script_spec` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 使用 `def` 普通函数 定义 `build_default_skill_dispatcher()`。扫描 backend/skills 子目录，加载全部有效 Skill 并创建调度器。
def build_default_skill_dispatcher(skills_root: Optional[Path] = None) -> AgentSkillDispatcher:
    """自动扫描 skills 目录，构建当前仓库默认的 skill 调度器。"""
    # 调用 `Path(skills_root) if skills_root else Path(__file__).resolve().parents[2] / "skills"` 完成当前解析、查询或对象构造，并把返回值保存到 `root`；它表示准备扫描 Skill 子目录的 backend/skills 根路径。
    root = Path(skills_root) if skills_root else Path(__file__).resolve().parents[2] / "skills"
    # 声明局部变量 `bundles` 的类型为 `dict[str, AgentSkillBundle]`，并用 `{}` 初始化；该变量用于系统扫描并加载完成的全部 Skill 能力包映射。
    bundles: dict[str, AgentSkillBundle] = {}
    # 使用 `if` 判断 `not root.exists()`；条件成立时执行下面分支。
    if not root.exists():
        # 从 `build_default_skill_dispatcher()` 返回 `AgentSkillDispatcher(bundles)`，并结束当前函数。
        return AgentSkillDispatcher(bundles)

    # 使用 `for` 循环 `skill_dir in sorted(path for path in root.iterdir() if path.is_dir())`，逐项处理集合数据。
    for skill_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        # 调用 `build_skill_bundle_from_directory(skill_dir)` 完成当前解析、查询或对象构造，并把返回值保存到 `bundle`；它表示一个 Skill 的元数据和全部执行阶段。
        bundle = build_skill_bundle_from_directory(skill_dir)
        # 使用 `if` 判断 `not bundle`；条件成立时执行下面分支。
        if not bundle:
            # 使用 `continue` 跳过当前循环项，继续处理下一项数据。
            continue
        # 执行字典或映射赋值 `bundles[bundle.intent] = bundle`；业务作用是按业务意图注册 Skill 能力包，供 Agent 根据 intent 查找对应 Skill。
        bundles[bundle.intent] = bundle
    # 从 `build_default_skill_dispatcher()` 返回 `AgentSkillDispatcher(bundles)`，并结束当前函数。
    return AgentSkillDispatcher(bundles)
