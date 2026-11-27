"""
* 文件作用
  * 这是 Agent 内部决策类型定义。AgentToolSpec 描述工具名、意图、页面路由、能力说明和前置条件；ReActDecision 描述模型受控决策结果，并把 action 限制为 chat、use_tool、ask_user，防止模型构造未注册动作。
* 类说明
  * `AgentToolSpec`：不可变工具规格，限制 Agent 只能在注册表声明的工具与前置条件范围内行动。
  * `ReActDecision`：受控 ReAct 决策对象，保存思考摘要、动作、意图、工具参数、观察结果和置信度。
* 主要函数与方法说明
"""

# 导入 dataclass，用于定义只保存数据、不承担业务执行的内部结构。
from dataclasses import dataclass
# 导入 List, Optional，用于声明参数、返回值、异步生成器和可选字段的类型。
from typing import List, Optional

# 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
"""
    共享数据类
"""

# 使用 `@dataclass` 把下方类声明为轻量数据对象，并禁止实例创建后修改字段；业务上用于稳定保存工具或 Skill 配置。
@dataclass(frozen=True)
# 使用 `class` 定义 `AgentToolSpec`；该类的业务作用是：不可变工具规格，限制 Agent 只能在注册表声明的工具与前置条件范围内行动。
class AgentToolSpec:
    """
    Agent 可规划调用的工具声明

    字段:
        name：工具名称，例如 generate_jd。
        intent：意图标签，例如 jd、resume_screening。
        route：前端应该跳转或关联的页面路由。
        description：工具能力说明。
        prerequisites：执行前置条件。
    """

    # 声明参数 `name`，表示工具注册表中的唯一工具名，受控 ReAct 只能选择这里声明的名称，类型标注为 `str`。
    name: str
    # 声明参数 `intent`，表示该工具处理的业务意图标签，类型标注为 `str`。
    intent: str
    # 声明参数 `route`，表示工具完成后对应的前端业务页面，类型标注为 `Optional[str]`。
    route: Optional[str]
    # 声明参数 `description`，表示告诉 ReAct 模型该工具能解决什么问题的说明，类型标注为 `str`。
    description: str
    # 声明参数 `prerequisites`，表示调用工具前必须存在的附件、JD 或候选人数据，类型标注为 `List[str]`。
    prerequisites: List[str]

# 使用 `@dataclass` 把下方类声明为轻量数据对象，并禁止实例创建后修改字段；业务上用于稳定保存工具或 Skill 配置。
@dataclass(frozen=True)
# 使用 `class` 定义 `ReActDecision`；该类的业务作用是：受控 ReAct 决策对象，保存思考摘要、动作、意图、工具参数、观察结果和置信度。
class ReActDecision:
    """
    ReAct 单轮决策结果:表示一次 Agent 决策结果
        这里的 ReAct 不是开放式工具调用，而是“受控 ReAct”：模型只能在固定动作空间里选，不能随意执行代码或调用未知工具。

    字段说明：
        mode：chat 或 tool。
        intent：识别出的业务意图。
        action：chat、use_tool、ask_user。
        thought：给产品侧展示的简短推理摘要。不承载模型的完整隐藏推理过程
        observation：当前观察或预期结果。
        confidence：置信度。
        reply：普通聊天时的自然语言回复。
        source：来源，例如 react_llm 或 fallback。
    """

    # 声明参数 `mode`，表示`mode` 在当前函数中承载的中间业务值，类型标注为 `str`。
    mode: str
    # 声明参数 `intent`，表示模型判断出的预定义业务意图，类型标注为 `str`。
    intent: str
    # 声明参数 `action`，表示被限制为 chat、use_tool 或 ask_user 的动作类型，类型标注为 `str`。
    action: str
    # 声明参数 `thought`，表示模型给出的简短决策摘要，不直接作为可执行代码，类型标注为 `str`。
    thought: str
    # 声明参数 `observation`，表示当前信息是否满足工具前置条件的说明，类型标注为 `str`。
    observation: str
    # 声明参数 `confidence`，表示模型对本次意图和动作判断的置信度，类型标注为 `Optional[float]`，默认值为 `None`。
    confidence: Optional[float] = None
    # 声明参数 `reply`，表示`reply` 在当前函数中承载的中间业务值，类型标注为 `Optional[str]`，默认值为 `None`。
    reply: Optional[str] = None
    # 声明参数 `source`，表示`source` 在当前函数中承载的中间业务值，类型标注为 `str`，默认值为 `"react"`。
    source: str = "react"
