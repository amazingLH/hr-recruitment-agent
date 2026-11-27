"""
* 文件作用
  * 这是自然语言意图识别与页面路由服务。它先使用关键词和正则表达式快速识别固定 HR 意图，无法确定时调用 LLM 返回受限标签，再把意图映射到前端路由；知识问答类请求还可以调用 KBSelectionService 自动选择知识库。该服务用于低成本路由，不直接执行 JD、简历或考试业务。
* 类说明
  * `IntentService`：规则与 LLM 混合意图识别服务，并负责把识别结果映射到前端业务路由。
* 主要函数与方法说明
  * `IntentService.classify_intent_fast()`：使用固定关键词和正则表达式快速识别高频 HR 意图，不产生模型调用成本。
  * `IntentService.classify_intent_with_llm()`：规则无法确定时调用 LLM，并把输出限制到预定义意图标签集合。
  * `IntentService.classify_intent()`：先执行快速规则分类，未命中时再调用 LLM，形成规则优先、模型兜底。
  * `IntentService.get_route_for_intent()`：将意图转换为前端路由；知识问答意图可附带自动选择的知识库 ID。
  * `IntentService.route_query()`：串联意图识别与路由解析，返回前端可直接执行的导航结果。
"""

# 导入 logging，用于记录 Agent 规划、工具执行和降级异常。
import logging
# 导入类型注解：Dict 表示字典，Any 表示任意类型，Optional 表示可选类型。
from typing import Dict, Any, Optional
# 导入 UUID，用于使用 UUID 标识用户、会话和业务资源。
from uuid import UUID
# 本文件自身不直接查询数据库，但需要把 db 传给知识库选择服务。
from sqlalchemy.ext.asyncio import AsyncSession
# 导入项目封装的大模型调用服务。
# 当关键词无法判断意图时，本文件会通过它请求 LLM。
from app.services.llm_service import LLMService
# 导入知识库选择服务。
# 当用户意图是 kb_qa 时，本文件通过它自动选择最相关的知识库。
from app.services.kb_selection_service import KBSelectionService

# 创建当前模块的日志记录器。
# __name__ 表示当前 Python 文件的模块名，便于日志系统区分来源。
logger = logging.getLogger(__name__)

# 定义意图识别服务类。
# 类可以把“数据库会话、路由映射、关键词规则”等相关数据集中保存起来。
class IntentService:
    # 类的说明字符串，表示这个类负责意图分类和前端路由生成。
    """意图分类和路由服务。"""

    # 定义构造方法。
    # self 代表当前正在创建的 IntentService 对象，db 是调用方传入的数据库会话。
    # 异步数据库会话 AsyncSession是用来异步查询和操作数据库，这里主要作用如下：
        # 1、查询当前用户有哪些文档和知识库；
        # 2、获取文档与 knowledge_base_id 的关联；
        # 3、确保只从当前用户有权限访问的知识库中选择；
        # 4、根据选择结果查到对应的知识库 ID。
    # 使用 `def` 普通函数 定义 `__init__()`。
    def __init__(self, db: AsyncSession):
        # 把数据库会话保存到对象属性中。
        # 后续 get_route_for_intent() 创建 KBSelectionService 时会使用 self.db。
        self.db = db

        # 先把大模型服务设置为空。
        # 这样不会在每次只进行关键词匹配时都立即初始化 LLM 客户端。
        # 只有关键词匹配失败、确实需要 LLM 时，才会延迟创建 LLMService。
        self.llm_service = None # 这行代码本身就是在定义了一个属性llm_service为空

        # 创建“意图标签 -> 前端页面路由”的字典。
        # 字典的左侧是后端识别出来的业务意图，右侧是 Vue Router 使用的页面地址。
        self.intent_routes = {
            # jd 表示生成或编辑职位描述。
            "jd": "/recruitment/jd-generator",
            # resume_screening 表示简历筛选或候选人简历评价。
            "resume_screening": "/recruitment/resume-screening",
            # interview_plan 表示生成面试方案、面试流程或面试题。
            "interview_plan": "/recruitment/smart-interview",
            # exam_generate 表示生成试卷或进行考试相关操作。
            "exam_generate": "/training/exam-generator",
            # email_notification 表示候选人邮件、面试邀约或考试邀请。
            "email_notification": "/system/email-configs",
            # kb_qa 表示基于知识库进行问答。
            "kb_qa": "/assistant/qa",
        # 结束 `IntentService.__init__` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

        # 创建关键词规则列表，用于不调用 LLM 的快速分类。
        # 每个元素都是一个二元组：(意图标签, 关键词列表)。
        # 只要用户输入包含某个关键词，就返回对应的意图标签。
        self.keywords = [
            # 简历相关关键词命中时，判断为简历筛选意图。
            ("resume_screening", ["简历筛选", "筛选简历", "简历评分", "评价简历", "上传简历", "候选人筛选", "批量筛选"]),
            # JD 相关关键词命中时，判断为职位描述生成意图。
            ("jd", ["写jd", "生成jd", "职位描述", "岗位职责", "招聘jd", "job description", "JD"]),
            # 邮件相关关键词命中时，判断为邮件通知意图。
            ("email_notification", ["邮件通知", "邮件草稿", "写邮件", "发邮件", "发送邮件", "通知邮件", "邮件邀约", "邀约邮件", "面试邀约", "面试邀请邮件", "考试邀请", "考试邀请邮件", "通知候选人", "offer邮件"]),
            # 考试相关关键词命中时，判断为试卷生成意图。
            ("exam_generate", ["试卷", "生成试卷", "考试", "题库", "出题", "考试试卷", "自动阅卷"]),
            # 面试相关关键词命中时，判断为面试方案意图。
            ("interview_plan", ["面试方案", "面试流程", "面试题", "面试计划", "生成面试", "安排面试"]),
            # 知识库和问答相关关键词命中时，判断为知识库问答意图。
            ("kb_qa", ["知识库", "问答", "根据知识库", "查文档", "文档问答", "QA"]),
        # 结束 `IntentService.__init__` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ]

    # 定义快速关键词分类方法。
    # 这个方法不需要 await，因为字符串匹配是同步且很快的操作。
    def classify_intent_fast(self, query: str) -> Optional[str]:
        # 方法说明文档，解释参数和返回值。
        """
        使用关键词匹配快速识别用户意图。

        参数：
            query：用户在前端输入的自然语言文本。

        返回：
            如果匹配到关键词，返回意图标签字符串；
            如果没有匹配到，返回 None。
        """
        # 将用户输入转换为小写，降低英文大小写对匹配结果的影响。
        # 例如 JD、jd、Jd 都会被统一转换为小写形式。
        q = query.lower()

        # 遍历关键词规则列表。
        # 每次循环会把一个意图标签和它对应的关键词列表分别赋值给 intent、words。
        for intent, words in self.keywords:
            # 使用 `for` 循环 `w in words`，逐项处理集合数据。
            for w in words:
                # 将关键词也转成小写，再判断关键词是否出现在用户输入中。
                # in 是 Python 的包含判断运算符，返回 True 或 False。
                if w.lower() in q:
                    # 一旦命中关键词，立即返回当前意图。
                    # return 会结束整个函数，不再继续检查后面的规则。
                    return intent

        # 所有关键词都没有命中时，返回 None。
        # None 在这里表示“快速分类无法确定意图”。
        return None

    # 定义异步 LLM 分类方法。
    # async 表示这个方法内部可以使用 await 调用耗时的网络请求。
    async def classify_intent_with_llm(self, query: str) -> str:
        # 方法说明文档，解释这个方法的作用、参数和返回值。
        """
        使用大模型在预定义类别中判断用户意图。

        参数：
            query：用户在前端输入的自然语言文本。

        返回：
            一个已知的意图标签，例如 jd、kb_qa 或 exam_generate。
        """
        # 构造发送给大模型的提示词。
        # 这里要求模型只能返回预定义标签，减少模型自由发挥导致的解析问题。
        prompt = (
            # 第一段告诉模型它扮演的角色和任务。
            "你是HR系统的路由分类器。根据用户输入在以下意图中选择一个，并只返回对应标签：\n"
            # 下面每一行定义一个允许返回的意图标签及其含义。
            "- jd: 生成/撰写职位JD\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- resume_screening: 上传/筛选/评价/评分候选人简历\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- interview_plan: 生成面试方案/流程/题目\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- exam_generate: 生成考试试卷/出题/考试管理\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- email_notification: 生成候选人邮件通知/面试邀约/考试邀请\n"
            # 把该字符串作为提示词或输出文本的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
            "- kb_qa: 基于知识库的问答/查询文档\n\n"
            # 把用户原始输入插入提示词，让模型根据它进行分类。
            f"用户输入：{query}\n\n"
            # 再次约束模型只返回六个标签之一，不要返回解释性文字。
            "输出：仅返回上述六个标签之一，不要解释。"
        # 结束 `IntentService.classify_intent_with_llm` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )

        # try 用来捕获大模型请求过程中可能发生的异常。
        # 例如网络错误、API Key 错误、模型服务异常或响应格式异常。
        try:
            # 如果当前对象还没有创建 LLMService，就在第一次需要时创建它。
            if self.llm_service is None:
                # 初始化项目统一的大模型服务，并保存起来供后续请求复用。
                self.llm_service = LLMService()

            # 异步调用大模型生成分类结果。
            # await 会等待网络请求完成，同时允许异步服务器处理其他请求。
            result = await self.llm_service.generate_response(prompt)

            # 清除模型返回结果两端的空格，并转换为小写。
            # 例如“ JD\n”会变成“jd”。
            intent = result.strip().lower()

            # 检查模型返回的标签是否存在于预定义的路由字典中。
            if intent not in self.intent_routes:
                # 模型有时会返回“这是一个 jd 意图”等额外文字。
                # 下面开始根据返回文本中是否包含关键词进行兼容处理。
                if "jd" in intent:
                    # 返回文本包含 jd 时，统一归类为 jd。
                    return "jd"

                # 返回文本包含 resume、简历或候选人时，归类为简历筛选。
                if "resume" in intent or "简历" in intent or "候选人" in intent:
                    # 从 `IntentService.classify_intent_with_llm()` 返回 `"resume_screening"`，并结束当前函数。
                    return "resume_screening"

                # 返回文本包含 interview 或 面试时，归类为面试方案。
                if "interview" in intent or "面试" in intent:
                    # 从 `IntentService.classify_intent_with_llm()` 返回 `"interview_plan"`，并结束当前函数。
                    return "interview_plan"

                # 返回文本包含 exam、考试或试卷时，归类为考试生成。
                if "exam" in intent or "考试" in intent or "试卷" in intent:
                    # 从 `IntentService.classify_intent_with_llm()` 返回 `"exam_generate"`，并结束当前函数。
                    return "exam_generate"

                # 返回文本包含 email、mail、邮件或邀约时，归类为邮件通知。
                if "email" in intent or "mail" in intent or "邮件" in intent or "邀约" in intent:
                    # 从 `IntentService.classify_intent_with_llm()` 返回 `"email_notification"`，并结束当前函数。
                    return "email_notification"

                # 返回文本包含 kb、知识或问答时，归类为知识库问答。
                if "kb" in intent or "知识" in intent or "问答" in intent:
                    # 从 `IntentService.classify_intent_with_llm()` 返回 `"kb_qa"`，并结束当前函数。
                    return "kb_qa"

                # 如果以上所有兼容判断都失败，使用知识库问答作为安全默认值。
                return "kb_qa"

            # 如果模型直接返回了合法标签，直接返回该标签。
            return intent

        # 捕获所有异常，保证单次 LLM 故障不会让整个意图接口崩溃。
        except Exception as e:
            # 将异常内容写入日志，方便开发者排查模型服务问题。
            logger.error(f"LLM意图分类失败: {e}")

            # LLM 失败时返回知识库问答作为兜底路由。
            return "kb_qa"

    # 定义统一的意图分类入口。
    # 这个方法负责决定先走关键词分类还是 LLM 分类。
    async def classify_intent(self, query: str) -> str:
        # 方法说明文档，解释分类顺序。
        """
        对用户查询进行意图分类：先关键词匹配，失败后再使用 LLM。

        参数：
            query：用户输入的自然语言文本。

        返回：
            分类后的意图标签字符串。
        """
        # 如果 query 是空字符串，直接返回知识库问答意图。（如果有人绕过前端、直接调用后端接口并传入空字符串，后端才会返回 kb_qa）
        # if not query: 如果 query 没有有效内容（为空时），就执行下面的代码。
        if not query:
            # 从 `IntentService.classify_intent()` 返回 `"kb_qa"# kb_qa 是知识库问答意图的标签`，并结束当前函数。
            return "kb_qa"# kb_qa 是知识库问答意图的标签

        # 第一步：调用快速关键词分类方法。
        # 因为该方法是同步方法，所以这里不需要 await。
        intent = self.classify_intent_fast(query)

        # 如果关键词分类成功，intent 会是非空字符串。
        if intent:
            # 直接返回关键词分类结果，不再调用 LLM。
            return intent

        # 关键词无法判断时，调用异步 LLM 分类方法。
        # await 会等待大模型返回结果。
        return await self.classify_intent_with_llm(query)

    # 定义异步路由生成方法。
    # 该方法把意图标签转换为页面路由，并在需要时选择知识库。
    async def get_route_for_intent(self, intent: str, query: str, user_id: UUID) -> Dict[str, Any]:
        # 方法说明文档，解释参数和返回值。
        """
        根据意图生成前端路由信息。

        参数：
            intent：已经识别出的意图标签。
            query：用户的原始问题。
            user_id：当前登录用户的 UUID。

        返回：
            包含 intent、route、query 和 kb_id 的字典。
        """
        # 从意图路由字典中取出目标路由。
        # dict.get(key, default) 表示：key 存在就返回对应值，否则返回 default。
        # 这里的默认路由是知识库问答页面，避免未知意图导致前端无法跳转。
        # 举例说明：
            # 1、如果intent = "jd"（路由字典例表中有）,route = "/recruitment/jd-generator"。
            # 2、如果intent = "unknown"（路由字典例表中没有），self.intent_routes["kb_qa"]。
        # 出现kb_qa有三种情况：
            # 1、关键词直接识别为知识库问答；
            # 2、LLM 判断用户是在进行知识库问答；
            # 3、关键词和 LLM 都无法确定，系统兜底设置为 kb_qa。
        # 调用 `self.intent_routes.get(intent, self.intent_routes["kb_qa"])` 完成当前解析、查询或对象构造，并把返回值保存到 `route`；它表示该意图对应的前端页面路径，供前端跳转或展示下一步入口。
        route = self.intent_routes.get(intent, self.intent_routes["kb_qa"])

        # 先把知识库 ID 设置为空。
        # 非知识库任务不需要选择知识库，因此返回 None 是合理的。
        kb_id = None

        # 只有用户意图是 kb_qa 时，才需要自动选择知识库。
        if intent == "kb_qa":
            # try 用于捕获知识库查询过程中可能出现的异常。
            try:
                # 创建一个“知识库选择服务”对象，并把当前数据库会话 self.db 传给它。
                # 后面通过 selector 根据用户问题，从该用户可用的知识库中选择最合适的一个。
                selector = KBSelectionService(self.db)

                # 异步调用知识库选择方法。
                # question 是用户问题，user_id 用于限制只能访问当前用户的数据。
                # max_candidates=200 表示最多从 200 个候选知识库或相关项中进行选择。
                result = await selector.select_kb_for_question(
                    # 使用关键字参数语法，把 `query` 传入 `question` 参数；它表示`question` 在当前函数中承载的中间业务值。
                    question=query,
                    # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
                    user_id=user_id,
                    # 使用关键字参数语法，把 `200` 传入 `max_candidates` 参数；它表示`max_candidates` 在当前函数中承载的中间业务值。
                    max_candidates=200,
                # 结束 `IntentService.get_route_for_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
                )

                # 从知识库选择服务返回的字典中读取 knowledge_base_id。
                # knowledge_base_id 表示后端根据当前 query，从该用户可用的知识库中选出的目标知识库 ID。它返回前端后，问答页面会用这个 ID 指定检索哪个知识库。
                kb_id = result["knowledge_base_id"]

            # 捕获知识库选择异常，避免影响整个意图路由接口。
            except Exception as e:
                # 记录知识库选择失败原因，便于排查数据库或检索服务问题。
                logger.error(f"知识库选择失败: {e}")

                # 选择失败时保持 kb_id=None，但仍然返回问答页面路由。
                kb_id = None

        # 构造一个 Python 字典作为接口返回值。
        # FastAPI 会自动把这个字典转换为 JSON 响应。
        return {
            # 返回识别出的意图标签。
            "intent": intent,
            # 返回前端应该跳转的页面路径。
            "route": route,
            # 返回用户原始输入，前端可以把它带到目标页面继续使用。
            "query": query,
            # 返回自动选出的知识库 ID；非知识库任务通常为 None。
            "kb_id": kb_id
        # 结束 `IntentService.get_route_for_intent` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 定义完整的意图路由入口方法。
    # intent_router.py 主要调用的就是这个方法。
    async def route_query(self, query: str, user_id: UUID) -> Dict[str, Any]:
        # 方法说明文档，解释整个方法的执行流程。
        """
        对用户查询进行分类，并返回完整的前端路由信息。

        参数：
            query：用户输入的自然语言需求。
            user_id：当前登录用户的 UUID。

        返回：
            包含意图、路由、原始查询和知识库 ID 的字典。
        """
        # 如果用户没有输入内容，直接把它作为知识库问答处理。
        if not query:
            # await 等待异步路由生成方法完成。
            return await self.get_route_for_intent("kb_qa", query, user_id)

        # 调用统一分类方法，得到 jd、kb_qa 等意图标签。
        intent = await self.classify_intent(query)

        # 根据意图标签生成最终路由信息并返回。
        return await self.get_route_for_intent(intent, query, user_id)
