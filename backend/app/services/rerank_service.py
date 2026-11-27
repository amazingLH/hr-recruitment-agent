"""
* 文件作用
  * 这是 RAG 检索后的重排序服务，负责把第一阶段召回的候选文档与用户问题一起发送给 Qwen/DashScope 重排模型，取得更精确的相关性分数并截取最终 Top-K。类采用单例与延迟初始化减少模型客户端重复创建，在服务关闭、配置缺失或远程调用失败时保留原排序作为降级策略。该文件体现两阶段检索、外部模型 SDK 调用、分数回填、异常兜底和配置开关，是解释为什么“先多召回、再精排”能够提高 RAG 答案质量的关键。
* 类说明
  * `RerankService`：检索结果重排序服务，采用单例和延迟初始化复用 DashScope/Qwen 重排客户端。它接收问题与第一阶段召回文档，调用重排模型取得新的相关性分数，按分数降序排列并截取最终数量；配置关闭、没有候选文档或调用失败时返回原排序，保证 RAG 可以降级运行。
* 函数与方法说明
  * `RerankService.__init__()`：这是 `RerankService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。内部调用 self._initialize_model() 完成主要工作，最终主要通过修改对象状态、提交事务或调用下游服务产生效果。
  * `RerankService._initialize_model()`：根据配置初始化重排模型。
  * `RerankService._compute_rerank_scores()`：使用Qwen模型计算文档的重排分数。内部调用 self._compute_qwen_rerank_scores() 完成主要工作，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RerankService._compute_qwen_rerank_scores()`：通过DashScope API使用Qwen模型计算重排分数。
  * `RerankService.rerank_documents()`：使用Qwen模型根据查询相关性对文档进行重排 Args: query: 搜索查询 documents: LangChain文档列表 sources: 源元数据列表 top_k: 要返回的顶级结果数（默认为RERANK_FINAL_K） Returns: (重排文档, 重排源)的元组。
  * `RerankService.is_enabled()`：检查重排是否启用且模型可用。
  * `get_rerank_service()`：获取全局重排服务实例。内部调用 RerankService() 完成主要工作，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
"""

"""
检索结果重排服务：
    根据配置初始化 Qwen/DashScope相关重排能力，对 RAG检索出的文档重新打分并排序；
    如果重排不可用，会保留原始结果。get_rerank_service() 提供单例入口。
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 从 `typing` 导入当前模块需要的 `List, Dict, Any, Optional, Tuple`。
from typing import List, Dict, Any, Optional, Tuple
# 导入 `asyncio`，供后续代码调用其功能。
import asyncio
# 从 `concurrent.futures` 导入当前模块需要的 `ThreadPoolExecutor`。
from concurrent.futures import ThreadPoolExecutor

# 从 `langchain_core.documents` 导入当前模块需要的 `Document as LangChainDocument`。
from langchain_core.documents import Document as LangChainDocument

# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)

# 导入DashScope用于Qwen重排
# 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
try:
    # 导入 `dashscope`，供后续代码调用其功能。
    import dashscope
    # 从 `dashscope` 导入当前模块需要的 `TextReRank`。
    from dashscope import TextReRank
    # 计算或设置 `DASHSCOPE_AVAILABLE`，用于保存后续流程需要的中间结果。
    DASHSCOPE_AVAILABLE = True
# 捕获指定异常，避免底层错误未经处理直接中断请求。
except ImportError:
    # 计算或设置 `DASHSCOPE_AVAILABLE`，用于保存后续流程需要的中间结果。
    DASHSCOPE_AVAILABLE = False
    # 记录当前执行状态或异常信息，便于开发和运行排查。
    logger.warning("DashScope不可用，Qwen重排将无法工作")


# 定义 `RerankService` 类，集中封装相关数据或业务能力。
class RerankService:
    # 补充当前字符串、提示词或响应正文内容。
    """使用Qwen模型提高检索结果质量的重排服务"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self):
        # 计算或设置 `self.model`，用于保存调用的模型名称。
        self.model = None
        # 计算或设置 `self.executor`，用于保存后续流程需要的中间结果。
        self.executor = ThreadPoolExecutor(max_workers=2)
        # 调用当前对象的方法，推进本步骤的业务处理。
        self._initialize_model()

    # 定义 `_initialize_model()`，执行该函数对应的业务流程。
    def _initialize_model(self):
        # 补充当前字符串、提示词或响应正文内容。
        """根据配置初始化重排模型"""
        # 判断 `not settings.RERANK_ENABLED` 是否成立，以选择正确的处理分支。
        if not settings.RERANK_ENABLED:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info("配置中已禁用重排")
            # 结束当前函数，并把处理结果返回给调用方。
            return

        # 判断 `not DASHSCOPE_AVAILABLE` 是否成立，以选择正确的处理分支。
        if not DASHSCOPE_AVAILABLE:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error("DashScope不可用，Qwen重排无法工作")
            # 结束当前函数，并把处理结果返回给调用方。
            return

        # 判断 `not settings.QWEN_API_KEY` 是否成立，以选择正确的处理分支。
        if not settings.QWEN_API_KEY:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error("QWEN_API_KEY未设置，Qwen重排无法工作")
            # 结束当前函数，并把处理结果返回给调用方。
            return

        # 使用API密钥初始化DashScope
        # 计算或设置 `dashscope.api_key`，用于保存访问外部服务的鉴权密钥。
        dashscope.api_key = settings.QWEN_API_KEY
        # 计算或设置 `self.model`，用于保存调用的模型名称。
        self.model = "qwen"
        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info("Qwen重排模型初始化成功")

    # 定义 `_compute_rerank_scores()`，执行该函数对应的业务流程。
    def _compute_rerank_scores(self, query: str, documents: List[str]) -> List[float]:
        # 补充当前字符串、提示词或响应正文内容。
        """使用Qwen模型计算文档的重排分数"""
        # 判断 `not self.model` 是否成立，以选择正确的处理分支。
        if not self.model:
            # 如果模型不可用，返回原始顺序分数
            # 结束当前函数，并把处理结果返回给调用方。
            return [1.0 - i * 0.1 for i in range(len(documents))]

        # 处理Qwen重排
        # 结束当前函数，并把处理结果返回给调用方。
        return self._compute_qwen_rerank_scores(query, documents)

    # 定义 `_compute_qwen_rerank_scores()`，执行该函数对应的业务流程。
    def _compute_qwen_rerank_scores(self, query: str, documents: List[str]) -> List[float]:
        # 补充当前字符串、提示词或响应正文内容。
        """通过DashScope API使用Qwen模型计算重排分数"""
        # 判断 `not DASHSCOPE_AVAILABLE or not settings.QWEN_API_KEY` 是否成立，以选择正确的处理分支。
        if not DASHSCOPE_AVAILABLE or not settings.QWEN_API_KEY:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error("DashScope不可用或QWEN_API_KEY未设置")
            # 结束当前函数，并把处理结果返回给调用方。
            return [1.0 - i * 0.1 for i in range(len(documents))]

        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 调用DashScope TextReRank API
            # 开始组装多行调用参数、集合或对象内容。
            response = TextReRank.call(
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=settings.QWEN_MODEL,
                # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
                query=query,
                # 计算或设置 `documents`，用于保存候选文档或文档列表。
                documents=documents,
                # 计算或设置 `top_n`，用于保存后续流程需要的中间结果。
                top_n=len(documents),  # 返回所有带分数的文档
                # 计算或设置 `return_documents`，用于保存后续流程需要的中间结果。
                return_documents=True  # 返回带分数的文档
            # 结束上方多行参数、集合或对象的定义。
            )

            # 判断 `response.status_code != 200` 是否成立，以选择正确的处理分支。
            if response.status_code != 200:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.error(f"Qwen重排API调用失败，状态码 {response.status_code}: {response.message}")
                # 结束当前函数，并把处理结果返回给调用方。
                return [1.0 - i * 0.1 for i in range(len(documents))]

            # 从响应中提取分数
            # 响应应包含带分数的文档列表
            # 计算或设置 `results`，用于保存后续流程需要的中间结果。
            results = response.output.results
            # 计算或设置 `scores`，用于保存后续流程需要的中间结果。
            scores = [0.0] * len(documents)

            # 将分数映射回原始文档顺序
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for result in results:
                # 判断 `0 <= result.index < len(documents)` 是否成立，以选择正确的处理分支。
                if 0 <= result.index < len(documents):
                    # 计算或设置 `scores[result.index]`，用于保存后续流程需要的中间结果。
                    scores[result.index] = result.relevance_score

            # 结束当前函数，并把处理结果返回给调用方。
            return scores

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"计算Qwen重排分数时出错: {e}")
            # 返回备用分数
            # 结束当前函数，并把处理结果返回给调用方。
            return [1.0 - i * 0.1 for i in range(len(documents))]

    # 定义 `rerank_documents()`，执行该函数对应的业务流程。
    async def rerank_documents(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `query`，表示查询文本或数据库查询对象。
        query: str,
        # 声明参数或字段 `documents`，表示候选文档或文档列表。
        documents: List[LangChainDocument],
        # 声明参数或字段 `sources`，表示答案引用来源列表。
        sources: List[Dict[str, Any]],
        # 声明参数或字段 `top_k`，表示最终保留的高相关结果数量。
        top_k: Optional[int] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> Tuple[List[LangChainDocument], List[Dict[str, Any]]]:
        """
        使用Qwen模型根据查询相关性对文档进行重排

        Args:
            query: 搜索查询
            documents: LangChain文档列表
            sources: 源元数据列表
            top_k: 要返回的顶级结果数（默认为RERANK_FINAL_K）

        Returns:
            (重排文档, 重排源)的元组
        """
        # 判断 `not settings.RERANK_ENABLED or not self.model or not documents` 是否成立，以选择正确的处理分支。
        if not settings.RERANK_ENABLED or not self.model or not documents:
            # 结束当前函数，并把处理结果返回给调用方。
            return documents, sources

        # 判断 `top_k is None` 是否成立，以选择正确的处理分支。
        if top_k is None:
            # 计算或设置 `top_k`，用于保存最终保留的高相关结果数量。
            top_k = settings.RERANK_FINAL_K

        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 限制候选数量为RERANK_TOP_K以提高效率
            # 计算或设置 `max_candidates`，用于保存后续流程需要的中间结果。
            max_candidates = min(len(documents), settings.RERANK_TOP_K)
            # 计算或设置 `candidate_docs`，用于保存后续流程需要的中间结果。
            candidate_docs = documents[:max_candidates]
            # 计算或设置 `candidate_sources`，用于保存后续流程需要的中间结果。
            candidate_sources = sources[:max_candidates]

            # 提取文档文本用于重排
            # 计算或设置 `doc_texts`，用于保存后续流程需要的中间结果。
            doc_texts = [doc.page_content for doc in candidate_docs]

            # 在线程池中计算重排分数以避免阻塞
            # 计算或设置 `loop`，用于保存后续流程需要的中间结果。
            loop = asyncio.get_event_loop()
            # 开始组装多行调用参数、集合或对象内容。
            rerank_scores = await loop.run_in_executor(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                self.executor,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                self._compute_rerank_scores,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                query,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                doc_texts
            # 结束上方多行参数、集合或对象的定义。
            )

            # 将文档与其重排分数结合
            # 计算或设置 `doc_score_pairs`，用于保存后续流程需要的中间结果。
            doc_score_pairs = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for i, (doc, source, rerank_score) in enumerate(zip(candidate_docs, candidate_sources, rerank_scores)):
                # 存储原始分数以供比较
                original_score = source.get('combined_score', 0.0)

                # 使用重排信息更新源
                # 计算或设置 `updated_source`，用于保存后续流程需要的中间结果。
                updated_source = source.copy()
                # 开始组装多行调用参数、集合或对象内容。
                updated_source.update({
                    # 补充当前字符串、提示词或响应正文内容。
                    'rerank_score': float(rerank_score),
                    # 补充当前字符串、提示词或响应正文内容。
                    'original_score': float(original_score),
                    # 补充当前字符串、提示词或响应正文内容。
                    'rerank_enabled': True,
                    # 补充当前字符串、提示词或响应正文内容。
                    'rerank_model': settings.QWEN_MODEL
                # 结束上方多行参数、集合或对象的定义。
                })

                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                doc_score_pairs.append((doc, updated_source, float(rerank_score)))

            # 按重排分数降序排序
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            doc_score_pairs.sort(key=lambda x: x[2], reverse=True)

            # 提取top_k结果
            # 计算或设置 `top_pairs`，用于保存后续流程需要的中间结果。
            top_pairs = doc_score_pairs[:top_k]
            # 计算或设置 `reranked_docs`，用于保存后续流程需要的中间结果。
            reranked_docs = [pair[0] for pair in top_pairs]
            # 计算或设置 `reranked_sources`，用于保存后续流程需要的中间结果。
            reranked_sources = [pair[1] for pair in top_pairs]

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"重排了{len(candidate_docs)}个文档，返回前{len(reranked_docs)}个")

            # 结束当前函数，并把处理结果返回给调用方。
            return reranked_docs, reranked_sources

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"重排过程中出错: {e}")
            # 出错时返回原始结果
            # 结束当前函数，并把处理结果返回给调用方。
            return documents[:top_k], sources[:top_k]

    # 定义 `is_enabled()`，执行该函数对应的业务流程。
    def is_enabled(self) -> bool:
        # 补充当前字符串、提示词或响应正文内容。
        """检查重排是否启用且模型可用"""
        # 结束当前函数，并把处理结果返回给调用方。
        return settings.RERANK_ENABLED and self.model is not None


# 全局重排服务实例
# 计算或设置 `_rerank_service`，用于保存后续流程需要的中间结果。
_rerank_service = None


# 定义 `get_rerank_service()`，执行该函数对应的业务流程。
def get_rerank_service() -> RerankService:
    # 补充当前字符串、提示词或响应正文内容。
    """获取全局重排服务实例"""
    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
    global _rerank_service
    # 判断 `_rerank_service is None` 是否成立，以选择正确的处理分支。
    if _rerank_service is None:
        # 计算或设置 `_rerank_service`，用于保存后续流程需要的中间结果。
        _rerank_service = RerankService()
    # 结束当前函数，并把处理结果返回给调用方。
    return _rerank_service
