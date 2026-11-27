"""
* 文件作用
  * 这是知识问答模块最核心的 RAG 编排文件，完整实现“是否需要知识库—查询改写—关键词与向量多路召回—分数归一化和融合—可选重排—拼接上下文—LLM 流式生成”的链路。它结合 PostgreSQL 全文检索、PGVector 向量相似度、Embedding、LangChain Prompt/Chain、ChatOpenAI 兼容模型和 RerankService，并通过 metadata 中的 knowledge_base_id、user_id 限制检索范围。学习时不仅要看最终 ask_question_stream，还要理解每个私有辅助函数怎样解决召回率、相关性、权限隔离、上下文长度和无知识库闲聊等问题。
* 类说明
  * `RAGService`：知识问答的核心 RAG 编排类，实例中组合数据库会话、Embedding、重排服务和聊天模型。它负责问题是否需要知识库的判断、LLM 查询改写、关键词提取、PGVector 向量召回、PostgreSQL tsvector 全文召回、分数归一化与融合、可选 Rerank、Prompt 拼接以及答案流式生成；无须知识库的问题会转入普通聊天链。
* 函数与方法说明
  * `RAGService.__init__()`：这是 `RAGService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。内部调用 get_embedding_service()、self.embedding_service.get_embeddings()、get_rerank_service() 完成主要工作，使用 OpenAI 兼容大模型调用，最终主要通过修改对象状态、提交事务或调用下游服务产生效果。
  * `RAGService._normalize_scores()`：对单路检索分数执行 Min-Max 归一化，把不同检索器产生的分数转换到可比较范围；当所有分数相同时使用稳定的统一值，避免除零。
  * `RAGService._extract_keyword_terms()`：提取用于关键词召回的中英文术语。内部调用 re.findall() 完成主要工作，使用 正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RAGService._enhance_query_for_kb()`：基于LLM的语义增强：重写查询并提供扩展关键词。 返回: { "rewritten_query": str, "expanded_keywords": List[str] }。内部调用 ChatPromptTemplate.from_messages()、ChatOpenAI()、StrOutputParser() 完成主要工作，使用 OpenAI 兼容大模型调用、LangChain Prompt 模板、JSON 序列化/反序列化、正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RAGService._tsvector_search()`：基于langchain_pg_embedding.document的PostgreSQL全文检索（tsvector）。 - 集合名严格过滤：cmetadata->>'collection_name' - 英文和数字使用 OR 前缀 tsquery - 中文及扩展关键词分别使用 ILIKE，按关键词命中比例排序 返回 (LangChainDocument, score) 列表，与向量检索输出格式一致。
  * `RAGService._merge_docs_with_scores()`：把向量召回与 PostgreSQL 全文检索结果按文档块身份去重，分别计算内容分数和文本分数，再按配置权重或 RRF 融合；过滤低分结果后可调用重排服务，并截取最终 Top-K。
  * `RAGService._create_rag_chain_with_docs()`：创建带预检索文档的RAG链。内部调用 ChatPromptTemplate.from_messages()、StrOutputParser() 完成主要工作，使用 LangChain Prompt 模板、正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RAGService._create_rag_chain_with_docs.format_docs()`：RAG 链内部的上下文格式化函数，按检索排序遍历 LangChain Document 列表，提取每个文档块的 page_content，并用分隔符拼接成一段上下文文本；该文本随后填入 Prompt 的 context 位置，使聊天模型能够依据召回资料生成答案。
  * `RAGService._should_use_knowledge_base()`：决定用户问题是否应使用知识库检索。 如果返回True则应使用KB，否则返回False。内部调用 re.sub()、ChatOpenAI() 完成主要工作，使用 OpenAI 兼容大模型调用、正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RAGService._create_general_chat_chain()`：不带KB上下文的通用聊天链。内部调用 ChatPromptTemplate.from_messages()、StrOutputParser() 完成主要工作，使用 LangChain Prompt 模板、正则表达式文本处理，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `RAGService.ask_question_stream()`：执行完整流式 RAG 问答：先判断问题是否需要知识库，必要时用 LLM 改写问题和扩展关键词，同时进行向量检索与全文检索，融合并重排候选块，构造带来源的 Prompt，最后通过异步生成器逐块输出答案和引用来源。
  * `RAGService.get_conversation_context()`：处理对话历史以获取上下文 Args: conversation_history: 对话消息列表 max_messages: 要保留的最大消息数 Returns: 处理后的对话历史。
"""

"""
RAG检索增强生成服务：
    使用LangChain、PGVector、ChatOpenAI、rerank 服务和 PostgreSQL全文检索，
    把问题扩展、向量检索、关键词检索、结果合并重排、上下文拼接、最终回答串起来。若判断问题不需要知识库，也能走普通聊天链路。
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 导入 `re`，供后续代码调用其功能。
import re
# 从 `typing` 导入当前模块需要的 `List, Dict, Any, Optional`。
from typing import List, Dict, Any, Optional
# 从 `uuid` 导入当前模块需要的 `UUID`。
from uuid import UUID
# 从 `sqlalchemy.ext.asyncio` 导入当前模块需要的 `AsyncSession`。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy` 导入当前模块需要的 `text`。
from sqlalchemy import text

# LangChain导入
# 从 `langchain_core.documents` 导入当前模块需要的 `Document as LangChainDocument`。
from langchain_core.documents import Document as LangChainDocument
# 从 `langchain_core.prompts` 导入当前模块需要的 `ChatPromptTemplate, MessagesPlaceholder`。
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
# 从 `langchain_core.output_parsers` 导入当前模块需要的 `StrOutputParser`。
from langchain_core.output_parsers import StrOutputParser
# 从 `langchain_core.runnables` 导入当前模块需要的 `RunnablePassthrough`。
from langchain_core.runnables import RunnablePassthrough
# 从 `langchain_postgres` 导入当前模块需要的 `PGVector`。
from langchain_postgres import PGVector
# 从 `langchain_openai` 导入当前模块需要的 `ChatOpenAI`。
from langchain_openai import ChatOpenAI

# 从 `app.services.embedding_service` 导入当前模块需要的 `get_embedding_service`。
from app.services.embedding_service import get_embedding_service
# 从 `app.services.rerank_service` 导入当前模块需要的 `get_rerank_service`。
from app.services.rerank_service import get_rerank_service
# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `RAGService` 类，集中封装相关数据或业务能力。
class RAGService:
    # 补充当前字符串、提示词或响应正文内容。
    """实现LangChain标准工作流的RAG服务"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self, db: AsyncSession):
        # 计算或设置 `self.db`，用于保存异步数据库会话。
        self.db = db

        # 初始化嵌入服务
        # 计算或设置 `self.embedding_service`，用于保存后续流程需要的中间结果。
        self.embedding_service = get_embedding_service()
        # 计算或设置 `self.embeddings`，用于保存后续流程需要的中间结果。
        self.embeddings = self.embedding_service.get_embeddings()

        # 初始化重排服务
        # 计算或设置 `self.rerank_service`，用于保存后续流程需要的中间结果。
        self.rerank_service = get_rerank_service()

        # 初始化LLM
        # 开始组装多行调用参数、集合或对象内容。
        self.llm = ChatOpenAI(
            # 计算或设置 `model`，用于保存调用的模型名称。
            model=settings.LLM_MODEL,
            # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
            api_key=settings.LLM_API_KEY,
            # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
            base_url=settings.LLM_BASE_URL,
            # 计算或设置 `temperature`，用于保存后续流程需要的中间结果。
            temperature=0.7,
            # 计算或设置 `max_tokens`，用于保存后续流程需要的中间结果。
            max_tokens=2000
        # 结束上方多行参数、集合或对象的定义。
        )

        # PGVector的数据库连接字符串
        # 计算或设置 `self.connection_string`，用于保存后续流程需要的中间结果。
        self.connection_string = settings.DATABASE_URL

        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info("RAG服务已使用LangChain组件初始化")

    # 应用装饰器，为下方类或函数增加框架行为。
    @staticmethod
    # 定义 `_normalize_scores()`，执行该函数对应的业务流程。
    def _normalize_scores(results: List[tuple]) -> List[tuple]:
        # 补充当前字符串、提示词或响应正文内容。
        """对单路检索分数做 Min-Max 归一化。"""
        # 判断 `not results` 是否成立，以选择正确的处理分支。
        if not results:
            # 结束当前函数，并把处理结果返回给调用方。
            return []

        # 计算或设置 `raw_scores`，用于保存后续流程需要的中间结果。
        raw_scores = [float(score) for _, score in results]
        # 计算或设置 `min_score`，用于保存后续流程需要的中间结果。
        min_score = min(raw_scores)
        # 计算或设置 `max_score`，用于保存后续流程需要的中间结果。
        max_score = max(raw_scores)

        # 计算或设置 `score_range`，用于保存后续流程需要的中间结果。
        score_range = max_score - min_score
        # 判断 `score_range == 0` 是否成立，以选择正确的处理分支。
        if score_range == 0:
            # 结束当前函数，并把处理结果返回给调用方。
            return [
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                (doc, raw_score, 0.0)
                # 遍历当前集合中的每一项，逐项完成相同处理。
                for (doc, _), raw_score in zip(results, raw_scores)
            # 结束上方多行参数、集合或对象的定义。
            ]

        # 结束当前函数，并把处理结果返回给调用方。
        return [
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            (doc, raw_score, (raw_score - min_score) / score_range)
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for (doc, _), raw_score in zip(results, raw_scores)
        # 结束上方多行参数、集合或对象的定义。
        ]

    # 应用装饰器，为下方类或函数增加框架行为。
    @staticmethod
    # 定义 `_extract_keyword_terms()`，执行该函数对应的业务流程。
    def _extract_keyword_terms(query: str, extra_terms: Optional[List[str]] = None) -> List[str]:
        # 补充当前字符串、提示词或响应正文内容。
        """提取用于关键词召回的中英文术语。"""
        # 开始组装多行调用参数、集合或对象内容。
        stop_phrases = {
            # 补充当前字符串、提示词或响应正文内容。
            "有哪些", "有多少", "是什么", "有什么", "哪些", "多少", "什么",
            # 补充当前字符串、提示词或响应正文内容。
            "怎么样", "怎么办", "需要",
            # 补充当前字符串、提示词或响应正文内容。
            "如何", "怎么", "为何", "为什么", "是否", "能否", "可以吗",
            # 补充当前字符串、提示词或响应正文内容。
            "请问", "麻烦", "帮我", "告诉我", "介绍一下", "说明一下",
            # 补充当前字符串、提示词或响应正文内容。
            "的", "和", "与", "及", "或",
        # 结束上方多行参数、集合或对象的定义。
        }
        # 开始组装多行调用参数、集合或对象内容。
        split_pattern = "|".join(
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            re.escape(phrase)
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for phrase in sorted(stop_phrases, key=len, reverse=True)
        # 结束上方多行参数、集合或对象的定义。
        )

        # 计算或设置 `candidates`，用于保存后续流程需要的中间结果。
        candidates = [query, *(extra_terms or [])]
        # 声明参数或字段 `terms`，表示当前业务需要的数据。
        terms: List[str] = []
        # 计算或设置 `seen`，用于保存后续流程需要的中间结果。
        seen = set()
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for candidate in candidates:
            # 计算或设置 `normalized`，用于保存后续流程需要的中间结果。
            normalized = str(candidate).strip().lower()
            # 判断 `not normalized` 是否成立，以选择正确的处理分支。
            if not normalized:
                # 跳过当前项并继续处理下一项。
                continue
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for segment in re.split(split_pattern, normalized):
                # 计算或设置 `tokens`，用于保存后续流程需要的中间结果。
                tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]+", segment)
                # 遍历当前集合中的每一项，逐项完成相同处理。
                for token in tokens:
                    # 判断 `len(token) < 2 or token in stop_phrases or token in seen` 是否成立，以选择正确的处理分支。
                    if len(token) < 2 or token in stop_phrases or token in seen:
                        # 跳过当前项并继续处理下一项。
                        continue
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    seen.add(token)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    terms.append(token)
        # 结束当前函数，并把处理结果返回给调用方。
        return terms

    # 定义 `_enhance_query_for_kb()`，执行该函数对应的业务流程。
    def _enhance_query_for_kb(self, question: str, conversation_history: Optional[List[Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        基于LLM的语义增强：重写查询并提供扩展关键词。
        返回: { "rewritten_query": str, "expanded_keywords": List[str] }
        """
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 判断 `not getattr(settings, "KB_QUERY_ENHANCE_ENABLED", False)` 是否成立，以选择正确的处理分支。
            if not getattr(settings, "KB_QUERY_ENHANCE_ENABLED", False):
                # 结束当前函数，并把处理结果返回给调用方。
                return {"rewritten_query": question, "expanded_keywords": []}

            # 开始组装多行调用参数、集合或对象内容。
            system_prompt = (
                # 补充当前字符串、提示词或响应正文内容。
                "你是一个检索查询增强器。\n"
                # 补充当前字符串、提示词或响应正文内容。
                "通过对上下文理解，理解用户真实意图，输出更清晰的检索查询和若干关键术语扩展。\n"
                # 补充当前字符串、提示词或响应正文内容。
                "返回严格的 JSON 对象：{{\"rewritten_query\": \"...\", \"expanded_keywords\": [\"...\"]}}。\n"
                # 补充当前字符串、提示词或响应正文内容。
                "注意：扩展术语需短而准，避免过长句子。"
            # 结束上方多行参数、集合或对象的定义。
            )

            # 计算或设置 `conversation_history`，用于保存此前的多轮对话历史。
            conversation_history = conversation_history or []
            # 开始组装多行调用参数、集合或对象内容。
            prompt = ChatPromptTemplate.from_messages([
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                ("system", system_prompt),
                # 设置调用参数 `MessagesPlaceholder(variable_name`，把对应业务数据传给下游方法。
                MessagesPlaceholder(variable_name="chat_history"),
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                ("human", "原始查询：{question}\n请返回 JSON 格式结果")
            # 结束上方多行参数、集合或对象的定义。
            ])

            # 开始组装多行调用参数、集合或对象内容。
            enhancer_llm = ChatOpenAI(
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=settings.LLM_MODEL,
                # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
                api_key=settings.LLM_API_KEY,
                # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
                base_url=settings.LLM_BASE_URL,
                # 计算或设置 `temperature`，用于保存后续流程需要的中间结果。
                temperature=0.2,
                # 计算或设置 `max_tokens`，用于保存后续流程需要的中间结果。
                max_tokens=512
            # 结束上方多行参数、集合或对象的定义。
            )

            # 开始组装多行调用参数、集合或对象内容。
            chain = (
                # 开始组装多行调用参数、集合或对象内容。
                {
                    # 补充当前字符串、提示词或响应正文内容。
                    "question": RunnablePassthrough(),
                    # 补充当前字符串、提示词或响应正文内容。
                    "chat_history": lambda x: conversation_history
                # 结束上方多行参数、集合或对象的定义。
                }
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                | prompt
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                | enhancer_llm
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                | StrOutputParser()
            # 结束上方多行参数、集合或对象的定义。
            )

            # 计算或设置 `raw`，用于保存后续流程需要的中间结果。
            raw = chain.invoke(question)
            # 计算或设置 `rewritten_query`，用于保存后续流程需要的中间结果。
            rewritten_query = question
            # 声明参数或字段 `expanded_keywords`，表示当前业务需要的数据。
            expanded_keywords: List[str] = []

            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            try:
                # 导入 `json as pyjson`，供后续代码调用其功能。
                import json as pyjson
                # 计算或设置 `data`，用于保存后续流程需要的中间结果。
                data = pyjson.loads(raw)
                # 计算或设置 `rewritten_query`，用于保存后续流程需要的中间结果。
                rewritten_query = data.get("rewritten_query") or question
                # 计算或设置 `ek`，用于保存后续流程需要的中间结果。
                ek = data.get("expanded_keywords") or []
                # 判断 `isinstance(ek, list)` 是否成立，以选择正确的处理分支。
                if isinstance(ek, list):
                    # 计算或设置 `expanded_keywords`，用于保存后续流程需要的中间结果。
                    expanded_keywords = [str(t).strip() for t in ek if str(t).strip()]
                # 判断 `isinstance(ek, str)` 是否成立，以选择正确的处理分支。
                elif isinstance(ek, str):
                    # 计算或设置 `expanded_keywords`，用于保存后续流程需要的中间结果。
                    expanded_keywords = [t.strip() for t in ek.split(',') if t.strip()]
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except Exception:
                # 备用方案：直接提取令牌
                terms = re.findall(r"[A-Za-z0-9]+|[\u4e00-\u9fff]+", raw)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                expanded_keywords = [t.lower() for t in terms if len(t) >= 2]

            # 计算或设置 `max_terms`，用于保存后续流程需要的中间结果。
            max_terms = getattr(settings, "KB_QUERY_EXPANSION_MAX_TERMS", 6)
            # 判断 `len(expanded_keywords) > max_terms` 是否成立，以选择正确的处理分支。
            if len(expanded_keywords) > max_terms:
                # 计算或设置 `expanded_keywords`，用于保存后续流程需要的中间结果。
                expanded_keywords = expanded_keywords[:max_terms]

            # 结束当前函数，并把处理结果返回给调用方。
            return {"rewritten_query": rewritten_query, "expanded_keywords": expanded_keywords}
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.warning(f"查询增强失败: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            return {"rewritten_query": question, "expanded_keywords": []}

    # 定义 `_tsvector_search()`，执行该函数对应的业务流程。
    async def _tsvector_search(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `collection_name`，表示当前业务需要的数据。
        collection_name: str,
        # 声明参数或字段 `query`，表示查询文本或数据库查询对象。
        query: str,
        # 声明参数或字段 `k`，表示当前业务需要的数据。
        k: int = 5,
        # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
        knowledge_base_id: Optional[UUID] = None,
        # 声明参数或字段 `extra_terms`，表示当前业务需要的数据。
        extra_terms: Optional[List[str]] = None
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[tuple]:
        """
        基于langchain_pg_embedding.document的PostgreSQL全文检索（tsvector）。
        - 集合名严格过滤：cmetadata->>'collection_name'
        - 英文和数字使用 OR 前缀 tsquery
        - 中文及扩展关键词分别使用 ILIKE，按关键词命中比例排序
        返回 (LangChainDocument, score) 列表，与向量检索输出格式一致。
        """
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `terms`，用于保存后续流程需要的中间结果。
            terms = self._extract_keyword_terms(query, extra_terms)
            # 判断 `not terms` 是否成立，以选择正确的处理分支。
            if not terms:
                # 结束当前函数，并把处理结果返回给调用方。
                return []

            # 计算或设置 `english_terms`，用于保存后续流程需要的中间结果。
            english_terms = [term for term in terms if re.fullmatch(r"[a-z0-9]+", term)]
            # 计算或设置 `tsquery_or`，用于保存后续流程需要的中间结果。
            tsquery_or = " | ".join(f"{term}:*" for term in english_terms) if english_terms else None

            # 开始组装多行调用参数、集合或对象内容。
            params = {
                # 补充当前字符串、提示词或响应正文内容。
                "collection_name": collection_name,
                # 补充当前字符串、提示词或响应正文内容。
                "limit": k,
            # 结束上方多行参数、集合或对象的定义。
            }
            # 计算或设置 `keyword_conditions`，用于保存后续流程需要的中间结果。
            keyword_conditions = []
            # 计算或设置 `keyword_score_parts`，用于保存后续流程需要的中间结果。
            keyword_score_parts = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for index, term in enumerate(terms):
                # 计算或设置 `param_name`，用于保存后续流程需要的中间结果。
                param_name = f"term_{index}"
                # 计算或设置 `escaped_term`，用于保存后续流程需要的中间结果。
                escaped_term = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                # 计算或设置 `params[param_name]`，用于保存后续流程需要的中间结果。
                params[param_name] = f"%{escaped_term}%"
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                keyword_conditions.append(f"document ILIKE :{param_name} ESCAPE E'\\\\'")
                # 开始组装多行调用参数、集合或对象内容。
                keyword_score_parts.append(
                    # 补充当前字符串、提示词或响应正文内容。
                    f"CASE WHEN document ILIKE :{param_name} ESCAPE E'\\\\' THEN 1.0 ELSE 0.0 END"
                # 结束上方多行参数、集合或对象的定义。
                )

            # 计算或设置 `keyword_match_sql`，用于保存后续流程需要的中间结果。
            keyword_match_sql = " OR ".join(keyword_conditions)
            # 计算或设置 `keyword_score_sql`，用于保存后续流程需要的中间结果。
            keyword_score_sql = f"({' + '.join(keyword_score_parts)}) / {len(terms)}.0"
            # 计算或设置 `match_conditions`，用于保存后续流程需要的中间结果。
            match_conditions = [keyword_match_sql]
            # 判断 `tsquery_or` 是否成立，以选择正确的处理分支。
            if tsquery_or:
                # 计算或设置 `params["tsq"]`，用于保存后续流程需要的中间结果。
                params["tsq"] = tsquery_or
                # 计算或设置 `ts_rank_sql`，用于保存后续流程需要的中间结果。
                ts_rank_sql = "ts_rank_cd(to_tsvector('simple', document), to_tsquery('simple', :tsq))"
                # 开始组装多行调用参数、集合或对象内容。
                match_conditions.insert(
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    0,
                    # 补充当前字符串、提示词或响应正文内容。
                    "to_tsvector('simple', document) @@ to_tsquery('simple', :tsq)"
                # 结束上方多行参数、集合或对象的定义。
                )
            # 前面的判断条件均不成立时，执行当前备用分支。
            else:
                # 计算或设置 `ts_rank_sql`，用于保存后续流程需要的中间结果。
                ts_rank_sql = "0.0"
            # 计算或设置 `relevance_score_sql`，用于保存后续流程需要的中间结果。
            relevance_score_sql = f"({ts_rank_sql} + {keyword_score_sql})"
            # 开始组装多行调用参数、集合或对象内容。
            base_sql = (
                # 补充当前字符串、提示词或响应正文内容。
                "SELECT id, document, cmetadata, "
                # 补充当前字符串、提示词或响应正文内容。
                f"{relevance_score_sql} AS relevance_score "
                # 补充当前字符串、提示词或响应正文内容。
                "FROM langchain_pg_embedding "
                # 补充当前字符串、提示词或响应正文内容。
                "WHERE cmetadata->>'collection_name' = :collection_name "
                # 补充当前字符串、提示词或响应正文内容。
                f"AND ({' OR '.join(match_conditions)}) "
            # 结束上方多行参数、集合或对象的定义。
            )

            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            if knowledge_base_id:
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                base_sql += "AND cmetadata->>'knowledge_base_id' = :kb_id "
                # 计算或设置 `params["kb_id"]`，用于保存后续流程需要的中间结果。
                params["kb_id"] = str(knowledge_base_id)

            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            base_sql += "ORDER BY relevance_score DESC LIMIT :limit"

            # 异步调用下游方法，并把得到的后续流程需要的中间结果保存到 `res2`。
            res2 = await self.db.execute(text(base_sql), params)
            # 计算或设置 `rows`，用于保存后续流程需要的中间结果。
            rows = res2.fetchall()
            # 声明参数或字段 `results`，表示当前业务需要的数据。
            results: List[tuple] = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for r in rows:
                # 计算或设置 `doc_text`，用于保存后续流程需要的中间结果。
                doc_text = r[1]
                # 计算或设置 `metadata`，用于保存后续流程需要的中间结果。
                metadata = r[2] or {}
                # 计算或设置 `page_content`，用于保存后续流程需要的中间结果。
                page_content = doc_text
                # 计算或设置 `lc_doc`，用于保存后续流程需要的中间结果。
                lc_doc = LangChainDocument(page_content=page_content, metadata=metadata)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                results.append((lc_doc, float(r[3]) if r[3] is not None else 0.0))
            # 结束当前函数，并把处理结果返回给调用方。
            return results
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.warning(f"tsvector搜索错误: {e}")
            # 异步等待 `self.db.rollback` 完成，避免阻塞其他并发请求。
            await self.db.rollback()
            # 结束当前函数，并把处理结果返回给调用方。
            return []

    # 定义 `_merge_docs_with_scores()`，执行该函数对应的业务流程。
    async def _merge_docs_with_scores(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `content_results`，表示当前业务需要的数据。
        content_results: List[tuple],
        # 声明参数或字段 `text_results`，表示当前业务需要的数据。
        text_results: List[tuple],
        # 声明参数或字段 `query`，表示查询文本或数据库查询对象。
        query: str,
        # 声明参数或字段 `top_k`，表示最终保留的高相关结果数量。
        top_k: int = 5,
        # 声明参数或字段 `min_similarity_score`，表示当前业务需要的数据。
        min_similarity_score: float = 0.2
    # 结束上方多行参数、集合或对象的定义。
    ) -> (List[LangChainDocument], List[Dict[str, Any]]):
        """
        使用可配置的融合方法合并向量内容结果和PostgreSQL tsvector结果。
        支持RRF（倒数排名融合）和加权和方法，可选择重排。
        """
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 声明参数或字段 `merged_map`，表示当前业务需要的数据。
            merged_map: Dict[tuple, Dict[str, Any]] = {}

            # 开始组装多行调用参数、集合或对象内容。
            normalized_content_results = [
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                result
                # 遍历当前集合中的每一项，逐项完成相同处理。
                for result in self._normalize_scores(content_results)
                # 判断 `result[2] >= min_similarity_score` 是否成立，以选择正确的处理分支。
                if result[2] >= min_similarity_score
            # 结束上方多行参数、集合或对象的定义。
            ]
            # 开始组装多行调用参数、集合或对象内容。
            normalized_text_results = [
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                result
                # 遍历当前集合中的每一项，逐项完成相同处理。
                for result in self._normalize_scores(text_results)
                # 判断 `result[2] >= min_similarity_score` 是否成立，以选择正确的处理分支。
                if result[2] >= min_similarity_score
            # 结束上方多行参数、集合或对象的定义。
            ]

            # 收集向量搜索结果
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for doc, raw_score, normalized_score in normalized_content_results:
                # 计算或设置 `key`，用于保存后续流程需要的中间结果。
                key = (doc.metadata.get("document_id"), doc.metadata.get("chunk_index"))
                # 计算或设置 `merged_map[key]`，用于保存后续流程需要的中间结果。
                merged_map[key] = merged_map.get(key, {"doc": doc, "content_score": 0.0})
                # 计算或设置 `merged_map[key]["doc"]`，用于保存后续流程需要的中间结果。
                merged_map[key]["doc"] = doc
                # 计算或设置 `merged_map[key]["content_score"]`，用于保存后续流程需要的中间结果。
                merged_map[key]["content_score"] = normalized_score
                # 计算或设置 `merged_map[key]["content_score_raw"]`，用于保存后续流程需要的中间结果。
                merged_map[key]["content_score_raw"] = raw_score

            # 集成tsvector文本搜索结果
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for doc, raw_score, normalized_score in normalized_text_results:
                # 计算或设置 `key`，用于保存后续流程需要的中间结果。
                key = (doc.metadata.get("document_id"), doc.metadata.get("chunk_index"))
                # 计算或设置 `entry`，用于保存后续流程需要的中间结果。
                entry = merged_map.get(key, {"doc": doc, "content_score": 0.0})
                # 计算或设置 `entry["doc"]`，用于保存后续流程需要的中间结果。
                entry["doc"] = entry.get("doc") or doc
                # 计算或设置 `entry["text_score"]`，用于保存后续流程需要的中间结果。
                entry["text_score"] = normalized_score
                # 计算或设置 `entry["text_score_raw"]`，用于保存后续流程需要的中间结果。
                entry["text_score_raw"] = raw_score
                # 计算或设置 `merged_map[key]`，用于保存后续流程需要的中间结果。
                merged_map[key] = entry

            # 合并分数：优先考虑向量相似性，然后是关键词匹配
            # 声明参数或字段 `combined_list`，表示当前业务需要的数据。
            combined_list: List[tuple] = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for key, entry in merged_map.items():
                # 计算或设置 `content_score`，用于保存后续流程需要的中间结果。
                content_score = float(entry.get("content_score", 0.0))
                # 计算或设置 `text_score`，用于保存后续流程需要的中间结果。
                text_score = float(entry.get("text_score", 0.0))
                # 加权组合：使用配置文件中的权重
                # 计算或设置 `combined_score`，用于保存后续流程需要的中间结果。
                combined_score = (settings.RAG_CONTENT_WEIGHT * content_score +
                                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                                settings.RAG_TEXT_WEIGHT * text_score)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                combined_list.append((entry["doc"], combined_score, entry))

            # 按combined_score降序排序（更高相关性优先）
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            combined_list.sort(key=lambda x: x[1], reverse=True)

            # 构建输出
            # 计算或设置 `top`，用于保存后续流程需要的中间结果。
            top = combined_list[:top_k]
            # 声明参数或字段 `docs`，表示当前业务需要的数据。
            docs: List[LangChainDocument] = []
            # 声明参数或字段 `sources`，表示答案引用来源列表。
            sources: List[Dict[str, Any]] = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for doc, combined_score, entry in top:
                # 计算或设置 `final_page_content`，用于保存后续流程需要的中间结果。
                final_page_content = doc.page_content
                # 计算或设置 `final_doc`，用于保存后续流程需要的中间结果。
                final_doc = LangChainDocument(page_content=final_page_content, metadata=doc.metadata)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                docs.append(final_doc)
                # 开始组装多行调用参数、集合或对象内容。
                sources.append({
                    # 补充当前字符串、提示词或响应正文内容。
                    "document_id": doc.metadata.get("document_id"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "document_title": doc.metadata.get("filename", "Unknown"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "chunk_id": doc.metadata.get("chunk_id"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "chunk_index": doc.metadata.get("chunk_index", 0),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content": final_page_content,
                    # 补充当前字符串、提示词或响应正文内容。
                    "combined_score": float(combined_score),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content_score": float(entry.get("content_score", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content_score_norm": float(entry.get("content_score", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content_score_raw": float(entry.get("content_score_raw", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "text_score": float(entry.get("text_score", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "text_score_norm": float(entry.get("text_score", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "text_score_raw": float(entry.get("text_score_raw", 0.0)),
                    # 补充当前字符串、提示词或响应正文内容。
                    "metadata": doc.metadata
                # 结束上方多行参数、集合或对象的定义。
                })

            # 如果启用则应用重排
            # 判断 `settings.RERANK_ENABLED and self.rerank_service.is_enabled()` 是否成立，以选择正确的处理分支。
            if settings.RERANK_ENABLED and self.rerank_service.is_enabled():
                # 开始组装多行调用参数、集合或对象内容。
                docs, sources = await self.rerank_service.rerank_documents(
                    # 计算或设置 `query`，用于保存查询文本或数据库查询对象。
                    query=query,
                    # 计算或设置 `documents`，用于保存候选文档或文档列表。
                    documents=docs,
                    # 计算或设置 `sources`，用于保存答案引用来源列表。
                    sources=sources,
                    # 计算或设置 `top_k`，用于保存最终保留的高相关结果数量。
                    top_k=settings.RERANK_FINAL_K
                # 结束上方多行参数、集合或对象的定义。
                )
            # 前面的判断条件均不成立时，执行当前备用分支。
            else:
                # 如果没有重排则只取top_k
                # 计算或设置 `docs`，用于保存后续流程需要的中间结果。
                docs = docs[:top_k]
                # 计算或设置 `sources`，用于保存答案引用来源列表。
                sources = sources[:top_k]

            # 结束当前函数，并把处理结果返回给调用方。
            return docs, sources

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.warning(f"合并多路径结果时出错: {e}")
            # 备用方案：仅返回content_results
            # 计算或设置 `docs`，用于保存后续流程需要的中间结果。
            docs = [doc for doc, _ in content_results[:top_k]]
            # 计算或设置 `sources`，用于保存答案引用来源列表。
            sources = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for doc, score in content_results[:top_k]:
                # 开始组装多行调用参数、集合或对象内容。
                sources.append({
                    # 补充当前字符串、提示词或响应正文内容。
                    "document_id": doc.metadata.get("document_id"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "document_title": doc.metadata.get("filename", "Unknown"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "chunk_id": doc.metadata.get("chunk_id"),
                    # 补充当前字符串、提示词或响应正文内容。
                    "chunk_index": doc.metadata.get("chunk_index", 0),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content": doc.page_content,
                    # 补充当前字符串、提示词或响应正文内容。
                    "combined_score": float(score),
                    # 补充当前字符串、提示词或响应正文内容。
                    "content_score": float(score),
                    # 补充当前字符串、提示词或响应正文内容。
                    "text_score": 0.0,
                    # 补充当前字符串、提示词或响应正文内容。
                    "metadata": doc.metadata
                # 结束上方多行参数、集合或对象的定义。
                })
            # 结束当前函数，并把处理结果返回给调用方。
            return docs, sources

    # 定义 `_create_rag_chain_with_docs()`，执行该函数对应的业务流程。
    def _create_rag_chain_with_docs(self, docs: List[LangChainDocument], conversation_history: List[Dict[str, str]]):
        # 补充当前字符串、提示词或响应正文内容。
        """创建带预检索文档的RAG链。"""
        # 从 `langchain_core.messages` 导入当前模块需要的 `HumanMessage, AIMessage`。
        from langchain_core.messages import HumanMessage, AIMessage
        
        # 转换对话历史格式
        # 计算或设置 `formatted_history`，用于保存后续流程需要的中间结果。
        formatted_history = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for msg in conversation_history:
            # 判断 `msg.get("role") == "user"` 是否成立，以选择正确的处理分支。
            if msg.get("role") == "user":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append(HumanMessage(content=msg.get("content", "")))
            # 判断 `msg.get("role") == "assistant"` 是否成立，以选择正确的处理分支。
            elif msg.get("role") == "assistant":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append(AIMessage(content=msg.get("content", "")))

        # 创建提示模板
        system_prompt = """你是一个智能助手，基于提供的上下文信息回答用户问题。

                        上下文信息：
                        {context}
                        
                        请根据上下文信息回答用户的问题。如果上下文信息不足以回答问题，请诚实地说明。
                        保持回答准确、有用且简洁。"""

        # 开始组装多行调用参数、集合或对象内容。
        prompt = ChatPromptTemplate.from_messages([
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            ("system", system_prompt),
            # 设置调用参数 `MessagesPlaceholder(variable_name`，把对应业务数据传给下游方法。
            MessagesPlaceholder(variable_name="chat_history"),
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            ("human", "{question}")
        # 结束上方多行参数、集合或对象的定义。
        ])

        # 格式化文档
        # 定义 `format_docs()`，执行该函数对应的业务流程。
        def format_docs(docs_list):
            # 结束当前函数，并把处理结果返回给调用方。
            return "\n\n".join(doc.page_content for doc in docs_list)

        # 使用预检索文档创建链
        # 开始组装多行调用参数、集合或对象内容。
        rag_chain = (
            # 开始组装多行调用参数、集合或对象内容。
            {
                # 补充当前字符串、提示词或响应正文内容。
                "context": lambda x: format_docs(docs),
                # 补充当前字符串、提示词或响应正文内容。
                "question": RunnablePassthrough(),
                # 补充当前字符串、提示词或响应正文内容。
                "chat_history": lambda x: formatted_history
            # 结束上方多行参数、集合或对象的定义。
            }
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | prompt
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | self.llm
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | StrOutputParser()
        # 结束上方多行参数、集合或对象的定义。
        )

        # 结束当前函数，并把处理结果返回给调用方。
        return rag_chain

    # 定义 `_should_use_knowledge_base()`，执行该函数对应的业务流程。
    def _should_use_knowledge_base(self, question: str) -> bool:
        """决定用户问题是否应使用知识库检索。
        如果返回True则应使用KB，否则返回False。
        """
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 先做高置信度闲聊预筛，避免为明确闲聊问题调用分类模型
            # 开始组装多行调用参数、集合或对象内容。
            chitchat_patterns = [
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(你好|您好|嗨|哈喽|哈啰|hello|hi)(呀|啊|哦|哟|嘛)?$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(早上好|上午好|中午好|下午好|晚上好|晚安)$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(谢谢|感谢|多谢)(你|您)?(了|啦|呀)?$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(再见|拜拜|bye|回头见|下次见)(了|啦)?$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(你是谁|你叫什么|介绍一下你自己|你能做什么)$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(讲|说)(一个|个)?笑话(吧|呀)?$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(作|写)(一首|首)?诗(吧|呀)?$",
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                r"^(陪我聊聊|聊聊天|聊天|在吗|在不在)$",
            # 结束上方多行参数、集合或对象的定义。
            ]
            # 计算或设置 `normalized_question`，用于保存后续流程需要的中间结果。
            normalized_question = re.sub(r"[\W_]+", "", question.strip().lower(), flags=re.UNICODE)
            # 判断 `any(re.fullmatch(pattern, normalized_question) for pattern in chitchat_patterns)` 是否成立，以选择正确的处理分支。
            if any(re.fullmatch(pattern, normalized_question) for pattern in chitchat_patterns):
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.info(f"闲聊正则预筛命中，跳过KB检索: {question}")
                # 结束当前函数，并把处理结果返回给调用方。
                return False

            # 带明确指令和示例的基于LLM的分类
            # 开始组装多行调用参数、集合或对象内容。
            classification_prompt = (
                # 补充当前字符串、提示词或响应正文内容。
                "你是一个分类器。判断该问题是否需要基于知识库内容回答还是由大模型自主回答。\n"
                # 补充当前字符串、提示词或响应正文内容。
                "只有用户在明显是闲聊的内容才输出GENERAL。\n"
                # 补充当前字符串、提示词或响应正文内容。
                "其他情况都输出KB\n"
                # 补充当前字符串、提示词或响应正文内容。
                "示例：\n"
                # 补充当前字符串、提示词或响应正文内容。
                "问：讲个笑话\n答：GENERAL\n"
                # 补充当前字符串、提示词或响应正文内容。
                "问：你好\n答：GENERAL\n"
                # 补充当前字符串、提示词或响应正文内容。
                "问：你是谁\n答：GENERAL\n"
                # 补充当前字符串、提示词或响应正文内容。
                "问：作首诗\n答：GENERAL\n"
                # 补充当前字符串、提示词或响应正文内容。
                "而其他情况或者判断不准的时候，都输出KB\n"
                # 补充当前字符串、提示词或响应正文内容。
                "重要规则：你只需要输出一个词。只能回答KB或者GENERAL。不要解释，不要说其他任何话。\n"
                # 补充当前字符串、提示词或响应正文内容。
                f"问：{question}\n答："
            # 结束上方多行参数、集合或对象的定义。
            )
            # 使用确定性分类器LLM
            # 从 `langchain_openai` 导入当前模块需要的 `ChatOpenAI`。
            from langchain_openai import ChatOpenAI
            # 开始组装多行调用参数、集合或对象内容。
            classifier_llm = ChatOpenAI(
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=settings.LLM_MODEL,
                # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
                api_key=settings.LLM_API_KEY,
                # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
                base_url=settings.LLM_BASE_URL,
                # 计算或设置 `temperature`，用于保存后续流程需要的中间结果。
                temperature=0,
                # 计算或设置 `max_tokens`，用于保存后续流程需要的中间结果。
                max_tokens=50
            # 结束上方多行参数、集合或对象的定义。
            )
            # 计算或设置 `resp`，用于保存后续流程需要的中间结果。
            resp = classifier_llm.invoke(classification_prompt)
            # 计算或设置 `content`，用于保存当前处理的正文内容。
            content = getattr(resp, "content", str(resp))
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"KB意图分类结果: question='{question[:50]}...', response='{content}'")
            # 如果明确包含 GENERAL 且不包含 KB，不走知识库
            if "GENERAL" in (content or "").upper() and "KB" not in (content or "").upper():
                # 结束当前函数，并把处理结果返回给调用方。
                return False
            # 其余情况一律走知识库（默认走 KB 更安全）
            # 结束当前函数，并把处理结果返回给调用方。
            return True
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.warning(f"KB意图检测失败，默认使用KB: {e}")
            # 结束当前函数，并把处理结果返回给调用方。
            return True

    # 定义 `_create_general_chat_chain()`，执行该函数对应的业务流程。
    def _create_general_chat_chain(self, conversation_history: List[Dict[str, str]]):
        # 补充当前字符串、提示词或响应正文内容。
        """不带KB上下文的通用聊天链。"""
        # 从 `langchain_core.messages` 导入当前模块需要的 `HumanMessage, AIMessage`。
        from langchain_core.messages import HumanMessage, AIMessage
        
        # 转换对话历史格式
        # 计算或设置 `formatted_history`，用于保存后续流程需要的中间结果。
        formatted_history = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for msg in conversation_history:
            # 判断 `msg.get("role") == "user"` 是否成立，以选择正确的处理分支。
            if msg.get("role") == "user":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append(HumanMessage(content=msg.get("content", "")))
            # 判断 `msg.get("role") == "assistant"` 是否成立，以选择正确的处理分支。
            elif msg.get("role") == "assistant":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append(AIMessage(content=msg.get("content", "")))
        
        # 开始组装多行调用参数、集合或对象内容。
        system_prompt = (
            # 补充当前字符串、提示词或响应正文内容。
            "你是一个智能助手。直接根据用户问题进行回答，不使用任何知识库上下文。"
            # 补充当前字符串、提示词或响应正文内容。
            "保持回答准确、简洁、有帮助。"
        # 结束上方多行参数、集合或对象的定义。
        )
        # 开始组装多行调用参数、集合或对象内容。
        prompt = ChatPromptTemplate.from_messages([
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            ("system", system_prompt),
            # 设置调用参数 `MessagesPlaceholder(variable_name`，把对应业务数据传给下游方法。
            MessagesPlaceholder(variable_name="chat_history"),
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            ("human", "{question}")
        # 结束上方多行参数、集合或对象的定义。
        ])
        # 开始组装多行调用参数、集合或对象内容。
        chain = (
            # 开始组装多行调用参数、集合或对象内容。
            {
                # 补充当前字符串、提示词或响应正文内容。
                "question": RunnablePassthrough(),
                # 补充当前字符串、提示词或响应正文内容。
                "chat_history": lambda x: formatted_history
            # 结束上方多行参数、集合或对象的定义。
            }
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | prompt
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | self.llm
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            | StrOutputParser()
        # 结束上方多行参数、集合或对象的定义。
        )
        # 结束当前函数，并把处理结果返回给调用方。
        return chain

    # 定义 `ask_question_stream()`，执行该函数对应的业务流程。
    async def ask_question_stream(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `question`，表示用户当前问题。
        question: str,
        # 声明参数或字段 `user_id`，表示用户唯一标识。
        user_id: UUID,
        # 声明参数或字段 `knowledge_base_id`，表示所属知识库唯一标识。
        knowledge_base_id: Optional[UUID] = None,
        # 声明参数或字段 `conversation_history`，表示此前的多轮对话历史。
        conversation_history: Optional[List[Dict[str, str]]] = None,
        # 声明参数或字段 `context_limit`，表示当前业务需要的数据。
        context_limit: int = settings.CONTEXT_LIMIT
    # 结束上方多行参数、集合或对象的定义。
    ):
        """
        使用RAG工作流进行流式响应的问题提问

        Args:
            question: 用户问题
            user_id: 用于文档过滤的用户ID
            knowledge_base_id: 可选的知识库ID用于过滤
            conversation_history: 之前的对话消息
            context_limit: 要检索的最大上下文文档数

        Yields:
            包含流式响应数据的字典
        """
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `conversation_history`，用于保存此前的多轮对话历史。
            conversation_history = conversation_history or []

            # 首先进行意图检测
            # 计算或设置 `use_kb`，用于保存后续流程需要的中间结果。
            use_kb = self._should_use_knowledge_base(question)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('use_kb=======', use_kb)
            # 如果提供了特定的知识库，总是使用KB检索
            # if knowledge_base_id:
            #     use_kb = True

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info(f"ask_question_stream: use_kb={use_kb}, knowledge_base_id={knowledge_base_id}, user_id={user_id}")

            # 不使用知识库，直接访问LLM对话
            # 判断 `not use_kb` 是否成立，以选择正确的处理分支。
            if not use_kb:
                # 流式传输不使用KB检索的通用LLM答案
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield {
                    # 补充当前字符串、提示词或响应正文内容。
                    "type": "start",
                    # 补充当前字符串、提示词或响应正文内容。
                    "question": question,
                    # 补充当前字符串、提示词或响应正文内容。
                    "sources": [],
                    # 补充当前字符串、提示词或响应正文内容。
                    "context_used": False,
                    # 补充当前字符串、提示词或响应正文内容。
                    "num_sources": 0
                # 结束上方多行参数、集合或对象的定义。
                }
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield {
                    # 补充当前字符串、提示词或响应正文内容。
                    "type": "chunk",
                    # 补充当前字符串、提示词或响应正文内容。
                    "content": "说明：以下回复为通用闲聊内容，不是从知识库中获取的。\n\n"
                # 结束上方多行参数、集合或对象的定义。
                }
                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 计算或设置 `general_chain`，用于保存后续流程需要的中间结果。
                    general_chain = self._create_general_chat_chain(conversation_history)
                    # 使用真正的流式输出
                    async for chunk in general_chain.astream({"question": question}):
                        # 处理不同类型的chunk
                        # 判断 `isinstance(chunk, str)` 是否成立，以选择正确的处理分支。
                        if isinstance(chunk, str):
                            # 判断 `chunk.strip():  # 只发送非空字符串` 是否成立，以选择正确的处理分支。
                            if chunk.strip():  # 只发送非空字符串
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk}
                        # 判断 `isinstance(chunk, dict)` 是否成立，以选择正确的处理分支。
                        elif isinstance(chunk, dict):
                            # 处理字典类型的chunk
                            if "content" in chunk and chunk["content"].strip():
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk["content"]}
                            # 判断 `"output" in chunk and chunk["output"].strip()` 是否成立，以选择正确的处理分支。
                            elif "output" in chunk and chunk["output"].strip():
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk["output"]}
                    
                    # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                    yield {"type": "end", "complete": True, "sources": [], "num_sources": 0}
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except GeneratorExit:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.info("客户端断开连接，停止流式响应")
                    # 结束当前函数，并把处理结果返回给调用方。
                    return
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except Exception as e:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.error(f"流式响应生成错误: {str(e)}")
                    # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                    yield {"type": "error", "error": str(e)}
                # 结束当前函数，并把处理结果返回给调用方。
                return



            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('conversation_history =======', conversation_history)
            # 可选地为KB检索增强查询：查询语句改写
            # 计算或设置 `enhance`，用于保存后续流程需要的中间结果。
            enhance = self._enhance_query_for_kb(question, conversation_history)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('enhance=======', enhance)
            # 改写后的query
            rewritten_query = enhance.get("rewritten_query", question)
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('rewritten_query=======', rewritten_query)
            # 扩展关键字
            expanded_keywords = enhance.get("expanded_keywords", [])
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            print('expanded_keywords=======', expanded_keywords)

            # 为用户的文档创建集合名称（仅块）
            collection_name = f"document_chunks_{user_id}".replace("-", "_")

            # 连接到向量存储
            # 开始组装多行调用参数、集合或对象内容。
            vector_store = PGVector(
                # 计算或设置 `connection`，用于保存后续流程需要的中间结果。
                connection=self.connection_string,
                # 计算或设置 `embeddings`，用于保存后续流程需要的中间结果。
                embeddings=self.embeddings,
                # 计算或设置 `collection_name`，用于保存后续流程需要的中间结果。
                collection_name=collection_name,
                # 计算或设置 `use_jsonb`，用于保存后续流程需要的中间结果。
                use_jsonb=True
            # 结束上方多行参数、集合或对象的定义。
            )

            # 构建过滤条件
            # 计算或设置 `filter_conditions`，用于保存后续流程需要的中间结果。
            filter_conditions = {}
            # 判断 `knowledge_base_id` 是否成立，以选择正确的处理分支。
            if knowledge_base_id:
                # 计算或设置 `filter_conditions["knowledge_base_id"]`，用于保存后续流程需要的中间结果。
                filter_conditions["knowledge_base_id"] = str(knowledge_base_id)

            # 多路径检索：内容（向量）+ 文本（tsvector）
            # 开始组装多行调用参数、集合或对象内容。
            content_results = vector_store.similarity_search_with_relevance_scores(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                rewritten_query,
                # 计算或设置 `k`，用于保存后续流程需要的中间结果。
                k=context_limit,
                # 计算或设置 `filter`，用于保存后续流程需要的中间结果。
                filter=filter_conditions if filter_conditions else None
            # 结束上方多行参数、集合或对象的定义。
            )
            # 开始组装多行调用参数、集合或对象内容。
            text_results = await self._tsvector_search(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                collection_name,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                question,
                # 计算或设置 `k`，用于保存后续流程需要的中间结果。
                k=context_limit,
                # 计算或设置 `knowledge_base_id`，用于保存所属知识库唯一标识。
                knowledge_base_id=knowledge_base_id,
                # 计算或设置 `extra_terms`，用于保存后续流程需要的中间结果。
                extra_terms=expanded_keywords
            # 结束上方多行参数、集合或对象的定义。
            )

            # # 调试：输出两个路径的结果对比
            # logger.info(f"=== 向量搜索结果 (content_results) ===")
            # for i, (doc, score) in enumerate(content_results):
            #     logger.info(f"向量结果 {i+1}: 分数={score:.4f}, 文档ID={doc.metadata.get('document_id')}, 块索引={doc.metadata.get('chunk_index')}")
            #     logger.info(f"内容预览: {doc.page_content[:200]}...")
            
            # logger.info(f"=== 全文检索结果 (text_results) ===")
            # for i, (doc, score) in enumerate(text_results):
            #     logger.info(f"文本结果 {i+1}: 分数={score:.4f}, 文档ID={doc.metadata.get('document_id')}, 块索引={doc.metadata.get('chunk_index')}")
            #     logger.info(f"内容预览: {doc.page_content[:200]}...")

            # 合并并选择最终文档和来源
            # 开始组装多行调用参数、集合或对象内容。
            relevant_docs, sources = await self._merge_docs_with_scores(
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                content_results,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                text_results,
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                question,
                # 计算或设置 `top_k`，用于保存最终保留的高相关结果数量。
                top_k=context_limit,
                # 计算或设置 `min_similarity_score`，用于保存后续流程需要的中间结果。
                min_similarity_score=settings.RAG_MIN_SIMILARITY_SCORE
            # 结束上方多行参数、集合或对象的定义。
            )

            # 没有足够相关的文档时，直接走纯 LLM 生成
            # 判断 `not relevant_docs` 是否成立，以选择正确的处理分支。
            if not relevant_docs:
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield {
                    # 补充当前字符串、提示词或响应正文内容。
                    "type": "start",
                    # 补充当前字符串、提示词或响应正文内容。
                    "question": question,
                    # 开始组装多行调用参数、集合或对象内容。
                    "query_rewrite": {
                        # 补充当前字符串、提示词或响应正文内容。
                        "rewritten_query": rewritten_query,
                        # 补充当前字符串、提示词或响应正文内容。
                        "expanded_keywords": expanded_keywords
                    # 结束上方多行参数、集合或对象的定义。
                    },
                    # 补充当前字符串、提示词或响应正文内容。
                    "sources": [],
                    # 补充当前字符串、提示词或响应正文内容。
                    "context_used": False,
                    # 补充当前字符串、提示词或响应正文内容。
                    "num_sources": 0
                # 结束上方多行参数、集合或对象的定义。
                }

                # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
                try:
                    # 计算或设置 `general_chain`，用于保存后续流程需要的中间结果。
                    general_chain = self._create_general_chat_chain(conversation_history)
                    # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                    async for chunk in general_chain.astream({"question": question}):
                        # 判断 `isinstance(chunk, str)` 是否成立，以选择正确的处理分支。
                        if isinstance(chunk, str):
                            # 判断 `chunk.strip()` 是否成立，以选择正确的处理分支。
                            if chunk.strip():
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk}
                        # 判断 `isinstance(chunk, dict)` 是否成立，以选择正确的处理分支。
                        elif isinstance(chunk, dict):
                            # 判断 `"content" in chunk and chunk["content"].strip()` 是否成立，以选择正确的处理分支。
                            if "content" in chunk and chunk["content"].strip():
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk["content"]}
                            # 判断 `"output" in chunk and chunk["output"].strip()` 是否成立，以选择正确的处理分支。
                            elif "output" in chunk and chunk["output"].strip():
                                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                                yield {"type": "chunk", "content": chunk["output"]}
                    # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                    yield {"type": "end", "complete": True, "sources": [], "num_sources": 0}
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except GeneratorExit:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.info("客户端断开连接，停止纯LLM流式响应")
                    # 结束当前函数，并把处理结果返回给调用方。
                    return
                # 捕获指定异常，避免底层错误未经处理直接中断请求。
                except Exception as e:
                    # 记录当前执行状态或异常信息，便于开发和运行排查。
                    logger.error(f"纯LLM流式响应生成错误: {str(e)}")
                    # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                    yield {"type": "error", "error": str(e)}
                # 结束当前函数，并把处理结果返回给调用方。
                return

            # 使用来源产生初始数据
            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
            yield {
                # 补充当前字符串、提示词或响应正文内容。
                "type": "start",
                # 补充当前字符串、提示词或响应正文内容。
                "question": question,
                # 开始组装多行调用参数、集合或对象内容。
                "query_rewrite": {
                    # 补充当前字符串、提示词或响应正文内容。
                    "rewritten_query": rewritten_query,
                    # 补充当前字符串、提示词或响应正文内容。
                    "expanded_keywords": expanded_keywords
                # 结束上方多行参数、集合或对象的定义。
                },
                # 补充当前字符串、提示词或响应正文内容。
                "sources": sources,
                # 补充当前字符串、提示词或响应正文内容。
                "context_used": True,
                # 补充当前字符串、提示词或响应正文内容。
                "num_sources": len(sources)
            # 结束上方多行参数、集合或对象的定义。
            }

            # 使用预检索文档创建RAG链（无额外检索）
            # 计算或设置 `rag_chain`，用于保存后续流程需要的中间结果。
            rag_chain = self._create_rag_chain_with_docs(relevant_docs, conversation_history)

            # 流式传输LLM的响应
            # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
            try:
                # 使用真正的流式输出
                async for chunk in rag_chain.astream({"question": question}):
                    # 处理不同类型的chunk
                    # 判断 `isinstance(chunk, str)` 是否成立，以选择正确的处理分支。
                    if isinstance(chunk, str):
                        # 判断 `chunk.strip():  # 只发送非空字符串` 是否成立，以选择正确的处理分支。
                        if chunk.strip():  # 只发送非空字符串
                            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                            yield {"type": "chunk", "content": chunk}
                    # 判断 `isinstance(chunk, dict)` 是否成立，以选择正确的处理分支。
                    elif isinstance(chunk, dict):
                        # 处理字典类型的chunk
                        if "content" in chunk and chunk["content"].strip():
                            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                            yield {"type": "chunk", "content": chunk["content"]}
                        # 判断 `"output" in chunk and chunk["output"].strip()` 是否成立，以选择正确的处理分支。
                        elif "output" in chunk and chunk["output"].strip():
                            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                            yield {"type": "chunk", "content": chunk["output"]}
                    
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except GeneratorExit:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.info("客户端断开连接，停止RAG流式响应")
                # 结束当前函数，并把处理结果返回给调用方。
                return
            # 捕获指定异常，避免底层错误未经处理直接中断请求。
            except Exception as e:
                # 记录当前执行状态或异常信息，便于开发和运行排查。
                logger.error(f"RAG流式响应生成错误: {str(e)}")
                # 向流式响应输出当前数据片段，同时保留生成器执行状态。
                yield {"type": "error", "error": str(e)}

            # 使用来源产生完成信号以供前端显示
            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
            yield {
                # 补充当前字符串、提示词或响应正文内容。
                "type": "end",
                # 补充当前字符串、提示词或响应正文内容。
                "complete": True,
                # 补充当前字符串、提示词或响应正文内容。
                "sources": sources,
                # 补充当前字符串、提示词或响应正文内容。
                "num_sources": len(sources)
            # 结束上方多行参数、集合或对象的定义。
            }

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"流式RAG问答中出错: {e}")
            # 向流式响应输出当前数据片段，同时保留生成器执行状态。
            yield {
                # 补充当前字符串、提示词或响应正文内容。
                "type": "error",
                # 补充当前字符串、提示词或响应正文内容。
                "error": str(e)
            # 结束上方多行参数、集合或对象的定义。
            }


    # 定义 `get_conversation_context()`，执行该函数对应的业务流程。
    async def get_conversation_context(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `conversation_history`，表示此前的多轮对话历史。
        conversation_history: List[Dict[str, str]],
        # 声明参数或字段 `max_messages`，表示当前业务需要的数据。
        max_messages: int = 10
    # 结束上方多行参数、集合或对象的定义。
    ) -> List[Dict[str, str]]:
        """
        处理对话历史以获取上下文

        Args:
            conversation_history: 对话消息列表
            max_messages: 要保留的最大消息数

        Returns:
            处理后的对话历史
        """
        # 判断 `not conversation_history` 是否成立，以选择正确的处理分支。
        if not conversation_history:
            # 结束当前函数，并把处理结果返回给调用方。
            return []

        # 仅保留最后max_messages条消息
        # 计算或设置 `recent_history`，用于保存后续流程需要的中间结果。
        recent_history = conversation_history[-max_messages:]

        # 为LangChain格式化
        # 计算或设置 `formatted_history`，用于保存后续流程需要的中间结果。
        formatted_history = []
        # 遍历当前集合中的每一项，逐项完成相同处理。
        for msg in recent_history:
            # 判断 `msg.get("role") == "user"` 是否成立，以选择正确的处理分支。
            if msg.get("role") == "user":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append({"role": "human", "content": msg["content"]})
            # 判断 `msg.get("role") == "assistant"` 是否成立，以选择正确的处理分支。
            elif msg.get("role") == "assistant":
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                formatted_history.append({"role": "ai", "content": msg["content"]})

        # 结束当前函数，并把处理结果返回给调用方。
        return formatted_history
