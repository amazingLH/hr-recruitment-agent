"""
* 文件作用
  * 这是对 OpenAI 兼容 Embedding HTTP API 的适配器，使阿里百炼等兼容服务能够按 LangChain Embeddings 接口被项目调用。类内部同时创建同步和异步客户端，并分别实现批量文档向量化、单条查询向量化及对应的异步版本；返回值是浮点数数组，后续会写入 PGVector或用于相似度检索。该文件体现适配器模式、SDK 客户端封装、同步/异步 API 对称设计和模型配置注入。
* 类说明
  * `CompatibleOpenAIEmbeddings`：OpenAI 兼容 Embedding API 的 LangChain 适配类，内部同时维护同步 OpenAI 客户端和异步 AsyncOpenAI 客户端。embed_documents()/aembed_documents() 批量生成文档向量，embed_query()/aembed_query() 生成查询向量，使阿里百炼兼容接口可以直接用于 LangChain 和 PGVector。
* 函数与方法说明
  * `CompatibleOpenAIEmbeddings.__init__()`：这是 `CompatibleOpenAIEmbeddings` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。
  * `CompatibleOpenAIEmbeddings.embed_documents()`：嵌入搜索文档，自动分批次处理以符合API限制。内部调用 self.client.embeddings.create() 完成主要工作，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `CompatibleOpenAIEmbeddings.embed_query()`：嵌入查询文本。内部调用 self.client.embeddings.create() 完成主要工作，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
  * `CompatibleOpenAIEmbeddings.aembed_documents()`：异步嵌入搜索文档。
  * `CompatibleOpenAIEmbeddings.aembed_query()`：异步嵌入查询文本。
"""

"""
 OpenAI 兼容 embedding 封装。
    CompatibleOpenAIEmbeddings 实现 LangChain 的 Embeddings 接口，
    内部用 OpenAI 兼容 API 生成文本向量，同时提供同步和异步方法。它让项目可以使用自定义 base_url、模型名和向量维度。
"""

# 导入 `asyncio`，供后续代码调用其功能。
import asyncio
# 从 `typing` 导入当前模块需要的 `List, Optional`。
from typing import List, Optional
# 从 `openai` 导入当前模块需要的 `OpenAI`。
from openai import OpenAI
# 从 `langchain_core.embeddings` 导入当前模块需要的 `Embeddings`。
from langchain_core.embeddings import Embeddings
# 从 `..core.config` 导入当前模块需要的 `settings`。
from ..core.config import settings
# 导入 `logging`，供后续代码调用其功能。
import logging
# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)

# 定义 `CompatibleOpenAIEmbeddings` 类，集中封装相关数据或业务能力。
class CompatibleOpenAIEmbeddings(Embeddings):
    # 补充当前字符串、提示词或响应正文内容。
    """使用OpenAI兼容API的兼容嵌入。"""

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(
        # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
        self,
        # 声明参数或字段 `api_key`，表示访问外部服务的鉴权密钥。
        api_key: Optional[str] = None,
        # 声明参数或字段 `base_url`，表示外部模型服务基础地址。
        base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1",
        # 声明参数或字段 `model`，表示调用的模型名称。
        model: str = "text-embedding-v1",
        # 声明参数或字段 `dimensions`，表示当前业务需要的数据。
        dimensions: int = None
    # 结束上方多行参数、集合或对象的定义。
    ):
        # 计算或设置 `self.api_key`，用于保存访问外部服务的鉴权密钥。
        self.api_key = api_key or settings.EMBEDDING_API_KEY
        # 计算或设置 `self.base_url`，用于保存外部模型服务基础地址。
        self.base_url = base_url
        # 计算或设置 `self.model`，用于保存调用的模型名称。
        self.model = model
        # 计算或设置 `self.dimensions`，用于保存后续流程需要的中间结果。
        self.dimensions = dimensions or settings.VECTOR_DIMENSION

        # 开始组装多行调用参数、集合或对象内容。
        self.client = OpenAI(
            # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
            api_key=self.api_key,
            # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
            base_url=self.base_url
        # 结束上方多行参数、集合或对象的定义。
        )

        # 记录当前执行状态或异常信息，便于开发和运行排查。
        logger.info(f"DashScope兼容嵌入已使用模型初始化: {self.model}")

    # 计算或设置 `BATCH_SIZE`，用于保存后续流程需要的中间结果。
    BATCH_SIZE = 20  # API限制为25，留余量

    # 定义 `embed_documents()`，执行该函数对应的业务流程。
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        # 补充当前字符串、提示词或响应正文内容。
        """嵌入搜索文档，自动分批次处理以符合API限制。"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 计算或设置 `all_embeddings`，用于保存后续流程需要的中间结果。
            all_embeddings = []
            # 遍历当前集合中的每一项，逐项完成相同处理。
            for i in range(0, len(texts), self.BATCH_SIZE):
                # 计算或设置 `batch`，用于保存后续流程需要的中间结果。
                batch = texts[i:i + self.BATCH_SIZE]
                # 开始组装多行调用参数、集合或对象内容。
                response = self.client.embeddings.create(
                    # 计算或设置 `model`，用于保存调用的模型名称。
                    model=self.model,
                    # 计算或设置 `input`，用于保存后续流程需要的中间结果。
                    input=batch,
                    # 计算或设置 `dimensions`，用于保存后续流程需要的中间结果。
                    dimensions=self.dimensions,
                    # 计算或设置 `encoding_format`，用于保存后续流程需要的中间结果。
                    encoding_format="float"
                # 结束上方多行参数、集合或对象的定义。
                )
                # 计算或设置 `sorted_data`，用于保存后续流程需要的中间结果。
                sorted_data = sorted(response.data, key=lambda x: x.index)
                # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
                all_embeddings.extend(d.embedding for d in sorted_data)
            # 结束当前函数，并把处理结果返回给调用方。
            return all_embeddings
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"嵌入文档时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `embed_query()`，执行该函数对应的业务流程。
    def embed_query(self, text: str) -> List[float]:
        # 补充当前字符串、提示词或响应正文内容。
        """嵌入查询文本。"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 开始组装多行调用参数、集合或对象内容。
            response = self.client.embeddings.create(
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=self.model,
                # 计算或设置 `input`，用于保存后续流程需要的中间结果。
                input=text,
                # 计算或设置 `dimensions`，用于保存后续流程需要的中间结果。
                dimensions=self.dimensions,
                # 计算或设置 `encoding_format`，用于保存后续流程需要的中间结果。
                encoding_format="float"
            # 结束上方多行参数、集合或对象的定义。
            )
            # 结束当前函数，并把处理结果返回给调用方。
            return response.data[0].embedding
        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"嵌入查询时出错: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `aembed_documents()`，执行该函数对应的业务流程。
    async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
        # 补充当前字符串、提示词或响应正文内容。
        """异步嵌入搜索文档。"""
        # 计算或设置 `loop`，用于保存后续流程需要的中间结果。
        loop = asyncio.get_event_loop()
        # 结束当前函数，并把处理结果返回给调用方。
        return await loop.run_in_executor(None, self.embed_documents, texts)

    # 定义 `aembed_query()`，执行该函数对应的业务流程。
    async def aembed_query(self, text: str) -> List[float]:
        # 补充当前字符串、提示词或响应正文内容。
        """异步嵌入查询文本。"""
        # 计算或设置 `loop`，用于保存后续流程需要的中间结果。
        loop = asyncio.get_event_loop()
        # 结束当前函数，并把处理结果返回给调用方。
        return await loop.run_in_executor(None, self.embed_query, text)
