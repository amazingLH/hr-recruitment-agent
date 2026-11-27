"""
* 文件作用
  * 这是 Embedding 能力的统一初始化和复用入口，使用单例模式保证整个进程只创建一套向量模型客户端与文本切分器，减少重复连接和配置开销。它从 Settings 读取 API Key、Base URL、模型名和分块参数，构造 CompatibleOpenAIEmbeddings 与 RecursiveCharacterTextSplitter，再通过访问器提供给文档处理和检索代码。学习重点包括 __new__ 实现单例、延迟初始化、OpenAI 兼容接口以及 chunk_size/chunk_overlap 对检索完整性和成本的影响。
* 类说明
  * `EmbeddingService`：Embedding 与文本切分器的单例管理类，通过 __new__() 保证一个进程只初始化一次。它从 settings 读取模型密钥、服务地址和模型名，创建 CompatibleOpenAIEmbeddings，并创建 RecursiveCharacterTextSplitter；其他服务通过 get_embeddings() 和 get_text_splitter() 复用同一套实例。
* 函数与方法说明
  * `EmbeddingService.__new__()`：EmbeddingService 的单例创建入口：首次实例化时由 object.__new__() 创建对象并保存到类变量 _instance，后续实例化直接返回同一对象；这样 Embedding 客户端和文本切分器不会在每次请求中重复初始化。
  * `EmbeddingService.__init__()`：这是 `EmbeddingService` 的构造方法，负责接收依赖并初始化后续方法共享的实例状态。内部调用 self._initialize() 完成主要工作，最终主要通过修改对象状态、提交事务或调用下游服务产生效果。
  * `EmbeddingService._initialize()`：初始化嵌入服务组件。
  * `EmbeddingService.get_embeddings()`：获取兼容的OpenAI嵌入实例。
  * `EmbeddingService.get_text_splitter()`：获取文本分割器实例。
  * `EmbeddingService.get_instance()`：获取单例实例。
  * `get_embedding_service()`：获取单例EmbeddingService实例。内部调用 EmbeddingService.get_instance() 完成主要工作，最终通过 return 把对象、列表、状态或转换后的结果交还调用者。
"""

"""
单例embedding嵌入服务，避免重复初始化OpenAIEmbeddings：
    EmbeddingService 初始化 CompatibleOpenAIEmbeddings 和 LangChain 文本切分器，
    统一提供 get_embeddings()、get_text_splitter()。get_embedding_service() 是全局获取入口，避免每次请求重复初始化模型配置
"""
# 导入 `logging`，供后续代码调用其功能。
import logging
# 从 `typing` 导入当前模块需要的 `Optional, List`。
from typing import Optional, List
# 从 `langchain_text_splitters` 导入当前模块需要的 `RecursiveCharacterTextSplitter`。
from langchain_text_splitters import RecursiveCharacterTextSplitter
# 从 `app.core.config` 导入当前模块需要的 `settings`。
from app.core.config import settings
# 从 `.compatible_embeddings` 导入当前模块需要的 `CompatibleOpenAIEmbeddings`。
from .compatible_embeddings import CompatibleOpenAIEmbeddings

# 计算或设置 `logger`，用于保存后续流程需要的中间结果。
logger = logging.getLogger(__name__)


# 定义 `EmbeddingService` 类，集中封装相关数据或业务能力。
class EmbeddingService:
    # 补充当前字符串、提示词或响应正文内容。
    """用于管理嵌入和文本分割的单例服务"""

    # 声明参数或字段 `_instance`，表示当前业务需要的数据。
    _instance: Optional['EmbeddingService'] = None
    # 声明参数或字段 `_initialized`，表示当前业务需要的数据。
    _initialized: bool = False

    # 定义 `__new__()`，执行该函数对应的业务流程。
    def __new__(cls) -> 'EmbeddingService':
        # 判断 `cls._instance is None` 是否成立，以选择正确的处理分支。
        if cls._instance is None:
            # 计算或设置 `cls._instance`，用于保存后续流程需要的中间结果。
            cls._instance = super().__new__(cls)
        # 结束当前函数，并把处理结果返回给调用方。
        return cls._instance

    # 定义 `__init__()`，执行该函数对应的业务流程。
    def __init__(self):
        # 判断 `not self._initialized` 是否成立，以选择正确的处理分支。
        if not self._initialized:
            # 调用当前对象的方法，推进本步骤的业务处理。
            self._initialize()
            # 计算或设置 `self._initialized`，用于保存后续流程需要的中间结果。
            self._initialized = True

    # 定义 `_initialize()`，执行该函数对应的业务流程。
    def _initialize(self):
        # 补充当前字符串、提示词或响应正文内容。
        """初始化嵌入服务组件"""
        # 开始执行可能失败的业务步骤，并交由后续异常分支统一处理。
        try:
            # 初始化兼容的OpenAI嵌入
            # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
            api_key = settings.EMBEDDING_API_KEY or settings.LLM_API_KEY
            # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
            base_url = settings.EMBEDDING_BASE_URL or "https://dashscope.aliyuncs.com/compatible-mode/v1"
            # 计算或设置 `model`，用于保存调用的模型名称。
            model = settings.EMBEDDING_MODEL or "text-embedding-v1"

            # 使用新的CompatibleOpenAIEmbeddings类
            # 开始组装多行调用参数、集合或对象内容。
            self.embeddings = CompatibleOpenAIEmbeddings(
                # 计算或设置 `api_key`，用于保存访问外部服务的鉴权密钥。
                api_key=api_key,
                # 计算或设置 `base_url`，用于保存外部模型服务基础地址。
                base_url=base_url,
                # 计算或设置 `model`，用于保存调用的模型名称。
                model=model
            # 结束上方多行参数、集合或对象的定义。
            )

            # 初始化文本分割器
            # 开始组装多行调用参数、集合或对象内容。
            self.text_splitter = RecursiveCharacterTextSplitter(
                # 计算或设置 `chunk_size`，用于保存后续流程需要的中间结果。
                chunk_size=300,
                # 计算或设置 `chunk_overlap`，用于保存后续流程需要的中间结果。
                chunk_overlap=100,
                # 计算或设置 `length_function`，用于保存后续流程需要的中间结果。
                length_function=len,
                # 计算或设置 `separators`，用于保存后续流程需要的中间结果。
                separators=["\n\n", "\n", " ", ""]
            # 结束上方多行参数、集合或对象的定义。
            )

            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.info("EmbeddingService已成功初始化OpenAI嵌入")

        # 捕获指定异常，避免底层错误未经处理直接中断请求。
        except Exception as e:
            # 记录当前执行状态或异常信息，便于开发和运行排查。
            logger.error(f"初始化EmbeddingService失败: {e}")
            # 执行当前语句，完成本步骤的数据准备、调用或状态更新。
            raise

    # 定义 `get_embeddings()`，执行该函数对应的业务流程。
    def get_embeddings(self) -> CompatibleOpenAIEmbeddings:
        # 补充当前字符串、提示词或响应正文内容。
        """获取兼容的OpenAI嵌入实例"""
        # 结束当前函数，并把处理结果返回给调用方。
        return self.embeddings

    # 定义 `get_text_splitter()`，执行该函数对应的业务流程。
    def get_text_splitter(self) -> RecursiveCharacterTextSplitter:
        # 补充当前字符串、提示词或响应正文内容。
        """获取文本分割器实例"""
        # 结束当前函数，并把处理结果返回给调用方。
        return self.text_splitter

    # 应用装饰器，为下方类或函数增加框架行为。
    @classmethod
    # 定义 `get_instance()`，执行该函数对应的业务流程。
    def get_instance(cls) -> 'EmbeddingService':
        # 补充当前字符串、提示词或响应正文内容。
        """获取单例实例"""
        # 判断 `cls._instance is None` 是否成立，以选择正确的处理分支。
        if cls._instance is None:
            # 计算或设置 `cls._instance`，用于保存后续流程需要的中间结果。
            cls._instance = cls()
        # 结束当前函数，并把处理结果返回给调用方。
        return cls._instance


# 获取单例实例的全局函数
# 定义 `get_embedding_service()`，执行该函数对应的业务流程。
def get_embedding_service() -> EmbeddingService:
    # 补充当前字符串、提示词或响应正文内容。
    """获取单例EmbeddingService实例"""
    # 结束当前函数，并把处理结果返回给调用方。
    return EmbeddingService.get_instance()
