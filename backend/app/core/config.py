"""
* 文件作用
  * 这是项目的集中配置入口，使用 Pydantic BaseSettings 把 .env 环境变量解析成具有类型约束的 Settings 对象。文件统一管理应用端口、JWT、PostgreSQL、远程 HR 服务、CORS、LLM、Embedding、查询增强、上传限制、Dify、重排和 RAG 权重，并通过 validator 对特殊字段进行预处理。所有服务读取的是这里生成的 settings，因此排查模型地址、密钥、向量维度或上传限制时，应先确认环境变量名、类型转换和默认值是否与本文件一致。
* 类说明
  * `Settings`：基于 Pydantic BaseSettings 的集中配置类，负责从 .env 读取并校验数据库、JWT、CORS、LLM、Embedding、上传、Dify、查询增强、重排和 RAG 权重配置。业务代码统一访问 settings.xxx，避免在不同服务中重复读取环境变量或写死密钥与模型参数。
* 函数与方法说明
  * `Settings.assemble_cors_origins()`：组装CORS源 assemble_cors_origins() 是一个字段校验器: 1. 如果是普通逗号字符串，就拆成列表； 2. 如果已经是列表或JSON字符串，就直接返回。
  * `Settings.DATABASE_NAME_FROM_URL()`：从DATABASE_URL中提取数据库名称： DATABASE_NAME_FROM_URL 会从 DATABASE_URL 最后一个 / 后面提取数据库名。 如果 URL 不符合格式，则返回 DATABASE_NAME。
"""

"""
    应用程序配置设置
"""
# 从 `pathlib` 导入当前模块需要的 `Path`。
from pathlib import Path
# 从 `typing` 导入当前模块需要的 `List, Optional`。
from typing import List, Optional
# 从 `pydantic` 导入当前模块需要的 `field_validator, Field`。
from pydantic import field_validator, Field
# 从 `pydantic_settings` 导入当前模块需要的 `BaseSettings`。
from pydantic_settings import BaseSettings


'''
    config.py 是后端的全局配置文件：
        项目里很多模块都会导入这里的 settings，统一读取应用端口、数据库、LLM、RAG、上传、日志等配置。
        Settings继承自 pydantic_settings.BaseSettings。配置值可以来自两处：
            1. 类里写的默认值。
            2. .env 文件或环境变量中的同名配置。
'''

# 定义 `Settings` 类，集中封装相关数据或业务能力。
class Settings(BaseSettings):
    # 补充当前字符串、提示词或响应正文内容。
    """应用程序设置"""

    # 基本应用设置:
    #   定义项目名称、版本、调试模式、监听地址和端口：
    PROJECT_NAME: str = "HR Agent"
    # 声明参数或字段 `VERSION`，表示当前业务需要的数据。
    VERSION: str = "1.0.0"
    # 声明参数或字段 `DEBUG`，表示当前业务需要的数据。
    DEBUG: bool = False
    # 声明参数或字段 `HOST`，表示当前业务需要的数据。
    HOST: str = "0.0.0.0"
    # 声明参数或字段 `PORT`，表示当前业务需要的数据。
    PORT: int = 8000

    # API设置
    API_V1_STR: str = "/api/v1"

    # 安全设置
    SECRET_KEY: str = Field(min_length=32)
    # 声明参数或字段 `ACCESS_TOKEN_EXPIRE_MINUTES`，表示当前业务需要的数据。
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8  # 8天

    # CORS设置
    BACKEND_CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8080"]

    # 应用装饰器，为下方类或函数增加框架行为。
    @field_validator("BACKEND_CORS_ORIGINS", mode="before")
    # 应用装饰器，为下方类或函数增加框架行为。
    @classmethod
    # 定义 `assemble_cors_origins()`，执行该函数对应的业务流程。
    def assemble_cors_origins(cls, v):
        """
            组装CORS源

            assemble_cors_origins() 是一个字段校验器:
                1. 如果是普通逗号字符串，就拆成列表；
                2. 如果已经是列表或JSON字符串，就直接返回
        """
        # 判断 `isinstance(v, str) and not v.startswith("[")` 是否成立，以选择正确的处理分支。
        if isinstance(v, str) and not v.startswith("["):
            # 结束当前函数，并把处理结果返回给调用方。
            return [i.strip() for i in v.split(",")]
        # 判断 `isinstance(v, (list, str))` 是否成立，以选择正确的处理分支。
        elif isinstance(v, (list, str)):
            # 结束当前函数，并把处理结果返回给调用方。
            return v
        # 抛出异常并终止当前失败流程，由上层统一转换或返回错误。
        raise ValueError(v)

    # 数据库设置:
    #    DATABASE_URL是完整连接串，
    #    DATABASE_POOL_SIZE 和 DATABASE_MAX_OVERFLOW 控制 SQLAlchemy连接池。
    DATABASE_URL: str = ""
    # 声明参数或字段 `DATABASE_HOST`，表示当前业务需要的数据。
    DATABASE_HOST: str = "127.0.0.1"
    # 声明参数或字段 `DATABASE_PORT`，表示当前业务需要的数据。
    DATABASE_PORT: int = 5432
    # 声明参数或字段 `DATABASE_NAME`，表示当前业务需要的数据。
    DATABASE_NAME: str = "hr_agent"
    # 声明参数或字段 `DATABASE_USER`，表示当前业务需要的数据。
    DATABASE_USER: str = "username"
    # 声明参数或字段 `DATABASE_PASSWORD`，表示当前业务需要的数据。
    DATABASE_PASSWORD: str = ""
    # 声明参数或字段 `DATABASE_POOL_SIZE`，表示当前业务需要的数据。
    DATABASE_POOL_SIZE: int = 10
    # 声明参数或字段 `DATABASE_MAX_OVERFLOW`，表示当前业务需要的数据。
    DATABASE_MAX_OVERFLOW: int = 20

    # 远程服务配置
    HR_SERVICE_HOST: str = "127.0.0.1"
    # 声明参数或字段 `HR_SERVICE_PORT`，表示当前业务需要的数据。
    HR_SERVICE_PORT: int = 8000
    # 声明参数或字段 `HR_SERVICE_APIKEY`，表示当前业务需要的数据。
    HR_SERVICE_APIKEY: str = ""

    # 应用装饰器，为下方类或函数增加框架行为。
    @property
    # 定义 `DATABASE_NAME_FROM_URL()`，执行该函数对应的业务流程。
    def DATABASE_NAME_FROM_URL(self) -> str:
        """
            从DATABASE_URL中提取数据库名称：
                DATABASE_NAME_FROM_URL 会从 DATABASE_URL 最后一个 / 后面提取数据库名。
                如果 URL 不符合格式，则返回 DATABASE_NAME。
        """
        # 判断 `"/" in self.DATABASE_URL` 是否成立，以选择正确的处理分支。
        if "/" in self.DATABASE_URL:
            # 结束当前函数，并把处理结果返回给调用方。
            return self.DATABASE_URL.split("/")[-1]
        # 结束当前函数，并把处理结果返回给调用方。
        return self.DATABASE_NAME


    # AI / RAG 配置
    """
        AI 能力相关配置：
            VECTOR_DIMENSION：向量维度，当前是 1536。
            LLM_API_KEY、LLM_BASE_URL、LLM_MODEL：大模型配置。
            EMBEDDING_API_KEY、EMBEDDING_BASE_URL、EMBEDDING_MODEL： Embedding配置。
            QWEN_API_KEY、QWEN_MODEL： 通义千问重排模型配置。
            RERANK_ENABLED、RERANK_TOP_K、RERANK_FINAL_K：重排开关和数量。
            KB_QUERY_ENHANCE_ENABLED、KB_QUERY_EXPANSION_MAX_TERMS：知识库查询改写配置。
            RAG_CONTENT_WEIGHT、RAG_TEXT_WEIGHT、RAG_MIN_SIMILARITY_SCORE：RAG 检索结果打分权重和过滤阈值。
            CONTEXT_LIMIT：检索上下文文档数量。
        这些配置会被知识库、RAG、重排、LLM 服务等模块使用。
    """

    # 向量数据库设置
    # 声明参数或字段 `VECTOR_DIMENSION`，表示当前业务需要的数据。
    VECTOR_DIMENSION: int = 1536  # 通义千问 text-embedding-v1维度

    # LLM设置
    # 声明参数或字段 `LLM_API_KEY`，表示当前业务需要的数据。
    LLM_API_KEY: Optional[str] = None
    # 声明参数或字段 `LLM_BASE_URL`，表示当前业务需要的数据。
    LLM_BASE_URL: Optional[str] = None
    # 声明参数或字段 `LLM_MODEL`，表示当前业务需要的数据。
    LLM_MODEL: str = "qwen-max"

    # 嵌入设置
    # 声明参数或字段 `EMBEDDING_API_KEY`，表示当前业务需要的数据。
    EMBEDDING_API_KEY: Optional[str] = None
    # 声明参数或字段 `EMBEDDING_BASE_URL`，表示当前业务需要的数据。
    EMBEDDING_BASE_URL: Optional[str] = None
    # 声明参数或字段 `EMBEDDING_MODEL`，表示当前业务需要的数据。
    EMBEDDING_MODEL: str = "text-embedding-v1"

    # Qwen设置
    # 声明参数或字段 `QWEN_API_KEY`，表示当前业务需要的数据。
    QWEN_API_KEY: Optional[str] = None
    # 声明参数或字段 `QWEN_MODEL`，表示当前业务需要的数据。
    QWEN_MODEL: str = "gte-rerank-v2"  # 默认Qwen重排模型

    # 文件上传设置
    # 声明参数或字段 `MAX_FILE_SIZE`，表示当前业务需要的数据。
    MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB
    # 声明参数或字段 `UPLOAD_DIR`，表示当前业务需要的数据。
    UPLOAD_DIR: str = "uploads"

    # Dify 工作流配置
    DIFY_BASE_URL: str = ""
    # 声明参数或字段 `DIFY_API_KEY`，表示当前业务需要的数据。
    DIFY_API_KEY: str = ""
    # 声明参数或字段 `DIFY_USER_ID`，表示当前业务需要的数据。
    DIFY_USER_ID: str = "hr-agent-user"

    # 重排设置
    # 声明参数或字段 `RERANK_ENABLED`，表示当前业务需要的数据。
    RERANK_ENABLED: bool = True  # 启用重排（使用Qwen模型）
    # 声明参数或字段 `RERANK_TOP_K`，表示当前业务需要的数据。
    RERANK_TOP_K: int = 10  # 需要重排的候选项数量
    # 声明参数或字段 `RERANK_FINAL_K`，表示当前业务需要的数据。
    RERANK_FINAL_K: int = 10  # 重排后的最终结果数量

    # 查询改写设置
    # 声明参数或字段 `KB_QUERY_ENHANCE_ENABLED`，表示当前业务需要的数据。
    KB_QUERY_ENHANCE_ENABLED: bool = True  # 启用查询改写功能
    # 声明参数或字段 `KB_QUERY_EXPANSION_MAX_TERMS`，表示当前业务需要的数据。
    KB_QUERY_EXPANSION_MAX_TERMS: int = 6  # 查询扩展最大术语数量

    # RAG 分数组合权重设置
    # 声明参数或字段 `RAG_CONTENT_WEIGHT`，表示当前业务需要的数据。
    RAG_CONTENT_WEIGHT: float = 0.7  # 向量相似度权重
    # 声明参数或字段 `RAG_TEXT_WEIGHT`，表示当前业务需要的数据。
    RAG_TEXT_WEIGHT: float = 0.3  # 文本匹配权重
    # 声明参数或字段 `RAG_MIN_SIMILARITY_SCORE`，表示当前业务需要的数据。
    RAG_MIN_SIMILARITY_SCORE: float = 0.2  # 低于该阈值的检索结果将被过滤

    # 上下文设置
    CONTEXT_LIMIT: int = Field(10, description="要检索的上下文文档数量")  # 要检索的上下文文档数量

    # 日志设置
    LOG_LEVEL: str = "INFO"
    # 声明参数或字段 `LOG_FORMAT`，表示当前业务需要的数据。
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # 远程服务请求/响应日志设置
    # 声明参数或字段 `REMOTE_SERVICE_LOG_ENABLED`，表示当前业务需要的数据。
    REMOTE_SERVICE_LOG_ENABLED: bool = False  # 是否输出远程服务请求/响应详情

    """
        model_config 是 pydantic-settings 的配置项，用来告诉 Settings 这个配置类怎么读取和处理配置。
          1. env_file：指定从哪个 .env 文件读取配置。
          2. 所以 Settings() 初始化时，会自动读取 backend/.env 里的环境变量
          3. 如果 .env里有, Settings类没有声明的配置项，就忽略它，不报错。
    
        参数说明：
          env_file： .env 文件路径是 backend/.env。
          case_sensitive： 环境变量大小写敏感。
          extra： .env 里多出来但 Settings 没定义的字段会被忽略，不会报错。
    """
    # 开始组装多行调用参数、集合或对象内容。
    model_config = {
        # 补充当前字符串、提示词或响应正文内容。
        "env_file": Path(__file__).parent.parent.parent / ".env",  # 正确的.env文件路径
        # 补充当前字符串、提示词或响应正文内容。
        "case_sensitive": True,
        # 补充当前字符串、提示词或响应正文内容。
        "extra": "ignore"
    # 结束上方多行参数、集合或对象的定义。
    }


# 计算或设置 `settings`，用于保存后续流程需要的中间结果。
settings = Settings()
