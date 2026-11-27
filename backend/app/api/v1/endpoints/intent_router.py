"""
意图路由接口模块：接收前端自然语言需求，并返回应进入的业务页面。

功能：
    1. 提供 POST /api/v1/intent/route 接口。
    2. 接收前端传入的 {"query": "用户需求"} 数据。
    3. 通过 FastAPI Depends 获取当前登录用户和异步数据库会话。
    4. 创建 IntentService，并把用户输入交给业务层分类。
    5. 返回 intent、route、query 和可选 kb_id 等路由结果。
        5.1、intent：识别出的任务类型，例如 jd 表示生成 JD
        5.2、route：前端需要跳转的页面地址
        5.3、query：用户输入的原始需求
        5.4、kb_id：知识库 ID，只有知识库问答时才有值，其他任务通常为 null
注意： /api/v1/intent/route 接口组成原理
    1、@router.post("/route")
        → intent_router.py 定义模块内部路径 /route
    2、api_router.include_router(..., prefix="/intent")
        → api.py 添加业务模块前缀 /intent
    3、app.include_router(..., prefix="/api/v1")
        → main.py 添加 API 版本前缀 /api/v1
这样分层的好处是：
    main.py 统一管理 API 版本。
    api.py 统一管理业务模块。
    intent_router.py 只管理本模块内部接口。
    避免每个接口重复编写完整路径。

调用顺序：
    Dashboard.vue 的 handleIntentSubmit()
        -> request.post('/intent/route', { query })
        -> route_by_intent()
        -> get_current_user() 获取当前用户
        -> get_db() 获取 AsyncSession
        -> IntentService.route_query()
        -> classify_intent() 分类意图
        -> get_route_for_intent() 生成前端路由
        -> 返回 JSON 给 Dashboard.vue
        -> router.push() 跳转业务页面。

技术与分层：
    - APIRouter：声明和管理当前模块的 HTTP 路由。
    - Depends：使用 FastAPI 依赖注入认证用户和数据库会话。
    - AsyncSession：向 IntentService 提供异步 SQLAlchemy 会话。
    - Router 层只负责接收请求、组装依赖和返回结果，不负责关键词匹配或 LLM 业务逻辑。
"""

# 导入 Any，允许接口响应包含字符串、列表、字典等多种类型。
from typing import Any, Dict
# 导入 FastAPI 路由器和依赖注入函数。
from fastapi import APIRouter, Depends
# 导入 SQLAlchemy 异步数据库会话类型。
from sqlalchemy.ext.asyncio import AsyncSession
# 导入项目统一的数据库会话依赖。
from app.core.database import get_db
# 导入项目统一的当前用户认证依赖。
from app.api.deps import get_current_user
# 导入用户响应 Schema，用于标注当前登录用户的类型。
from app.schemas.user import User as UserSchema
# 导入意图识别业务服务。
from app.services.intent_service import IntentService

# 创建一个 FastAPI 路由对象，用来收集当前文件中的接口（即把 POST /route 接口注册到 router 中）。
router = APIRouter()


# 声明 POST /route 接口；最终完整路径会由上层路由拼接为 /api/v1/intent/route。
@router.post("/route")
# 定义根据用户输入进行意图分类和页面路由的方法。
async def route_by_intent(
    # 接收前端 JSON 请求体，例如 {"query": "生成前端工程师JD"}:payload 是前端发送的 JSON 请求体；Dict[str, Any]表示“Dict：字典；str：字典的键必须是字符串；Any：字典的值可以是任意类型”
    # 例如前端发送JSON“{"query": "生成前端工程师JD"}”，Dict[str, Any] 自动解析为 Python 字典，后端得到payload = {"query": "生成前端工程师JD"}。
    payload: Dict[str, Any],# ":"表示数据类型，即“变量名:数据类型”
    # 通过Depends依赖注入获取当前登录用户，未登录时接口不会继续执行。（即FastAPI 在执行当前接口之前，自动调用 get_current_user()；验证登录成功后，把得到的用户对象赋值给 current_user）
    current_user: UserSchema = Depends(get_current_user),
    # 通过Depends依赖注入获取当前请求专用的AsyncSession异步数据库会话。（即表示 FastAPI 自动调用项目中的 get_db()，为当前请求提供一个异步数据库会话，并赋值给 db）
    # get_db() 为当前 HTTP 请求创建一个 SQLAlchemy 异步数据库会话，并在请求结束后关闭它。
    db: AsyncSession = Depends(get_db)
) -> Any: # “async def route_by_intent(...) -> Any:”表示这个函数可以返回任意类型的数据。
    # 函数说明：对用户查询进行分类，并返回前端需要跳转的路由信息。
    """
    对用户查询进行分类并返回前端路由。

    请求体：{"query": "用户输入内容"}
    返回值：包含 intent、route、query、kb_id 的字典。
    """
    # 如果 payload 有值就使用 payload；如果它为空，则使用空字典 {}，strip()删除字符串首尾的空格，避免程序报错。
    query = (payload or {}).get("query", "").strip()

    # 创建一个 IntentService 对象，并把数据库会话 db 传进去（即创建意图识别服务+给它数据库操作能力）。之所以传入 db，是因为知识库问答意图需要查询当前用户可使用的文档和知识库。
    intent_service = IntentService(db)

    # 根据当前用户和用户输入完成意图识别，确定应该跳转到哪个功能页面。（“query：用户输入的需求";"current_user.id：当前登录用户的 ID";"await 表示等待这个异步方法执行完成，再取得返回结果"）
    result = await intent_service.route_query(query, current_user.id)

    # 将业务层生成的结果序列化为 JSON 返回给前端。
    return result
