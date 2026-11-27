"""
岗位描述（JD）管理接口层。

【文件定位】
本文件是 JD 模块的 FastAPI Controller，负责暴露新增、更新、详情、分页列表和删除接口。
它只处理 HTTP 请求、登录用户依赖、参数校验及异常转换，具体业务交给
JobDescriptionService。本项目的 JD 不直接写入本地 PostgreSQL，而是由 Service 继续
调用 remote_service_client，将数据交给远程 HR 服务持久化。

【主要函数】
- save_job_description：接收前端新增 JD JSON，取得当前用户后调用 Service 保存，返回远程服务
  分配的 JD ID 和完整 JD 数据；对应 POST /save。
- update_job_description：根据路径中的 jd_id 更新已有 JD；找不到记录返回 404，其他失败返回 500；
  对应 PUT /{jd_id}。
- get_job_description：按 jd_id 查询当前用户可访问的单条 JD；对应 GET /{jd_id}。
- list_job_descriptions：校验 page、size 和可选状态条件，调用 Service 返回分页列表；对应 GET /。
- delete_job_description：按 jd_id 删除 JD，并由 Service 继续处理关联评分标准；对应 DELETE /{jd_id}。

【完整调用关系】
JDGenerator.vue -> jobDescriptionApi -> 本文件 -> JobDescriptionService
-> remote_service_client -> 远程 HR 服务。

【Python/FastAPI 阅读重点】
- @router.xxx 是装饰器，用于登记请求方法和相对路径。
- Depends(...) 是依赖注入，用于获得当前用户和数据库会话。
- 类型名后面的冒号是类型标注，-> Any 是返回值类型提示。
- await 等待异步 HTTP 操作，等待期间不会阻塞整个 Web 服务。
- response_model 使用 Pydantic 校验并序列化返回数据。
"""

# 导入 Python 日志模块，用于记录接口运行和异常信息。
import logging
# 导入 Any 和 Optional，供函数的类型标注使用。
from typing import Any, Optional
# 导入 FastAPI 的路由、依赖注入、异常、状态码和查询参数工具。
from fastapi import APIRouter, Depends, HTTPException, status, Query
# 导入 SQLAlchemy 异步会话类型，用于标注注入的数据库会话。
from sqlalchemy.ext.asyncio import AsyncSession

# 导入 get_db；FastAPI 调用它为每个请求创建并回收数据库会话。
from app.core.database import get_db
# 导入 get_current_user；它会解析 Token 并取得当前登录用户。
from app.api.deps import get_current_user
# 导入用户响应模型，并起别名 UserSchema，避免与 ORM User 混淆。
from app.schemas.user import User as UserSchema
# 从 JD Schema 文件中导入请求和响应数据模型。
from app.schemas.job_description import (
    # 新增 JD 时使用的请求模型，负责校验前端提交字段。
    JobDescriptionCreate,
    # 更新 JD 时使用的请求模型，通常允许只提交需要修改的字段。
    JobDescriptionUpdate,
    # 单条 JD 的响应模型，用来约束返回给前端的 JSON 结构。
    JobDescriptionResponse,
    # JD 分页列表响应模型，通常包含 items、total、page 和 size。
    JobDescriptionListResponse
# 结束本次跨行导入列表。
)
# 导入 JD 业务服务，接口函数通过它访问远程 HR 服务。
from app.services.job_description_service import JobDescriptionService

# 创建当前模块的日志记录器；__name__ 是当前 Python 模块名称。
logger = logging.getLogger(__name__)
# 创建 FastAPI 子路由对象，下面定义的所有 JD 接口都会登记到这里。
router = APIRouter()


# 注册新增 JD 接口；上层再拼接 /api/v1/job-descriptions，形成最终 URL。
@router.post("/save", response_model=JobDescriptionResponse)
# 定义异步接口函数；async 表示函数中可以使用 await 等待 I/O。
async def save_job_description(
    # jd_data 是 FastAPI 根据前端 JSON 自动构造并校验的新增 JD 对象。
    jd_data: JobDescriptionCreate,
    # 冒号后是类型，等号后 Depends 表示该参数由 FastAPI 自动注入。
    current_user: UserSchema = Depends(get_current_user),
    # 注入当前请求的异步数据库会话；Service 为兼容统一接口而接收它。
    db: AsyncSession = Depends(get_db)
# 结束参数列表；-> Any 表示这里只提供宽泛的返回值类型提示。
) -> Any:
    """接收前端的新 JD 数据，保存到远程 HR 服务并返回带 ID 的 JD。"""
    # 开始捕获保存过程中可能出现的远程请求或数据转换异常。
    try:
        # 创建 JD 业务服务对象，并把当前数据库会话传入构造方法。
        service = JobDescriptionService(db)
        # 异步调用新增方法；同时传入当前用户 ID，用于远程服务的数据隔离。
        jd = await service.create_job_description(jd_data, current_user.id)
        # 将 Service 返回的 JobDescriptionResponse 交给 FastAPI 序列化为 JSON。
        return jd
    # 捕获所有普通异常并用变量 e 保存异常对象。
    except Exception as e:
        # 抛出 HTTP 500，让前端知道服务端保存失败，而不是返回不完整数据。
        raise HTTPException(
            # 指定响应状态为 500 Internal Server Error。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将底层异常文本放入响应 detail，方便开发阶段定位原因。
            detail=str(e)
        # 结束 HTTPException 构造调用。
        )


# 注册更新接口；{jd_id} 是路径参数，例如 /123e4567-...。
@router.put("/{jd_id}", response_model=JobDescriptionResponse)
# 定义“根据 ID 更新 JD”的异步接口函数。
async def update_job_description(
    # 从 URL 路径中取得要更新的 JD ID；这里先按字符串接收。
    jd_id: str,
    # FastAPI 把请求体 JSON 校验并转换成 JobDescriptionUpdate 对象。
    jd_data: JobDescriptionUpdate,
    # 自动解析 Bearer Token，取得当前登录用户。
    current_user: UserSchema = Depends(get_current_user),
    # 自动获得当前请求的异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 声明该接口可返回任意兼容 FastAPI 序列化的结果。
) -> Any:
    """使用 jd_id 定位已有记录，并用前端提交的字段更新它。"""
    # 开始执行可能失败的更新流程。
    try:
        # 创建业务服务对象，将接口层和远程调用细节分离。
        service = JobDescriptionService(db)
        # 等待远程更新完成；参数依次是 JD ID、修改数据和当前用户 ID。
        jd = await service.update_job_description(jd_id, jd_data, current_user.id)
        # 返回远程服务更新后的最新 JD 数据。
        return jd
    # ValueError 通常表示远程服务返回资源不存在等可预期业务错误。
    except ValueError as e:
        # 将“资源不存在”转换成更准确的 HTTP 404 响应。
        raise HTTPException(
            # 404 表示无法找到指定 JD。
            status_code=status.HTTP_404_NOT_FOUND,
            # 把具体错误原因放到响应 detail。
            detail=str(e)
        # 结束 404 异常构造。
        )
    # 捕获除 ValueError 之外的其他运行异常。
    except Exception as e:
        # 未知服务端异常统一转换为 HTTP 500。
        raise HTTPException(
            # 设置服务端错误状态码。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 返回异常的文字说明。
            detail=str(e)
        # 结束 500 异常构造。
        )


# 注册单条 JD 查询接口；response_model 保证返回结构符合 JD Schema。
@router.get("/{jd_id}", response_model=JobDescriptionResponse)
# 定义根据 JD ID 查询详情的异步接口。
async def get_job_description(
    # 从请求 URL 中接收目标 JD ID。
    jd_id: str,
    # 注入当前用户，远程查询会用用户 ID 限制数据访问范围。
    current_user: UserSchema = Depends(get_current_user),
    # 注入异步数据库会话并交给业务服务。
    db: AsyncSession = Depends(get_db)
# 结束函数签名并声明宽泛返回类型。
) -> Any:
    """查询并返回当前用户有权访问的指定 JD。"""
    # 开始执行详情查询并捕获异常。
    try:
        # 实例化 JD 业务服务。
        service = JobDescriptionService(db)
        # 调用远程查询；jd_id 定位记录，current_user.id 用于权限隔离。
        jd = await service.get_job_description(jd_id, current_user.id)
        # 将查询到的 JD 返回前端。
        return jd
    # 捕获“JD 不存在”等 ValueError。
    except ValueError as e:
        # 转换成 HTTP 404 资源不存在响应。
        raise HTTPException(
            # 指定 404 状态码。
            status_code=status.HTTP_404_NOT_FOUND,
            # 返回具体错误内容。
            detail=str(e)
        # 结束异常构造。
        )
    # 捕获网络故障、解析失败等其他异常。
    except Exception as e:
        # 转换为 HTTP 500 服务端错误。
        raise HTTPException(
            # 指定 500 状态码。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 返回具体异常内容。
            detail=str(e)
        # 结束异常构造。
        )


# 注册 JD 分页列表接口；路径为当前路由前缀的根路径。
@router.get("/", response_model=JobDescriptionListResponse)
# 定义查询当前用户 JD 列表的异步接口。
async def list_job_descriptions(
    # page 来自 URL 查询参数，默认 1；ge=1 表示必须大于等于 1。
    page: int = Query(1, ge=1, description="页码"),
    # size 默认 10，取值限制在 1 到 100，防止一次查询过多数据。
    size: int = Query(10, ge=1, le=100, description="每页数量"),
    # status_filter 是可选状态过滤条件；不传时为 None，表示不过滤。
    status_filter: Optional[str] = Query(None, description="状态筛选"),
    # 注入当前登录用户，只返回该用户范围内的 JD。
    current_user: UserSchema = Depends(get_current_user),
    # 注入当前请求的数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数签名。
) -> Any:
    """按页查询当前用户的 JD，可选按状态过滤。"""
    # 开始列表查询流程。
    try:
        # 创建 JD 业务服务。
        service = JobDescriptionService(db)
        # 异步调用分页查询方法，并等待远程 HR 服务返回结果字典。
        result = await service.list_job_descriptions(
            # current_user.id 是 JWT 对应用户的唯一 ID。
            user_id=current_user.id,
            # 把前端请求的页码传给 Service。
            page=page,
            # 把每页数量传给 Service。
            size=size,
            # 把可选状态条件传给 Service。
            status_filter=status_filter
        # 结束分页查询调用。
        )

        # **result 把结果字典展开为响应模型的命名参数，并进行最终结构校验。
        return JobDescriptionListResponse(**result)
    # 捕获分页查询和响应模型构造过程中出现的异常。
    except Exception as e:
        # 将异常统一转换为 HTTP 500。
        raise HTTPException(
            # 设置服务端错误状态码。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把异常文字返回到 detail。
            detail=str(e)
        # 结束异常构造。
        )


# 注册删除接口；URL 中的 jd_id 指定要删除的记录。
@router.delete("/{jd_id}")
# 定义删除 JD 的异步接口函数。
async def delete_job_description(
    # 从路径参数中接收目标 JD ID。
    jd_id: str,
    # 注入当前登录用户，用于校验数据归属。
    current_user: UserSchema = Depends(get_current_user),
    # 注入当前异步数据库会话。
    db: AsyncSession = Depends(get_db)
# 结束函数签名。
) -> Any:
    """删除指定 JD；具体删除方式及关联评分标准处理由 Service 决定。"""
    # 开始删除业务流程。
    try:
        # 创建 JD 业务服务对象。
        service = JobDescriptionService(db)
        # 等待 Service 删除指定 JD，并传入用户 ID 进行权限隔离。
        result = await service.delete_job_description(jd_id, current_user.id)
        # 返回删除结果，例如成功消息及关联数据处理情况。
        return result
    # 捕获远程资源不存在产生的 ValueError。
    except ValueError as e:
        # 将其转换为 HTTP 404。
        raise HTTPException(
            # 设置资源不存在状态码。
            status_code=status.HTTP_404_NOT_FOUND,
            # 返回具体错误原因。
            detail=str(e)
        # 结束异常构造。
        )
    # 捕获删除期间的其他异常。
    except Exception as e:
        # 将未知错误转换为 HTTP 500。
        raise HTTPException(
            # 设置服务端错误状态码。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 返回异常文本，便于开发阶段排查。
            detail=str(e)
        # 结束异常构造。
        )
