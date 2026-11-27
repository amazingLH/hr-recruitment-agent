"""
简历评分标准 CRUD 接口层。

【文件定位】
本文件使用 FastAPI 暴露评分标准的保存、更新、详情、列表和删除接口。它不负责调用
Dify 生成内容；生成动作位于 hr_workflows.py。当前文件接收已经生成/编辑好的评分标准，
通过 ScoringCriteriaService 交给远程 HR 服务持久化。

【调用结构】
前端 scoringCriteriaApi -> 本文件路由 -> ScoringCriteriaService
-> remote_service_client -> 远程 HR 服务。

【主要函数】
- save_scoring_criteria：接收生成或编辑后的评分标准，调用 Service 新增记录并返回 criteria_id（表示该岗位使用的哪套评分标准）；对应 POST /save。
- update_scoring_criteria：根据 criteria_id 修改已有评分标准；对应 PUT /{criteria_id}。
- get_scoring_criteria：根据 criteria_id 查询单条评分标准；不存在时返回 404；对应 GET /{criteria_id}。
- get_scoring_criteria_list：接收分页参数及可选 job_description_id，查询当前用户的评分标准；传入 JD ID（表示哪个招聘岗位） 时可查找某个岗位对应的标准；对应 GET /。
- delete_scoring_criteria：根据 criteria_id 删除评分标准，并把远程服务异常转换成 HTTP 错误；对应 DELETE /{criteria_id}。

【阅读重点】
response_model 约束返回 JSON；Depends 注入当前用户和数据库会话；criteria_id 标识要
操作的评分标准；job_description_id 用于把评分标准关联到具体 JD。
"""

# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Any, Optional
# 引入依赖，用于把业务失败转换为明确的 HTTP 错误。
from fastapi import APIRouter, Depends, HTTPException, status, Query
# 引入依赖，用于异步操作本地 PostgreSQL。
from sqlalchemy.ext.asyncio import AsyncSession

# 引入依赖，用于为请求提供本地异步数据库会话。
from app.core.database import get_db
# 引入依赖，用于根据 Token 获取当前登录用户。
from app.api.deps import get_current_user
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.schemas.user import User as UserSchema
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.schemas.scoring_criteria import (
    # 引入或传递 `ScoringCriteriaCreate`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaCreate,
    # 引入或传递 `ScoringCriteriaUpdate`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaUpdate,
    # 引入或传递 `ScoringCriteriaResponse`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaResponse,
    # 引入或传递 `ScoringCriteriaListResponse`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaListResponse
# 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
)
# 引入依赖，用于管理远程 HR 服务中的评分标准。
from app.services.scoring_criteria_service import ScoringCriteriaService
# 引入依赖，用于记录业务执行过程和失败原因。
import logging

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)
# 创建当前模块的 FastAPI 路由容器，供上层统一挂载。
router = APIRouter()


# 对外提供 POST `/save` 接口，入口函数负责“完成当前模块初始化”。
@router.post("/save", response_model=ScoringCriteriaResponse)
# 封装“保存生成后的评分标准”流程，供接口层或其他业务服务调用。
async def save_scoring_criteria(
    # 提供需要新增或更新的评分标准内容
    criteria_data: ScoringCriteriaCreate,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“保存生成后的评分标准”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    保存生成的评分标准到数据库
    """
    # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = ScoringCriteriaService(db)
        # 调用 `service.save_scoring_criteria` 执行“保存生成后的评分标准”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.save_scoring_criteria(criteria_data, current_user.id)
        # 把“保存生成后的评分标准”得到的最终业务结果返回给接口层或前端。
        return result
    # 捕获“保存生成后的评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 记录“保存生成后的评分标准”的关键状态或失败原因，方便排查下游服务问题。
        logger.error(f"保存评分标准失败: {str(e)}")
        # 终止“保存生成后的评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"保存评分标准失败: {str(e)}"
        # 完成“保存生成后的评分标准”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 PUT `/{criteria_id}` 接口，入口函数负责“完成当前模块初始化”。
@router.put("/{criteria_id}", response_model=ScoringCriteriaResponse)
# 封装“更新已有评分标准”流程，供接口层或其他业务服务调用。
async def update_scoring_criteria(
    # 定位本次操作的评分标准记录
    criteria_id: str,
    # 提供需要新增或更新的评分标准内容
    criteria_data: ScoringCriteriaUpdate,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“更新已有评分标准”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    更新已保存的评分标准
    """
    # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = ScoringCriteriaService(db)
        # 调用 `service.update_scoring_criteria` 执行“更新已有评分标准”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.update_scoring_criteria(criteria_id, criteria_data, current_user.id)
        # 把“更新已有评分标准”得到的最终业务结果返回给接口层或前端。
        return result
    # 捕获“更新已有评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except ValueError as e:
        # 终止“更新已有评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_404_NOT_FOUND,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=str(e)
        # 完成“更新已有评分标准”所需的数据封装或下游调用，准备处理其结果。
        )
    # 捕获“更新已有评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 记录“更新已有评分标准”的关键状态或失败原因，方便排查下游服务问题。
        logger.error(f"更新评分标准失败: {str(e)}")
        # 终止“更新已有评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"更新评分标准失败: {str(e)}"
        # 完成“更新已有评分标准”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 GET `/{criteria_id}` 接口，入口函数负责“完成当前模块初始化”。
@router.get("/{criteria_id}", response_model=ScoringCriteriaResponse)
# 封装“查询指定评分标准”流程，供接口层或其他业务服务调用。
async def get_scoring_criteria(
    # 定位本次操作的评分标准记录
    criteria_id: str,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“查询指定评分标准”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    获取单个评分标准详情
    """
    # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = ScoringCriteriaService(db)
        # 调用 `service.get_scoring_criteria` 执行“查询指定评分标准”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.get_scoring_criteria(criteria_id, current_user.id)
        # 把“查询指定评分标准”得到的最终业务结果返回给接口层或前端。
        return result
    # 捕获“查询指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except ValueError as e:
        # 终止“查询指定评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_404_NOT_FOUND,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=str(e)
        # 完成“查询指定评分标准”所需的数据封装或下游调用，准备处理其结果。
        )
    # 捕获“查询指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 记录“查询指定评分标准”的关键状态或失败原因，方便排查下游服务问题。
        logger.error(f"获取评分标准失败: {str(e)}")
        # 终止“查询指定评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"获取评分标准失败: {str(e)}"
        # 完成“查询指定评分标准”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 GET `/` 接口，入口函数负责“完成当前模块初始化”。
@router.get("/", response_model=ScoringCriteriaListResponse)
# 封装“分页查询评分标准”流程，供接口层或其他业务服务调用。
async def get_scoring_criteria_list(
    # 指定要查询的结果页
    page: int = Query(1, ge=1, description="页码"),
    # 限制当前页返回的记录数量
    size: int = Query(10, ge=1, le=100, description="每页数量"),
    # 把评分或简历评价关联到指定岗位 JD
    job_description_id: Optional[str] = Query(None, description="关联的JD ID"),
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“分页查询评分标准”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    获取评分标准列表
    """
    # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = ScoringCriteriaService(db)
        # 调用 `service.get_scoring_criteria_list` 执行“分页查询评分标准”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.get_scoring_criteria_list(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id,
            # 保存当前页码，供远程分页查询。
            page=page,
            # 保存每页数量，限制远程接口返回规模。
            size=size,
            # 关联对应的岗位 JD，供评分和后续招聘流程使用。
            job_description_id=job_description_id
        # 完成“分页查询评分标准”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把“分页查询评分标准”得到的最终业务结果返回给接口层或前端。
        return result
    # 捕获“分页查询评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 记录“分页查询评分标准”的关键状态或失败原因，方便排查下游服务问题。
        logger.error(f"获取评分标准列表失败: {str(e)}")
        # 终止“分页查询评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"获取评分标准列表失败: {str(e)}"
        # 完成“分页查询评分标准”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 DELETE `/{criteria_id}` 接口，入口函数负责“完成当前模块初始化”。
@router.delete("/{criteria_id}")
# 封装“删除指定评分标准”流程，供接口层或其他业务服务调用。
async def delete_scoring_criteria(
    # 定位本次操作的评分标准记录
    criteria_id: str,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“删除指定评分标准”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    删除评分标准（软删除）
    """
    # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = ScoringCriteriaService(db)
        # 调用 `service.delete_scoring_criteria` 执行“删除指定评分标准”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.delete_scoring_criteria(criteria_id, current_user.id)
        # 把“删除指定评分标准”得到的最终业务结果返回给接口层或前端。
        return result
    # 捕获“删除指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except ValueError as e:
        # 终止“删除指定评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_404_NOT_FOUND,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=str(e)
        # 完成“删除指定评分标准”所需的数据封装或下游调用，准备处理其结果。
        )
    # 捕获“删除指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 记录“删除指定评分标准”的关键状态或失败原因，方便排查下游服务问题。
        logger.error(f"删除评分标准失败: {str(e)}")
        # 终止“删除指定评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"删除评分标准失败: {str(e)}"
        # 完成“删除指定评分标准”所需的数据封装或下游调用，准备处理其结果。
        )
