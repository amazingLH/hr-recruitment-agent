"""
智能面试方案 CRUD 接口层。

【文件定位】
本文件把面试方案管理能力暴露为 FastAPI 接口，包括保存生成结果、更新、详情、列表和
删除。Dify 生成接口位于 hr_workflows.py；这里处理的是生成完成后的持久化和查询。

【调用结构】
SmartInterview.vue -> hrWorkflowsApi -> 本文件路由 -> InterviewPlanService
-> 本地简历权限校验 / 远程 HR 服务。

【主要函数】
- create_interview_plan：接收 Dify 已生成的面试方案及 resume_evaluation_id，调用 Service 校验
  简历归属并保存；对应 POST /save-generated。
- update_interview_plan：根据 plan_id 保存用户编辑后的方案内容；对应 PUT /{plan_id}。
- get_interview_plan：根据 plan_id 查询单份方案，返回候选人、岗位和正文等信息；对应 GET /{plan_id}。
- list_interview_plans：校验 page 和 size，并可按 resume_evaluation_id 查询某位候选人的已有方案；
  对应 GET /。
- delete_interview_plan：根据 plan_id 删除方案并返回处理结果；对应 DELETE /{plan_id}。

【阅读重点】
plan_id 是面试方案 ID；resume_evaluation_id 是简历评价 ID；current_user 用于权限隔离；
Pydantic 请求/响应模型负责验证字段并把 Python 对象序列化为 JSON。
"""

# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Any, List, Optional
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID
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
from app.schemas.interview_plan import (
    # 引入或传递 `InterviewPlanCreate`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanCreate,
    # 引入或传递 `InterviewPlanUpdate`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanUpdate,
    # 引入或传递 `InterviewPlanResponse`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanResponse,
    # 引入或传递 `InterviewPlanListResponse`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanListResponse,
    # 引入或传递 `InterviewPlanSaveRequest`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanSaveRequest,
    # 引入或传递 `InterviewPlanGenerateRequest`，供“完成当前模块初始化”的后续步骤使用。
    InterviewPlanGenerateRequest
# 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
)
# 引入依赖，用于管理远程 HR 服务中的面试方案。
from app.services.interview_plan_service import InterviewPlanService

# 创建当前模块的 FastAPI 路由容器，供上层统一挂载。
router = APIRouter()


# 对外提供 POST `/save-generated` 接口，入口函数负责“完成当前模块初始化”。
@router.post("/save-generated", response_model=InterviewPlanResponse)
# 封装“校验候选人归属并保存面试方案”流程，供接口层或其他业务服务调用。
async def create_interview_plan(
    # 提供需要保存或更新的面试方案内容
    plan_data: InterviewPlanCreate,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“校验候选人归属并保存面试方案”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    创建面试方案
    """
    # 执行 面试方案查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = InterviewPlanService(db)
        # 调用 `service.create_interview_plan` 执行“校验候选人归属并保存面试方案”所需的下游操作，并保存返回结果供下一步使用。
        interview_plan = await service.create_interview_plan(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id,
            # 把 `plan_data` 字段写入当前请求、响应或数据对象，供 `create_interview_plan` 后续步骤使用。
            plan_data=plan_data
        # 完成“校验候选人归属并保存面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把“校验候选人归属并保存面试方案”得到的最终业务结果返回给接口层或前端。
        return interview_plan
        
    # 保留已经带有正确状态码的 HTTP 异常，避免被再次包装成不准确的 500。
    except HTTPException:
        # 引入或传递 `raise`，供“校验候选人归属并保存面试方案”的后续步骤使用。
        raise
    # 捕获“校验候选人归属并保存面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 终止“校验候选人归属并保存面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"创建面试方案失败: {str(e)}"
        # 完成“校验候选人归属并保存面试方案”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 PUT `/{plan_id}` 接口，入口函数负责“完成当前模块初始化”。
@router.put("/{plan_id}", response_model=InterviewPlanResponse)
# 封装“更新已经保存的面试方案”流程，供接口层或其他业务服务调用。
async def update_interview_plan(
    # 定位本次操作的面试方案记录
    plan_id: UUID,
    # 提供需要保存或更新的面试方案内容
    plan_data: InterviewPlanUpdate,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“更新已经保存的面试方案”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
        更新面试方案
    """
    # 执行 面试方案查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = InterviewPlanService(db)
        # 调用 `service.update_interview_plan` 执行“更新已经保存的面试方案”所需的下游操作，并保存返回结果供下一步使用。
        interview_plan = await service.update_interview_plan(
            # 把 `plan_id` 字段写入当前请求、响应或数据对象，供 `update_interview_plan` 后续步骤使用。
            plan_id=plan_id,
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id,
            # 把 `plan_data` 字段写入当前请求、响应或数据对象，供 `update_interview_plan` 后续步骤使用。
            plan_data=plan_data
        # 完成“更新已经保存的面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把“更新已经保存的面试方案”得到的最终业务结果返回给接口层或前端。
        return interview_plan
        
    # 保留已经带有正确状态码的 HTTP 异常，避免被再次包装成不准确的 500。
    except HTTPException:
        # 引入或传递 `raise`，供“更新已经保存的面试方案”的后续步骤使用。
        raise
    # 捕获“更新已经保存的面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 终止“更新已经保存的面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"更新面试方案失败: {str(e)}"
        # 完成“更新已经保存的面试方案”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 GET `/{plan_id}` 接口，入口函数负责“完成当前模块初始化”。
@router.get("/{plan_id}", response_model=InterviewPlanResponse)
# 封装“查询指定面试方案”流程，供接口层或其他业务服务调用。
async def get_interview_plan(
    # 定位本次操作的面试方案记录
    plan_id: UUID,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“查询指定面试方案”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    获取面试方案详情
    """
    # 执行 面试方案查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = InterviewPlanService(db)
        # 调用 `service.get_interview_plan` 执行“查询指定面试方案”所需的下游操作，并保存返回结果供下一步使用。
        interview_plan = await service.get_interview_plan(
            # 把 `plan_id` 字段写入当前请求、响应或数据对象，供 `get_interview_plan` 后续步骤使用。
            plan_id=plan_id,
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id
        # 完成“查询指定面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把“查询指定面试方案”得到的最终业务结果返回给接口层或前端。
        return interview_plan
        
    # 保留已经带有正确状态码的 HTTP 异常，避免被再次包装成不准确的 500。
    except HTTPException:
        # 引入或传递 `raise`，供“查询指定面试方案”的后续步骤使用。
        raise
    # 捕获“查询指定面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 终止“查询指定面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"获取面试方案失败: {str(e)}"
        # 完成“查询指定面试方案”所需的数据封装或下游调用，准备处理其结果。
        )


# 对外提供 GET `/` 接口，入口函数负责“完成当前模块初始化”。
@router.get("/", response_model=InterviewPlanListResponse)
# 封装“分页查询候选人的面试方案”流程，供接口层或其他业务服务调用。
async def list_interview_plans(
    # 指定要查询的结果页
    page: int = Query(1, ge=1, description="页码"),
    # 限制当前页返回的记录数量
    size: int = Query(10, ge=1, le=100, description="每页数量"),
    # 关联对应的简历评价，供面试方案查询和权限校验。
    resume_evaluation_id: Optional[UUID] = Query(None, description="简历评价ID"),
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“分页查询候选人的面试方案”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    获取面试方案列表
    """
    # 执行 面试方案查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = InterviewPlanService(db)
        # 调用 `service.list_interview_plans` 执行“分页查询候选人的面试方案”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.list_interview_plans(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id,
            # 保存当前页码，供远程分页查询。
            page=page,
            # 保存每页数量，限制远程接口返回规模。
            size=size,
            # 关联对应的简历评价，供面试方案查询和权限校验。
            resume_evaluation_id=resume_evaluation_id
        # 完成“分页查询候选人的面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        
        # 把“分页查询候选人的面试方案”得到的最终业务结果返回给接口层或前端。
        return InterviewPlanListResponse(
            # 保存当前页业务记录，供构造分页响应。
            items=result["items"],
            # 保存符合条件的记录总数，供前端计算分页。
            total=result["total"],
            # 保存当前页码，供远程分页查询。
            page=result["page"],
            # 保存每页数量，限制远程接口返回规模。
            size=result["size"],
            # 把 `pages` 字段写入当前请求、响应或数据对象，供 `list_interview_plans` 后续步骤使用。
            pages=result["pages"]
        # 完成“分页查询候选人的面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        
    # 捕获“分页查询候选人的面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 终止“分页查询候选人的面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"获取面试方案列表失败: {str(e)}"
        # 完成“分页查询候选人的面试方案”所需的数据封装或下游调用，准备处理其结果。
        )

# todo：删除面试方案不是软删除，前端未实现
@router.delete("/{plan_id}")
# 封装“删除指定面试方案”流程，供接口层或其他业务服务调用。
async def delete_interview_plan(
    # 定位本次操作的面试方案记录
    plan_id: UUID,
    # 取得当前登录用户，用于权限校验和数据隔离
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    db: AsyncSession = Depends(get_db)
# 完成“删除指定面试方案”所需参数的定义，下面开始执行具体业务步骤。
) -> Any:
    """
    删除面试方案
    """
    # 执行 面试方案查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    try:
        # 创建对应业务服务，集中处理当前模块逻辑。
        service = InterviewPlanService(db)
        # 调用 `service.delete_interview_plan` 执行“删除指定面试方案”所需的下游操作，并保存返回结果供下一步使用。
        result = await service.delete_interview_plan(
            # 把 `plan_id` 字段写入当前请求、响应或数据对象，供 `delete_interview_plan` 后续步骤使用。
            plan_id=plan_id,
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=current_user.id
        # 完成“删除指定面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把“删除指定面试方案”得到的最终业务结果返回给接口层或前端。
        return result
        
    # 保留已经带有正确状态码的 HTTP 异常，避免被再次包装成不准确的 500。
    except HTTPException:
        # 引入或传递 `raise`，供“删除指定面试方案”的后续步骤使用。
        raise
    # 捕获“删除指定面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    except Exception as e:
        # 终止“删除指定面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            detail=f"删除面试方案失败: {str(e)}"
        # 完成“删除指定面试方案”所需的数据封装或下游调用，准备处理其结果。
        )
