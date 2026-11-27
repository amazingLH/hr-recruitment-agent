"""
* 文件作用
  * 这是试卷与考试结果的 FastAPI 管理接口，提供试卷列表、保存、详情、修改、删除、公开分享，以及考试结果列表、详情和 CSV 导出。所有接口通过 Depends 注入当前用户和 AsyncSession，再调用 ExamService 完成 SQLAlchemy 查询或事务操作；该文件只负责 HTTP 层的数据接收与响应转换。
* 类说明
  * 本文件没有定义业务类，使用模块级 FastAPI 接口函数组织功能。
* 函数与方法说明
  * `get_exam_list()`：试卷列表功能，使用 skip/limit 分页并按可选关键词过滤 exams 表，返回列表和总数量。
  * `save_exam()`：保存试卷功能，把前端解析后的试卷配置和 questions 数组写入 exams、questions 表；同一事务提交，失败时回滚，保证试卷与题目一致。
  * `get_exam_detail()`：按 exam_id 查询试卷，并使用 selectinload 一次加载关联 questions，按 order_index 返回完整试卷详情。
  * `update_exam()`：按 exam_id 更新试卷字段，并重新处理需要变更的题目数据；成功后提交事务，目标不存在时返回对应失败结果。
  * `delete_exam()`：删除指定试卷；Exam.questions 配置了 delete-orphan 级联，因此删除 exams 记录时会一并删除其 questions。
  * `get_exam_for_share()`：无需当前用户身份即可按 ID 读取允许分享的试卷及题目，供公开答题页面展示。
  * `get_exam_results()`：分页查询 exam_results，可按关键词、试卷 ID 和部门过滤，并计算每次考试的得分率。
  * `get_exam_result()`：读取单次考试结果详情，返回考生、总分、实际得分、提交时间和 exam_data 中的逐题评分明细。
  * `export_exam_result()`：考试结果导出接口，调用 ExamService 生成 CSV 文本，并以文件下载响应返回。
"""

# 从 `typing` 导入 `Any, Dict, List, Optional`，供本文件调用。
from typing import Any, Dict, List, Optional
# 从 `uuid` 导入 `UUID`，供本文件调用。
from uuid import UUID

# 从 `fastapi` 导入 `APIRouter, Depends, HTTPException, status, Query`，供本文件调用。
from fastapi import APIRouter, Depends, HTTPException, status, Query
# 从 `sqlalchemy.ext.asyncio` 导入 `AsyncSession`，供本文件调用。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `pydantic` 导入 `BaseModel`，供本文件调用。
from pydantic import BaseModel

# 从 `app.core.database` 导入 `get_db`，供本文件调用。
from app.core.database import get_db
# 从 `app.schemas.user` 导入 `User as UserSchema`，供本文件调用。
from app.schemas.user import User as UserSchema
# from app.models.exam import Exam, Question  # 注释掉未使用的导入
# from app.models.exam_result import ExamResult  # 注释掉未使用的导入
# 从 `app.api.deps` 导入 `get_current_user`，供本文件调用。
from app.api.deps import get_current_user
# 从 `app.core.logging` 导入 `logger`，供本文件调用。
from app.core.logging import logger
# 从 `app.services.exam_service` 导入 `ExamService`，供本文件调用。
from app.services.exam_service import ExamService
# 从 `app.schemas.exam` 导入 `ExamGenerateRequest, ExamSubmitRequest, ExamCreateRequest`，供本文件调用。
from app.schemas.exam import ExamGenerateRequest, ExamSubmitRequest, ExamCreateRequest

# 将 `APIRouter()` 的结果保存到 `router`，供后续流程使用。
router = APIRouter()


# 获取试卷列表
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/papers")
# 定义 `get_exam_list()`，实现该函数对应的接口或业务流程。
async def get_exam_list(
    # 声明 `skip` 参数（分页跳过数量），类型为 `int`，默认值为 `Query(0`。
    skip: int = Query(0, ge=0),
    # 声明 `limit` 参数（分页返回上限），类型为 `int`，默认值为 `Query(20`。
    limit: int = Query(20, ge=1, le=100),
    # 声明 `search` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `Query(None`。
    search: Optional[str] = Query(None),
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    获取试卷列表
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.get_exam_list(skip, limit, search)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.get_exam_list(skip, limit, search)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error getting exam list: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"获取试卷列表失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"获取试卷列表失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 保存试卷
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/papers")
# 定义 `save_exam()`，实现该函数对应的接口或业务流程。
async def save_exam(
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    exam_data: ExamCreateRequest,
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    保存试卷到数据库
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.save_exam(exam_data.dict(), current_user.id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.save_exam(exam_data.dict(), current_user.id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error saving exam to database: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"保存试卷失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"保存试卷失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 获取试卷详情
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/papers/{paper_id}")
# 定义 `get_exam_detail()`，实现该函数对应的接口或业务流程。
async def get_exam_detail(
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    paper_id: str,
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    获取试卷详情
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.get_exam_detail(paper_id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.get_exam_detail(paper_id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error getting exam detail: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"获取试卷详情失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"获取试卷详情失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 更新试卷
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.put("/papers/{paper_id}")
# 定义 `update_exam()`，实现该函数对应的接口或业务流程。
async def update_exam(
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    paper_id: str,
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `ExamCreateRequest`。
    exam_data: ExamCreateRequest,
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    更新试卷
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.update_exam(paper_id, exam_data.dict(), current_user.id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.update_exam(paper_id, exam_data.dict(), current_user.id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error updating exam: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"更新试卷失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"更新试卷失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 删除试卷
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.delete("/papers/{paper_id}")
# 定义 `delete_exam()`，实现该函数对应的接口或业务流程。
async def delete_exam(
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    paper_id: str,
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    删除试卷
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.delete_exam(paper_id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.delete_exam(paper_id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error deleting exam: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"删除试卷失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"删除试卷失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 获取单个试卷（用于分享页面）
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/papers/{paper_id}/share")
# 定义 `get_exam_for_share()`，实现该函数对应的接口或业务流程。
async def get_exam_for_share(
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
    paper_id: str,
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    获取单个试卷（用于分享页面，无需认证）
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.get_exam_for_share(paper_id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.get_exam_for_share(paper_id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error getting exam: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"获取试卷失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"获取试卷失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 获取考试结果列表
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/exam-results")
# 定义 `get_exam_results()`，实现该函数对应的接口或业务流程。
async def get_exam_results(
    # 声明 `page` 参数（当前步骤使用的数据），类型为 `int`，默认值为 `Query(1`。
    page: int = Query(1, ge=1, description="页码"),
    # 声明 `page_size` 参数（当前步骤使用的数据），类型为 `int`，默认值为 `Query(20`。
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    # 声明 `search` 参数（当前步骤使用的数据），类型为 `str`，默认值为 `Query(None`。
    search: str = Query(None, description="搜索关键词（学生姓名或考试名称）"),
    # 声明 `exam_id` 参数（试卷唯一标识），类型为 `Optional[UUID]`，默认值为 `Query(None`。
    exam_id: Optional[UUID] = Query(None, description="考试ID筛选"),
    # 声明 `department` 参数（考生部门），类型为 `str`，默认值为 `Query(None`。
    department: str = Query(None, description="部门筛选"),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db),
    # 声明 `current_user` 参数（当前登录用户），类型为 `dict`，默认值为 `Depends(get_current_user`。
    current_user: dict = Depends(get_current_user)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    获取考试结果列表，支持分页和筛选
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 转换UUID为字符串
        # 将 `str(exam_id) if exam_id else None` 的结果保存到 `exam_id_str`，供后续流程使用。
        exam_id_str = str(exam_id) if exam_id else None
        # 将 `await exam_service.get_exam_results(page, page_size, search, exam_id_str, department)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.get_exam_results(page, page_size, search, exam_id_str, department)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error getting exam results: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"获取考试结果失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"获取考试结果失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )




# 获取考试结果
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/exam-results/{result_id}")
# 定义 `get_exam_result()`，实现该函数对应的接口或业务流程。
async def get_exam_result(
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    result_id: str,
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
) -> Dict[str, Any]:
    """
    获取考试结果详情
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.get_exam_result(result_id)` 的结果保存到 `result`，供后续流程使用。
        result = await exam_service.get_exam_result(result_id)
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error getting exam result: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"获取考试结果失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"获取考试结果失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )


# 导出考试结果为CSV
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.get("/exam-results/{result_id}/export")
# 定义 `export_exam_result()`，实现该函数对应的接口或业务流程。
async def export_exam_result(
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
    result_id: str,
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 结束上方多行参数或数据结构。
):
    """
    导出考试结果为CSV文件
    """
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)
        # 将 `await exam_service.export_exam_result_to_csv(result_id)` 的结果保存到 `csv_content`，供后续流程使用。
        csv_content = await exam_service.export_exam_result_to_csv(result_id)

        # 创建响应头，指定为CSV文件下载
        # 从 `fastapi` 导入 `Response`，供本文件调用。
        from fastapi import Response

        # 将 `{` 的结果保存到 `headers`，供后续流程使用。
        headers = {
            # 设置 `Content-Disposition` 字段，值来自 `f"attachment; filename=exam_result_{result_id}.csv"`。
            "Content-Disposition": f"attachment; filename=exam_result_{result_id}.csv",
            # 设置 `Content-Type` 字段，值来自 `"text/csv; charset=utf-8"`。
            "Content-Type": "text/csv; charset=utf-8"
        # 结束上方多行参数或数据结构。
        }

        # 返回响应，使用 utf-8-sig 编码确保Excel正确显示中文
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return Response(
            # 将 `csv_content.encode('utf-8-sig')` 的结果保存到 `content`，供后续流程使用。
            content=csv_content.encode('utf-8-sig'),
            # 将 `headers` 的结果保存到 `headers`，供后续流程使用。
            headers=headers
        # 结束上方多行参数或数据结构。
        )

    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_404_NOT_FOUND,
            # 将 `str(e)` 的结果保存到 `detail`，供后续流程使用。
            detail=str(e)
        # 结束上方多行参数或数据结构。
        )
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error exporting exam result: {str(e)}")
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 将 `f"导出考试结果失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"导出考试结果失败: {str(e)}"
        # 结束上方多行参数或数据结构。
        )
