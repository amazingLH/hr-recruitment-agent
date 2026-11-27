"""
简历评分标准业务服务。

【文件定位】
本文件负责评分标准的新增、更新、详情、分页列表和删除。评分标准正文由 Dify 在
hr_workflows.py 中生成；本服务接收生成后的内容，通过 remote_service_client 保存到
远程 HR 服务，并转换成统一的 ScoringCriteriaResponse。

【主要函数】
- __init__：保留可选 db 参数以兼容其他 Service 的创建方式；评分标准实际由远程服务保存。
- save_scoring_criteria：把 ScoringCriteriaCreate 转成 JSON，携带用户 ID 请求远程保存接口，
  返回包含 criteria_id 和 job_description_id 的统一响应。
- update_scoring_criteria：根据 criteria_id 把修改字段发送到远程 PUT 接口，返回更新后的标准。
- _build_scoring_criteria_response：合并远程返回与原请求，补全 ID、用户、时间和 workflow_type，
  解决远程响应字段不完整时的 Schema 构造问题。
- get_scoring_criteria：按 criteria_id 和用户 ID 查询单条评分标准。
- get_scoring_criteria_list：按用户、分页参数和可选 JD ID 查询列表，并转换成统一分页响应。
- delete_scoring_criteria：按 criteria_id 请求远程服务删除记录并返回处理结果。

【核心关联】
每套评分标准通过 job_description_id 对应一个 JD，简历评分时会按 JD ID 查找该标准；
找不到专属标准时，ResumeEvaluationService 会使用内置默认评分模型。
"""

# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Any, Optional, Dict, List
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID
# 引入依赖，用于记录业务执行过程和失败原因。
import logging
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from datetime import datetime, timezone
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.schemas.scoring_criteria import (
    # 引入或传递 `ScoringCriteriaCreate`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaCreate,
    # 引入或传递 `ScoringCriteriaUpdate`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaUpdate,
    # 引入或传递 `ScoringCriteriaListResponse`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaListResponse,
    # 引入或传递 `ScoringCriteriaResponse`，供“完成当前模块初始化”的后续步骤使用。
    ScoringCriteriaResponse
# 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
)
# 引入依赖，用于统一访问远程 HR 服务。
from app.services.remote_service_client import remote_service_client

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)


# 集中管理 `ScoringCriteriaService` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class ScoringCriteriaService:
    """评分标准服务类"""

    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def __init__(self, db=None):
        # 不再需要数据库会话，但保留参数以保持接口兼容
        self.db = db

    # 封装“保存生成后的评分标准”流程，供接口层或其他业务服务调用。
    async def save_scoring_criteria(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 提供需要新增或更新的评分标准内容
        criteria_data: ScoringCriteriaCreate,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“保存生成后的评分标准”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ScoringCriteriaResponse:
        """
        保存生成的评分标准到数据库

        Args:
            criteria_data: 评分标准创建数据
            user_id: 用户ID

        Returns:
            保存的评分标准对象
        """
        # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备请求数据，使用mode='json'确保UUID等类型被正确序列化
            request_data = criteria_data.model_dump(mode='json')
            
            # 发送POST请求到远程服务
            result_data = await remote_service_client.post(
                # 指定远程 HR 服务的目标业务接口。
                endpoint="/scoring-criteria/save",
                # 把待保存或更新的业务内容发给远程 HR 服务。
                data=request_data,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“保存生成后的评分标准”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把“保存生成后的评分标准”得到的最终业务结果返回给接口层或前端。
            return self._build_scoring_criteria_response(result_data, request_data, user_id)

        # 捕获“保存生成后的评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“保存生成后的评分标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"保存评分标准失败: {str(e)}")
            # 引入或传递 `raise`，供“保存生成后的评分标准”的后续步骤使用。
            raise

    # 封装“更新已有评分标准”流程，供接口层或其他业务服务调用。
    async def update_scoring_criteria(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 定位本次操作的评分标准记录
        criteria_id: str,
        # 提供需要新增或更新的评分标准内容
        criteria_data: ScoringCriteriaUpdate,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“更新已有评分标准”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ScoringCriteriaResponse:
        """
        更新已保存的评分标准

        Args:
            criteria_id: 评分标准ID
            criteria_data: 评分标准更新数据
            user_id: 用户ID

        Returns:
            更新后的评分标准对象
        """
        # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备请求数据，使用mode='json'确保UUID等类型被正确序列化
            request_data = criteria_data.model_dump(mode='json', exclude_unset=True)
            
            # 发送PUT请求到远程服务
            result_data = await remote_service_client.put(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/scoring-criteria/{criteria_id}",
                # 把待保存或更新的业务内容发给远程 HR 服务。
                data=request_data,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“更新已有评分标准”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把“更新已有评分标准”得到的最终业务结果返回给接口层或前端。
            return self._build_scoring_criteria_response(result_data, request_data, user_id, criteria_id=criteria_id)

        # 捕获“更新已有评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“更新已有评分标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"更新评分标准失败: {str(e)}")
            # 引入或传递 `raise`，供“更新已有评分标准”的后续步骤使用。
            raise

    # 封装“整理远程评分标准为统一响应结构”流程，供接口层或其他业务服务调用。
    def _build_scoring_criteria_response(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `_build_scoring_criteria_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        result_data: Dict[str, Any],
        # 为 `_build_scoring_criteria_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        request_data: Dict[str, Any],
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 定位本次操作的评分标准记录
        criteria_id: Optional[str] = None,
    # 完成“整理远程评分标准为统一响应结构”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ScoringCriteriaResponse:
        # 封装将要发送给远程 HR 服务的业务字段。
        payload = result_data.get("data") if isinstance(result_data.get("data"), dict) else result_data
        # 合并远程返回和原始请求，补齐远程响应中可能缺少的字段。
        merged_data = {
            # 把当前字段纳入 `_build_scoring_criteria_response` 的请求或响应结构，确保后续步骤获得完整业务数据。
            **request_data,
            # 把当前字段纳入 `_build_scoring_criteria_response` 的请求或响应结构，确保后续步骤获得完整业务数据。
            **payload,
        # 完成“整理远程评分标准为统一响应结构”所需的数据封装或下游调用，准备处理其结果。
        }
        # 把 `now` 字段写入当前请求、响应或数据对象，供 `_build_scoring_criteria_response` 后续步骤使用。
        now = datetime.now(timezone.utc).isoformat()
        # 根据 `criteria_id` 判断当前数据应进入哪条业务分支，保证 `_build_scoring_criteria_response` 使用正确处理方式。
        if criteria_id:
            # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
            merged_data.setdefault("id", criteria_id)
        # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("user_id", str(user_id))
        # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("workflow_type", request_data.get("workflow_type") or "scoring_criteria_generation")
        # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("created_at", now)
        # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("updated_at", now)
        # 调用 `merged_data.setdefault` 执行 `_build_scoring_criteria_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("is_active", True)
        # 把“整理远程评分标准为统一响应结构”得到的最终业务结果返回给接口层或前端。
        return ScoringCriteriaResponse(**merged_data)

    # 封装“查询指定评分标准”流程，供接口层或其他业务服务调用。
    async def get_scoring_criteria(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 定位本次操作的评分标准记录
        criteria_id: str,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“查询指定评分标准”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ScoringCriteriaResponse:
        """
        获取单个评分标准详情

        Args:
            criteria_id: 评分标准ID
            user_id: 用户ID

        Returns:
            评分标准对象
        """
        # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 发送GET请求到远程服务
            result_data = await remote_service_client.get(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/scoring-criteria/{criteria_id}",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“查询指定评分标准”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把“查询指定评分标准”得到的最终业务结果返回给接口层或前端。
            return ScoringCriteriaResponse(**result_data)

        # 捕获“查询指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“查询指定评分标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取评分标准失败: {str(e)}")
            # 引入或传递 `raise`，供“查询指定评分标准”的后续步骤使用。
            raise

    # 封装“分页查询评分标准”流程，供接口层或其他业务服务调用。
    async def get_scoring_criteria_list(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 指定要查询的结果页
        page: int = 1,
        # 限制当前页返回的记录数量
        size: int = 10,
        # 把评分或简历评价关联到指定岗位 JD
        job_description_id: Optional[str] = None
    # 完成“分页查询评分标准”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ScoringCriteriaListResponse:
        """
        获取评分标准列表

        Args:
            user_id: 用户ID
            page: 页码
            size: 每页数量
            job_description_id: 关联的JD ID

        Returns:
            评分标准列表响应对象
        """
        # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备查询参数
            additional_params = {
                # 向下游请求或返回结果写入 `page` 业务字段。
                "page": page,
                # 向下游请求或返回结果写入 `size` 业务字段。
                "size": size
            # 完成“分页查询评分标准”所需的数据封装或下游调用，准备处理其结果。
            }
            # 根据 `job_description_id` 判断当前数据应进入哪条业务分支，保证 `get_scoring_criteria_list` 使用正确处理方式。
            if job_description_id:
                # 把当前结果写入 `additional_params["job_description_id"]`，补充后续下游调用或响应需要的业务字段。
                additional_params["job_description_id"] = job_description_id
            
            # 发送GET请求到远程服务
            result_data = await remote_service_client.get(
                # 指定远程 HR 服务的目标业务接口。
                endpoint="/scoring-criteria/",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id,
                # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `get_scoring_criteria_list` 后续步骤使用。
                additional_params=additional_params
            # 完成“分页查询评分标准”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把“分页查询评分标准”得到的最终业务结果返回给接口层或前端。
            return ScoringCriteriaListResponse(**result_data)

        # 捕获“分页查询评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“分页查询评分标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取评分标准列表失败: {str(e)}")
            # 引入或传递 `raise`，供“分页查询评分标准”的后续步骤使用。
            raise

    # 封装“删除指定评分标准”流程，供接口层或其他业务服务调用。
    async def delete_scoring_criteria(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 定位本次操作的评分标准记录
        criteria_id: str,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“删除指定评分标准”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Dict[str, str]:
        """
        删除评分标准（软删除）

        Args:
            criteria_id: 评分标准ID
            user_id: 用户ID

        Returns:
            删除结果信息
        """
        # 执行 评分标准查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 发送DELETE请求到远程服务
            result_data = await remote_service_client.delete(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/scoring-criteria/{criteria_id}",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“删除指定评分标准”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把“删除指定评分标准”得到的最终业务结果返回给接口层或前端。
            return result_data

        # 捕获“删除指定评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“删除指定评分标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"删除评分标准失败: {str(e)}")
            # 引入或传递 `raise`，供“删除指定评分标准”的后续步骤使用。
            raise
