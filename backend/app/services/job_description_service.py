"""
岗位 JD 业务服务。

【文件定位】
本文件位于接口层和远程 HR 服务之间，负责 JD 的新增、更新、详情、分页列表和删除。
当前项目没有本地 JobDescription ORM 表；这些方法通过 remote_service_client 调用远程
HR 服务持久化 JD，再用 Pydantic Schema 把远程结果整理成前端需要的统一结构。

【主要函数】
- __init__：接收可选数据库会话以保持调用形式统一；JD 实际由远程 HR 服务保存。
- create_job_description：把 JobDescriptionCreate 转成 JSON，携带 user_id 调用远程保存接口，
  并返回包含新 JD ID 的响应对象。
- _build_job_description_response：合并远程返回和原请求，补全 ID、用户、时间及 workflow_type，
  再构造统一的 JobDescriptionResponse。
- update_job_description：把前端修改字段转换成 JSON，根据 JD ID 调用远程 PUT 接口并返回最新版。
- get_job_description：根据 JD ID 和用户 ID 查询单条远程 JD，并校验为响应 Schema。
- list_job_descriptions：传递页码、数量和状态条件，查询当前用户的远程 JD 分页列表。
- delete_job_description：删除 JD 前查询并删除关联评分标准，再请求远程服务删除 JD，避免残留孤立数据。

【阅读重点】
model_dump(mode="json") 把 Pydantic 对象转成可发送的字典；await 表示等待远程 HTTP；
user_id 会随请求传递，用于远程服务进行数据隔离和权限判断。
"""

# 引入依赖，用于记录业务执行过程和失败原因。
import logging
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Any, Dict, List, Optional, Tuple
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from datetime import datetime, timezone
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID

# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from pydantic import ValidationError

# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.schemas.job_description import (
    # 引入或传递 `JobDescriptionCreate`，供“完成当前模块初始化”的后续步骤使用。
    JobDescriptionCreate,
    # 引入或传递 `JobDescriptionUpdate`，供“完成当前模块初始化”的后续步骤使用。
    JobDescriptionUpdate,
    # 引入或传递 `JobDescriptionResponse`，供“完成当前模块初始化”的后续步骤使用。
    JobDescriptionResponse,
    # 引入或传递 `JobDescriptionListResponse`，供“完成当前模块初始化”的后续步骤使用。
    JobDescriptionListResponse
# 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
)
# 引入依赖，用于统一访问远程 HR 服务。
from app.services.remote_service_client import remote_service_client

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)


# 集中管理 `JobDescriptionService` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class JobDescriptionService:
    """职位描述服务类"""
    
    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def __init__(self, db=None):
        # 不再需要数据库会话，但保留参数以保持接口兼容
        self.db = db
    
    # 封装“在远程 HR 服务中新建 JD”流程，供接口层或其他业务服务调用。
    async def create_job_description(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self, 
        # 提供需要新增或更新的 JD 内容
        jd_data: JobDescriptionCreate, 
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“在远程 HR 服务中新建 JD”所需参数的定义，下面开始执行具体业务步骤。
    ) -> JobDescriptionResponse:
        """
        创建新的职位描述
        
        Args:
            jd_data: 职位描述创建数据
            user_id: 用户ID
            
        Returns:
            创建的职位描述对象
            
        Raises:
            Exception: 创建失败时抛出异常
        """
        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备请求数据，使用 mode='json' 确保 UUID/日期等类型可序列化
            request_data = jd_data.model_dump(mode="json")
            
            # 发送POST请求到远程服务
            result_data = await remote_service_client.post(
                # 指定远程 HR 服务的目标业务接口。
                endpoint="/job-descriptions/save",
                # 把待保存或更新的业务内容发给远程 HR 服务。
                data=request_data,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“在远程 HR 服务中新建 JD”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 把 `jd_response` 字段写入当前请求、响应或数据对象，供 `create_job_description` 后续步骤使用。
            jd_response = self._build_job_description_response(result_data, request_data, user_id)
            # 记录“在远程 HR 服务中新建 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"成功创建职位描述: {jd_response.id} - {jd_response.title}")
            # 把“在远程 HR 服务中新建 JD”得到的最终业务结果返回给接口层或前端。
            return jd_response

        # 捕获“在远程 HR 服务中新建 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“在远程 HR 服务中新建 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"创建职位描述失败: {str(e)}", exc_info=True)
            # 引入或传递 `raise`，供“在远程 HR 服务中新建 JD”的后续步骤使用。
            raise

    # 封装“整理远程 JD 为统一响应结构”流程，供接口层或其他业务服务调用。
    def _build_job_description_response(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `_build_job_description_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        result_data: Dict[str, Any],
        # 为 `_build_job_description_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        request_data: Dict[str, Any],
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 定位本次要查询、修改或删除的岗位 JD
        jd_id: Optional[str] = None,
    # 完成“整理远程 JD 为统一响应结构”所需参数的定义，下面开始执行具体业务步骤。
    ) -> JobDescriptionResponse:
        """兼容远程保存接口返回部分字段，避免已落库但本地响应校验误报失败。"""
        # 封装将要发送给远程 HR 服务的业务字段。
        payload = result_data.get("data") if isinstance(result_data.get("data"), dict) else result_data
        # 合并远程返回和原始请求，补齐远程响应中可能缺少的字段。
        merged_data = {
            # 把当前字段纳入 `_build_job_description_response` 的请求或响应结构，确保后续步骤获得完整业务数据。
            **request_data,
            # 把当前字段纳入 `_build_job_description_response` 的请求或响应结构，确保后续步骤获得完整业务数据。
            **payload,
        # 完成“整理远程 JD 为统一响应结构”所需的数据封装或下游调用，准备处理其结果。
        }
        # 把 `now` 字段写入当前请求、响应或数据对象，供 `_build_job_description_response` 后续步骤使用。
        now = datetime.now(timezone.utc).isoformat()
        # 根据 `jd_id` 判断当前数据应进入哪条业务分支，保证 `_build_job_description_response` 使用正确处理方式。
        if jd_id:
            # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
            merged_data.setdefault("id", jd_id)
        # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("user_id", str(user_id))
        # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("workflow_type", request_data.get("workflow_type") or "jd_generation")
        # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("created_at", now)
        # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("updated_at", now)
        # 调用 `merged_data.setdefault` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
        merged_data.setdefault("is_active", True)

        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 把“整理远程 JD 为统一响应结构”得到的最终业务结果返回给接口层或前端。
            return JobDescriptionResponse(**merged_data)
        # 捕获“整理远程 JD 为统一响应结构”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValidationError as exc:
            # 记录“整理远程 JD 为统一响应结构”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(
                # 向 `_build_job_description_response` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                "远程 JD 已返回但无法组装响应，result_data=%s, request_title=%s",
                # 引入或传递 `result_data`，供“整理远程 JD 为统一响应结构”的后续步骤使用。
                result_data,
                # 调用 `request_data.get` 执行 `_build_job_description_response` 的当前步骤，并将产生的数据交给后续流程。
                request_data.get("title"),
                # 把 `exc_info` 字段写入当前请求、响应或数据对象，供 `_build_job_description_response` 后续步骤使用。
                exc_info=True,
            # 完成“整理远程 JD 为统一响应结构”所需的数据封装或下游调用，准备处理其结果。
            )
            # 发现无法继续的业务问题，终止“整理远程 JD 为统一响应结构”并交给上层统一处理。
            raise exc
    
    # 封装“更新远程 HR 服务中的 JD”流程，供接口层或其他业务服务调用。
    async def update_job_description(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self, 
        # 定位本次要查询、修改或删除的岗位 JD
        jd_id: str, 
        # 提供需要新增或更新的 JD 内容
        jd_data: JobDescriptionUpdate, 
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“更新远程 HR 服务中的 JD”所需参数的定义，下面开始执行具体业务步骤。
    ) -> JobDescriptionResponse:
        """
        更新职位描述
        
        Args:
            jd_id: 职位描述ID
            jd_data: 更新数据
            user_id: 用户ID
            
        Returns:
            更新后的职位描述对象
            
        Raises:
            ValueError: 职位描述不存在或无权限访问
            Exception: 更新失败时抛出异常
        """
        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备请求数据
            request_data = jd_data.model_dump(exclude_unset=True)
            
            # 发送PUT请求到远程服务
            result_data = await remote_service_client.put(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/job-descriptions/{jd_id}",
                # 把待保存或更新的业务内容发给远程 HR 服务。
                data=request_data,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“更新远程 HR 服务中的 JD”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录“更新远程 HR 服务中的 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"成功更新职位描述: {jd_id}")
            # 把“更新远程 HR 服务中的 JD”得到的最终业务结果返回给接口层或前端。
            return self._build_job_description_response(result_data, request_data, user_id, jd_id=jd_id)

        # 捕获“更新远程 HR 服务中的 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 引入或传递 `raise`，供“更新远程 HR 服务中的 JD”的后续步骤使用。
            raise
        # 捕获“更新远程 HR 服务中的 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“更新远程 HR 服务中的 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"更新职位描述失败: {str(e)}")
            # 引入或传递 `raise`，供“更新远程 HR 服务中的 JD”的后续步骤使用。
            raise
    
    # 封装“查询指定 JD”流程，供接口层或其他业务服务调用。
    async def get_job_description(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self, 
        # 定位本次要查询、修改或删除的岗位 JD
        jd_id: str, 
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“查询指定 JD”所需参数的定义，下面开始执行具体业务步骤。
    ) -> JobDescriptionResponse:
        """
        获取指定的职位描述
        
        Args:
            jd_id: 职位描述ID
            user_id: 用户ID
            
        Returns:
            职位描述对象
            
        Raises:
            ValueError: 职位描述不存在或无权限访问
            Exception: 获取失败时抛出异常
        """
        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 发送GET请求到远程服务
            result_data = await remote_service_client.get(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/job-descriptions/{jd_id}",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“查询指定 JD”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录“查询指定 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.debug(f"成功获取职位描述: {jd_id}")
            # 把“查询指定 JD”得到的最终业务结果返回给接口层或前端。
            return JobDescriptionResponse(**result_data)

        # 捕获“查询指定 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 引入或传递 `raise`，供“查询指定 JD”的后续步骤使用。
            raise
        # 捕获“查询指定 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“查询指定 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取职位描述失败: {str(e)}")
            # 引入或传递 `raise`，供“查询指定 JD”的后续步骤使用。
            raise
    
    # 封装“分页查询当前用户的 JD”流程，供接口层或其他业务服务调用。
    async def list_job_descriptions(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 指定要查询的结果页
        page: int = 1,
        # 限制当前页返回的记录数量
        size: int = 10,
        # 按业务状态筛选结果
        status_filter: Optional[str] = None
    # 完成“分页查询当前用户的 JD”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Dict[str, Any]:
        """
        获取用户的职位描述列表
        
        Args:
            user_id: 用户ID
            page: 页码
            size: 每页数量
            status_filter: 状态筛选
            
        Returns:
            包含职位描述列表和分页信息的字典
            
        Raises:
            Exception: 查询失败时抛出异常
        """
        # 记录“分页查询当前用户的 JD”的关键状态或失败原因，方便排查下游服务问题。
        logger.info(f"📥 获取JD列表请求: page={page}, size={size}, status_filter={status_filter}, user_id={user_id}")

        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 准备查询参数
            additional_params = {
                # 向下游请求或返回结果写入 `page` 业务字段。
                "page": page,
                # 向下游请求或返回结果写入 `size` 业务字段。
                "size": size
            # 完成“分页查询当前用户的 JD”所需的数据封装或下游调用，准备处理其结果。
            }
            # 根据 `status_filter` 判断当前数据应进入哪条业务分支，保证 `list_job_descriptions` 使用正确处理方式。
            if status_filter:
                # 把当前结果写入 `additional_params["status_filter"]`，补充后续下游调用或响应需要的业务字段。
                additional_params["status_filter"] = status_filter
            
            # 发送GET请求到远程服务
            result_data = await remote_service_client.get(
                # 指定远程 HR 服务的目标业务接口。
                endpoint="/job-descriptions/",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id,
                # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `list_job_descriptions` 后续步骤使用。
                additional_params=additional_params
            # 完成“分页查询当前用户的 JD”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录“分页查询当前用户的 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"📋 返回 {len(result_data.get('items', []))} 条记录")
            # 把“分页查询当前用户的 JD”得到的最终业务结果返回给接口层或前端。
            return result_data

        # 捕获“分页查询当前用户的 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“分页查询当前用户的 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"❌ 获取JD列表失败: {str(e)}", exc_info=True)
            # 引入或传递 `raise`，供“分页查询当前用户的 JD”的后续步骤使用。
            raise
    
    # 封装“删除 JD 及其关联数据”流程，供接口层或其他业务服务调用。
    async def delete_job_description(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self, 
        # 定位本次要查询、修改或删除的岗位 JD
        jd_id: str, 
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“删除 JD 及其关联数据”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Dict[str, Any]:
        """
        删除职位描述（软删除）
        
        Args:
            jd_id: 职位描述ID
            user_id: 用户ID
            
        Returns:
            删除结果消息
            
        Raises:
            ValueError: 职位描述不存在或无权限访问
            Exception: 删除失败时抛出异常
        """
        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 把 `deleted_criteria` 字段写入当前请求、响应或数据对象，供 `delete_job_description` 后续步骤使用。
            deleted_criteria = []
            # 把 `criteria_failures` 字段写入当前请求、响应或数据对象，供 `delete_job_description` 后续步骤使用。
            criteria_failures = []
            # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 引入依赖，用于管理远程 HR 服务中的评分标准。
                from app.services.scoring_criteria_service import ScoringCriteriaService

                # 把 `scoring_service` 字段写入当前请求、响应或数据对象，供 `delete_job_description` 后续步骤使用。
                scoring_service = ScoringCriteriaService(self.db)
                # 调用 `scoring_service.get_scoring_criteria_list` 执行“删除 JD 及其关联数据”所需的下游操作，并保存返回结果供下一步使用。
                criteria_list = await scoring_service.get_scoring_criteria_list(
                    # 携带当前用户 ID，让下游服务只操作该用户的数据。
                    user_id=user_id,
                    # 保存当前页码，供远程分页查询。
                    page=1,
                    # 保存每页数量，限制远程接口返回规模。
                    size=100,
                    # 关联对应的岗位 JD，供评分和后续招聘流程使用。
                    job_description_id=jd_id,
                # 完成“删除 JD 及其关联数据”所需的数据封装或下游调用，准备处理其结果。
                )
                # 逐项处理“删除 JD 及其关联数据”所需的数据集合，构造最终列表或查找目标记录。
                for criteria in criteria_list.items:
                    # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                    try:
                        # 调用 `await scoring_service.delete_scoring_criteria` 执行 `delete_job_description` 的当前步骤，并将产生的数据交给后续流程。
                        await scoring_service.delete_scoring_criteria(str(criteria.id), user_id)
                        # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                        deleted_criteria.append({"id": str(criteria.id), "title": criteria.title})
                    # 捕获“删除 JD 及其关联数据”中的失败，记录原因或转换成上层能够识别的业务错误。
                    except Exception as exc:
                        # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                        criteria_failures.append({"id": str(criteria.id), "title": criteria.title, "error": str(exc)})
                        # 记录“删除 JD 及其关联数据”的关键状态或失败原因，方便排查下游服务问题。
                        logger.warning("删除 JD 关联评分标准失败 jd_id=%s criteria_id=%s: %s", jd_id, criteria.id, exc)
            # 捕获“删除 JD 及其关联数据”中的失败，记录原因或转换成上层能够识别的业务错误。
            except Exception as exc:
                # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                criteria_failures.append({"error": str(exc)})
                # 记录“删除 JD 及其关联数据”的关键状态或失败原因，方便排查下游服务问题。
                logger.warning("查询 JD 关联评分标准失败 jd_id=%s: %s", jd_id, exc)

            # 发送DELETE请求到远程服务
            result_data = await remote_service_client.delete(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"/job-descriptions/{jd_id}",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“删除 JD 及其关联数据”所需的数据封装或下游调用，准备处理其结果。
            )
            # 确认 `result_data` 的数据类型符合后续字段读取要求，避免解析外部响应时发生类型错误。
            if isinstance(result_data, dict):
                # 保存远程 HR 服务解析后的业务结果。
                result_data["deleted_scoring_criteria"] = deleted_criteria
                # 保存远程 HR 服务解析后的业务结果。
                result_data["scoring_criteria_failures"] = criteria_failures
            
            # 记录“删除 JD 及其关联数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"成功删除职位描述: {jd_id}，关联删除评分标准 {len(deleted_criteria)} 条")
            # 把“删除 JD 及其关联数据”得到的最终业务结果返回给接口层或前端。
            return result_data

        # 捕获“删除 JD 及其关联数据”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 引入或传递 `raise`，供“删除 JD 及其关联数据”的后续步骤使用。
            raise
        # 捕获“删除 JD 及其关联数据”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“删除 JD 及其关联数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"删除职位描述失败: {str(e)}")
            # 引入或传递 `raise`，供“删除 JD 及其关联数据”的后续步骤使用。
            raise
    

    
