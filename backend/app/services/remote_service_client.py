"""
远程 HR 服务通用 HTTP 客户端。

【文件定位】
当前 FastAPI 项目有一部分业务数据不写入本地 PostgreSQL，而由另一套 HR 服务保存。
本文件集中封装对该服务的 GET、POST、PUT、DELETE 请求，供 JD、评分标准和面试方案
服务复用，避免每个业务类重复处理地址、API Key、用户参数、超时和错误状态码。

【主要函数】
- __init__：根据 HR_SERVICE_HOST、PORT 和 APIKEY 构造远程服务配置，并设置超时和日志开关。
- _get_headers：生成 Content-Type 和 X-API-Key 请求头，用于远程 HR 服务鉴权。
- _get_params：把 current_user_id 与分页、筛选等参数合并，保证远程服务按用户隔离数据。
- _handle_response：把 401、403、404 和其他失败状态转换成明确异常，成功时解析并返回 JSON。
- post：向远程服务提交新增数据，例如保存 JD、评分标准或面试方案。
- put：根据资源 ID 更新远程数据，例如再次保存已经存在的 JD 或面试方案。
- get：携带用户及查询参数读取远程详情或分页列表。
- delete：请求远程服务删除指定资源，并返回删除结果。
- remote_service_client：文件末尾创建的共享客户端实例，各业务 Service 导入后直接复用。

【边界】
本类只是客户端代理；数据最终怎样建表和保存由远程 HR 服务决定。
"""

# 引入依赖，用于记录业务执行过程和失败原因。
import logging
# 引入依赖，用于编码请求数据或解析 AI/远程服务返回的 JSON。
import json
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Any, Optional, Dict
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID
# 引入依赖，用于异步请求 Dify 或远程 HR 服务。
import httpx
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.core.config import settings

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)


# 集中管理 `RemoteServiceClient` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class RemoteServiceClient:
    """远程服务HTTP客户端工具类"""

    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def __init__(self):
        # 读取项目配置并保存为 `base_url`，供后续所有下游请求复用。
        self.base_url = f"http://{settings.HR_SERVICE_HOST}:{settings.HR_SERVICE_PORT}/api/v1"
        # 读取项目配置并保存为 `api_key`，供后续所有下游请求复用。
        self.api_key = settings.HR_SERVICE_APIKEY
        # 保存当前对象的 `timeout` 业务状态，供同一服务的其他方法复用。
        self.timeout = 30.0
        # 读取项目配置并保存为 `log_enabled`，供后续所有下游请求复用。
        self.log_enabled = settings.REMOTE_SERVICE_LOG_ENABLED

    # 封装“构造访问远程 HR 服务的鉴权请求头”流程，供接口层或其他业务服务调用。
    def _get_headers(self) -> Dict[str, str]:
        """获取默认请求头"""
        # 把“构造访问远程 HR 服务的鉴权请求头”得到的最终业务结果返回给接口层或前端。
        return {
            # 声明请求体采用 JSON 格式。
            "Content-Type": "application/json",
            # 向下游请求或返回结果写入 `X-API-Key` 业务字段。
            "X-API-Key": self.api_key
        # 完成“构造访问远程 HR 服务的鉴权请求头”所需的数据封装或下游调用，准备处理其结果。
        }

    # 封装“为远程请求加入当前用户和筛选参数”流程，供接口层或其他业务服务调用。
    def _get_params(self, user_id: UUID, **kwargs) -> Dict[str, Any]:
        """构建查询参数，包含用户ID"""
        # 封装远程请求的用户范围和筛选条件。
        params = {"current_user_id": str(user_id)}
        # 把可选业务字段合并到下游请求中，确保已有公共字段不会丢失。
        params.update(kwargs)
        # 把“为远程请求加入当前用户和筛选参数”得到的最终业务结果返回给接口层或前端。
        return params

    # 封装“统一检查远程服务状态并解析返回 JSON”流程，供接口层或其他业务服务调用。
    def _handle_response(self, response: httpx.Response, expected_status: int = 200) -> Dict[str, Any]:
        """
        统一处理HTTP响应
        
        Args:
            response: HTTP响应对象
            expected_status: 期望的状态码，默认为200
            
        Returns:
            解析后的JSON数据
            
        Raises:
            ValueError: 当响应状态码不符合预期时
        """
        # 检查响应状态码
        if response.status_code == 401:
            # 发现无法继续的业务问题，终止“统一检查远程服务状态并解析返回 JSON”并交给上层统一处理。
            raise ValueError("API密钥未提供")
        # 检查下游 HTTP 状态码，失败时停止解析正常结果并进入错误处理。
        elif response.status_code == 403:
            # 发现无法继续的业务问题，终止“统一检查远程服务状态并解析返回 JSON”并交给上层统一处理。
            raise ValueError("API密钥认证失败或余额不足或超过有效期")
        # 检查下游 HTTP 状态码，失败时停止解析正常结果并进入错误处理。
        elif response.status_code == 404:
            # 发现无法继续的业务问题，终止“统一检查远程服务状态并解析返回 JSON”并交给上层统一处理。
            raise ValueError("资源未找到")
        # 检查下游 HTTP 状态码，失败时停止解析正常结果并进入错误处理。
        elif response.status_code != expected_status:
            # 发现无法继续的业务问题，终止“统一检查远程服务状态并解析返回 JSON”并交给上层统一处理。
            raise ValueError(f"远程服务返回错误: {response.status_code} - {response.text}")
        
        # 记录剩余调用次数
        remaining_calls = response.headers.get("X-Remaining-Calls", "unknown")
        # 记录“统一检查远程服务状态并解析返回 JSON”的关键状态或失败原因，方便排查下游服务问题。
        logger.info(f"远程服务剩余调用次数: {remaining_calls}")
        
        # 解析并返回响应数据
        return response.json()

    # 封装“向远程 HR 服务新增数据”流程，供接口层或其他业务服务调用。
    async def post(self, endpoint: str, data: Dict[str, Any], user_id: UUID, 
                   # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `post` 后续步骤使用。
                   additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        发送POST请求
        
        Args:
            endpoint: API端点路径
            data: 请求体数据
            user_id: 用户ID
            additional_params: 额外的查询参数
            
        Returns:
            响应数据
        """
        # 拼出目标服务的完整接口地址。
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        # 保存下游 HTTP 请求所需的鉴权和内容类型。
        headers = self._get_headers()
        # 封装远程请求的用户范围和筛选条件。
        params = self._get_params(user_id, **(additional_params or {}))

        # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
        logger.info(f"准备请求远程服务: {url},参数 :{self.api_key}")
        
        # 记录请求详情
        if self.log_enabled:
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 40)
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] POST {url}")
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Headers: {json.dumps(headers, ensure_ascii=False)}")
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Params: {json.dumps(params, ensure_ascii=False)}")
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Body: {json.dumps(data, ensure_ascii=False, indent=2)}")
            # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 40)
        
        # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
        async with httpx.AsyncClient() as client:
            # 调用 `client.post` 执行“向远程 HR 服务新增数据”所需的下游操作，并保存返回结果供下一步使用。
            response = await client.post(
                # 引入或传递 `url`，供“向远程 HR 服务新增数据”的后续步骤使用。
                url,
                # 把完整业务请求体以 JSON 形式发给下游服务。
                json=data,
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                headers=headers,
                # 封装远程请求的用户范围和筛选条件。
                params=params,
                # 限制下游请求等待时间，避免外部服务无响应时长期占用连接。
                timeout=self.timeout
            # 完成“向远程 HR 服务新增数据”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录响应详情
            if self.log_enabled:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把 `response_data` 字段写入当前请求、响应或数据对象，供 `post` 后续步骤使用。
                    response_data = response.json()
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("-" * 40)
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Status: {response.status_code}")
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Headers: {json.dumps(dict(response.headers), ensure_ascii=False)}")
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Body: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("-" * 40)
                # 捕获“向远程 HR 服务新增数据”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“向远程 HR 服务新增数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"[RESPONSE] Failed to parse response body: {e}")
            
            # 把“向远程 HR 服务新增数据”得到的最终业务结果返回给接口层或前端。
            return self._handle_response(response)

    # 封装“向远程 HR 服务更新数据”流程，供接口层或其他业务服务调用。
    async def put(self, endpoint: str, data: Dict[str, Any], user_id: UUID,
                  # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `put` 后续步骤使用。
                  additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        发送PUT请求
        
        Args:
            endpoint: API端点路径
            data: 请求体数据
            user_id: 用户ID
            additional_params: 额外的查询参数
            
        Returns:
            响应数据
        """
        # 拼出目标服务的完整接口地址。
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        # 保存下游 HTTP 请求所需的鉴权和内容类型。
        headers = self._get_headers()
        # 封装远程请求的用户范围和筛选条件。
        params = self._get_params(user_id, **(additional_params or {}))
        
        # 记录请求详情
        if self.log_enabled:
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] PUT {url}")
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Headers: {json.dumps(headers, ensure_ascii=False)}")
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Params: {json.dumps(params, ensure_ascii=False)}")
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Body: {json.dumps(data, ensure_ascii=False, indent=2)}")
            # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
        
        # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
        async with httpx.AsyncClient() as client:
            # 调用 `client.put` 执行“向远程 HR 服务更新数据”所需的下游操作，并保存返回结果供下一步使用。
            response = await client.put(
                # 引入或传递 `url`，供“向远程 HR 服务更新数据”的后续步骤使用。
                url,
                # 把完整业务请求体以 JSON 形式发给下游服务。
                json=data,
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                headers=headers,
                # 封装远程请求的用户范围和筛选条件。
                params=params,
                # 限制下游请求等待时间，避免外部服务无响应时长期占用连接。
                timeout=self.timeout
            # 完成“向远程 HR 服务更新数据”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录响应详情
            if self.log_enabled:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把 `response_data` 字段写入当前请求、响应或数据对象，供 `put` 后续步骤使用。
                    response_data = response.json()
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Status: {response.status_code}")
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Headers: {json.dumps(dict(response.headers), ensure_ascii=False)}")
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Body: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                # 捕获“向远程 HR 服务更新数据”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“向远程 HR 服务更新数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"[RESPONSE] Failed to parse response body: {e}")
            
            # 把“向远程 HR 服务更新数据”得到的最终业务结果返回给接口层或前端。
            return self._handle_response(response)

    # 封装“向远程 HR 服务查询数据”流程，供接口层或其他业务服务调用。
    async def get(self, endpoint: str, user_id: UUID,
                  # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `get` 后续步骤使用。
                  additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        发送GET请求
        
        Args:
            endpoint: API端点路径
            user_id: 用户ID
            additional_params: 额外的查询参数
            
        Returns:
            响应数据
        """
        # 拼出目标服务的完整接口地址。
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        # 保存下游 HTTP 请求所需的鉴权和内容类型。
        headers = self._get_headers()
        # 封装远程请求的用户范围和筛选条件。
        params = self._get_params(user_id, **(additional_params or {}))
        
        # 记录请求详情
        if self.log_enabled:
            # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
            # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] GET {url}")
            # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Headers: {json.dumps(headers, ensure_ascii=False)}")
            # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Params: {json.dumps(params, ensure_ascii=False)}")
            # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
        
        # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
        async with httpx.AsyncClient() as client:
            # 调用 `client.get` 执行“向远程 HR 服务查询数据”所需的下游操作，并保存返回结果供下一步使用。
            response = await client.get(
                # 引入或传递 `url`，供“向远程 HR 服务查询数据”的后续步骤使用。
                url,
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                headers=headers,
                # 封装远程请求的用户范围和筛选条件。
                params=params,
                # 限制下游请求等待时间，避免外部服务无响应时长期占用连接。
                timeout=self.timeout
            # 完成“向远程 HR 服务查询数据”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录响应详情
            if self.log_enabled:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把 `response_data` 字段写入当前请求、响应或数据对象，供 `get` 后续步骤使用。
                    response_data = response.json()
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Status: {response.status_code}")
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Headers: {json.dumps(dict(response.headers), ensure_ascii=False)}")
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Body: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                # 捕获“向远程 HR 服务查询数据”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“向远程 HR 服务查询数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"[RESPONSE] Failed to parse response body: {e}")
            
            # 把“向远程 HR 服务查询数据”得到的最终业务结果返回给接口层或前端。
            return self._handle_response(response)

    # 封装“向远程 HR 服务删除数据”流程，供接口层或其他业务服务调用。
    async def delete(self, endpoint: str, user_id: UUID,
                     # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `delete` 后续步骤使用。
                     additional_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        发送DELETE请求
        
        Args:
            endpoint: API端点路径
            user_id: 用户ID
            additional_params: 额外的查询参数
            
        Returns:
            响应数据
        """
        # 拼出目标服务的完整接口地址。
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        # 保存下游 HTTP 请求所需的鉴权和内容类型。
        headers = self._get_headers()
        # 封装远程请求的用户范围和筛选条件。
        params = self._get_params(user_id, **(additional_params or {}))
        
        # 记录请求详情
        if self.log_enabled:
            # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
            # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] DELETE {url}")
            # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Headers: {json.dumps(headers, ensure_ascii=False)}")
            # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"[REQUEST] Params: {json.dumps(params, ensure_ascii=False)}")
            # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
            logger.info("=" * 80)
        
        # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
        async with httpx.AsyncClient() as client:
            # 调用 `client.delete` 执行“向远程 HR 服务删除数据”所需的下游操作，并保存返回结果供下一步使用。
            response = await client.delete(
                # 引入或传递 `url`，供“向远程 HR 服务删除数据”的后续步骤使用。
                url,
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                headers=headers,
                # 封装远程请求的用户范围和筛选条件。
                params=params,
                # 限制下游请求等待时间，避免外部服务无响应时长期占用连接。
                timeout=self.timeout
            # 完成“向远程 HR 服务删除数据”所需的数据封装或下游调用，准备处理其结果。
            )
            
            # 记录响应详情
            if self.log_enabled:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把 `response_data` 字段写入当前请求、响应或数据对象，供 `delete` 后续步骤使用。
                    response_data = response.json()
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Status: {response.status_code}")
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Headers: {json.dumps(dict(response.headers), ensure_ascii=False)}")
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"[RESPONSE] Body: {json.dumps(response_data, ensure_ascii=False, indent=2)}")
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("=" * 80)
                # 捕获“向远程 HR 服务删除数据”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“向远程 HR 服务删除数据”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"[RESPONSE] Failed to parse response body: {e}")
            
            # 把“向远程 HR 服务删除数据”得到的最终业务结果返回给接口层或前端。
            return self._handle_response(response)


# 创建全局实例
remote_service_client = RemoteServiceClient()
