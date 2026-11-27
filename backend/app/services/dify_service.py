"""
* 文件作用
  * 这是项目调用 Dify Workflow API 的统一客户端，从 settings 读取 DIFY_BASE_URL、DIFY_API_KEY 和 DIFY_USER_ID，通过 httpx 发送 Bearer 鉴权请求。call_workflow_stream() 使用 streaming 模式解析 SSE 并逐块 yield，适合试卷生成；call_workflow_sync() 使用 blocking 模式等待完整 JSON，适合逐道简答题评分。workflow_type 会写入 Dify inputs.type，由工作流内部据此选择业务分支。
* 类说明
  * `DifyService`：Dify HTTP 客户端类，统一封装工作流地址、API Key、调用用户、流式/同步请求格式、SSE 解析、超时和错误转换。
* 函数与方法说明
  * `DifyService.__init__()`：从 settings 读取 Dify 服务地址、Bearer API Key 和用户标识；缺少关键配置时记录错误，供后续请求统一使用。
  * `DifyService.call_workflow_stream()`：向 Dify /workflows/run 发送 response_mode=streaming 请求，把 workflow_type 写入 inputs.type，解析 SSE data 行并过滤 ping、空行和结束控制消息，再逐块 yield 有效答案。
  * `DifyService.call_workflow_sync()`：向 Dify /workflows/run 发送 response_mode=blocking 请求，等待完整 JSON 响应并提取 outputs.answer；主要用于 workflow_type=6 的单道简答题评分。
"""

# 导入 `json` 模块，供后续代码使用。
import json
# 导入 `uuid` 模块，供后续代码使用。
import uuid
# 从 `typing` 导入 `Dict, Any, AsyncGenerator, Optional`，供本文件调用。
from typing import Dict, Any, AsyncGenerator, Optional
# 导入 `httpx` 模块，供后续代码使用。
import httpx
# 从 `fastapi` 导入 `HTTPException`，供本文件调用。
from fastapi import HTTPException
# 从 `app.core.config` 导入 `settings`，供本文件调用。
from app.core.config import settings
# 从 `app.core.logging` 导入 `logger`，供本文件调用。
from app.core.logging import logger


# 集中管理 `DifyService` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class DifyService:
    """用于与Dify工作流交互的服务"""

    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    # 定义 `__init__()`，实现该函数对应的接口或业务流程。
    def __init__(self):
        # 读取项目配置并保存为 `base_url`，供后续所有下游请求复用。
        # 将 `settings.DIFY_BASE_URL` 的结果保存到 `self.base_url`，供后续流程使用。
        # 将 `settings.DIFY_BASE_URL` 的结果保存到 `self.base_url`，供后续流程使用。
        # 将 `settings.DIFY_BASE_URL` 的结果保存到 `self.base_url`，供后续流程使用。
        self.base_url = settings.DIFY_BASE_URL
        # 读取项目配置并保存为 `api_key`，供后续所有下游请求复用。
        # 将 `settings.DIFY_API_KEY` 的结果保存到 `self.api_key`，供后续流程使用。
        # 将 `settings.DIFY_API_KEY` 的结果保存到 `self.api_key`，供后续流程使用。
        # 将 `settings.DIFY_API_KEY` 的结果保存到 `self.api_key`，供后续流程使用。
        self.api_key = settings.DIFY_API_KEY
        # 读取项目配置并保存为 `user_id`，供后续所有下游请求复用。
        # 将 `settings.DIFY_USER_ID` 的结果保存到 `self.user_id`，供后续流程使用。
        # 将 `settings.DIFY_USER_ID` 的结果保存到 `self.user_id`，供后续流程使用。
        # 将 `settings.DIFY_USER_ID` 的结果保存到 `self.user_id`，供后续流程使用。
        self.user_id = settings.DIFY_USER_ID

        # 启动调用前确认已配置 API Key，避免发送必然失败的未鉴权请求。
        # 判断 `if not self.api_key`，决定是否执行当前分支。
        # 判断 `if not self.api_key`，决定是否执行当前分支。
        # 判断 `if not self.api_key`，决定是否执行当前分支。
        if not self.api_key:
            # 发现无法继续的业务问题，终止“完成 `init` 对应的业务处理”并交给上层统一处理。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise ValueError("DIFY_API_KEY是必需的但未配置")

    # 封装“流式调用 Dify 并持续转发生成内容”流程，供接口层或其他业务服务调用。
    # 定义 `call_workflow_stream()`，实现该函数对应的接口或业务流程。
    async def call_workflow_stream(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 告诉 Dify 当前要执行哪一种 HR AI 任务
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        workflow_type: int,
        # 提供本次发送给 Dify 或检索服务的核心提示内容
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        query: str,
        # 可选地延续同一次 Dify 对话上下文
        # 声明 `conversation_id` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 补充 Dify 工作流节点需要的结构化业务字段
        # 声明 `additional_inputs` 参数（当前步骤使用的数据），类型为 `Optional[Dict[str`。
        additional_inputs: Optional[Dict[str, Any]] = None
    # 完成“流式调用 Dify 并持续转发生成内容”所需参数的定义，下面开始执行具体业务步骤。
    # 结束上方多行参数或数据结构。
    ) -> AsyncGenerator[str, None]:
        """
        调用Dify工作流并流式响应

        Args:
            workflow_type: 工作流类型（1=JD生成，2=评分标准，3=简历评价，4=面试方案）
            query: 用户查询/提示
            conversation_id: 可选的对话ID以提供上下文
            additional_inputs: 额外的输入参数

        Yields:
            流式响应数据
        """
        # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 准备请求数据
            # 将 `{"type": workflow_type}` 的结果保存到 `inputs`，供后续流程使用。
            inputs = {"type": workflow_type}
            # 仅在存在附加业务字段时合并到 Dify inputs，避免传入无效空数据。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            if additional_inputs:
                # 把可选业务字段合并到下游请求中，确保已有公共字段不会丢失。
                # 调用 `inputs.update()`，更新当前数据。
                inputs.update(additional_inputs)

            # 封装即将发送给下游服务的完整请求数据。
            # 将 `{` 的结果保存到 `request_data`，供后续流程使用。
            request_data = {
                # 向 Dify 提供任务类型和结构化业务字段。
                # 设置 `inputs` 字段，值来自 `inputs`。
                "inputs": inputs,
                # 向 Dify 提供本次任务的主要提示内容。
                # 设置 `query` 字段，值来自 `query`。
                "query": query,
                # 指定 Dify 使用流式或阻塞响应。
                # 设置 `response_mode` 字段，值来自 `"streaming"`。
                "response_mode": "streaming",
                # 关联可选的多轮对话上下文。
                # 设置 `conversation_id` 字段，值来自 `conversation_id or ""`。
                "conversation_id": conversation_id or "",
                # 标识 Dify 请求的调用用户。
                # 设置 `user` 字段，值来自 `self.user_id`。
                "user": self.user_id
            # 完成“流式调用 Dify 并持续转发生成内容”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            }

            # 保存下游 HTTP 请求所需的鉴权和内容类型。
            # 将 `{` 的结果保存到 `headers`，供后续流程使用。
            headers = {
                # 携带 Dify Bearer API Key 完成鉴权。
                # 设置 `Authorization` 字段，值来自 `f"Bearer {self.api_key}"`。
                "Authorization": f"Bearer {self.api_key}",
                # 声明请求体采用 JSON 格式。
                # 设置 `Content-Type` 字段，值来自 `"application/json"`。
                "Content-Type": "application/json"
            # 完成“流式调用 Dify 并持续转发生成内容”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            }

            # 记录“流式调用 Dify 并持续转发生成内容”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"调用Dify工作流类型 {workflow_type}，查询: {query[:100]}...")

            # 向Dify发起流式请求
            # 调用 `httpx.AsyncClient()`，完成当前函数调用并取得其返回结果。
            async with httpx.AsyncClient(timeout=60.0) as client:
                # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
                # 调用 `client.stream()`，完成当前函数调用并取得其返回结果。
                async with client.stream(
                    # 使用 HTTP POST 向 Dify 或远程 HR 服务提交生成/新增请求。
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "POST",
                    # 使用已配置的基础地址拼出本次要访问的 Dify 或远程 HR 接口 URL。
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"{self.base_url}/chat-messages",
                    # 保存下游 HTTP 请求所需的鉴权和内容类型。
                    # 将 `headers` 的结果保存到 `headers`，供后续流程使用。
                    headers=headers,
                    # 把完整业务请求体以 JSON 形式发给下游服务。
                    # 将 `request_data` 的结果保存到 `json`，供后续流程使用。
                    json=request_data
                # 完成“流式调用 Dify 并持续转发生成内容”所需的数据封装或下游调用，准备处理其结果。
                # 结束上方多行参数或数据结构。
                ) as response:

                    # 检查下游 HTTP 状态码，失败时停止解析正常结果并进入错误处理。
                    # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                    # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                    # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                    if response.status_code != 200:
                        # 调用 `response.aread` 执行“流式调用 Dify 并持续转发生成内容”所需的下游操作，并保存返回结果供下一步使用。
                        # 将 `await response.aread()` 的结果保存到 `error_text`，供后续流程使用。
                        error_text = await response.aread()
                        # 记录“流式调用 Dify 并持续转发生成内容”的关键状态或失败原因，方便排查下游服务问题。
                        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
                        logger.error(f"Dify API错误: {response.status_code} - {error_text}")
                        # 终止“流式调用 Dify 并持续转发生成内容”并向前端返回明确的 HTTP 状态和失败原因。
                        # 抛出异常，中止当前失败流程。
                        # 抛出异常，中止当前失败流程。
                        # 抛出异常，中止当前失败流程。
                        raise HTTPException(
                            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
                            # 将 `response.status_code` 的结果保存到 `status_code`，供后续流程使用。
                            status_code=response.status_code,
                            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
                            # 将 `f"Dify API错误: {error_text.decode()}"` 的结果保存到 `detail`，供后续流程使用。
                            detail=f"Dify API错误: {error_text.decode()}"
                        # 完成“流式调用 Dify 并持续转发生成内容”所需的数据封装或下游调用，准备处理其结果。
                        # 结束上方多行参数或数据结构。
                        )

                    # 流式传输响应
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    async for chunk in response.aiter_lines():
                        # 根据 `chunk` 判断当前数据应进入哪条业务分支，保证 `call_workflow_stream` 使用正确处理方式。
                        # 判断 `if chunk`，决定是否执行当前分支。
                        # 判断 `if chunk`，决定是否执行当前分支。
                        # 判断 `if chunk`，决定是否执行当前分支。
                        if chunk:
                            # 把 `chunk` 字段写入当前请求、响应或数据对象，供 `call_workflow_stream` 后续步骤使用。
                            chunk = chunk.strip()

                            # 跳过SSE控制行和心跳包，避免被前端误当作正文
                            # 将 `chunk.lower()` 的结果保存到 `lowered`，供后续流程使用。
                            lowered = chunk.lower()
                            # 根据 `(` 判断当前数据应进入哪条业务分支，保证 `call_workflow_stream` 使用正确处理方式。
                            # 判断 `if (`，决定是否执行当前分支。
                            # 判断 `if (`，决定是否执行当前分支。
                            # 判断 `if (`，决定是否执行当前分支。
                            if (
                                # 调用 `lowered.startswith` 执行 `call_workflow_stream` 的当前步骤，并将产生的数据交给后续流程。
                                # 调用 `lowered.startswith()`，完成当前函数调用并取得其返回结果。
                                lowered.startswith("event:")
                                # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                                # 调用 `lowered.startswith()`，完成当前函数调用并取得其返回结果。
                                or lowered.startswith("id:")
                                # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                                # 调用 `lowered.startswith()`，完成当前函数调用并取得其返回结果。
                                or lowered.startswith("retry:")
                                # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                                # 执行表达式 `or lowered == "ping"`，完成本行明确的数据运算或控制操作。
                                or lowered == "ping"
                                # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                                # 执行表达式 `or lowered == "event: ping"`，完成本行明确的数据运算或控制操作。
                                or lowered == "event: ping"
                            # 完成“流式调用 Dify 并持续转发生成内容”所需参数的定义，下面开始执行具体业务步骤。
                            # 结束上方多行参数或数据结构。
                            ):
                                # 引入或传递 `continue`，供“流式调用 Dify 并持续转发生成内容”的后续步骤使用。
                                # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                                continue

                            # 如果存在'data: '前缀则移除
                            # 判断 `if chunk.startswith("data: ")`，决定是否执行当前分支。
                            # 判断 `if chunk.startswith("data: ")`，决定是否执行当前分支。
                            # 判断 `if chunk.startswith("data: ")`，决定是否执行当前分支。
                            if chunk.startswith("data: "):
                                # 把 `chunk` 字段写入当前请求、响应或数据对象，供 `call_workflow_stream` 后续步骤使用。
                                chunk = chunk[6:].strip()

                            # 跳过空行和[DONE]标记
                            # 判断 `if not chunk or chunk == "[DONE]"`，决定是否执行当前分支。
                            # 判断 `if not chunk or chunk == "[DONE]"`，决定是否执行当前分支。
                            # 判断 `if not chunk or chunk == "[DONE]"`，决定是否执行当前分支。
                            if not chunk or chunk == "[DONE]":
                                # 引入或传递 `continue`，供“流式调用 Dify 并持续转发生成内容”的后续步骤使用。
                                # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                                continue

                            # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                            # 开始执行可能失败的步骤，由后续异常分支统一处理。
                            # 开始执行可能失败的步骤，由后续异常分支统一处理。
                            # 开始执行可能失败的步骤，由后续异常分支统一处理。
                            try:
                                # 解析JSON块
                                # 将 `json.loads(chunk)` 的结果保存到 `data`，供后续流程使用。
                                data = json.loads(chunk)
                                # 根据 `str(data.get("event") or "").lower() == "ping"` 判断当前数据应进入哪条业务分支，保证 `call_workflow_stream` 使用正确处理方式。
                                # 判断 `if str(data.get("event") or "").lower() == "ping"`，决定是否执行当前分支。
                                # 判断 `if str(data.get("event") or "").lower() == "ping"`，决定是否执行当前分支。
                                # 判断 `if str(data.get("event") or "").lower() == "ping"`，决定是否执行当前分支。
                                if str(data.get("event") or "").lower() == "ping":
                                    # 引入或传递 `continue`，供“流式调用 Dify 并持续转发生成内容”的后续步骤使用。
                                    # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                                    continue
                                # 把当前 Dify 内容分片立即推送给前端，实现边生成边展示。
                                # 产出当前流式数据片段，并保留生成器状态。
                                # 产出当前流式数据片段，并保留生成器状态。
                                # 产出当前流式数据片段，并保留生成器状态。
                                yield chunk
                            # 捕获“流式调用 Dify 并持续转发生成内容”中的失败，记录原因或转换成上层能够识别的业务错误。
                            # 捕获指定异常，记录或转换为上层能够识别的错误。
                            # 捕获指定异常，记录或转换为上层能够识别的错误。
                            # 捕获指定异常，记录或转换为上层能够识别的错误。
                            except json.JSONDecodeError:
                                # 非JSON正文才透传，SSE控制内容在上面已过滤
                                # 产出当前流式数据片段，并保留生成器状态。
                                # 产出当前流式数据片段，并保留生成器状态。
                                # 产出当前流式数据片段，并保留生成器状态。
                                yield chunk

        # 捕获“流式调用 Dify 并持续转发生成内容”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except httpx.TimeoutException:
            # 记录“流式调用 Dify 并持续转发生成内容”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error("Dify API请求超时")
            # 终止“流式调用 Dify 并持续转发生成内容”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=504, detail="Dify API请求超时")
        # 捕获“流式调用 Dify 并持续转发生成内容”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except httpx.RequestError as e:
            # 记录“流式调用 Dify 并持续转发生成内容”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"Dify API请求错误: {str(e)}")
            # 终止“流式调用 Dify 并持续转发生成内容”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=503, detail=f"Dify API请求错误: {str(e)}")
        # 捕获“流式调用 Dify 并持续转发生成内容”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录“流式调用 Dify 并持续转发生成内容”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"Dify工作流调用中出现意外错误: {str(e)}")
            # 终止“流式调用 Dify 并持续转发生成内容”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=500, detail=f"内部服务器错误: {str(e)}")

    # 封装“同步调用 Dify 并取得完整结果”流程，供接口层或其他业务服务调用。
    # 定义 `call_workflow_sync()`，实现该函数对应的接口或业务流程。
    async def call_workflow_sync(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 告诉 Dify 当前要执行哪一种 HR AI 任务
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        # 声明 `workflow_type` 参数（当前步骤使用的数据），类型为 `int`。
        workflow_type: int,
        # 提供本次发送给 Dify 或检索服务的核心提示内容
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `query` 参数（当前步骤使用的数据），类型为 `str`。
        query: str,
        # 可选地延续同一次 Dify 对话上下文
        # 声明 `conversation_id` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 补充 Dify 工作流节点需要的结构化业务字段
        # 声明 `additional_inputs` 参数（当前步骤使用的数据），类型为 `Optional[Dict[str`。
        additional_inputs: Optional[Dict[str, Any]] = None
    # 完成“同步调用 Dify 并取得完整结果”所需参数的定义，下面开始执行具体业务步骤。
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        调用Dify工作流并同步响应

        Args:
            workflow_type: 工作流类型
            query: 用户查询/提示
            conversation_id: 可选的对话ID
            additional_inputs: 额外的输入参数

        Returns:
            完整的响应数据
        """
        # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 保存传给 Dify 的任务类型和附加业务字段。
            # 将 `{"type": workflow_type}` 的结果保存到 `inputs`，供后续流程使用。
            inputs = {"type": workflow_type}
            # 仅在存在附加业务字段时合并到 Dify inputs，避免传入无效空数据。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            # 判断 `if additional_inputs`，决定是否执行当前分支。
            if additional_inputs:
                # 把可选业务字段合并到下游请求中，确保已有公共字段不会丢失。
                # 调用 `inputs.update()`，更新当前数据。
                inputs.update(additional_inputs)

            # 封装即将发送给下游服务的完整请求数据。
            # 将 `{` 的结果保存到 `request_data`，供后续流程使用。
            request_data = {
                # 向 Dify 提供任务类型和结构化业务字段。
                # 设置 `inputs` 字段，值来自 `inputs`。
                "inputs": inputs,
                # 向 Dify 提供本次任务的主要提示内容。
                # 设置 `query` 字段，值来自 `query`。
                "query": query,
                # 指定 Dify 使用流式或阻塞响应。
                # 设置 `response_mode` 字段，值来自 `"blocking"`。
                "response_mode": "blocking",
                # 关联可选的多轮对话上下文。
                # 设置 `conversation_id` 字段，值来自 `conversation_id or ""`。
                "conversation_id": conversation_id or "",
                # 标识 Dify 请求的调用用户。
                # 设置 `user` 字段，值来自 `self.user_id`。
                "user": self.user_id
            # 完成“同步调用 Dify 并取得完整结果”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            }

            # 保存下游 HTTP 请求所需的鉴权和内容类型。
            # 将 `{` 的结果保存到 `headers`，供后续流程使用。
            headers = {
                # 携带 Dify Bearer API Key 完成鉴权。
                # 设置 `Authorization` 字段，值来自 `f"Bearer {self.api_key}"`。
                "Authorization": f"Bearer {self.api_key}",
                # 声明请求体采用 JSON 格式。
                # 设置 `Content-Type` 字段，值来自 `"application/json"`。
                "Content-Type": "application/json"
            # 完成“同步调用 Dify 并取得完整结果”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            }

            # 记录“同步调用 Dify 并取得完整结果”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"调用Dify工作流类型 {workflow_type} (同步)，查询: {query[:100]}...")

            # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
            # 调用 `httpx.AsyncClient()`，完成当前函数调用并取得其返回结果。
            async with httpx.AsyncClient(timeout=120.0) as client:
                # 调用 `client.post` 执行“同步调用 Dify 并取得完整结果”所需的下游操作，并保存返回结果供下一步使用。
                # 将 `await client.post(` 的结果保存到 `response`，供后续流程使用。
                response = await client.post(
                    # 使用已配置的基础地址拼出本次要访问的 Dify 或远程 HR 接口 URL。
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"{self.base_url}/chat-messages",
                    # 保存下游 HTTP 请求所需的鉴权和内容类型。
                    # 将 `headers` 的结果保存到 `headers`，供后续流程使用。
                    headers=headers,
                    # 把完整业务请求体以 JSON 形式发给下游服务。
                    # 将 `request_data` 的结果保存到 `json`，供后续流程使用。
                    json=request_data
                # 完成“同步调用 Dify 并取得完整结果”所需的数据封装或下游调用，准备处理其结果。
                # 结束上方多行参数或数据结构。
                )

                # 检查下游 HTTP 状态码，失败时停止解析正常结果并进入错误处理。
                # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                # 判断 `if response.status_code != 200`，决定是否执行当前分支。
                if response.status_code != 200:
                    # 把 `error_text` 字段写入当前请求、响应或数据对象，供 `call_workflow_sync` 后续步骤使用。
                    error_text = response.text
                    # 记录“同步调用 Dify 并取得完整结果”的关键状态或失败原因，方便排查下游服务问题。
                    # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
                    logger.error(f"Dify API错误: {response.status_code} - {error_text}")
                    # 终止“同步调用 Dify 并取得完整结果”并向前端返回明确的 HTTP 状态和失败原因。
                    # 抛出异常，中止当前失败流程。
                    # 抛出异常，中止当前失败流程。
                    # 抛出异常，中止当前失败流程。
                    raise HTTPException(
                        # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
                        # 将 `response.status_code` 的结果保存到 `status_code`，供后续流程使用。
                        status_code=response.status_code,
                        # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
                        # 将 `f"Dify API错误: {error_text}"` 的结果保存到 `detail`，供后续流程使用。
                        detail=f"Dify API错误: {error_text}"
                    # 完成“同步调用 Dify 并取得完整结果”所需的数据封装或下游调用，准备处理其结果。
                    # 结束上方多行参数或数据结构。
                    )

                # 把“同步调用 Dify 并取得完整结果”得到的最终业务结果返回给接口层或前端。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return response.json()

        # 捕获“同步调用 Dify 并取得完整结果”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except httpx.TimeoutException:
            # 记录“同步调用 Dify 并取得完整结果”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error("Dify API请求超时")
            # 终止“同步调用 Dify 并取得完整结果”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=504, detail="Dify API请求超时")
        # 捕获“同步调用 Dify 并取得完整结果”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except httpx.RequestError as e:
            # 记录“同步调用 Dify 并取得完整结果”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"Dify API请求错误: {str(e)}")
            # 终止“同步调用 Dify 并取得完整结果”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=503, detail=f"Dify API请求错误: {str(e)}")
        # 捕获“同步调用 Dify 并取得完整结果”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录“同步调用 Dify 并取得完整结果”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"Dify工作流调用中出现意外错误: {str(e)}")
            # 终止“同步调用 Dify 并取得完整结果”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=500, detail=f"内部服务器错误: {str(e)}")
