"""
简历解析与 AI 评价核心业务服务。

【文件定位】
本文件串联简历筛选的完整业务：校验上传文件、提取正文、查询远程 JD、读取评分标准、
调用 Dify type=3、清洗 AI JSON、保存文件、写入本地 resume_evaluations 表，并提供评价
历史、详情、删除和候选人状态更新等能力。

【核心对象】
- AsyncSession：操作本地 PostgreSQL 中的 ResumeEvaluation。
- ResumeParserService：校验 PDF/TXT/DOC/DOCX 并提取文字。
- remote_service_client：读取远程 JD 与评分标准。
- DifyService：执行结构化 AI 评分。
- LLMService：自动导入等场景下辅助匹配最合适的 JD。

【主流程】
evaluate_resume = 文件校验 -> 文本提取 -> 获取 JD -> 获取专属/默认评分标准
-> Dify 评价 -> 解析 JSON -> 文件落盘 -> 数据库 commit -> 返回前端。

【主要函数】
- __init__：保存 AsyncSession，并初始化 DifyService、LLMService 和 ResumeParserService。
- evaluate_resume：手动选择 JD 后的主评价入口；校验并解析文件、读取 JD/评分标准、调用 Dify，
  保存原文件和 resume_evaluations 记录，最后返回候选人及分数。
- _save_uploaded_file_content：按用户创建上传目录并异步写入简历文件，返回数据库要保存的路径。
- evaluate_resume_auto：用于自动导入场景；先解析简历，再根据邮件主题/简历内容匹配 JD 后复用主流程。
- evaluate_resume_text_auto：处理已经取得正文的简历，匹配 JD 后构造评价结果，适合邮件等无直接上传对象的入口。
- _fetch_jds：从远程 HR 服务读取用户可用 JD 列表，为自动匹配准备候选集合。
- _match_best_jd：优先按岗位信息匹配 JD，必要时调用 LLM 选择最合适的 JD ID。
- _partial_match / _fuzzy_match_jd：通过文本包含和关键词重叠完成低成本 JD 模糊匹配。
- _get_job_description：根据 JD ID 和所属用户从远程服务取得完整岗位内容。
- _get_evaluation_model：按 JD ID 查询专属评分标准；查不到或请求失败时返回默认标准。
- _get_default_evaluation_model：提供内置评分提示词，规定学历、经验、技能、项目和综合素质分值。
- _call_dify_evaluation：使用 type=3，把简历、岗位和评分模型交给 Dify，返回结构化结果及原始响应。
- _parse_ai_response：提取 answer/data.answer，清理代码块并兼容中英文字段，构造 AIEvaluationResult。
- _create_default_result：AI JSON 无法解析时返回 60 分综合评价及“未知”候选人字段，保证流程可落库。
- _save_evaluation_result：创建 ResumeEvaluation ORM 对象并 commit 到本地 PostgreSQL；失败时 rollback。
- get_evaluation_history / get_evaluation_history_with_pagination：按用户和状态查询简历评价历史及总数。
- get_evaluation_by_id / get_evaluation_detail：按评价 ID 查询当前用户可访问的简历详情。
- _parse_work_years_to_float：从“3年”等文本中提取数值，统一保存为浮点年限。
- validate_uuid_param / validate_evaluation_params：把外部字符串 ID 转成 UUID，并提前拦截非法参数。
- validate_status_param：把状态文本转换为 ResumeStatus，只允许 pending、rejected、interview。
- get_supported_formats：返回前端可上传的简历格式和大小限制。
- delete_evaluation：删除简历评价前清理关联面试方案和简历文件，避免遗留无效资源。
- update_evaluation_status：更新候选人状态；设置为 interview 后会进入智能面试候选列表。
- _build_evaluation_prompt：组合简历、JD 和评分标准，生成可供 AI 评价的完整提示文本。

【阅读重点】
本文件同时包含手动上传和自动匹配 JD 的扩展路径，因此较长。阅读时优先从
evaluate_resume、_get_job_description、_get_evaluation_model、_call_dify_evaluation、
_parse_ai_response、_save_evaluation_result 六个函数串起主链路。
"""

# 引入依赖，用于记录业务执行过程和失败原因。
import logging
# 引入依赖，用于编码请求数据或解析 AI/远程服务返回的 JSON。
import json
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import re
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import os
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import datetime
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from pathlib import Path
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from typing import Dict, Any, Optional, List, Tuple
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from uuid import UUID
# 引入依赖，用于异步操作本地 PostgreSQL。
from sqlalchemy.ext.asyncio import AsyncSession
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from sqlalchemy import select, func
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import numpy as np
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.models.user import User
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
import aiofiles
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from datetime import datetime
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.core.config import settings
# 引入依赖，用于异步请求 Dify 或远程 HR 服务。
import httpx

# 引入依赖，用于读写本地简历评价记录。
from app.models.resume_evaluation import ResumeEvaluation, ResumeStatus
# from app.models.job_description import JobDescription
from app.schemas.resume_evaluation import (
    # 引入或传递 `ResumeEvaluationCreate`，供“完成当前模块初始化”的后续步骤使用。
    ResumeEvaluationCreate,
    # 引入或传递 `AIEvaluationResult`，供“完成当前模块初始化”的后续步骤使用。
    AIEvaluationResult,
    # 引入或传递 `EvaluationMetric`，供“完成当前模块初始化”的后续步骤使用。
    EvaluationMetric,
    # 引入或传递 `ResumeEvaluationResponse`，供“完成当前模块初始化”的后续步骤使用。
    ResumeEvaluationResponse,
    # 引入或传递 `ResumeEvaluationListResponse`，供“完成当前模块初始化”的后续步骤使用。
    ResumeEvaluationListResponse
# 完成“完成当前模块初始化”所需的数据封装或下游调用，准备处理其结果。
)
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.schemas.job_description import JobDescriptionResponse
# 引入依赖，用于调用 Dify 工作流完成 AI 生成。
from app.services.dify_service import DifyService
# 引入依赖，用于校验简历并提取正文。
from app.services.resume_parser_service import ResumeParserService
# 引入当前模块完成“完成当前模块初始化”需要的类型或工具。
from app.services.llm_service import LLMService
# 引入依赖，用于统一访问远程 HR 服务。
from app.services.remote_service_client import remote_service_client
# 通过大模型进行JD匹配，而非embedding

# 创建模块日志记录器，统一记录业务过程和异常。
logger = logging.getLogger(__name__)


# 集中管理 `ResumeEvaluationService` 对应模块的状态和业务方法，避免逻辑散落在接口层。
class ResumeEvaluationService:
    """简历评价服务"""

    # 封装“完成 `init` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def __init__(self, db: AsyncSession):
        # 保存当前对象的 `db` 业务状态，供同一服务的其他方法复用。
        self.db = db
        # 初始化 `dify_service` 依赖，供当前服务后续业务步骤直接调用。
        self.dify_service = DifyService()
        # 初始化 `llm_service` 依赖，供当前服务后续业务步骤直接调用。
        self.llm_service = LLMService()
        # 初始化 `resume_parser` 依赖，供当前服务后续业务步骤直接调用。
        self.resume_parser = ResumeParserService()

    # 封装“解析简历并完成 AI 评分”流程，供接口层或其他业务服务调用。
    async def evaluate_resume(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 提供刚上传的简历二进制内容供解析和保存
        file_content: bytes,
        # 提供原始文件名，用于格式校验和结果展示
        filename: str,
        # 把评分或简历评价关联到指定岗位 JD
        job_description_id: UUID,
        # 可选地延续同一次 Dify 对话上下文
        conversation_id: Optional[UUID] = None,
        # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
        email_id: Optional[str] = None,
        # 把 `jd_user_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
        jd_user_id: Optional[UUID] = None
    # 完成“解析简历并完成 AI 评分”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Dict[str, Any]:
        """评价简历

        Args:
            user_id: 评价记录归属的用户ID
            jd_user_id: JD所属的用户ID，用于查询JD详情。不传则使用user_id
        """
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 1. 验证文件
            is_valid, message = self.resume_parser.validate_file(filename, len(file_content))
            # 确认 `is_valid` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume`。
            if not is_valid:
                # 发现无法继续的业务问题，终止“解析简历并完成 AI 评分”并交给上层统一处理。
                raise ValueError(message)

            # 2. 提取文本内容
            resume_text = await self.resume_parser.extract_text_from_file(file_content, filename)
            # 确认 `resume_text.strip()` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume`。
            if not resume_text.strip():
                # 发现无法继续的业务问题，终止“解析简历并完成 AI 评分”并交给上层统一处理。
                raise ValueError("无法从文件中提取到有效内容")

            # 3. 获取文件信息
            file_info = self.resume_parser.get_file_info(filename, file_content)

            # 4. 获取JD信息（使用JD所属用户ID查询）
            _jd_uid = jd_user_id or user_id
            # 调用 `self._get_job_description` 执行“解析简历并完成 AI 评分”所需的下游操作，并保存返回结果供下一步使用。
            jd = await self._get_job_description(job_description_id, _jd_uid)
            # 确认关联 JD 存在；没有岗位要求就无法继续评分或生成面试方案。
            if not jd:
                # 发现无法继续的业务问题，终止“解析简历并完成 AI 评分”并交给上层统一处理。
                raise ValueError("职位描述不存在")

            # 5. 获取评价模型
            evaluation_model = await self._get_evaluation_model(job_description_id)

            # 6. 调用Dify API进行评价
            ai_result, raw_response = await self._call_dify_evaluation(
                # 保存解析后的简历正文，供 Dify 评价和数据库留档。
                resume_text=resume_text,
                # 保存当前 JD 对应的评分标准，供 Dify 按规则评分。
                evaluation_model=evaluation_model,
                # 把 `jd_info` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
                jd_info=jd
            # 完成“解析简历并完成 AI 评分”所需的数据封装或下游调用，准备处理其结果。
            )

            # 7. 保存上传的文件到磁盘
            saved_file_path = await self._save_uploaded_file_content(filename, file_content, user_id)
            # 记录“解析简历并完成 AI 评分”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"文件已保存到: {saved_file_path}")

            # 更新file_info以包含保存的文件路径
            file_info['file_path'] = saved_file_path

            # 8. 保存评价结果
            evaluation_record = await self._save_evaluation_result(
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id,
                # 把 `created_by` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
                created_by=user_id,
                # 保存简历名称、格式、大小、哈希和路径等元数据。
                file_info=file_info,
                # 保存解析后的简历正文，供 Dify 评价和数据库留档。
                resume_text=resume_text,
                # 保存结构化 AI 评价，供写入数据库和返回前端。
                ai_result=ai_result,
                # 关联对应的岗位 JD，供评分和后续招聘流程使用。
                job_description_id=job_description_id,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                conversation_id=conversation_id,
                # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
                email_id=email_id,
                # 保留 Dify 原始响应，便于追踪评分问题。
                raw_response=raw_response
            # 完成“解析简历并完成 AI 评分”所需的数据封装或下游调用，准备处理其结果。
            )

            # 记录“解析简历并完成 AI 评分”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"评价记录已保存，ID: {evaluation_record.id}, 文件路径: {evaluation_record.file_path}")

            # 8. 返回完整结果
            return {
                # 向下游请求或返回结果写入 `id` 业务字段。
                "id": evaluation_record.id,
                # 向下游请求或返回结果写入 `evaluation_metrics` 业务字段。
                "evaluation_metrics": [metric.model_dump() for metric in ai_result.evaluation_metrics],
                # 向下游请求或返回结果写入 `total_score` 业务字段。
                "total_score": ai_result.total_score,
                # 向下游请求或返回结果写入 `name` 业务字段。
                "name": ai_result.name,
                # 向下游请求或返回结果写入 `position` 业务字段。
                "position": ai_result.position,
                # 向下游请求或返回结果写入 `workYears` 业务字段。
                "workYears": (self._parse_work_years_to_float(ai_result.workYears) or 0.0),
                # 向下游请求或返回结果写入 `education` 业务字段。
                "education": ai_result.教育水平,
                # 向下游请求或返回结果写入 `age` 业务字段。
                "age": ai_result.年龄,
                # 向下游请求或返回结果写入 `sex` 业务字段。
                "sex": ai_result.sex,
                # 向下游请求或返回结果写入 `school` 业务字段。
                "school": ai_result.school,
                # 向下游请求或返回结果写入 `resume_content` 业务字段。
                "resume_content": resume_text,
                # 向下游请求或返回结果写入 `original_filename` 业务字段。
                "original_filename": file_info['filename'],
                # 向下游请求或返回结果写入 `created_at` 业务字段。
                "created_at": evaluation_record.created_at.isoformat(),
                # 向下游请求或返回结果写入 `updated_at` 业务字段。
                "updated_at": evaluation_record.updated_at.isoformat()
            # 完成“解析简历并完成 AI 评分”所需的数据封装或下游调用，准备处理其结果。
            }

        # 捕获“解析简历并完成 AI 评分”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“解析简历并完成 AI 评分”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"简历评价失败: {e}")
            # 引入或传递 `raise`，供“解析简历并完成 AI 评分”的后续步骤使用。
            raise

    # 封装“完成 `save uploaded file content` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _save_uploaded_file_content(self, filename: str, file_content: bytes, user_id: UUID) -> str:
        """保存上传的文件内容到磁盘"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 创建用户特定的上传目录
            upload_dir = os.path.join(settings.UPLOAD_DIR, str(user_id))
            # 为 `_save_uploaded_file_content` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            os.makedirs(upload_dir, exist_ok=True)

            # 构建安全的文件路径
            safe_filename = Path(filename).name
            # 把 `file_path` 字段写入当前请求、响应或数据对象，供 `_save_uploaded_file_content` 后续步骤使用。
            file_path = os.path.join(upload_dir, safe_filename)

            # 如果文件名已存在，添加数字后缀
            counter = 1
            # 为 `_save_uploaded_file_content` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            base_name, extension = os.path.splitext(safe_filename)
            # 把 `original_file_path` 字段写入当前请求、响应或数据对象，供 `_save_uploaded_file_content` 后续步骤使用。
            original_file_path = file_path

            # 调用 `while os.path.exists` 执行 `_save_uploaded_file_content` 的当前步骤，并将产生的数据交给后续流程。
            while os.path.exists(file_path):
                # 把 `new_filename` 字段写入当前请求、响应或数据对象，供 `_save_uploaded_file_content` 后续步骤使用。
                new_filename = f"{base_name}_{counter}{extension}"
                # 把 `file_path` 字段写入当前请求、响应或数据对象，供 `_save_uploaded_file_content` 后续步骤使用。
                file_path = os.path.join(upload_dir, new_filename)
                # 为 `_save_uploaded_file_content` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                counter += 1

            # 实际保存文件内容
            async with aiofiles.open(file_path, 'wb') as f:
                # 调用 `await f.write` 执行 `_save_uploaded_file_content` 的当前步骤，并将产生的数据交给后续流程。
                await f.write(file_content)

            # 把“完成 `save uploaded file content` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return file_path
        # 捕获“完成 `save uploaded file content` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `save uploaded file content` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"保存上传文件内容失败: {e}")
            # 引入或传递 `raise`，供“完成 `save uploaded file content` 对应的业务处理”的后续步骤使用。
            raise

    # 根据简历内容与投递邮件的主题匹配最合适的JD
    async def evaluate_resume_auto(
            # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
            self,
            # 限定本次操作所属用户，防止访问其他用户的数据
            user_id: UUID,
            # 提供刚上传的简历二进制内容供解析和保存
            file_content: bytes,
            # 提供原始文件名，用于格式校验和结果展示
            filename: str,
            # 为 `evaluate_resume_auto` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            subject: str,
            # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_auto` 后续步骤使用。
            email_id: str = None,
            # 可选地延续同一次 Dify 对话上下文
            conversation_id: Optional[UUID] = None
        # 完成“完成 `evaluate resume auto` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
        ) -> Dict[str, Any]:
            """无JD输入时，自动匹配最合适的JD并进行评价"""
            # 解析文本（复用 evaluate_resume 的前置逻辑）
            is_valid, message = self.resume_parser.validate_file(filename, len(file_content))
            # 确认 `is_valid` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume_auto`。
            if not is_valid:
                # 发现无法继续的业务问题，终止“完成 `evaluate resume auto` 对应的业务处理”并交给上层统一处理。
                raise ValueError(message)
            # 调用 `self.resume_parser.extract_text_from_file` 执行“完成 `evaluate resume auto` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
            resume_text = await self.resume_parser.extract_text_from_file(file_content, filename)
            # 确认 `resume_text or not resume_text.strip()` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume_auto`。
            if not resume_text or not resume_text.strip():
                # 发现无法继续的业务问题，终止“完成 `evaluate resume auto` 对应的业务处理”并交给上层统一处理。
                raise ValueError("无法从简历中提取有效文本")
            # 自动匹配 JD
            #拼接主题与邮件正文
            logger.info(f"开始自动匹配JD，邮件主题: {subject}")
            # 调用 `self._match_best_jd` 获取 `jd_id, jd_owner_user_id`，供 `evaluate_resume_auto` 的下一步业务处理使用。
            jd_id, jd_owner_user_id = await self._match_best_jd(subject=subject, resume_text= resume_text, create_by=str(user_id))
            # 确认关联 JD 存在；没有岗位要求就无法继续评分或生成面试方案。
            if not jd_id:
                # 记录“完成 `evaluate resume auto` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.error(f"未匹配到合适的职位描述，邮件主题: {subject}")
                # 发现无法继续的业务问题，终止“完成 `evaluate resume auto` 对应的业务处理”并交给上层统一处理。
                raise ValueError("未匹配到合适的职位描述")
            # 记录“完成 `evaluate resume auto` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"匹配到JD: {jd_id}, JD所属用户: {jd_owner_user_id}")
            # 调用已有评价流程，传入JD所属用户ID用于查询JD详情和保存评价记录
            return await self.evaluate_resume(
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=jd_owner_user_id,
                # 保存上传的简历字节，供格式校验、文本提取和文件落盘。
                file_content=file_content,
                # 保存安全处理后的文件名或原文件名，供落盘和结果展示。
                filename=filename,
                # 关联对应的岗位 JD，供评分和后续招聘流程使用。
                job_description_id=jd_id,
                # 把 `jd_user_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_auto` 后续步骤使用。
                jd_user_id=jd_owner_user_id,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                conversation_id=conversation_id,
                # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_auto` 后续步骤使用。
                email_id=email_id
            # 完成“完成 `evaluate resume auto` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            )

    # 封装“完成 `evaluate resume text auto` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def evaluate_resume_text_auto(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `evaluate_resume_text_auto` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        login_name: str,
        # 提供已解析的简历正文供 AI 分析
        resume_text: str,
        # 提供原始文件名，用于格式校验和结果展示
        filename: Optional[str] = None,
        # 把 `subject` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
        subject: str = ""
    # 完成“完成 `evaluate resume text auto` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Dict[str, Any]:
        """
        处理文本简历的自动评价流程

        Args:
            login_name: 用户登录名
            resume_text: 简历文本内容
            filename: 文件名（可选）
            subject: 投递邮件主题（可选）

        Returns:
            评价结果字典
        """

        # 验证用户是否存在
        if not login_name:
            # 发现无法继续的业务问题，终止“完成 `evaluate resume text auto` 对应的业务处理”并交给上层统一处理。
            raise ValueError("login_name不能为空")

        # 把 `users` 业务参数传给下游方法，使其能够完成“完成 `evaluate resume text auto` 对应的业务处理”。
        users = await self.db.execute(select(User).where(User.username == login_name))
        # 把 `user` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
        user = users.scalar_one_or_none()
        # 确认 `user` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume_text_auto`。
        if not user:
            # 发现无法继续的业务问题，终止“完成 `evaluate resume text auto` 对应的业务处理”并交给上层统一处理。
            raise ValueError(f"用户{login_name}不存在")

        # 携带当前用户 ID，让下游服务只操作该用户的数据。
        user_id = user.id

        # 验证简历文本
        resume_text = (resume_text or "").strip()
        # 确认 `resume_text` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume_text_auto`。
        if not resume_text:
            # 发现无法继续的业务问题，终止“完成 `evaluate resume text auto` 对应的业务处理”并交给上层统一处理。
            raise ValueError("简历文本内容不能为空")

        # 生成或使用提供的文件名
        safe_default_name = f"resume_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        # 保存安全处理后的文件名或原文件名，供落盘和结果展示。
        filename = (filename or safe_default_name).strip() or safe_default_name
        # 简单清理可能的非法字符
        filename = Path(filename).name
        # 确认 `filename.lower().endswith('.txt')` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `evaluate_resume_text_auto`。
        if not filename.lower().endswith('.txt'):
            # 保存安全处理后的文件名或原文件名，供落盘和结果展示。
            filename = f"{filename}.txt"

        # 保存文本为本地附件 uploads/{user_id}/{filename}
        user_dir = os.path.join(settings.UPLOAD_DIR, str(user_id))
        # 为 `evaluate_resume_text_auto` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        os.makedirs(user_dir, exist_ok=True)
        # 把 `file_path` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
        file_path = os.path.join(user_dir, filename)

        # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
        async with aiofiles.open(file_path, "wb") as f:
            # 调用 `await f.write` 执行 `evaluate_resume_text_auto` 的当前步骤，并将产生的数据交给后续流程。
            await f.write(resume_text.encode("utf-8"))

        # 调用自动匹配与评分服务
        result = await self.evaluate_resume_auto(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=user_id,
            # 保存上传的简历字节，供格式校验、文本提取和文件落盘。
            file_content=resume_text.encode("utf-8"),
            # 保存安全处理后的文件名或原文件名，供落盘和结果展示。
            filename=filename,
            # 把 `subject` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
            subject=subject or ""
        # 完成“完成 `evaluate resume text auto` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        )

        # 回写 file_path 到评价记录
        try:
            # 引入依赖，用于读写本地简历评价记录。
            from app.models.resume_evaluation import ResumeEvaluation
            # 把 `eval_id` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
            eval_id = result.get("id")
            # 根据 `eval_id` 判断当前数据应进入哪条业务分支，保证 `evaluate_resume_text_auto` 使用正确处理方式。
            if eval_id:
                # 把 `stmt` 字段写入当前请求、响应或数据对象，供 `evaluate_resume_text_auto` 后续步骤使用。
                stmt = select(ResumeEvaluation).where(
                    # 把当前字段纳入 `evaluate_resume_text_auto` 的请求或响应结构，确保后续步骤获得完整业务数据。
                    ResumeEvaluation.id == eval_id,
                    # 把当前字段纳入 `evaluate_resume_text_auto` 的请求或响应结构，确保后续步骤获得完整业务数据。
                    ResumeEvaluation.user_id == user_id,
                # 完成“完成 `evaluate resume text auto` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                )
                # 调用 `self.db.execute` 执行“完成 `evaluate resume text auto` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
                db_result = await self.db.execute(stmt)
                # 创建或保存一条简历评价 ORM 记录，准备写入本地数据库。
                evaluation = db_result.scalar_one_or_none()
                # 根据 `evaluation` 判断当前数据应进入哪条业务分支，保证 `evaluate_resume_text_auto` 使用正确处理方式。
                if evaluation:
                    # 为 `evaluate_resume_text_auto` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                    evaluation.file_path = file_path
                    # 调用 `await self.db.commit` 执行 `evaluate_resume_text_auto` 的当前步骤，并将产生的数据交给后续流程。
                    await self.db.commit()
        # 捕获“完成 `evaluate resume text auto` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 不影响主流程，记录日志
            import logging
            # 创建模块日志记录器，统一记录业务过程和异常。
            logger = logging.getLogger(__name__)
            # 记录“完成 `evaluate resume text auto` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.warning(f"评价记录回写 file_path 失败: {e}")

        # 把“完成 `evaluate resume text auto` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return result

    # 封装“完成 `fetch jds` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _fetch_jds(self, user_id: UUID) -> List[JobDescriptionResponse]:
        """从远程服务获取指定用户的JD列表"""
        # 调用 `remote_service_client.get` 执行“完成 `fetch jds` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
        result_data = await remote_service_client.get(
            # 指定远程 HR 服务的目标业务接口。
            endpoint="job-descriptions/",
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=user_id,
            # 把 `additional_params` 字段写入当前请求、响应或数据对象，供 `_fetch_jds` 后续步骤使用。
            additional_params={"page": 1, "size": 100}
        # 完成“完成 `fetch jds` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        )
        # 把 `jd_items` 字段写入当前请求、响应或数据对象，供 `_fetch_jds` 后续步骤使用。
        jd_items = result_data.get("items", [])
        # 把“完成 `fetch jds` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return [JobDescriptionResponse(**jd_data) for jd_data in jd_items]

    # 封装“完成 `match best jd` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def _match_best_jd(self, subject:str ,resume_text: str,create_by: str) -> Tuple[Optional[UUID], Optional[UUID]]:
        """通过大模型服务判断最匹配的JD

        Returns:
            (jd_id, jd_owner_user_id) - 匹配的JD ID和该JD所属的用户ID
        """
        # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
        logger.info(f"开始查询职位描述，create_by: {create_by}")
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 调用远程服务获取所有JD
            jds = await self._fetch_jds(UUID(create_by))
            # 把外部传入的字符串 ID 转换为 UUID，确保后续查询使用统一标识类型。
            jd_owner_user_id = UUID(create_by)  # 记录当前JD所属的用户ID

            # 如果当前用户没有JD，尝试查找本地其他用户的JD
            if not jds:
                # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.info(f"用户 {create_by} 没有JD，尝试查找其他本地用户的JD")
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 引入当前模块完成“完成 `match best jd` 对应的业务处理”需要的类型或工具。
                    from sqlalchemy import select as sql_select
                    # 调用 `self.db.execute` 执行“完成 `match best jd` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
                    users_result = await self.db.execute(
                        # 调用 `sql_select` 执行 `_match_best_jd` 的当前步骤，并将产生的数据交给后续流程。
                        sql_select(User).where(User.is_active == True).limit(10)
                    # 完成“完成 `match best jd` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                    )
                    # 把 `local_users` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                    local_users = users_result.scalars().all()
                    # 逐项处理“完成 `match best jd` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                    for u in local_users:
                        # 根据 `str(u.id) == create_by` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                        if str(u.id) == create_by:
                            # 引入或传递 `continue`，供“完成 `match best jd` 对应的业务处理”的后续步骤使用。
                            continue
                        # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                        logger.info(f"尝试查询用户 {u.id} 的JD")
                        # 调用 `self._fetch_jds` 执行“完成 `match best jd` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
                        jds = await self._fetch_jds(u.id)
                        # 根据 `jds` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                        if jds:
                            # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                            logger.info(f"从用户 {u.id} 找到 {len(jds)} 个JD")
                            # 把 `jd_owner_user_id` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                            jd_owner_user_id = u.id
                            # 引入或传递 `break`，供“完成 `match best jd` 对应的业务处理”的后续步骤使用。
                            break
                # 捕获“完成 `match best jd` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.warning(f"查找其他用户JD失败: {e}")

            # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.info(f"查询到 {len(jds)} 个职位描述")

            # 输出所有JD标题用于调试
            for jd in jds:
                # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.info(f"JD标题: {jd.title}")

            # 确认关联 JD 存在；没有岗位要求就无法继续评分或生成面试方案。
            if not jds:
                # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return None, None

            # 新增逻辑：根据邮件主题关键词匹配职位描述
            # 主题格式：简历-职位名称-姓名
            if subject and "简历-" in subject:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 解析主题中的职位名称
                    parts = subject.split("-")
                    # 根据 `len(parts) >= 2` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                    if len(parts) >= 2:
                        # 把 `position_name` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                        position_name = parts[1].strip()  # 获取职位名称部分
                        # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                        logger.info(f"从邮件主题解析出职位名称: {position_name}")

                        # 在JD列表中查找匹配的职位名称
                        for jd in jds:
                            # 根据 `jd.title` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                            if jd.title:
                                # 使用更宽松的匹配策略，忽略大小写
                                jd_title_clean = jd.title.replace(" ", "").replace("-", "").lower()
                                # 把 `position_name_clean` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                                position_name_clean = position_name.replace(" ", "").replace("-", "").lower()

                                # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                                logger.info(f"比较职位名称: '{position_name_clean}' 与 JD标题: '{jd_title_clean}'")

                                # 多种匹配方式
                                if (position_name_clean in jd_title_clean or
                                    # 把当前字段纳入 `_match_best_jd` 的请求或响应结构，确保后续步骤获得完整业务数据。
                                    jd_title_clean in position_name_clean):
                                    # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                                    logger.info(f"通过邮件主题匹配到JD: {jd.title}")
                                    # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                                    return jd.id, jd_owner_user_id

                        # 如果没有精确匹配，尝试模糊匹配
                        logger.info("未找到精确匹配，尝试模糊匹配...")
                        # 把 `best_match` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                        best_match = self._fuzzy_match_jd(position_name, jds)
                        # 根据 `best_match` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                        if best_match:
                            # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                            logger.info(f"通过模糊匹配找到JD: {best_match.title}")
                            # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                            return best_match.id, jd_owner_user_id

                # 捕获“完成 `match best jd` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                except Exception as e:
                    # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.error(f"邮件主题解析失败: {e}")

            # 组装简洁的 JD 选项，避免超长提示
            jd_options = [
                # 把当前字段纳入 `_match_best_jd` 的请求或响应结构，确保后续步骤获得完整业务数据。
                {
                    # 向下游请求或返回结果写入 `id` 业务字段。
                    "id": str(jd.id),
                    # 向下游请求或返回结果写入 `title` 业务字段。
                    "title": jd.title
                # 完成“完成 `match best jd` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                }
                # 逐项处理“完成 `match best jd` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                for jd in jds
            # 完成“完成 `match best jd` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            ]
            # 构造提示词
            prompt = (
                # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                "你是一个职位匹配助手。"
                # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                "1、如果候选人投递邮件的主题中包括了他要投递的岗位，则直接根据主题subject从给定的JD列表中选择最匹配的一项，匹配到直接输出jd id，不需要再根据简历内容匹配。若没有匹配到，则直接输出None"
                # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                "2、如果投递邮件主题中没有要投递的岗位，则从候选人的简历内容，去从给定的JD列表中选择最匹配的一项。如果没有匹配到合适的岗位，则直接输出None"
                # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                "输出要求：不要给出匹配理由，直接输出jd id的值,或者None"
            # 完成“完成 `match best jd` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            )
            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 使用通用 LLM 服务进行匹配，而不是 Dify 工作流
                import json as _json
                # 把 `jd_compact` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                jd_compact = _json.dumps(jd_options, ensure_ascii=False)[:12000]
                # 把 `llm_input` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                llm_input = (
                    # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                    f"{prompt}\n\n候选人投递邮件主题：{subject}\n\n候选人简历：\n{resume_text[:8000]}\n\nJD列表(JSON)：\n{jd_compact}\n\n"
                    # 向 `_match_best_jd` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                    " "
                # 完成“完成 `match best jd` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                )
                # 调用 `self.llm_service.generate_response` 执行“完成 `match best jd` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
                jd_id_str = await self.llm_service.generate_response(message=llm_input)
                # 调用 `print` 执行 `_match_best_jd` 的当前步骤，并将产生的数据交给后续流程。
                print(f"LLM JD匹配结果：{jd_id_str}")
                # 确认关联 JD 存在；没有岗位要求就无法继续评分或生成面试方案。
                if not jd_id_str:
                    # 发现无法继续的业务问题，终止“完成 `match best jd` 对应的业务处理”并交给上层统一处理。
                    raise ValueError("匹配结果不包含jd_id")
                # 返回UUID
                for jd in jds:
                    # 根据 `str(jd.id) in jd_id_str` 判断当前数据应进入哪条业务分支，保证 `_match_best_jd` 使用正确处理方式。
                    if str(jd.id) in jd_id_str:
                        # 调用 `print` 执行 `_match_best_jd` 的当前步骤，并将产生的数据交给后续流程。
                        print(f"LLM JD匹配成功：{jd.title}")
                        # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                        return jd.id, jd_owner_user_id
                # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return None, None
            # 捕获“完成 `match best jd` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            except Exception as e:
                # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.error(f"LLM JD匹配失败，回退关键词匹配: {e}")
                # 回退：按关键词匹配（简单匹配）
                keywords = ["Java", "Python", "前端", "后端", "AI", "算法", "产品", "测试"]
                # 把 `scores` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                scores = []
                # 把 `lower_text` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                lower_text = resume_text.lower()
                # 逐项处理“完成 `match best jd` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
                for jd in jds:
                    # 把 `agg` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                    agg = " ".join([jd.title or "", jd.requirements or "", jd.skills or ""]).lower()
                    # 把 `score` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                    score = sum(1 for kw in keywords if kw.lower() in lower_text and kw.lower() in agg)
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    scores.append(score)
                # 把 `best_idx` 字段写入当前请求、响应或数据对象，供 `_match_best_jd` 后续步骤使用。
                best_idx = int(np.argmax(scores)) if scores else 0
                # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return (jds[best_idx].id, jd_owner_user_id) if jds else (None, None)
        # 捕获“完成 `match best jd` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `match best jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取所有JD失败: {e}")
            # 把“完成 `match best jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return None, None

    # 封装“完成 `partial match` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def _partial_match(self, term1: str, term2: str) -> bool:
        """检查两个字符串是否部分匹配"""
        # 确认 `term1 or not term2` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `_partial_match`。
        if not term1 or not term2:
            # 把“完成 `partial match` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return False

        # 如果一个字符串包含另一个字符串
        if term1 in term2 or term2 in term1:
            # 把“完成 `partial match` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return True

        # 检查是否有共同的子串（至少3个字符）
        min_len = min(len(term1), len(term2))
        # 根据 `min_len >= 3` 判断当前数据应进入哪条业务分支，保证 `_partial_match` 使用正确处理方式。
        if min_len >= 3:
            # 逐项处理“完成 `partial match` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            for i in range(min_len - 2):
                # 把 `substr` 业务参数传给下游方法，使其能够完成“完成 `partial match` 对应的业务处理”。
                substr = term1[i:i+3] if len(term1[i:i+3]) == 3 else None
                # 根据 `substr and substr in term2` 判断当前数据应进入哪条业务分支，保证 `_partial_match` 使用正确处理方式。
                if substr and substr in term2:
                    # 把“完成 `partial match` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    return True

        # 把“完成 `partial match` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return False

    # 封装“完成 `fuzzy match jd` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def _fuzzy_match_jd(self, position_name: str, jds: List[JobDescriptionResponse]) -> Optional[JobDescriptionResponse]:
        """模糊匹配职位描述"""
        # 确认关联 JD 存在；没有岗位要求就无法继续评分或生成面试方案。
        if not position_name or not jds:
            # 把“完成 `fuzzy match jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return None

        # 把 `position_name_clean` 字段写入当前请求、响应或数据对象，供 `_fuzzy_match_jd` 后续步骤使用。
        position_name_clean = position_name.replace(" ", "").replace("-", "").lower()
        # 记录“完成 `fuzzy match jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
        logger.info(f"开始模糊匹配职位名称: {position_name_clean}")

        # 尝试多种匹配方式
        for jd in jds:
            # 根据 `jd.title` 判断当前数据应进入哪条业务分支，保证 `_fuzzy_match_jd` 使用正确处理方式。
            if jd.title:
                # 把 `jd_title_clean` 字段写入当前请求、响应或数据对象，供 `_fuzzy_match_jd` 后续步骤使用。
                jd_title_clean = jd.title.replace(" ", "").replace("-", "").lower()
                # 记录“完成 `fuzzy match jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                logger.info(f"比较职位名称: '{position_name_clean}' 与 JD标题: '{jd_title_clean}'")

                # 1. 直接包含关系
                if position_name_clean in jd_title_clean or jd_title_clean in position_name_clean:
                    # 记录“完成 `fuzzy match jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"模糊匹配成功: {position_name} 匹配到 {jd.title}")
                    # 把“完成 `fuzzy match jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    return jd

                # 2. 关键词匹配
                position_keywords = set(position_name_clean)
                # 把 `jd_keywords` 字段写入当前请求、响应或数据对象，供 `_fuzzy_match_jd` 后续步骤使用。
                jd_keywords = set(jd_title_clean)

                # 如果有超过30%的字符重叠，认为匹配（降低阈值）
                union_len = len(position_keywords | jd_keywords)
                # 根据 `union_len > 0` 判断当前数据应进入哪条业务分支，保证 `_fuzzy_match_jd` 使用正确处理方式。
                if union_len > 0:
                    # 把 `overlap_ratio` 字段写入当前请求、响应或数据对象，供 `_fuzzy_match_jd` 后续步骤使用。
                    overlap_ratio = len(position_keywords & jd_keywords) / union_len
                    # 记录“完成 `fuzzy match jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"关键词重叠比例: {overlap_ratio}")
                    # 根据 `overlap_ratio > 0.3` 判断当前数据应进入哪条业务分支，保证 `_fuzzy_match_jd` 使用正确处理方式。
                    if overlap_ratio > 0.3:
                        # 记录“完成 `fuzzy match jd` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
                        logger.info(f"通过关键词匹配成功: {position_name} 匹配到 {jd.title}")
                        # 把“完成 `fuzzy match jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                        return jd

        # 把“完成 `fuzzy match jd` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return None

    # 封装“读取简历评价所关联的远程 JD”流程，供接口层或其他业务服务调用。
    async def _get_job_description(self, jd_id: UUID, user_id: UUID) -> Optional[JobDescriptionResponse]:
        """获取职位描述 - 使用远程服务"""
        # 执行 JD 查询或保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 调用 `remote_service_client.get` 执行“读取简历评价所关联的远程 JD”所需的下游操作，并保存返回结果供下一步使用。
            result_data = await remote_service_client.get(
                # 指定远程 HR 服务的目标业务接口。
                endpoint=f"job-descriptions/{jd_id}",
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“读取简历评价所关联的远程 JD”所需的数据封装或下游调用，准备处理其结果。
            )
            # 把“读取简历评价所关联的远程 JD”得到的最终业务结果返回给接口层或前端。
            return JobDescriptionResponse(**result_data)
        # 捕获“读取简历评价所关联的远程 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“读取简历评价所关联的远程 JD”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取职位描述失败: {e}")
            # 把“读取简历评价所关联的远程 JD”得到的最终业务结果返回给接口层或前端。
            return None

    # 封装“读取 JD 专属评分标准或使用默认标准”流程，供接口层或其他业务服务调用。
    async def _get_evaluation_model(self, jd_id: UUID) -> str:
        """获取评价模型"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 从远程服务获取与JD关联的评分标准
            # 注意：该接口不需要current_user_id参数，直接使用httpx调用
            url = f"{remote_service_client.base_url}/scoring-criteria/by-jd/{jd_id}"
            # 保存下游 HTTP 请求所需的鉴权和内容类型。
            headers = remote_service_client._get_headers()
            
            # 建立并自动释放异步 HTTP 连接，用于访问下游服务且避免连接泄漏。
            async with httpx.AsyncClient() as client:
                # 调用 `client.get` 执行“读取 JD 专属评分标准或使用默认标准”所需的下游操作，并保存返回结果供下一步使用。
                response = await client.get(
                    # 引入或传递 `url`，供“读取 JD 专属评分标准或使用默认标准”的后续步骤使用。
                    url,
                    # 保存下游 HTTP 请求所需的鉴权和内容类型。
                    headers=headers,
                    # 限制下游请求等待时间，避免外部服务无响应时长期占用连接。
                    timeout=remote_service_client.timeout
                # 完成“读取 JD 专属评分标准或使用默认标准”所需的数据封装或下游调用，准备处理其结果。
                )
            
            # 保存远程 HR 服务解析后的业务结果。
            result_data = remote_service_client._handle_response(response)

            # 检查返回数据中是否有content字段
            if result_data and result_data.get('content'):
                # 把“读取 JD 专属评分标准或使用默认标准”得到的最终业务结果返回给接口层或前端。
                return result_data['content']

            # 如果没有找到特定的评价模型，返回默认模型
            return self._get_default_evaluation_model()

        # 捕获“读取 JD 专属评分标准或使用默认标准”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“读取 JD 专属评分标准或使用默认标准”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取评价模型失败: {e}")
            # 把“读取 JD 专属评分标准或使用默认标准”得到的最终业务结果返回给接口层或前端。
            return self._get_default_evaluation_model()

    # 封装“提供无专属标准时的默认评分规则”流程，供接口层或其他业务服务调用。
    def _get_default_evaluation_model(self) -> str:
        """获取默认评价模型"""
        # 把“提供无专属标准时的默认评分规则”得到的最终业务结果返回给接口层或前端。
        return """
        请根据以下职位要求对简历进行评价，并按照指定的JSON格式返回结果：

        评价维度：
        1. 学历匹配度 (0-20分)
        2. 工作经验匹配度 (0-25分)
        3. 技能匹配度 (0-25分)
        4. 项目经验匹配度 (0-20分)
        5. 综合素质 (0-10分)

        请提取简历中的以下信息：
        - 姓名
        - 应聘岗位
        - 工作年限
        - 教育水平
        - 年龄
        - 性别
        - 毕业院校

        返回格式必须是有效的JSON：
        {
          "evaluation_metrics": [
            {
              "name": "学历",
              "score": 15,
              "max": 20,
              "reason": "本科学历，符合岗位要求"
            }
          ],
          "total_score": 85,
          "name": "张三",
          "position": "前端开发工程师",
          "workYears": "3年",
          "education": "本科",
          "age": 28,
          "sex": "男",
          "school": "上海理工大学"
        }
        """

    # 封装“调用 Dify 对简历进行结构化评分”流程，供接口层或其他业务服务调用。
    async def _call_dify_evaluation(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 提供已解析的简历正文供 AI 分析
        resume_text: str,
        # 提供本岗位采用的具体评分规则
        evaluation_model: str,
        # 提供候选人应聘岗位的 JD 信息
        jd_info: JobDescriptionResponse
    # 完成“调用 Dify 对简历进行结构化评分”所需参数的定义，下面开始执行具体业务步骤。
    ) -> tuple[AIEvaluationResult, str]:
        """调用Dify API进行简历评价"""
        # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:

            # 调用 Dify 的 type=3 简历评价工作流，传入简历正文、岗位名称和评分标准。
            response = await self.dify_service.call_workflow_sync(
                # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                workflow_type=3,  # 简历评价工作流
                # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                query=evaluation_model,
                # 保存 Dify 工作流需要的结构化输入字段。
                additional_inputs={
                    # 向下游请求或返回结果写入 `jianli` 业务字段。
                    "jianli": resume_text,
                    # 向下游请求或返回结果写入 `jobName` 业务字段。
                    "jobName": jd_info.title
                # 完成“调用 Dify 对简历进行结构化评分”所需的数据封装或下游调用，准备处理其结果。
                }
            # 完成“调用 Dify 对简历进行结构化评分”所需的数据封装或下游调用，准备处理其结果。
            )

            # 解析AI响应
            ai_result = self._parse_ai_response(response)
            # 保留 Dify 原始响应，便于追踪评分问题。
            raw_response = str(response)  # 保存原始响应
            # 把“调用 Dify 对简历进行结构化评分”得到的最终业务结果返回给接口层或前端。
            return ai_result, raw_response

        # 捕获“调用 Dify 对简历进行结构化评分”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“调用 Dify 对简历进行结构化评分”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"调用Dify API失败: {e}")
            # 发现无法继续的业务问题，终止“调用 Dify 对简历进行结构化评分”并交给上层统一处理。
            raise Exception(f"AI评价服务暂时不可用: {str(e)}")

    # 封装“把 Dify 输出清洗成系统可保存的评价对象”流程，供接口层或其他业务服务调用。
    def _parse_ai_response(self, response: Dict[str, Any]) -> AIEvaluationResult:
        """解析AI响应，提取评价结果"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 从Dify响应中提取答案文本
            answer_text = ""
            # 根据 `"answer" in response` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
            if "answer" in response:
                # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
                answer_text = response["answer"]
            # 根据 `"data" in response and "answer" in response["data"]` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
            elif "data" in response and "answer" in response["data"]:
                # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
                answer_text = response["data"]["answer"]
            # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_parse_ai_response` 能继续返回可处理结果。
            else:
                # 如果没有找到answer字段，尝试从其他可能的字段获取
                answer_text = str(response)

            # 确认已经取得 AI 正文；空响应无法继续转换为结构化业务结果。
            if not answer_text:
                # 发现无法继续的业务问题，终止“把 Dify 输出清洗成系统可保存的评价对象”并交给上层统一处理。
                raise ValueError("AI响应为空")

            # 尝试解析JSON
            try:
                # 尝试直接解析JSON
                if answer_text.startswith('{') and answer_text.endswith('}'):
                    # 保存远程 HR 服务解析后的业务结果。
                    result_data = json.loads(answer_text)
                # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_parse_ai_response` 能继续返回可处理结果。
                else:
                    # 如果响应包含代码块，提取JSON部分
                    if '```json' in answer_text:
                        # 记录 JSON 代码块正文的起始位置，便于从 AI 回复中准确截取结构化内容。
                        start = answer_text.find('```json') + 7
                        # 记录 JSON 代码块的结束位置，防止把后续说明文字交给 JSON 解析器。
                        end = answer_text.find('```', start)
                        # 去掉 AI 正文首尾空白，准备识别 Markdown 代码块并解析 JSON。
                        json_str = answer_text[start:end].strip()
                    # 根据 `'```' in answer_text` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
                    elif '```' in answer_text:
                        # 记录 JSON 代码块正文的起始位置，便于从 AI 回复中准确截取结构化内容。
                        start = answer_text.find('```') + 3
                        # 记录 JSON 代码块的结束位置，防止把后续说明文字交给 JSON 解析器。
                        end = answer_text.find('```', start)
                        # 去掉 AI 正文首尾空白，准备识别 Markdown 代码块并解析 JSON。
                        json_str = answer_text[start:end].strip()
                    # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_parse_ai_response` 能继续返回可处理结果。
                    else:
                        # 如果不是纯JSON，尝试提取JSON部分
                        json_start = answer_text.find('{')
                        # 把 `json_end` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        json_end = answer_text.rfind('}') + 1
                        # 根据 `json_start != -1 and json_end > json_start` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
                        if json_start != -1 and json_end > json_start:
                            # 去掉 AI 正文首尾空白，准备识别 Markdown 代码块并解析 JSON。
                            json_str = answer_text[json_start:json_end]
                        # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_parse_ai_response` 能继续返回可处理结果。
                        else:
                            # 发现无法继续的业务问题，终止“把 Dify 输出清洗成系统可保存的评价对象”并交给上层统一处理。
                            raise ValueError("No valid JSON found in response")

                    # 解析JSON
                    result_data = json.loads(json_str)

                # 验证必要字段
                if 'evaluation_metrics' not in result_data:
                    # 发现无法继续的业务问题，终止“把 Dify 输出清洗成系统可保存的评价对象”并交给上层统一处理。
                    raise ValueError("缺少evaluation_metrics字段")

                # 根据 `'total_score' not in result_data` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
                if 'total_score' not in result_data:
                    # 发现无法继续的业务问题，终止“把 Dify 输出清洗成系统可保存的评价对象”并交给上层统一处理。
                    raise ValueError("缺少total_score字段")

                # 构建评价指标列表
                metrics = []
                # 逐项处理“把 Dify 输出清洗成系统可保存的评价对象”所需的数据集合，构造最终列表或查找目标记录。
                for metric_data in result_data.get('evaluation_metrics', []):
                    # 把 `metric` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    metric = EvaluationMetric(
                        # 把 `name` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        name=metric_data.get('评价指标', ''),
                        # 把 `score` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        score=metric_data.get('score', 0),
                        # 把 `max` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        max=metric_data.get('max', 100),
                        # 把 `reason` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        reason=metric_data.get('reason', '')
                    # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                    )
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    metrics.append(metric)

                # 规范化字段别名，兼容不同返回命名
                normalized_work_years = (
                    # 调用 `result_data.get` 执行 `_parse_ai_response` 的当前步骤，并将产生的数据交给后续流程。
                    result_data.get('workYears')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('work_years')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('work_experience')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('工作年限')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('工作经验')
                # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                )
                # 处理workYears字段，确保能正确转换为float
                try:
                    # 根据 `normalized_work_years is not None and normalized_work_years != '' and normalized_work_years != '未知'` 判断当前数据应进入哪条业务分支，保证 `_parse_ai_response` 使用正确处理方式。
                    if normalized_work_years is not None and normalized_work_years != '' and normalized_work_years != '未知':
                        # 把 `normalized_work_years` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        normalized_work_years = float(normalized_work_years)
                    # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `_parse_ai_response` 能继续返回可处理结果。
                    else:
                        # 把 `normalized_work_years` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                        normalized_work_years = 0.0
                # 捕获“把 Dify 输出清洗成系统可保存的评价对象”中的失败，记录原因或转换成上层能够识别的业务错误。
                except (ValueError, TypeError):
                    # 把 `normalized_work_years` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    normalized_work_years = 0.0
                # 把 `normalized_education` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                normalized_education = (
                    # 调用 `result_data.get` 执行 `_parse_ai_response` 的当前步骤，并将产生的数据交给后续流程。
                    result_data.get('education')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('education_level')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('学历')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('教育水平')
                # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                )
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                try:
                    # 把 `normalized_age` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    normalized_age = int(result_data.get('age', result_data.get('年龄', 0)))
                # 捕获“把 Dify 输出清洗成系统可保存的评价对象”中的失败，记录原因或转换成上层能够识别的业务错误。
                except (ValueError, TypeError):
                    # 把 `normalized_age` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    normalized_age = 0

                # 把 `normalized_sex` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                normalized_sex = (
                    # 调用 `result_data.get` 执行 `_parse_ai_response` 的当前步骤，并将产生的数据交给后续流程。
                    result_data.get('sex')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('gender')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('性别')
                # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                )
                # 把 `normalized_school` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                normalized_school = (
                    # 调用 `result_data.get` 执行 `_parse_ai_response` 的当前步骤，并将产生的数据交给后续流程。
                    result_data.get('school')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('毕业院校')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('院校')
                    # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
                    or result_data.get('学校')
                # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                )

                # 构建AI评价结果
                ai_result = AIEvaluationResult(
                    # 把 `evaluation_metrics` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    evaluation_metrics=metrics,
                    # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    total_score=result_data.get('total_score', 0),
                    # 把 `name` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    name=result_data.get('name', ''),
                    # 把 `position` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    position=result_data.get('position', ''),
                    # 把 `workYears` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    workYears=normalized_work_years,
                    # 为 `_parse_ai_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                    教育水平=normalized_education or '',
                    # 为 `_parse_ai_response` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
                    年龄=normalized_age,
                    # 把 `sex` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    sex=normalized_sex or '',
                    # 把 `school` 字段写入当前请求、响应或数据对象，供 `_parse_ai_response` 后续步骤使用。
                    school=normalized_school or ''
                # 完成“把 Dify 输出清洗成系统可保存的评价对象”所需的数据封装或下游调用，准备处理其结果。
                )

                # 把“把 Dify 输出清洗成系统可保存的评价对象”得到的最终业务结果返回给接口层或前端。
                return ai_result

            # 捕获“把 Dify 输出清洗成系统可保存的评价对象”中的失败，记录原因或转换成上层能够识别的业务错误。
            except json.JSONDecodeError as e:
                # 记录“把 Dify 输出清洗成系统可保存的评价对象”的关键状态或失败原因，方便排查下游服务问题。
                logger.error(f"JSON解析失败: {e}, 原始响应: {answer_text}")
                # 返回默认结果
                return self._create_default_result(answer_text)

        # 捕获“把 Dify 输出清洗成系统可保存的评价对象”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“把 Dify 输出清洗成系统可保存的评价对象”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"解析AI响应失败: {e}")
            # 把“把 Dify 输出清洗成系统可保存的评价对象”得到的最终业务结果返回给接口层或前端。
            return self._create_default_result(str(e))

    # 封装“在 AI 结果无法解析时生成可用的兜底评价”流程，供接口层或其他业务服务调用。
    def _create_default_result(self, raw_response: str) -> AIEvaluationResult:
        """创建默认评价结果"""
        # 把“在 AI 结果无法解析时生成可用的兜底评价”得到的最终业务结果返回给接口层或前端。
        return AIEvaluationResult(
            # 把 `evaluation_metrics` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            evaluation_metrics=[
                # 调用 `EvaluationMetric` 执行 `_create_default_result` 的当前步骤，并将产生的数据交给后续流程。
                EvaluationMetric(
                    # 把 `name` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
                    name="综合评价",
                    # 把 `score` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
                    score=60,
                    # 把 `max` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
                    max=100,
                    # 把 `reason` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
                    reason="AI解析失败，给出默认评分"
                # 完成“在 AI 结果无法解析时生成可用的兜底评价”所需的数据封装或下游调用，准备处理其结果。
                )
            # 完成“在 AI 结果无法解析时生成可用的兜底评价”所需的数据封装或下游调用，准备处理其结果。
            ],
            # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            total_score=60,
            # 把 `name` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            name="未知",
            # 把 `position` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            position="未知",
            # 把 `workYears` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            workYears=0.0,
            # 为 `_create_default_result` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            教育水平="未知",
            # 为 `_create_default_result` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            年龄=None,
            # 把 `sex` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            sex="未知",
            # 把 `school` 字段写入当前请求、响应或数据对象，供 `_create_default_result` 后续步骤使用。
            school="未知"
        # 完成“在 AI 结果无法解析时生成可用的兜底评价”所需的数据封装或下游调用，准备处理其结果。
        )

    # 封装“把简历信息和 AI 评价写入本地数据库”流程，供接口层或其他业务服务调用。
    async def _save_evaluation_result(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 为 `_save_evaluation_result` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        created_by: UUID,
        # 为 `_save_evaluation_result` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        file_info: Dict[str, Any],
        # 提供已解析的简历正文供 AI 分析
        resume_text: str,
        # 为 `_save_evaluation_result` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        ai_result: AIEvaluationResult,
        # 把评分或简历评价关联到指定岗位 JD
        job_description_id: UUID,
        # 把 `raw_response` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
        raw_response: str = "",
        # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
        email_id: Optional[str] = None,
        # 可选地延续同一次 Dify 对话上下文
        conversation_id: Optional[UUID] = None
    # 完成“把简历信息和 AI 评价写入本地数据库”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ResumeEvaluation:
        """保存评价结果 - 从参数获取已保存的文件路径"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 直接创建ResumeEvaluation对象（文件已经保存）
            evaluation = ResumeEvaluation(
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id,
                # 把 `created_by` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                created_by = created_by,
                # 把 `updated_by` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                updated_by= created_by,
                # 把 `email_id` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                email_id =  email_id,
                # 把 `original_filename` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                original_filename=file_info['filename'],
                # 把 `file_path` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                file_path=file_info.get('file_path'),  # 使用传入的文件路径
                # 把 `file_type` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                file_type=file_info['file_type'],
                # 把 `file_size` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                file_size=file_info['file_size'],
                # 把 `resume_content` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                resume_content=resume_text,
                # 把 `candidate_name` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                candidate_name=ai_result.name,
                # 把 `candidate_position` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                candidate_position=ai_result.position,
                # 把 `candidate_age` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                candidate_age=ai_result.年龄,
                # 把 `candidate_gender` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                candidate_gender=ai_result.sex,
                # 把 `work_years` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                work_years=(self._parse_work_years_to_float(ai_result.workYears) or 0.0),
                # 把 `education_level` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                education_level=ai_result.教育水平,
                # 把 `school` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                school=ai_result.school,
                # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                total_score=ai_result.total_score,
                # 把经过 Pydantic 校验的业务对象转换成可发送给远程服务的 JSON 字典。
                evaluation_metrics=[metric.model_dump() for metric in ai_result.evaluation_metrics],
                # 关联对应的岗位 JD，供评分和后续招聘流程使用。
                job_description_id=job_description_id,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                conversation_id=str(conversation_id) if conversation_id else None,
                # 把 `ai_response` 字段写入当前请求、响应或数据对象，供 `_save_evaluation_result` 后续步骤使用。
                ai_response=raw_response
            # 完成“把简历信息和 AI 评价写入本地数据库”所需的数据封装或下游调用，准备处理其结果。
            )
            # 把新建的 ORM 记录加入数据库会话，等待 commit 正式持久化。
            self.db.add(evaluation)
            # 调用 `await self.db.commit` 执行 `_save_evaluation_result` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.commit()
            # 调用 `await self.db.refresh` 执行 `_save_evaluation_result` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.refresh(evaluation)

            # 把“把简历信息和 AI 评价写入本地数据库”得到的最终业务结果返回给接口层或前端。
            return evaluation

        # 捕获“把简历信息和 AI 评价写入本地数据库”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 调用 `await self.db.rollback` 执行 `_save_evaluation_result` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.rollback()
            # 记录“把简历信息和 AI 评价写入本地数据库”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"保存评价结果失败: {e}")
            # 引入或传递 `raise`，供“把简历信息和 AI 评价写入本地数据库”的后续步骤使用。
            raise

    # 封装“查询当前用户的简历评价历史”流程，供接口层或其他业务服务调用。
    async def get_evaluation_history(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 指定分页查询需要跳过的记录数量
        skip: int = 0,
        # 限制本次查询最多返回多少条记录
        limit: int = 20,
        # 把 `status` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history` 后续步骤使用。
        status: Optional['ResumeStatus'] = None
    # 完成“查询当前用户的简历评价历史”所需参数的定义，下面开始执行具体业务步骤。
    ) -> tuple[List[ResumeEvaluation], int]:
        """获取评价历史和总记录数"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 构建查询条件 - 按创建时间降序排序
            query = select(ResumeEvaluation).where(ResumeEvaluation.user_id == user_id).order_by(ResumeEvaluation.created_at.desc())

            # 添加状态过滤
            if status:
                # 提供本次发送给 Dify 或检索服务的核心提示内容
                query = query.where(ResumeEvaluation.status == status)

            # 获取总记录数
            count_query = select(func.count()).select_from(query.subquery())
            # 调用 `self.db.execute` 执行“查询当前用户的简历评价历史”所需的下游操作，并保存返回结果供下一步使用。
            count_result = await self.db.execute(count_query)
            # 保存符合条件的记录总数，供前端计算分页。
            total = count_result.scalar()

            # 添加分页
            query = query.offset(skip).limit(limit)

            # 调用 `self.db.execute` 执行“查询当前用户的简历评价历史”所需的下游操作，并保存返回结果供下一步使用。
            result = await self.db.execute(query)
            # 把 `evaluations` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history` 后续步骤使用。
            evaluations = result.scalars().all()

            # 添加调试日志
            logger.info(f"查询到 {len(evaluations)} 个评价记录，总记录数: {total}")
            # 根据 `evaluations` 判断当前数据应进入哪条业务分支，保证 `get_evaluation_history` 使用正确处理方式。
            if evaluations:
                # 记录“查询当前用户的简历评价历史”的关键状态或失败原因，方便排查下游服务问题。
                logger.info(f"第一个评价记录类型: {type(evaluations[0])}")
                # 根据 `hasattr(evaluations[0], 'id')` 判断当前数据应进入哪条业务分支，保证 `get_evaluation_history` 使用正确处理方式。
                if hasattr(evaluations[0], 'id'):
                    # 记录“查询当前用户的简历评价历史”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info(f"第一个评价记录ID: {evaluations[0].id}")
                # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `get_evaluation_history` 能继续返回可处理结果。
                else:
                    # 记录“查询当前用户的简历评价历史”的关键状态或失败原因，方便排查下游服务问题。
                    logger.info("第一个评价记录没有id属性")

            # 把“查询当前用户的简历评价历史”得到的最终业务结果返回给接口层或前端。
            return evaluations, total

        # 捕获“查询当前用户的简历评价历史”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“查询当前用户的简历评价历史”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取评价历史失败: {e}", exc_info=True)
            # 把“查询当前用户的简历评价历史”得到的最终业务结果返回给接口层或前端。
            return [], 0

    # 封装“完成 `get evaluation by id` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def get_evaluation_by_id(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `get_evaluation_by_id` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        evaluation_id: UUID,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“完成 `get evaluation by id` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Optional[ResumeEvaluation]:
        """根据ID获取评价结果"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 调用 `self.db.execute` 执行“完成 `get evaluation by id` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
            result = await self.db.execute(
                # 调用 `select` 执行 `get_evaluation_by_id` 的当前步骤，并将产生的数据交给后续流程。
                select(ResumeEvaluation)
                # 继续上方的链式调用或表达式，本行与上方代码共同组成一个结果。
                .where(
                    # 把当前字段纳入 `get_evaluation_by_id` 的请求或响应结构，确保后续步骤获得完整业务数据。
                    ResumeEvaluation.id == evaluation_id,
                    # 把当前字段纳入 `get_evaluation_by_id` 的请求或响应结构，确保后续步骤获得完整业务数据。
                    ResumeEvaluation.user_id == user_id
                # 完成“完成 `get evaluation by id` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                )
            # 完成“完成 `get evaluation by id` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            )
            # 把“完成 `get evaluation by id` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return result.scalar_one_or_none()

        # 捕获“完成 `get evaluation by id` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“完成 `get evaluation by id` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"获取评价结果失败: {e}")
            # 把“完成 `get evaluation by id` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return None

    # 封装“完成 `parse work years to float` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def _parse_work_years_to_float(self, text: Optional[str]) -> Optional[float]:
        """将工作年限字符串解析为数字（年）。支持格式如"3年"、"1.5年"、"1-3年"、"约2年"。
        - 解析到范围时取平均值；
        - 仅提取到一个数值时使用该数值；
        - 解析失败返回None。
        """
        # 确认 `text` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `_parse_work_years_to_float`。
        if not text:
            # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return None
        # 把 `s` 字段写入当前请求、响应或数据对象，供 `_parse_work_years_to_float` 后续步骤使用。
        s = str(text).strip().lower()
        # 常见非数值占位统一视为0（回退到调用处做 or 0.0）
        if s in {"未知", "不详", "none", "null", "n/a", "na", "--", "-", "", "应届", "应届生", "fresh"}:
            # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return 0.0
        # 匹配范围 "a-b" 或 "a – b"
        m_range = re.search(r"(\d+(?:\.\d+)?)\s*[\-~–—]\s*(\d+(?:\.\d+)?)", s)
        # 根据 `m_range` 判断当前数据应进入哪条业务分支，保证 `_parse_work_years_to_float` 使用正确处理方式。
        if m_range:
            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 把 `a` 字段写入当前请求、响应或数据对象，供 `_parse_work_years_to_float` 后续步骤使用。
                a = float(m_range.group(1))
                # 把 `b` 字段写入当前请求、响应或数据对象，供 `_parse_work_years_to_float` 后续步骤使用。
                b = float(m_range.group(2))
                # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return (a + b) / 2.0
            # 捕获“完成 `parse work years to float` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            except Exception:
                # 引入或传递 `pass`，供“完成 `parse work years to float` 对应的业务处理”的后续步骤使用。
                pass
        # 提取第一个数字
        m_single = re.search(r"(\d+(?:\.\d+)?)", s)
        # 根据 `m_single` 判断当前数据应进入哪条业务分支，保证 `_parse_work_years_to_float` 使用正确处理方式。
        if m_single:
            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return float(m_single.group(1))
            # 捕获“完成 `parse work years to float` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            except Exception:
                # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                return None
        # 把“完成 `parse work years to float` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return None

    # 静态方法用于参数验证
    @staticmethod
    # 封装“完成 `validate uuid param` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def validate_uuid_param(param: str, param_name: str) -> UUID:
        """验证UUID格式参数"""
        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 把“完成 `validate uuid param` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return UUID(param)
        # 捕获“完成 `validate uuid param` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 发现无法继续的业务问题，终止“完成 `validate uuid param` 对应的业务处理”并交给上层统一处理。
            raise ValueError(f"无效的{param_name}格式")

    # 把当前字段纳入 `当前业务` 的请求或响应结构，确保后续步骤获得完整业务数据。
    @staticmethod
    # 封装“完成 `validate evaluation params` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def validate_evaluation_params(
        # 把评分或简历评价关联到指定岗位 JD
        job_description_id: str,
        # 可选地延续同一次 Dify 对话上下文
        conversation_id: Optional[str] = None
    # 完成“完成 `validate evaluation params` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Tuple[UUID, Optional[UUID]]:
        """验证评价参数格式"""
        # 验证job_description_id格式
        try:
            # 把外部传入的字符串 ID 转换为 UUID，确保后续查询使用统一标识类型。
            jd_uuid = UUID(job_description_id)
        # 捕获“完成 `validate evaluation params` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 发现无法继续的业务问题，终止“完成 `validate evaluation params` 对应的业务处理”并交给上层统一处理。
            raise ValueError("无效的职位描述ID格式")

        # 验证conversation_id格式（如果提供）
        conv_uuid = None
        # 仅在提供对话 ID 时延续原有上下文，否则按新的独立任务处理。
        if conversation_id:
            # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            try:
                # 把外部传入的字符串 ID 转换为 UUID，确保后续查询使用统一标识类型。
                conv_uuid = UUID(conversation_id)
            # 捕获“完成 `validate evaluation params` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            except ValueError:
                # 发现无法继续的业务问题，终止“完成 `validate evaluation params` 对应的业务处理”并交给上层统一处理。
                raise ValueError("无效的对话ID格式")

        # 把“完成 `validate evaluation params` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return jd_uuid, conv_uuid

    # 把当前字段纳入 `当前业务` 的请求或响应结构，确保后续步骤获得完整业务数据。
    @staticmethod
    # 封装“完成 `validate status param` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def validate_status_param(status: Optional[str]) -> Optional[ResumeStatus]:
        """验证状态参数"""
        # 确认 `status` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `validate_status_param`。
        if not status:
            # 把“完成 `validate status param` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return None

        # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 把“完成 `validate status param` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            return ResumeStatus(status)
        # 捕获“完成 `validate status param` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        except ValueError:
            # 发现无法继续的业务问题，终止“完成 `validate status param` 对应的业务处理”并交给上层统一处理。
            raise ValueError("无效的状态值，支持的状态: pending, rejected, interview")

    # 把当前字段纳入 `当前业务` 的请求或响应结构，确保后续步骤获得完整业务数据。
    @staticmethod
    # 封装“完成 `get supported formats` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def get_supported_formats() -> Dict[str, Any]:
        """获取支持的文件格式"""
        # 把“完成 `get supported formats` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return {
            # 向下游请求或返回结果写入 `supported_extensions` 业务字段。
            "supported_extensions": [".pdf", ".txt", ".doc", ".docx"],
            # 向下游请求或返回结果写入 `max_file_size` 业务字段。
            "max_file_size": "10MB",
            # 向下游请求或返回结果写入 `description` 业务字段。
            "description": "支持PDF、TXT、DOC、DOCX格式的简历文件"
        # 完成“完成 `get supported formats` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        }

    # 封装“完成 `get evaluation history with pagination` 对应的业务处理”流程，供接口层或其他业务服务调用。
    async def get_evaluation_history_with_pagination(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 指定分页查询需要跳过的记录数量
        skip: int = 0,
        # 限制本次查询最多返回多少条记录
        limit: int = 20,
        # 把 `status` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
        status: Optional[ResumeStatus] = None
    # 完成“完成 `get evaluation history with pagination` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
    ) -> ResumeEvaluationListResponse:
        """获取评价历史并返回分页响应"""
        # 调用 `self.get_evaluation_history` 获取 `evaluations, total`，供 `get_evaluation_history_with_pagination` 的下一步业务处理使用。
        evaluations, total = await self.get_evaluation_history(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=user_id,
            # 根据页码计算要跳过的记录数，供数据库分页。
            skip=skip,
            # 限制本次数据库或远程查询返回的记录数。
            limit=limit,
            # 把 `status` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
            status=status
        # 完成“完成 `get evaluation history with pagination` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        )

        # 转换为响应格式
        evaluation_responses = []
        # 逐项处理“完成 `get evaluation history with pagination` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
        for evaluation in evaluations:
            # 保存下游 HTTP 响应，供状态检查和内容解析。
            response = ResumeEvaluationResponse(
                # 把 `id` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                id=evaluation.id,
                # 把 `original_filename` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                original_filename=evaluation.original_filename,
                # 把 `file_type` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                file_type=evaluation.file_type,
                # 把 `resume_content` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                resume_content=evaluation.resume_content,
                # 把 `candidate_name` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                candidate_name=evaluation.candidate_name,
                # 把 `candidate_position` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                candidate_position=evaluation.candidate_position,
                # 把 `candidate_age` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                candidate_age=evaluation.candidate_age,
                # 把 `candidate_gender` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                candidate_gender=evaluation.candidate_gender,
                # 把 `work_years` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                work_years=evaluation.work_years,
                # 把 `education_level` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                education_level=evaluation.education_level,
                # 把 `school` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                school=evaluation.school,
                # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                total_score=evaluation.total_score,
                # 把 `evaluation_metrics` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                evaluation_metrics=evaluation.evaluation_metrics,
                # 关联对应的岗位 JD，供评分和后续招聘流程使用。
                job_description_id=evaluation.job_description_id,
                # 把 `scoring_criteria_id` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                scoring_criteria_id=evaluation.scoring_criteria_id,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=evaluation.user_id,
                # 把 `created_at` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                created_at=evaluation.created_at,
                # 把 `updated_at` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
                updated_at=evaluation.updated_at
            # 完成“完成 `get evaluation history with pagination` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            )
            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
            evaluation_responses.append(response)

        # 计算分页信息
        page = (skip // limit) + 1 if limit > 0 else 1
        # 把 `pages` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
        pages = (total + limit - 1) // limit if limit > 0 else 1
        # 把“完成 `get evaluation history with pagination` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return ResumeEvaluationListResponse(
            # 保存当前页业务记录，供构造分页响应。
            items=evaluation_responses,
            # 保存符合条件的记录总数，供前端计算分页。
            total=total,
            # 保存当前页码，供远程分页查询。
            page=page,
            # 保存每页数量，限制远程接口返回规模。
            size=limit,
            # 把 `pages` 字段写入当前请求、响应或数据对象，供 `get_evaluation_history_with_pagination` 后续步骤使用。
            pages=pages
        # 完成“完成 `get evaluation history with pagination` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        )

    # 封装“查询一条简历评价详情”流程，供接口层或其他业务服务调用。
    async def get_evaluation_detail(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `get_evaluation_detail` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        evaluation_id: UUID,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“查询一条简历评价详情”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Optional[Dict[str, Any]]:
        """获取评价详情并返回扁平化结果"""
        # 调用 `self.get_evaluation_by_id` 执行“查询一条简历评价详情”所需的下游操作，并保存返回结果供下一步使用。
        evaluation = await self.get_evaluation_by_id(
            # 把 `evaluation_id` 字段写入当前请求、响应或数据对象，供 `get_evaluation_detail` 后续步骤使用。
            evaluation_id=evaluation_id,
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            user_id=user_id
        # 完成“查询一条简历评价详情”所需的数据封装或下游调用，准备处理其结果。
        )

        # 确认 `evaluation` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `get_evaluation_detail`。
        if not evaluation:
            # 把“查询一条简历评价详情”得到的最终业务结果返回给接口层或前端。
            return None

        # 获取关联的职位描述信息（保留但不参与返回，避免schema不匹配）
        jd = await self._get_job_description(evaluation.job_description_id, user_id)

        # 构建扁平化的结果，符合 ResumeEvaluationResult schema
        result = {
            # 向下游请求或返回结果写入 `id` 业务字段。
            "id": evaluation.id,
            # 向下游请求或返回结果写入 `evaluation_metrics` 业务字段。
            "evaluation_metrics": evaluation.evaluation_metrics,
            # 向下游请求或返回结果写入 `total_score` 业务字段。
            "total_score": evaluation.total_score,
            # 向下游请求或返回结果写入 `name` 业务字段。
            "name": evaluation.candidate_name,
            # 向下游请求或返回结果写入 `position` 业务字段。
            "position": evaluation.candidate_position,
            # 向下游请求或返回结果写入 `workYears` 业务字段。
            "workYears": evaluation.work_years,
            # 向下游请求或返回结果写入 `education` 业务字段。
            "education": evaluation.education_level,
            # 向下游请求或返回结果写入 `age` 业务字段。
            "age": evaluation.candidate_age,
            # 向下游请求或返回结果写入 `sex` 业务字段。
            "sex": evaluation.candidate_gender,
            # 向下游请求或返回结果写入 `school` 业务字段。
            "school": evaluation.school,
            # 向下游请求或返回结果写入 `resume_content` 业务字段。
            "resume_content": evaluation.resume_content,
            # 向下游请求或返回结果写入 `original_filename` 业务字段。
            "original_filename": evaluation.original_filename,
            # 向下游请求或返回结果写入 `created_at` 业务字段。
            "created_at": evaluation.created_at.isoformat(),
            # 向下游请求或返回结果写入 `updated_at` 业务字段。
            "updated_at": evaluation.updated_at.isoformat()
        # 完成“查询一条简历评价详情”所需的数据封装或下游调用，准备处理其结果。
        }

        # 把“查询一条简历评价详情”得到的最终业务结果返回给接口层或前端。
        return result

    # 封装“删除简历评价及其关联面试方案”流程，供接口层或其他业务服务调用。
    async def delete_evaluation(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `delete_evaluation` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        evaluation_id: UUID,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID
    # 完成“删除简历评价及其关联面试方案”所需参数的定义，下面开始执行具体业务步骤。
    ) -> bool:
        """删除评价记录"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 调用 `self.get_evaluation_by_id` 执行“删除简历评价及其关联面试方案”所需的下游操作，并保存返回结果供下一步使用。
            evaluation = await self.get_evaluation_by_id(
                # 把 `evaluation_id` 字段写入当前请求、响应或数据对象，供 `delete_evaluation` 后续步骤使用。
                evaluation_id=evaluation_id,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“删除简历评价及其关联面试方案”所需的数据封装或下游调用，准备处理其结果。
            )

            # 确认 `evaluation` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `delete_evaluation`。
            if not evaluation:
                # 把“删除简历评价及其关联面试方案”得到的最终业务结果返回给接口层或前端。
                return False

            # 先删除关联的面试计划记录 - 使用远程服务
            try:
                # 调用远程服务根据简历评价ID删除所有关联的面试方案
                result_data = await remote_service_client.delete(
                    # 指定远程 HR 服务的目标业务接口。
                    endpoint=f"/interview-plans/delete-by-evaluation/{evaluation_id}",
                    # 携带当前用户 ID，让下游服务只操作该用户的数据。
                    user_id=user_id
                # 完成“删除简历评价及其关联面试方案”所需的数据封装或下游调用，准备处理其结果。
                )
                # 记录“删除简历评价及其关联面试方案”的关键状态或失败原因，方便排查下游服务问题。
                logger.info(f"成功删除与评价 {evaluation_id} 关联的面试方案")
            # 捕获“删除简历评价及其关联面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
            except Exception as e:
                # 发现无法继续的业务问题，终止“删除简历评价及其关联面试方案”并交给上层统一处理。
                raise e
                # 记录“删除简历评价及其关联面试方案”的关键状态或失败原因，方便排查下游服务问题。
                logger.exception(f"删除关联面试方案失败: {e}")

            # 再删除评价记录
            await self.db.delete(evaluation)
            # 调用 `await self.db.commit` 执行 `delete_evaluation` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.commit()

            # 把“删除简历评价及其关联面试方案”得到的最终业务结果返回给接口层或前端。
            return True

        # 捕获“删除简历评价及其关联面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“删除简历评价及其关联面试方案”的关键状态或失败原因，方便排查下游服务问题。
            logger.exception(f"删除评价记录失败: {e}")
            # 调用 `await self.db.rollback` 执行 `delete_evaluation` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.rollback()
            # 引入或传递 `raise`，供“删除简历评价及其关联面试方案”的后续步骤使用。
            raise

    # 封装“更新候选人的招聘处理状态”流程，供接口层或其他业务服务调用。
    async def update_evaluation_status(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 为 `update_evaluation_status` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        evaluation_id: UUID,
        # 限定本次操作所属用户，防止访问其他用户的数据
        user_id: UUID,
        # 为 `update_evaluation_status` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        new_status: ResumeStatus
    # 完成“更新候选人的招聘处理状态”所需参数的定义，下面开始执行具体业务步骤。
    ) -> Optional[Dict[str, Any]]:
        """更新评价状态"""
        # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        try:
            # 调用 `self.get_evaluation_by_id` 执行“更新候选人的招聘处理状态”所需的下游操作，并保存返回结果供下一步使用。
            evaluation = await self.get_evaluation_by_id(
                # 把 `evaluation_id` 字段写入当前请求、响应或数据对象，供 `update_evaluation_status` 后续步骤使用。
                evaluation_id=evaluation_id,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                user_id=user_id
            # 完成“更新候选人的招聘处理状态”所需的数据封装或下游调用，准备处理其结果。
            )

            # 确认 `evaluation` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `update_evaluation_status`。
            if not evaluation:
                # 把“更新候选人的招聘处理状态”得到的最终业务结果返回给接口层或前端。
                return None

            # 更新状态
            evaluation.status = new_status
            # 调用 `await self.db.commit` 执行 `update_evaluation_status` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.commit()

            # 把“更新候选人的招聘处理状态”得到的最终业务结果返回给接口层或前端。
            return {
                # 向下游请求或返回结果写入 `message` 业务字段。
                "message": "状态更新成功",
                # 向下游请求或返回结果写入 `evaluation_id` 业务字段。
                "evaluation_id": str(evaluation.id),
                # 向下游请求或返回结果写入 `status` 业务字段。
                "status": new_status.value
            # 完成“更新候选人的招聘处理状态”所需的数据封装或下游调用，准备处理其结果。
            }

        # 捕获“更新候选人的招聘处理状态”中的失败，记录原因或转换成上层能够识别的业务错误。
        except Exception as e:
            # 记录“更新候选人的招聘处理状态”的关键状态或失败原因，方便排查下游服务问题。
            logger.error(f"更新简历状态失败: {e}")
            # 调用 `await self.db.rollback` 执行 `update_evaluation_status` 的当前步骤，并将产生的数据交给后续流程。
            await self.db.rollback()
            # 引入或传递 `raise`，供“更新候选人的招聘处理状态”的后续步骤使用。
            raise


    # 封装“完成 `build evaluation prompt` 对应的业务处理”流程，供接口层或其他业务服务调用。
    def _build_evaluation_prompt(
        # `self` 代表当前类实例，调用实例方法时 Python 会自动传入它。
        self,
        # 提供本岗位采用的具体评分规则
        evaluation_model: str,
        # 提供候选人应聘岗位的 JD 信息
        jd_info: JobDescriptionResponse,
        # 提供已解析的简历正文供 AI 分析
        resume_text: str,
        # 把 `candidate_name` 字段写入当前请求、响应或数据对象，供 `_build_evaluation_prompt` 后续步骤使用。
        candidate_name: Optional[str] = None
    # 完成“完成 `build evaluation prompt` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
    ) -> str:
        """构建评价提示词"""
        # 把 `base_prompt` 字段写入当前请求、响应或数据对象，供 `_build_evaluation_prompt` 后续步骤使用。
        base_prompt = f"""
        {evaluation_model}

        职位信息：
        - 职位名称: {jd_info.title}
        - 部门: {jd_info.department or "未知部门"}
        - 职位要求: {jd_info.requirements}
        - 技能要求: {jd_info.skills}
        - 教育要求: {jd_info.education}
        - 工作经验要求: {jd_info.experience_level or "不限"}

        简历内容：
        """

        # 根据 `candidate_name` 判断当前数据应进入哪条业务分支，保证 `_build_evaluation_prompt` 使用正确处理方式。
        if candidate_name:
            # 为 `_build_evaluation_prompt` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            base_prompt += f"候选人：{candidate_name}\n"

        # 为 `_build_evaluation_prompt` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
        base_prompt += f"简历内容：\n{resume_text}\n\n请严格按照JSON格式返回评价结果。"

        # 把“完成 `build evaluation prompt` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        return base_prompt
