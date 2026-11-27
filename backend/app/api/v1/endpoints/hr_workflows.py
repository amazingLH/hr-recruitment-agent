"""
* 文件作用
  * 这是 HR AI 工作流的 FastAPI 接口层，统一接收 JD、简历评分、面试和智能培训请求。智能培训部分由 parse_exam_intent() 把自然语言要求解析为试卷配置，generate_exam() 把配置交给 ExamService 并用 StreamingResponse 返回 Dify 生成内容，submit_exam() 把考生答案送入自动评分流程；接口层负责 JWT 鉴权、参数解析、HTTP 异常和流式响应，不直接操作试卷表。
* 类说明
  * 本文件没有定义业务类，使用模块级 FastAPI 接口函数组织功能。
* 函数与方法说明
  * `parse_requirements()`：将用户自然语言需求解析为结构化字段，供前端表单自动填充 示例输入："JAVA开发工程师、3-5年工作经验、工作地点北京，薪资15000-20000" 返回JSON字段：job_title, location, salary, experience, education, job_type, skills, benefits, department, additional_requirements。
  * `generate_job_description()`：生成岗位JD (Job Description) 工作流类型: type=1。
  * `generate_job_description.generate_stream()`：JD 生成接口的异步生成器，持续读取 Dify type=1 返回的文本分片并原样 yield 给 StreamingResponse，使前端可以边生成边展示岗位描述。
  * `generate_scoring_criteria()`：生成简历评分标准 基于JD内容生成对应的简历评分标准 工作流类型: type=2。
  * `generate_scoring_criteria.generate_stream()`：评分标准生成接口的异步生成器，持续转发 Dify type=2 的文本分片，并在工作流结束时关闭流式响应。
  * `evaluate_resume()`：上传简历并进行AI评价 工作流类型: type=3。
  * `generate_interview_plan_by_resume()`：根据简历ID生成面试方案 工作流类型: type=4。
  * `generate_interview_plan_by_resume.generate_stream()`：面试方案接口的异步生成器，持续转发 Dify type=4 根据简历生成的方案内容，供前端实时拼接显示。
  * `parse_exam_intent()`：自然语言试卷需求解析接口，使用 Dify workflow_type=5 提取标题、科目、难度、时长、总分和题量；模型结果缺字段时再用正则表达式与默认值补齐，并返回前端可使用的结构化配置。
  * `parse_exam_intent.find_int()`：使用传入的正则表达式从自然语言中提取第一个整数，匹配失败或转换失败时返回 None，用于补齐总分、时长和题量。
  * `parse_exam_intent.extract_named_title()`：从“名称、标题、试卷名”等中文表达中提取试卷标题，并清理多余标点；Dify 没有返回标题时作为规则兜底。
  * `parse_exam_intent.extract_subject()`：从自然语言中的科目、主题或岗位关键词提取考试科目，无法识别时返回默认科目，供试卷生成 Prompt 使用。
  * `parse_exam_intent.extract_question_count()`：根据单选、多选、简答等题型关键词和正则表达式提取对应题数，把自然语言数量转换为 question_counts 字段。
  * `parse_exam_intent.qc_val()`：从 Dify 返回的 question_counts 中兼容读取多个同义字段名，并把有效值转换为非负整数。
  * `generate_exam()`：试卷生成入口，把当前用户、试卷配置和所选知识库文档交给 ExamService.generate_exam()；stream=true 时用 StreamingResponse 以 text/event-stream 返回内容，非流式时返回完整结果。
  * `submit_exam()`：考试提交入口，把 exam_id、考生信息、答案和试卷内容交给 ExamService.submit_exam()，由服务完成标准答案读取、逐题评分、总分计算和 exam_results 落库。
"""

# 从 `typing` 导入 `Any, Optional, Dict, List`，供本文件调用。
from typing import Any, Optional, Dict, List
# 从 `uuid` 导入 `UUID`，供本文件调用。
from uuid import UUID
# 从 `fastapi` 导入 `APIRouter, Depends, HTTPException, status, Form, Query, UploadFile, File`，供本文件调用。
from fastapi import APIRouter, Depends, HTTPException, status, Form, Query, UploadFile, File
# 从 `fastapi.responses` 导入 `StreamingResponse`，供本文件调用。
from fastapi.responses import StreamingResponse
# 从 `sqlalchemy.ext.asyncio` 导入 `AsyncSession`，供本文件调用。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy` 导入 `select`，供本文件调用。
from sqlalchemy import select
# 导入 `json` 模块，供后续代码使用。
import json
# 导入 `time` 模块，供后续代码使用。
import time
# 导入 `logging` 模块，供后续代码使用。
import logging

# 从 `app.core.database` 导入 `get_db`，供本文件调用。
from app.core.database import get_db
# 从 `app.schemas.user` 导入 `User as UserSchema`，供本文件调用。
from app.schemas.user import User as UserSchema
# 从 `app.models.user` 导入 `User`，供本文件调用。
from app.models.user import User
# 从 `app.models.resume_evaluation` 导入 `ResumeEvaluation`，供本文件调用。
from app.models.resume_evaluation import ResumeEvaluation
# 从 `app.services.dify_service` 导入 `DifyService`，供本文件调用。
from app.services.dify_service import DifyService
# 从 `app.services.resume_evaluation_service` 导入 `ResumeEvaluationService`，供本文件调用。
from app.services.resume_evaluation_service import ResumeEvaluationService
# 从 `app.services.job_description_service` 导入 `JobDescriptionService`，供本文件调用。
from app.services.job_description_service import JobDescriptionService
# 从 `app.schemas.resume_evaluation` 导入 `ResumeEvaluationResult`，供本文件调用。
from app.schemas.resume_evaluation import ResumeEvaluationResult
# 从 `app.api.deps` 导入 `get_current_user`，供本文件调用。
from app.api.deps import get_current_user
# 从 `app.core.logging` 导入 `logger`，供本文件调用。
from app.core.logging import logger
# 从 `app.schemas.job_description` 导入 `JDGenerateRequest`，供本文件调用。
from app.schemas.job_description import JDGenerateRequest
# 从 `app.schemas.scoring_criteria` 导入 `ScoringCriteriaGenerateRequest`，供本文件调用。
from app.schemas.scoring_criteria import ScoringCriteriaGenerateRequest
# 从 `app.schemas.exam` 导入 `ExamGenerateRequest, ExamSubmitRequest`，供本文件调用。
from app.schemas.exam import ExamGenerateRequest, ExamSubmitRequest
# 从 `app.services.kb_selection_service` 导入 `KBSelectionService`，供本文件调用。
from app.services.kb_selection_service import KBSelectionService
# 从 `app.schemas.intent` 导入 `RequirementParseRequest, RequirementParseResponse, ExamIntentParseRequest, KnowledgeFileInfo, ExamIntentParseResponse`，供本文件调用。
from app.schemas.intent import RequirementParseRequest, RequirementParseResponse, ExamIntentParseRequest, KnowledgeFileInfo, ExamIntentParseResponse

# 创建当前模块的 FastAPI 路由容器，供上层统一挂载。
# 将 `APIRouter()` 的结果保存到 `router`，供后续流程使用。
router = APIRouter()

# 从 `pydantic` 导入 `BaseModel`，供本文件调用。
from pydantic import BaseModel

# 对外提供 POST `/parse-requirements` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/parse-requirements")
# 封装“把工作台自然语言整理成 JD 表单字段”流程，供接口层或其他业务服务调用。
# 定义 `parse_requirements()`，实现该函数对应的接口或业务流程。
async def parse_requirements(
    # 接收并使用前端已经校验的业务请求数据（request数据类型是RequirementParseRequest，它是从其他文件导入的：from app.schemas.intent import RequirementParseRequest）。
        # 定义在 backend/app/schemas/intent.py，是一个 Pydantic 请求模型，用来校验前端提交的数据，例如：{"text": "生成一个3年前端工程师JD"}，
        # FastAPI 会自动把前端 JSON 转换为 RequirementParseRequest 对象，因此后端可以通过：request.text取得用户输入的需求文本。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `RequirementParseRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `RequirementParseRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `RequirementParseRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `RequirementParseRequest`。
    request: RequirementParseRequest,
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“把工作台自然语言整理成 JD 表单字段”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    将用户自然语言需求解析为结构化字段，供前端表单自动填充
    示例输入："JAVA开发工程师、3-5年工作经验、工作地点北京，薪资15000-20000"
    返回JSON字段：job_title, location, salary, experience, education, job_type, skills, benefits, department, additional_requirements
    """
    # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 创建 Dify 客户端，准备执行 AI 工作流。
        # 将 `DifyService()` 的结果保存到 `dify_service`，供后续流程使用。
        dify_service = DifyService()
        # 保存发给 AI 的任务说明、输入数据和输出格式要求。
        # 将 `(` 的结果保存到 `prompt`，供后续流程使用。
        prompt = (
            # 向 `parse_requirements` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "你是一个招聘助手。请从以下中文需求中提取结构化字段，并严格以JSON格式返回。\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "不要添加解释，不要返回除JSON外的任何内容。\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "需求文本：\n" + request.text + "\n\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "JSON字段定义：{\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"job_title\": 岗位名称（如JAVA开发工程师、财务经理），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"department\": 部门（如技术部、财务部，若无法判断可为空），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"location\": 工作地点（城市名），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"salary\": 薪资范围（原样返回，如15000-20000或25-35K），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"experience\": 工作经验（如3-5年、5年以上），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"education\": 学历要求（如本科、专科，若未提及可为空），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"job_type\": 工作性质（如全职、兼职，若未提及可为空），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"skills\": 技能标签数组（如[\"Java\", \"Spring\"]），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"benefits\": 福利数组（如[\"五险一金\", \"带薪年假\"]），\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"additional_requirements\": 其他补充要求（原文提炼）。\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "}\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "示例返回：{\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"job_title\": \"JAVA开发工程师\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"department\": \"技术部\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"location\": \"北京\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"salary\": \"15000-20000\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"experience\": \"3-5年\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"education\": \"本科\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"job_type\": \"全职\",\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"skills\": [\"Java\", \"Spring\", \"MySQL\"],\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"benefits\": [\"五险一金\", \"带薪年假\"],\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"additional_requirements\": \"具备良好的沟通能力\"\n"
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "}"
        # 完成“把工作台自然语言整理成 JD 表单字段”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 调用 `dify_service.call_workflow_sync` 执行“把工作台自然语言整理成 JD 表单字段”所需的下游操作，并保存返回结果供下一步使用。
        # 将 `await dify_service.call_workflow_sync(` 的结果保存到 `ai_response`，供后续流程使用。
        ai_response = await dify_service.call_workflow_sync(
            # 把任务类型传给 Dify，使工作流进入正确的业务分支。
            # 将 `1` 的结果保存到 `workflow_type`，供后续流程使用。
            workflow_type=1,
            # 保存整理后的完整提示词，供下游 Dify 工作流使用。
            # 将 `prompt` 的结果保存到 `query`，供后续流程使用。
            query=prompt,
            # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
            # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
            conversation_id=request.conversation_id,
            # 保存 Dify 工作流需要的结构化输入字段。
            # 将 `{"task": "parse_requirements"}` 的结果保存到 `additional_inputs`，供后续流程使用。
            additional_inputs={"task": "parse_requirements"}
        # 完成“把工作台自然语言整理成 JD 表单字段”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
        # 将 `""` 的结果保存到 `answer_text`，供后续流程使用。
        answer_text = ""
        # 确认 Dify 返回的是字典结构，避免后续按字段读取时发生类型错误。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        if isinstance(ai_response, dict):
            # 优先检查 Dify 标准响应中是否存在 `answer` 正文字段。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            if "answer" in ai_response:
                # 从 Dify 标准 `answer` 字段取得生成正文，供后续结构化解析。
                # 将 `ai_response["answer"]` 的结果保存到 `answer_text`，供后续流程使用。
                answer_text = ai_response["answer"]
            # 兼容另一种 Dify 响应结构：正文被包装在 `data.answer` 中。
            # 判断 `elif "data" in ai_response and isinstance(ai_response["data"], dict) and "answer" in ai_response["data"]`，决定是否执行当前分支。
            # 判断 `elif "data" in ai_response and isinstance(ai_response["data"], dict) and "answer" in ai_response["data"]`，决定是否执行当前分支。
            # 判断 `elif "data" in ai_response and isinstance(ai_response["data"], dict) and "answer" in ai_response["data"]`，决定是否执行当前分支。
            elif "data" in ai_response and isinstance(ai_response["data"], dict) and "answer" in ai_response["data"]:
                # 从兼容结构 `data.answer` 中取得 Dify 正文，保证不同返回格式都能处理。
                # 将 `ai_response["data"]["answer"]` 的结果保存到 `answer_text`，供后续流程使用。
                answer_text = ai_response["data"]["answer"]
            # Dify 返回了字典但没有标准 answer 字段时，将完整字典保留下来尝试兜底解析。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 找不到标准正文时把完整响应转成文本，给后续兜底解析保留原始信息。
                # 将 `json.dumps(ai_response, ensure_ascii=False)` 的结果保存到 `answer_text`，供后续流程使用。
                answer_text = json.dumps(ai_response, ensure_ascii=False)
        # Dify 返回的不是字典时直接转成文本，使后续代码仍能统一执行字符串清洗。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
            # 将 `str(ai_response)` 的结果保存到 `answer_text`，供后续流程使用。
            answer_text = str(ai_response)

        # 去掉 AI 正文首尾空白，准备识别 Markdown 代码块并解析 JSON。
        # 将 `answer_text.strip()` 的结果保存到 `json_str`，供后续流程使用。
        json_str = answer_text.strip()
        # 检查 AI 正文是否包含 Markdown 代码块；如果包含就先截取代码块，避免 JSON 解析失败。
        # 判断 `if "```" in json_str`，决定是否执行当前分支。
        # 判断 `if "```" in json_str`，决定是否执行当前分支。
        # 判断 `if "```" in json_str`，决定是否执行当前分支。
        if "```" in json_str:
            # 判断代码块是否明确标记为 JSON，以便跳过正确长度的开头标记。
            # 判断 `if "```json" in json_str`，决定是否执行当前分支。
            # 判断 `if "```json" in json_str`，决定是否执行当前分支。
            # 判断 `if "```json" in json_str`，决定是否执行当前分支。
            if "```json" in json_str:
                # 记录 JSON 代码块正文的起始位置，便于从 AI 回复中准确截取结构化内容。
                # 将 `json_str.find("```json") + 7` 的结果保存到 `start`，供后续流程使用。
                start = json_str.find("```json") + 7
            # 代码块没有 json 语言标记时，按普通 Markdown 代码块的位置截取正文。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 记录 JSON 代码块正文的起始位置，便于从 AI 回复中准确截取结构化内容。
                # 将 `json_str.find("```") + 3` 的结果保存到 `start`，供后续流程使用。
                start = json_str.find("```") + 3
            # 记录 JSON 代码块的结束位置，防止把后续说明文字交给 JSON 解析器。
            # 将 `json_str.find("```", start)` 的结果保存到 `end`，供后续流程使用。
            end = json_str.find("```", start)
            # 只有找到有效结束标记时才截取，避免错误位置产生空字符串或残缺 JSON。
            # 判断 `if end > start`，决定是否执行当前分支。
            # 判断 `if end > start`，决定是否执行当前分支。
            # 判断 `if end > start`，决定是否执行当前分支。
            if end > start:
                # 去掉 AI 正文首尾空白，准备识别 Markdown 代码块并解析 JSON。
                # 将 `json_str[start:end].strip()` 的结果保存到 `json_str`，供后续流程使用。
                json_str = json_str[start:end].strip()

        # 初始化结构化解析结果；后续会填入从 AI 正文提取出的字段。
        # 声明 `parsed` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        parsed: Dict[str, Any] = {}
        # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 把 AI 返回的 JSON 文本解析成 Python 字典，供评分字段校验和保存。
            # 将 `json.loads(json_str)` 的结果保存到 `parsed`，供后续流程使用。
            parsed = json.loads(json_str)
        # 捕获“把工作台自然语言整理成 JD 表单字段”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception:
            # 导入 `re` 模块，供后续代码使用。
            import re
            # 把 `text` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            text = request.text
            # 保存解析后的 AI 结果字典，供 Pydantic 校验并返回前端。
            # 将 `{` 的结果保存到 `parsed`，供后续流程使用。
            parsed = {
                # 向下游请求或返回结果写入 `job_title` 业务字段。
                # 设置 `job_title` 字段，值来自 `None`。
                "job_title": None,
                # 向下游请求或返回结果写入 `department` 业务字段。
                # 设置 `department` 字段，值来自 `None`。
                "department": None,
                # 向下游请求或返回结果写入 `location` 业务字段。
                # 设置 `location` 字段，值来自 `None`。
                "location": None,
                # 向下游请求或返回结果写入 `salary` 业务字段。
                # 设置 `salary` 字段，值来自 `None`。
                "salary": None,
                # 向下游请求或返回结果写入 `experience` 业务字段。
                # 设置 `experience` 字段，值来自 `None`。
                "experience": None,
                # 向下游请求或返回结果写入 `education` 业务字段。
                # 设置 `education` 字段，值来自 `None`。
                "education": None,
                # 向下游请求或返回结果写入 `job_type` 业务字段。
                # 设置 `job_type` 字段，值来自 `None`。
                "job_type": None,
                # 向下游请求或返回结果写入 `skills` 业务字段。
                # 设置 `skills` 字段，值来自 `[]`。
                "skills": [],
                # 向下游请求或返回结果写入 `benefits` 业务字段。
                # 设置 `benefits` 字段，值来自 `[]`。
                "benefits": [],
                # 向下游请求或返回结果写入 `additional_requirements` 业务字段。
                # 设置 `additional_requirements` 字段，值来自 `text`。
                "additional_requirements": text
            # 完成“把工作台自然语言整理成 JD 表单字段”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            }
            # 把 `title_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            title_match = re.search(r"([A-Za-z]+开发工程师|[\u4e00-\u9fa5A-Za-z]+经理|[\u4e00-\u9fa5A-Za-z]+工程师)", text)
            # 根据 `title_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if title_match`，决定是否执行当前分支。
            # 判断 `if title_match`，决定是否执行当前分支。
            # 判断 `if title_match`，决定是否执行当前分支。
            if title_match:
                # 把当前结果写入 `parsed["job_title"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `title_match.group(1)` 的结果保存到 `parsed["job_title"]`，供后续流程使用。
                # 将 `title_match.group(1)` 的结果保存到 `parsed["job_title"]`，供后续流程使用。
                # 将 `title_match.group(1)` 的结果保存到 `parsed["job_title"]`，供后续流程使用。
                parsed["job_title"] = title_match.group(1)
            # 把 `exp_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            exp_match = re.search(r"(\d+\s*-\s*\d+年|\d+年以上)", text)
            # 根据 `exp_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if exp_match`，决定是否执行当前分支。
            # 判断 `if exp_match`，决定是否执行当前分支。
            # 判断 `if exp_match`，决定是否执行当前分支。
            if exp_match:
                # 把当前结果写入 `parsed["experience"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `exp_match.group(1).replace(" ", "")` 的结果保存到 `parsed["experience"]`，供后续流程使用。
                # 将 `exp_match.group(1).replace(" ", "")` 的结果保存到 `parsed["experience"]`，供后续流程使用。
                # 将 `exp_match.group(1).replace(" ", "")` 的结果保存到 `parsed["experience"]`，供后续流程使用。
                parsed["experience"] = exp_match.group(1).replace(" ", "")
            # 把 `loc_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            loc_match = re.search(r"北京|上海|深圳|广州|杭州|南京|成都|重庆|苏州|武汉|西安", text)
            # 根据 `loc_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if loc_match`，决定是否执行当前分支。
            # 判断 `if loc_match`，决定是否执行当前分支。
            # 判断 `if loc_match`，决定是否执行当前分支。
            if loc_match:
                # 把当前结果写入 `parsed["location"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `loc_match.group(0)` 的结果保存到 `parsed["location"]`，供后续流程使用。
                # 将 `loc_match.group(0)` 的结果保存到 `parsed["location"]`，供后续流程使用。
                # 将 `loc_match.group(0)` 的结果保存到 `parsed["location"]`，供后续流程使用。
                parsed["location"] = loc_match.group(0)
            # 把 `sal_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            sal_match = re.search(r"(\d+\s*-\s*\d+K|\d+\s*-\s*\d+|\d+K\s*-\s*\d+K)", text, re.IGNORECASE)
            # 根据 `sal_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if sal_match`，决定是否执行当前分支。
            # 判断 `if sal_match`，决定是否执行当前分支。
            # 判断 `if sal_match`，决定是否执行当前分支。
            if sal_match:
                # 把当前结果写入 `parsed["salary"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `sal_match.group(1).replace(" ", "")` 的结果保存到 `parsed["salary"]`，供后续流程使用。
                # 将 `sal_match.group(1).replace(" ", "")` 的结果保存到 `parsed["salary"]`，供后续流程使用。
                # 将 `sal_match.group(1).replace(" ", "")` 的结果保存到 `parsed["salary"]`，供后续流程使用。
                parsed["salary"] = sal_match.group(1).replace(" ", "")
            # 把 `edu_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            edu_match = re.search(r"本科|专科|硕士|博士", text)
            # 根据 `edu_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if edu_match`，决定是否执行当前分支。
            # 判断 `if edu_match`，决定是否执行当前分支。
            # 判断 `if edu_match`，决定是否执行当前分支。
            if edu_match:
                # 把当前结果写入 `parsed["education"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `edu_match.group(0)` 的结果保存到 `parsed["education"]`，供后续流程使用。
                # 将 `edu_match.group(0)` 的结果保存到 `parsed["education"]`，供后续流程使用。
                # 将 `edu_match.group(0)` 的结果保存到 `parsed["education"]`，供后续流程使用。
                parsed["education"] = edu_match.group(0)
            # 把 `jobtype_match` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            jobtype_match = re.search(r"全职|兼职|实习", text)
            # 根据 `jobtype_match` 判断当前数据应进入哪条业务分支，保证 `parse_requirements` 使用正确处理方式。
            # 判断 `if jobtype_match`，决定是否执行当前分支。
            # 判断 `if jobtype_match`，决定是否执行当前分支。
            # 判断 `if jobtype_match`，决定是否执行当前分支。
            if jobtype_match:
                # 把当前结果写入 `parsed["job_type"]`，补充后续下游调用或响应需要的业务字段。
                # 将 `jobtype_match.group(0)` 的结果保存到 `parsed["job_type"]`，供后续流程使用。
                # 将 `jobtype_match.group(0)` 的结果保存到 `parsed["job_type"]`，供后续流程使用。
                # 将 `jobtype_match.group(0)` 的结果保存到 `parsed["job_type"]`，供后续流程使用。
                parsed["job_type"] = jobtype_match.group(0)

        # 保存当前服务调用结果，供接口返回或继续处理。
        # 将 `RequirementParseResponse(` 的结果保存到 `result`，供后续流程使用。
        result = RequirementParseResponse(
            # 把 `job_title` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            job_title=parsed.get("job_title"),
            # 把 `department` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            department=parsed.get("department"),
            # 把 `location` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            location=parsed.get("location"),
            # 把 `salary` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            salary=parsed.get("salary"),
            # 把 `experience` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            experience=parsed.get("experience"),
            # 把 `education` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            education=parsed.get("education"),
            # 把 `job_type` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            job_type=parsed.get("job_type"),
            # 把 `skills` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            skills=parsed.get("skills") or [],
            # 把 `benefits` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            benefits=parsed.get("benefits") or [],
            # 把 `additional_requirements` 字段写入当前请求、响应或数据对象，供 `parse_requirements` 后续步骤使用。
            additional_requirements=parsed.get("additional_requirements")
        # 完成“把工作台自然语言整理成 JD 表单字段”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        # 把“把工作台自然语言整理成 JD 表单字段”得到的最终业务结果返回给接口层或前端。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result.model_dump()
    # 捕获“把工作台自然语言整理成 JD 表单字段”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“把工作台自然语言整理成 JD 表单字段”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error parsing requirements: {str(e)}")
        # 终止“把工作台自然语言整理成 JD 表单字段”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"解析需求失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"解析需求失败: {str(e)}"
        # 完成“把工作台自然语言整理成 JD 表单字段”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

# 对外提供 POST `/generate-jd` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/generate-jd")
# 封装“根据岗位要求生成 JD”流程，供接口层或其他业务服务调用。
# 定义 `generate_job_description()`，实现该函数对应的接口或业务流程。
async def generate_job_description(
    # 接收并使用前端已经校验的业务请求数据
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `JDGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `JDGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `JDGenerateRequest`。
    request: JDGenerateRequest,
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“根据岗位要求生成 JD”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    生成岗位JD (Job Description)
    工作流类型: type=1
    """
    # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 创建 Dify 客户端，准备执行 AI 工作流。
        # 将 `DifyService()` 的结果保存到 `dify_service`，供后续流程使用。
        dify_service = DifyService()
        
        # 构建查询内容
        # 将 `[f"请基于给定要求，生成岗位JD。要求如下：{request.requirements}"]` 的结果保存到 `query_parts`，供后续流程使用。
        query_parts = [f"请基于给定要求，生成岗位JD。要求如下：{request.requirements}"]
        
        # 根据 `request.position_title` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        if request.position_title:
            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
            # 调用 `query_parts.append()`，保存或创建当前数据。
            query_parts.append(f"岗位名称：{request.position_title}")
        # 根据 `request.department` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.department`，决定是否执行当前分支。
        # 判断 `if request.department`，决定是否执行当前分支。
        # 判断 `if request.department`，决定是否执行当前分支。
        if request.department:
            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
            # 调用 `query_parts.append()`，保存或创建当前数据。
            query_parts.append(f"部门：{request.department}")
        # 根据 `request.experience_level` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        if request.experience_level:
            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
            # 调用 `query_parts.append()`，保存或创建当前数据。
            query_parts.append(f"经验要求：{request.experience_level}")
        
        # 保存整理后的完整提示词，供下游 Dify 工作流使用。
        # 将 `"\n".join(query_parts)` 的结果保存到 `query`，供后续流程使用。
        query = "\n".join(query_parts)
        
        # 额外输入参数
        # 将 `{}` 的结果保存到 `additional_inputs`，供后续流程使用。
        additional_inputs = {}
        # 根据 `request.position_title` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        # 判断 `if request.position_title`，决定是否执行当前分支。
        if request.position_title:
            # 把 `"position_title"` 对应的岗位信息加入 Dify 结构化输入，使工作流获得完整业务条件。
            # 将 `request.position_title` 的结果保存到 `additional_inputs["position_title"]`，供后续流程使用。
            # 将 `request.position_title` 的结果保存到 `additional_inputs["position_title"]`，供后续流程使用。
            # 将 `request.position_title` 的结果保存到 `additional_inputs["position_title"]`，供后续流程使用。
            additional_inputs["position_title"] = request.position_title
        # 根据 `request.department` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.department`，决定是否执行当前分支。
        # 判断 `if request.department`，决定是否执行当前分支。
        # 判断 `if request.department`，决定是否执行当前分支。
        if request.department:
            # 把 `"department"` 对应的岗位信息加入 Dify 结构化输入，使工作流获得完整业务条件。
            # 将 `request.department` 的结果保存到 `additional_inputs["department"]`，供后续流程使用。
            # 将 `request.department` 的结果保存到 `additional_inputs["department"]`，供后续流程使用。
            # 将 `request.department` 的结果保存到 `additional_inputs["department"]`，供后续流程使用。
            additional_inputs["department"] = request.department
        # 根据 `request.experience_level` 判断当前数据应进入哪条业务分支，保证 `generate_job_description` 使用正确处理方式。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        # 判断 `if request.experience_level`，决定是否执行当前分支。
        if request.experience_level:
            # 把 `"experience_level"` 对应的岗位信息加入 Dify 结构化输入，使工作流获得完整业务条件。
            # 将 `request.experience_level` 的结果保存到 `additional_inputs["experience_level"]`，供后续流程使用。
            # 将 `request.experience_level` 的结果保存到 `additional_inputs["experience_level"]`，供后续流程使用。
            # 将 `request.experience_level` 的结果保存到 `additional_inputs["experience_level"]`，供后续流程使用。
            additional_inputs["experience_level"] = request.experience_level
        
        # 根据前端的流式开关选择实时输出或一次性返回。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        if request.stream:
            # 流式响应
            # 定义 `generate_stream()`，实现该函数对应的接口或业务流程。
            async def generate_stream():
                # 持续接收 Dify 生成分片，并在每个分片到达后立即进入转发处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                async for chunk in dify_service.call_workflow_stream(
                    # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                    # 将 `1` 的结果保存到 `workflow_type`，供后续流程使用。
                    workflow_type=1,
                    # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                    # 将 `query` 的结果保存到 `query`，供后续流程使用。
                    query=query,
                    # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                    # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                    conversation_id=request.conversation_id,
                    # 保存 Dify 工作流需要的结构化输入字段。
                    # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                    additional_inputs=additional_inputs
                # 完成“完成 `generate stream` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
                # 结束上方多行参数或数据结构。
                ):
                    # 把当前 Dify 内容分片立即推送给前端，实现边生成边展示。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    yield f"data: {chunk}\n\n"
                # 通知前端本次 AI 流式生成已经结束，可以停止读取响应。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                yield "data: [DONE]\n\n"
            
            # 把“根据岗位要求生成 JD”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return StreamingResponse(
                # 调用 `generate_stream` 执行 `generate_job_description` 的当前步骤，并将产生的数据交给后续流程。
                # 调用 `generate_stream()`，完成当前函数调用并取得其返回结果。
                generate_stream(),
                # 声明流式响应类型，使浏览器按照连续数据流读取。
                # 将 `"text/plain"` 的结果保存到 `media_type`，供后续流程使用。
                media_type="text/plain",
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                # 将 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 的结果保存到 `headers`，供后续流程使用。
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            # 完成“根据岗位要求生成 JD”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `generate_job_description` 能继续返回可处理结果。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 同步响应
            # 将 `await dify_service.call_workflow_sync(` 的结果保存到 `result`，供后续流程使用。
            result = await dify_service.call_workflow_sync(
                # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                # 将 `1` 的结果保存到 `workflow_type`，供后续流程使用。
                workflow_type=1,
                # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                # 将 `query` 的结果保存到 `query`，供后续流程使用。
                query=query,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                conversation_id=request.conversation_id,
                # 保存 Dify 工作流需要的结构化输入字段。
                # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                additional_inputs=additional_inputs
            # 完成“根据岗位要求生成 JD”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
            # 把“根据岗位要求生成 JD”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return result
            
    # 捕获“根据岗位要求生成 JD”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“根据岗位要求生成 JD”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"生成JD失败: {str(e)}")
        # 终止“根据岗位要求生成 JD”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"生成JD失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"生成JD失败: {str(e)}"
        # 完成“根据岗位要求生成 JD”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )


# 对外提供 POST `/generate-scoring-criteria` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/generate-scoring-criteria")
# 封装“根据 JD 生成简历评分标准”流程，供接口层或其他业务服务调用。
# 定义 `generate_scoring_criteria()`，实现该函数对应的接口或业务流程。
async def generate_scoring_criteria(
    # 接收并使用前端已经校验的业务请求数据
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ScoringCriteriaGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ScoringCriteriaGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ScoringCriteriaGenerateRequest`。
    request: ScoringCriteriaGenerateRequest,
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“根据 JD 生成简历评分标准”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    生成简历评分标准
    基于JD内容生成对应的简历评分标准
    工作流类型: type=2
    """
    # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 创建 Dify 客户端，准备执行 AI 工作流。
        # 将 `DifyService()` 的结果保存到 `dify_service`，供后续流程使用。
        dify_service = DifyService()
        
        # 构建查询内容
        # 将 `[` 的结果保存到 `query_parts`，供后续流程使用。
        query_parts = [
            # 向 `generate_scoring_criteria` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            f"请基于以下JD内容，生成详细的简历评分标准：\n{request.jd_content}",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "\n请生成包含以下维度的评分标准：",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "1. 技能匹配度（40%）",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "2. 工作经验匹配度（30%）",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "3. 教育背景匹配度（15%）",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "4. 项目经验匹配度（15%）",
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "\n每个维度请提供具体的评分细则和分数区间。"
        # 完成“根据 JD 生成简历评分标准”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        ]
        
        # 根据 `request.job_title` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        if request.job_title:
            # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
            # 调用 `query_parts.append()`，保存或创建当前数据。
            query_parts.append(f"\n岗位名称：{request.job_title}")
            
        # 根据 `request.requirements` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        if request.requirements:
            # 根据 `request.requirements.get('experience')` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
            # 判断 `if request.requirements.get('experience')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('experience')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('experience')`，决定是否执行当前分支。
            if request.requirements.get('experience'):
                # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"经验要求：{request.requirements['experience']}")
            # 根据 `request.requirements.get('education')` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
            # 判断 `if request.requirements.get('education')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('education')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('education')`，决定是否执行当前分支。
            if request.requirements.get('education'):
                # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"学历要求：{request.requirements['education']}")
            # 根据 `request.requirements.get('skills')` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
            # 判断 `if request.requirements.get('skills')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('skills')`，决定是否执行当前分支。
            # 判断 `if request.requirements.get('skills')`，决定是否执行当前分支。
            if request.requirements.get('skills'):
                # 把 `skills` 字段写入当前请求、响应或数据对象，供 `generate_scoring_criteria` 后续步骤使用。
                skills = request.requirements['skills']
                # 确认 `skills` 的数据类型符合后续字段读取要求，避免解析外部响应时发生类型错误。
                # 判断 `if isinstance(skills, list)`，决定是否执行当前分支。
                # 判断 `if isinstance(skills, list)`，决定是否执行当前分支。
                # 判断 `if isinstance(skills, list)`，决定是否执行当前分支。
                if isinstance(skills, list):
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append(f"技能要求：{', '.join(skills)}")
                # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `generate_scoring_criteria` 能继续返回可处理结果。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                # 前面的条件不成立时执行当前备用分支。
                else:
                    # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append(f"技能要求：{skills}")
        
        # 保存整理后的完整提示词，供下游 Dify 工作流使用。
        # 将 `"\n".join(query_parts)` 的结果保存到 `query`，供后续流程使用。
        query = "\n".join(query_parts)
        
        # 额外输入参数
        # 将 `{` 的结果保存到 `additional_inputs`，供后续流程使用。
        additional_inputs = {
            # 向下游请求或返回结果写入 `jd_content` 业务字段。
            # 设置 `jd_content` 字段，值来自 `request.jd_content`。
            "jd_content": request.jd_content
        # 完成“根据 JD 生成简历评分标准”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        }
        # 根据 `request.job_title` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        # 判断 `if request.job_title`，决定是否执行当前分支。
        if request.job_title:
            # 把 `"job_title"` 对应的岗位信息加入 Dify 结构化输入，使工作流获得完整业务条件。
            # 将 `request.job_title` 的结果保存到 `additional_inputs["job_title"]`，供后续流程使用。
            # 将 `request.job_title` 的结果保存到 `additional_inputs["job_title"]`，供后续流程使用。
            # 将 `request.job_title` 的结果保存到 `additional_inputs["job_title"]`，供后续流程使用。
            additional_inputs["job_title"] = request.job_title
        # 根据 `request.requirements` 判断当前数据应进入哪条业务分支，保证 `generate_scoring_criteria` 使用正确处理方式。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        # 判断 `if request.requirements`，决定是否执行当前分支。
        if request.requirements:
            # 把 `"requirements"` 对应的岗位信息加入 Dify 结构化输入，使工作流获得完整业务条件。
            # 将 `json.dumps(request.requirements, ensure_ascii=False)` 的结果保存到 `additional_inputs["requirements"]`，供后续流程使用。
            # 将 `json.dumps(request.requirements, ensure_ascii=False)` 的结果保存到 `additional_inputs["requirements"]`，供后续流程使用。
            # 将 `json.dumps(request.requirements, ensure_ascii=False)` 的结果保存到 `additional_inputs["requirements"]`，供后续流程使用。
            additional_inputs["requirements"] = json.dumps(request.requirements, ensure_ascii=False)
        
        # 根据前端的流式开关选择实时输出或一次性返回。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        if request.stream:
            # 流式响应
            # 定义 `generate_stream()`，实现该函数对应的接口或业务流程。
            async def generate_stream():
                # 持续接收 Dify 生成分片，并在每个分片到达后立即进入转发处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                async for chunk in dify_service.call_workflow_stream(
                    # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                    # 将 `2,  # 使用type=2用于评分标准生成` 的结果保存到 `workflow_type`，供后续流程使用。
                    workflow_type=2,  # 使用type=2用于评分标准生成
                    # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                    # 将 `query` 的结果保存到 `query`，供后续流程使用。
                    query=query,
                    # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                    # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                    conversation_id=request.conversation_id,
                    # 保存 Dify 工作流需要的结构化输入字段。
                    # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                    additional_inputs=additional_inputs
                # 完成“完成 `generate stream` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
                # 结束上方多行参数或数据结构。
                ):
                    # 把当前 Dify 内容分片立即推送给前端，实现边生成边展示。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    yield f"data: {chunk}\n\n"
                # 通知前端本次 AI 流式生成已经结束，可以停止读取响应。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                yield "data: [DONE]\n\n"
            
            # 把“根据 JD 生成简历评分标准”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return StreamingResponse(
                # 调用 `generate_stream` 执行 `generate_scoring_criteria` 的当前步骤，并将产生的数据交给后续流程。
                # 调用 `generate_stream()`，完成当前函数调用并取得其返回结果。
                generate_stream(),
                # 声明流式响应类型，使浏览器按照连续数据流读取。
                # 将 `"text/plain"` 的结果保存到 `media_type`，供后续流程使用。
                media_type="text/plain",
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                # 将 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 的结果保存到 `headers`，供后续流程使用。
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            # 完成“根据 JD 生成简历评分标准”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `generate_scoring_criteria` 能继续返回可处理结果。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 同步响应
            # 将 `await dify_service.call_workflow_sync(` 的结果保存到 `result`，供后续流程使用。
            result = await dify_service.call_workflow_sync(
                # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                # 将 `2` 的结果保存到 `workflow_type`，供后续流程使用。
                workflow_type=2,
                # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                # 将 `query` 的结果保存到 `query`，供后续流程使用。
                query=query,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                conversation_id=request.conversation_id,
                # 保存 Dify 工作流需要的结构化输入字段。
                # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                additional_inputs=additional_inputs
            # 完成“根据 JD 生成简历评分标准”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
            # 把“根据 JD 生成简历评分标准”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return result
        
    # 捕获“根据 JD 生成简历评分标准”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“根据 JD 生成简历评分标准”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"生成评分标准失败: {str(e)}")
        # 终止“根据 JD 生成简历评分标准”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"生成评分标准失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"生成评分标准失败: {str(e)}"
        # 完成“根据 JD 生成简历评分标准”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
    
# 对外提供 POST `/evaluate` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/evaluate", response_model=ResumeEvaluationResult)
# 封装“解析简历并完成 AI 评分”流程，供接口层或其他业务服务调用。
# 定义 `evaluate_resume()`，实现该函数对应的接口或业务流程。
async def evaluate_resume(
    # 把 `file` 字段写入当前请求、响应或数据对象，供 `evaluate_resume` 后续步骤使用。
    file: UploadFile = File(..., description="简历文件 (支持PDF、TXT、DOC、DOCX)"),
    # 把评分或简历评价关联到指定岗位 JD
    # 声明 `job_description_id` 参数（当前步骤使用的数据），类型为 `str`，默认值为 `Form(...`。
    job_description_id: str = Form(..., description="职位描述ID"),
    # 可选地延续同一次 Dify 对话上下文
    # 声明 `conversation_id` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `Form(None`。
    conversation_id: Optional[str] = Form(None, description="对话ID"),
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“解析简历并完成 AI 评分”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
):
    """
    上传简历并进行AI评价
    工作流类型: type=3
    """
    # 执行 简历解析、AI 评分或数据库保存 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 验证参数格式
        # 调用 `ResumeEvaluationService.validate_evaluation_params()`，完成当前函数调用并取得其返回结果。
        jd_uuid, conv_uuid = await ResumeEvaluationService.validate_evaluation_params(
            # 把当前字段纳入 `evaluate_resume` 的请求或响应结构，确保后续步骤获得完整业务数据。
            # 执行表达式 `job_description_id, conversation_id`，完成本行明确的数据运算或控制操作。
            job_description_id, conversation_id
        # 完成“解析简历并完成 AI 评分”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        
        # 读取文件内容
        # 将 `await file.read()` 的结果保存到 `file_content`，供后续流程使用。
        file_content = await file.read()
        # 拒绝空文件，避免把无内容的简历继续交给解析和 AI 评价。
        # 判断 `if not file_content`，决定是否执行当前分支。
        # 判断 `if not file_content`，决定是否执行当前分支。
        # 判断 `if not file_content`，决定是否执行当前分支。
        if not file_content:
            # 终止“解析简历并完成 AI 评分”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(status_code=400, detail="文件内容为空")
        
        # 创建评价服务并执行评价
        # 将 `ResumeEvaluationService(db)` 的结果保存到 `evaluation_service`，供后续流程使用。
        evaluation_service = ResumeEvaluationService(db)
        # 调用 `evaluation_service.evaluate_resume` 执行“解析简历并完成 AI 评分”所需的下游操作，并保存返回结果供下一步使用。
        # 将 `await evaluation_service.evaluate_resume(` 的结果保存到 `result`，供后续流程使用。
        result = await evaluation_service.evaluate_resume(
            # 携带当前用户 ID，让下游服务只操作该用户的数据。
            # 将 `current_user.id` 的结果保存到 `user_id`，供后续流程使用。
            user_id=current_user.id,
            # 保存上传的简历字节，供格式校验、文本提取和文件落盘。
            # 将 `file_content` 的结果保存到 `file_content`，供后续流程使用。
            file_content=file_content,
            # 保存安全处理后的文件名或原文件名，供落盘和结果展示。
            # 将 `file.filename` 的结果保存到 `filename`，供后续流程使用。
            filename=file.filename,
            # 关联对应的岗位 JD，供评分和后续招聘流程使用。
            # 将 `jd_uuid` 的结果保存到 `job_description_id`，供后续流程使用。
            job_description_id=jd_uuid,
            # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
            # 将 `conv_uuid` 的结果保存到 `conversation_id`，供后续流程使用。
            conversation_id=conv_uuid
        # 完成“解析简历并完成 AI 评分”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        
        # 把“解析简历并完成 AI 评分”得到的最终业务结果返回给接口层或前端。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return ResumeEvaluationResult(**result)
        
    # 捕获“解析简历并完成 AI 评分”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except ValueError as e:
        # 记录“解析简历并完成 AI 评分”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
        logger.warning(f"简历评价参数错误: {e}")
        # 终止“解析简历并完成 AI 评分”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(status_code=400, detail=str(e))
    # 捕获“解析简历并完成 AI 评分”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“解析简历并完成 AI 评分”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"简历评价失败: {e}")
        # 终止“解析简历并完成 AI 评分”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(status_code=500, detail="简历评价服务暂时不可用")
    
# 对外提供 POST `/generate-interview-plan-by-resume` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/generate-interview-plan-by-resume")
# 封装“根据候选人简历和 JD 生成面试方案”流程，供接口层或其他业务服务调用。
# 定义 `generate_interview_plan_by_resume()`，实现该函数对应的接口或业务流程。
async def generate_interview_plan_by_resume(
    # 定位用于生成面试方案的简历评价记录
    # 声明 `resume_id` 参数（当前步骤使用的数据），类型为 `str`，默认值为 `Form(...`。
    resume_id: str = Form(..., description="简历ID"),
    # 可选地延续同一次 Dify 对话上下文
    # 声明 `conversation_id` 参数（当前步骤使用的数据），类型为 `str`，默认值为 `Form(None`。
    conversation_id: str = Form(None, description="对话ID"),
    # 决定结果是边生成边返回还是生成完成后一次返回
    # 声明 `stream` 参数（当前步骤使用的数据），类型为 `bool`，默认值为 `Form(True`。
    stream: bool = Form(True, description="是否流式返回"),
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“根据候选人简历和 JD 生成面试方案”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    根据简历ID生成面试方案
    工作流类型: type=4
    """
    # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 查询简历评价记录
        # 将 `await db.execute(` 的结果保存到 `result`，供后续流程使用。
        result = await db.execute(
            # 调用 `select` 执行 `generate_interview_plan_by_resume` 的当前步骤，并将产生的数据交给后续流程。
            # 调用 `select()`，获取所需数据。
            select(ResumeEvaluation).where(
                # 把当前字段纳入 `generate_interview_plan_by_resume` 的请求或响应结构，确保后续步骤获得完整业务数据。
                # 执行表达式 `ResumeEvaluation.id == resume_id,`，完成本行明确的数据运算或控制操作。
                ResumeEvaluation.id == resume_id,
                # 把当前字段纳入 `generate_interview_plan_by_resume` 的请求或响应结构，确保后续步骤获得完整业务数据。
                # 执行表达式 `ResumeEvaluation.user_id == current_user.id`，完成本行明确的数据运算或控制操作。
                ResumeEvaluation.user_id == current_user.id
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        # 把 `resume_evaluation` 字段写入当前请求、响应或数据对象，供 `generate_interview_plan_by_resume` 后续步骤使用。
        resume_evaluation = result.scalar_one_or_none()
        
        # 确认简历评价存在且属于当前用户，否则不能生成或保存该候选人的面试方案。
        # 判断 `if not resume_evaluation`，决定是否执行当前分支。
        # 判断 `if not resume_evaluation`，决定是否执行当前分支。
        # 判断 `if not resume_evaluation`，决定是否执行当前分支。
        if not resume_evaluation:
            # 终止“根据候选人简历和 JD 生成面试方案”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(
                # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
                # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
                status_code=status.HTTP_404_NOT_FOUND,
                # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
                # 将 `"简历记录未找到"` 的结果保存到 `detail`，供后续流程使用。
                detail="简历记录未找到"
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        
        # 通过远程服务查询关联的JD
        # 将 `JobDescriptionService(db)` 的结果保存到 `jd_service`，供后续流程使用。
        jd_service = JobDescriptionService(db)
        # 执行 Dify 请求或响应 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 调用 `jd_service.get_job_description` 执行“根据候选人简历和 JD 生成面试方案”所需的下游操作，并保存返回结果供下一步使用。
            job_description = await jd_service.get_job_description(
                # 把 `jd_id` 字段写入当前请求、响应或数据对象，供 `generate_interview_plan_by_resume` 后续步骤使用。
                jd_id=str(resume_evaluation.job_description_id),
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                # 将 `current_user.id` 的结果保存到 `user_id`，供后续流程使用。
                user_id=current_user.id
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 捕获“根据候选人简历和 JD 生成面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except ValueError:
            # 终止“根据候选人简历和 JD 生成面试方案”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(
                # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
                # 将 `status.HTTP_404_NOT_FOUND` 的结果保存到 `status_code`，供后续流程使用。
                status_code=status.HTTP_404_NOT_FOUND,
                # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
                # 将 `"关联的职位描述未找到"` 的结果保存到 `detail`，供后续流程使用。
                detail="关联的职位描述未找到"
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 捕获“根据候选人简历和 JD 生成面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 记录“根据候选人简历和 JD 生成面试方案”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取职位描述失败: {str(e)}")
            # 终止“根据候选人简历和 JD 生成面试方案”并向前端返回明确的 HTTP 状态和失败原因。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise HTTPException(
                # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
                # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
                # 将 `f"获取职位描述失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
                detail=f"获取职位描述失败: {str(e)}"
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )

        # 构建简历内容
        # 将 `resume_evaluation.resume_content` 的结果保存到 `jianli_content`，供后续流程使用。
        jianli_content = resume_evaluation.resume_content
        
        # 创建 Dify 客户端，准备执行 AI 工作流。
        # 将 `DifyService()` 的结果保存到 `dify_service`，供后续流程使用。
        dify_service = DifyService()
        
        # 构建查询内容
        # 将 `f"请根据简历和JD要求生成面试方案。"` 的结果保存到 `query`，供后续流程使用。
        query = f"请根据简历和JD要求生成面试方案。"
        
        # 额外输入参数
        # 将 `{` 的结果保存到 `additional_inputs`，供后续流程使用。
        additional_inputs = {
            # 向下游请求或返回结果写入 `jianli` 业务字段。
            # 设置 `jianli` 字段，值来自 `jianli_content`。
            "jianli": jianli_content,
            # 向下游请求或返回结果写入 `jd` 业务字段。
            # 设置 `jd` 字段，值来自 `job_description.content`。
            "jd": job_description.content
        # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        }
        
        # 根据前端的流式开关选择实时输出或一次性返回。
        # 判断 `if stream`，决定是否执行当前分支。
        # 判断 `if stream`，决定是否执行当前分支。
        # 判断 `if stream`，决定是否执行当前分支。
        if stream:
            # 流式响应
            # 定义 `generate_stream()`，实现该函数对应的接口或业务流程。
            async def generate_stream():
                # 持续接收 Dify 生成分片，并在每个分片到达后立即进入转发处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                async for chunk in dify_service.call_workflow_stream(
                    # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                    # 将 `4` 的结果保存到 `workflow_type`，供后续流程使用。
                    workflow_type=4,
                    # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                    # 将 `query` 的结果保存到 `query`，供后续流程使用。
                    query=query,
                    # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                    # 将 `conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                    conversation_id=conversation_id,
                    # 保存 Dify 工作流需要的结构化输入字段。
                    # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                    additional_inputs=additional_inputs
                # 完成“完成 `generate stream` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
                # 结束上方多行参数或数据结构。
                ):
                    # 把当前 Dify 内容分片立即推送给前端，实现边生成边展示。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    yield f"data: {chunk}\n\n"
                # 通知前端本次 AI 流式生成已经结束，可以停止读取响应。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                # 产出当前流式数据片段，并保留生成器状态。
                yield "data: [DONE]\n\n"
            
            # 把“根据候选人简历和 JD 生成面试方案”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return StreamingResponse(
                # 调用 `generate_stream` 执行 `generate_interview_plan_by_resume` 的当前步骤，并将产生的数据交给后续流程。
                # 调用 `generate_stream()`，完成当前函数调用并取得其返回结果。
                generate_stream(),
                # 声明流式响应类型，使浏览器按照连续数据流读取。
                # 将 `"text/plain"` 的结果保存到 `media_type`，供后续流程使用。
                media_type="text/plain",
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                # 将 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 的结果保存到 `headers`，供后续流程使用。
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `generate_interview_plan_by_resume` 能继续返回可处理结果。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 同步响应
            # 将 `await dify_service.call_workflow_sync(` 的结果保存到 `result`，供后续流程使用。
            result = await dify_service.call_workflow_sync(
                # 把任务类型传给 Dify，使工作流进入正确的业务分支。
                # 将 `4` 的结果保存到 `workflow_type`，供后续流程使用。
                workflow_type=4,
                # 保存整理后的完整提示词，供下游 Dify 工作流使用。
                # 将 `query` 的结果保存到 `query`，供后续流程使用。
                query=query,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                # 将 `conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                conversation_id=conversation_id,
                # 保存 Dify 工作流需要的结构化输入字段。
                # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                additional_inputs=additional_inputs
            # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
            # 把“根据候选人简历和 JD 生成面试方案”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return result
            
    # 保留已经带有正确状态码的 HTTP 异常，避免被再次包装成不准确的 500。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except HTTPException:
        # 引入或传递 `raise`，供“根据候选人简历和 JD 生成面试方案”的后续步骤使用。
        # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
        raise
    # 捕获“根据候选人简历和 JD 生成面试方案”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“根据候选人简历和 JD 生成面试方案”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error generating interview plan by resume: {str(e)}")
        # 终止“根据候选人简历和 JD 生成面试方案”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"生成面试方案失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"生成面试方案失败: {str(e)}"
        # 完成“根据候选人简历和 JD 生成面试方案”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

# 对外提供 POST `/papers/parse-intent` 接口，入口函数负责“完成当前模块初始化”。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/papers/parse-intent")
# 封装“完成 `parse exam intent` 对应的业务处理”流程，供接口层或其他业务服务调用。
# 定义 `parse_exam_intent()`，实现该函数对应的接口或业务流程。
async def parse_exam_intent(
    # 接收并使用前端已经校验的业务请求数据
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamIntentParseRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamIntentParseRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamIntentParseRequest`。
    request: ExamIntentParseRequest,
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“完成 `parse exam intent` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    将自然语言试卷意图解析为结构化字段，供前端表单自动填充
    支持解析：标题/科目、总分、难度、时长、题量（单选/多选/简答/填空）、特殊要求
    """
    # 导入 `re` 模块，供后续代码使用。
    import re
    # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 创建 Dify 客户端，准备执行 AI 工作流。
        # 将 `DifyService()` 的结果保存到 `dify_service`，供后续流程使用。
        dify_service = DifyService()
        # 保存发给 AI 的任务说明、输入数据和输出格式要求。
        # 将 `(` 的结果保存到 `prompt`，供后续流程使用。
        prompt = (
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "你是考试出题助手。请从以下中文需求中提取结构化字段，并严格以JSON格式返回。\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "不要添加解释，不要返回除JSON外的任何内容。\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "需求文本：\n" + request.text + "\n\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "JSON字段定义：{\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"title\": 试卷标题（若未提及可从语义中提取或留空）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"subject\": 科目（如Java、市场营销，若未提及可留空）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"total_score\": 整数，总分（若未提及，默认100）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"difficulty\": 难度（easy/medium/hard）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"duration\": 整数，考试时长（分钟）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"question_counts\": 对象，包含各题型数量，如{\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"single_choice\": 单选题数量（整数，若未提及默认为5）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"multiple_choice\": 多选题数量（整数，若未提及默认为5）,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"short_answer\": 简答题数量（整数，若未提及默认为2）\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  },\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"special_requirements\": 其他补充要求（原文提炼，若无则空字符串）\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "}\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "示例返回：{\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"title\": \"Java基础测试\",\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"subject\": \"Java\",\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"total_score\": 100,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"difficulty\": \"medium\",\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"duration\": 90,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"question_counts\": {\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"single_choice\": 10,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"multiple_choice\": 5,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"short_answer\": 2,\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "    \"fill_blank\": 0\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  },\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "  \"special_requirements\": \"题目覆盖集合、泛型、异常处理等\"\n"
            # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
            # 补充当前 Prompt、日志、响应文本或多行字符串内容。
            "}"
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 调用 `dify_service.call_workflow_sync` 执行“完成 `parse exam intent` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
        # 将 `await dify_service.call_workflow_sync(` 的结果保存到 `ai_response`，供后续流程使用。
        ai_response = await dify_service.call_workflow_sync(
            # 把任务类型传给 Dify，使工作流进入正确的业务分支。
            # 将 `5` 的结果保存到 `workflow_type`，供后续流程使用。
            workflow_type=5,
            # 保存整理后的完整提示词，供下游 Dify 工作流使用。
            # 将 `prompt` 的结果保存到 `query`，供后续流程使用。
            query=prompt,
            # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
            # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
            conversation_id=request.conversation_id,
            # 保存 Dify 工作流需要的结构化输入字段。
            # 将 `{"task": "parse_exam_intent"}` 的结果保存到 `additional_inputs`，供后续流程使用。
            additional_inputs={"task": "parse_exam_intent"}
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
        # 将 `""` 的结果保存到 `answer_text`，供后续流程使用。
        answer_text = ""
        # 确认 Dify 返回的是字典结构，避免后续按字段读取时发生类型错误。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        # 判断 `if isinstance(ai_response, dict)`，决定是否执行当前分支。
        if isinstance(ai_response, dict):
            # 优先检查 Dify 标准响应中是否存在 `answer` 正文字段。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            # 判断 `if "answer" in ai_response`，决定是否执行当前分支。
            if "answer" in ai_response:
                # 从 Dify 标准 `answer` 字段取得生成正文，供后续结构化解析。
                # 将 `ai_response["answer"]` 的结果保存到 `answer_text`，供后续流程使用。
                answer_text = ai_response["answer"]
            # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `parse_exam_intent` 能继续返回可处理结果。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                try:
                    # 找不到标准正文时把完整响应转成文本，给后续兜底解析保留原始信息。
                    # 将 `json.dumps(ai_response, ensure_ascii=False)` 的结果保存到 `answer_text`，供后续流程使用。
                    answer_text = json.dumps(ai_response, ensure_ascii=False)
                # 捕获“完成 `parse exam intent` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                except Exception:
                    # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
                    # 将 `""` 的结果保存到 `answer_text`，供后续流程使用。
                    answer_text = ""
        # 根据 `isinstance(ai_response, str)` 判断当前数据应进入哪条业务分支，保证 `parse_exam_intent` 使用正确处理方式。
        # 判断 `elif isinstance(ai_response, str)`，决定是否执行当前分支。
        # 判断 `elif isinstance(ai_response, str)`，决定是否执行当前分支。
        # 判断 `elif isinstance(ai_response, str)`，决定是否执行当前分支。
        elif isinstance(ai_response, str):
            # 保存从 Dify 响应中提取的正文，供后续 JSON 清洗和字段解析。
            # 将 `ai_response` 的结果保存到 `answer_text`，供后续流程使用。
            answer_text = ai_response

        # 初始化结构化解析结果；后续会填入从 AI 正文提取出的字段。
        # 声明 `parsed` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        parsed: Dict[str, Any] = {}
        # 根据 `answer_text` 判断当前数据应进入哪条业务分支，保证 `parse_exam_intent` 使用正确处理方式。
        # 判断 `if answer_text`，决定是否执行当前分支。
        # 判断 `if answer_text`，决定是否执行当前分支。
        # 判断 `if answer_text`，决定是否执行当前分支。
        if answer_text:
            # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            try:
                # 把 AI 返回的 JSON 文本解析成 Python 字典，供评分字段校验和保存。
                # 将 `json.loads(answer_text)` 的结果保存到 `parsed`，供后续流程使用。
                parsed = json.loads(answer_text)
            # 捕获“完成 `parse exam intent` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            except json.JSONDecodeError:
                # 保存解析后的 AI 结果字典，供 Pydantic 校验并返回前端。
                # 将 `{}` 的结果保存到 `parsed`，供后续流程使用。
                parsed = {}

        # 把 `text` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        text = request.text
        # 封装“完成 `find int` 对应的业务处理”流程，供接口层或其他业务服务调用。
        # 定义 `find_int()`，实现该函数对应的接口或业务流程。
        def find_int(pattern: str) -> Optional[int]:
            # 把 `m` 字段写入当前请求、响应或数据对象，供 `find_int` 后续步骤使用。
            m = re.search(pattern, text)
            # 根据 `m` 判断当前数据应进入哪条业务分支，保证 `find_int` 使用正确处理方式。
            # 判断 `if m`，决定是否执行当前分支。
            # 判断 `if m`，决定是否执行当前分支。
            # 判断 `if m`，决定是否执行当前分支。
            if m:
                # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                try:
                    # 把“完成 `find int` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return int(m.group(1))
                # 捕获“完成 `find int` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                except Exception:
                    # 把“完成 `find int` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return None
            # 把“完成 `find int` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 封装“完成 `extract named title` 对应的业务处理”流程，供接口层或其他业务服务调用。
        # 定义 `extract_named_title()`，实现该函数对应的接口或业务流程。
        def extract_named_title(raw_text: str) -> Optional[str]:
            # 把 `patterns` 字段写入当前请求、响应或数据对象，供 `extract_named_title` 后续步骤使用。
            patterns = [
                # 调用 `r"` 执行 `extract_named_title` 的当前步骤，并将产生的数据交给后续流程。
                # 执行表达式 `r"(?:试卷名称|试卷名|标题|名称|名字)(?:是|为|叫|设为|定为)?[:：]?\s*([^\n，,。；;]+)",`，完成本行明确的数据运算或控制操作。
                r"(?:试卷名称|试卷名|标题|名称|名字)(?:是|为|叫|设为|定为)?[:：]?\s*([^\n，,。；;]+)",
                # 调用 `r"` 执行 `extract_named_title` 的当前步骤，并将产生的数据交给后续流程。
                # 执行表达式 `r"(?:就叫|叫做|叫|命名为|取名为|起名为)\s*([^\n，,。；;]+?)(?:吧|。|$)",`，完成本行明确的数据运算或控制操作。
                r"(?:就叫|叫做|叫|命名为|取名为|起名为)\s*([^\n，,。；;]+?)(?:吧|。|$)",
            # 完成“完成 `extract named title` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            ]
            # 逐项处理“完成 `extract named title` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for pattern in patterns:
                # 把 `match` 字段写入当前请求、响应或数据对象，供 `extract_named_title` 后续步骤使用。
                match = re.search(pattern, raw_text)
                # 根据 `match` 判断当前数据应进入哪条业务分支，保证 `extract_named_title` 使用正确处理方式。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                if match:
                    # 把 `title_value` 字段写入当前请求、响应或数据对象，供 `extract_named_title` 后续步骤使用。
                    title_value = match.group(1).strip()
                    # 把“完成 `extract named title` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return re.sub(r"(?:的)?试卷$", "", title_value).strip() or None
            # 把“完成 `extract named title` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 封装“完成 `extract subject` 对应的业务处理”流程，供接口层或其他业务服务调用。
        # 定义 `extract_subject()`，实现该函数对应的接口或业务流程。
        def extract_subject(raw_text: str) -> Optional[str]:
            # 把 `patterns` 字段写入当前请求、响应或数据对象，供 `extract_subject` 后续步骤使用。
            patterns = [
                # 调用 `r"` 执行 `extract_subject` 的当前步骤，并将产生的数据交给后续流程。
                # 执行表达式 `r"(?:生成|出|创建|制作)(?:一份|一个)?\s*([^\n，,。；;]{1,30}?)(?:的)?(?:试卷|考试|测试|笔试)",`，完成本行明确的数据运算或控制操作。
                r"(?:生成|出|创建|制作)(?:一份|一个)?\s*([^\n，,。；;]{1,30}?)(?:的)?(?:试卷|考试|测试|笔试)",
                # 调用 `r"` 执行 `extract_subject` 的当前步骤，并将产生的数据交给后续流程。
                # 执行表达式 `r"([^\n，,。；;]{1,30}?)(?:的)?(?:试卷|考试|测试|笔试)",`，完成本行明确的数据运算或控制操作。
                r"([^\n，,。；;]{1,30}?)(?:的)?(?:试卷|考试|测试|笔试)",
            # 完成“完成 `extract subject` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            ]
            # 逐项处理“完成 `extract subject` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for pattern in patterns:
                # 把 `match` 字段写入当前请求、响应或数据对象，供 `extract_subject` 后续步骤使用。
                match = re.search(pattern, raw_text)
                # 根据 `match` 判断当前数据应进入哪条业务分支，保证 `extract_subject` 使用正确处理方式。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                if match:
                    # 把 `subject_value` 字段写入当前请求、响应或数据对象，供 `extract_subject` 后续步骤使用。
                    subject_value = match.group(1).strip(" 的")
                    # 把“完成 `extract subject` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return subject_value or None
            # 把“完成 `extract subject` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 封装“完成 `extract question count` 对应的业务处理”流程，供接口层或其他业务服务调用。
        # 定义 `extract_question_count()`，实现该函数对应的接口或业务流程。
        def extract_question_count(labels: List[str]) -> Optional[int]:
            # 逐项处理“完成 `extract question count` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for label in labels:
                # 把 `match` 字段写入当前请求、响应或数据对象，供 `extract_question_count` 后续步骤使用。
                match = re.search(rf"(\d+)\s*(?:道|个)?\s*{label}|{label}\s*(\d+)\s*(?:道|个)?", text)
                # 根据 `match` 判断当前数据应进入哪条业务分支，保证 `extract_question_count` 使用正确处理方式。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                # 判断 `if match`，决定是否执行当前分支。
                if match:
                    # 把“完成 `extract question count` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return int(match.group(1) or match.group(2))
            # 把“完成 `extract question count` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 把 `difficulty_map` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        difficulty_map = {
            # 向下游请求或返回结果写入 `简单` 业务字段。
            # 设置 `简单` 字段，值来自 `'easy', '易': 'easy', '基础': 'easy'`。
            '简单': 'easy', '易': 'easy', '基础': 'easy',
            # 向下游请求或返回结果写入 `中等` 业务字段。
            # 设置 `中等` 字段，值来自 `'medium', '一般': 'medium'`。
            '中等': 'medium', '一般': 'medium',
            # 向下游请求或返回结果写入 `困难` 业务字段。
            # 设置 `困难` 字段，值来自 `'hard', '难': 'hard', '高级': 'hard'`。
            '困难': 'hard', '难': 'hard', '高级': 'hard'
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        }
        # 把 `difficulty` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        difficulty = parsed.get('difficulty') or next((difficulty_map[k] for k in difficulty_map if k in text), None) or 'medium'

        # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        total_score = parsed.get('total_score')
        # 确认 `isinstance(total_score, int)` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `parse_exam_intent`。
        # 判断 `if not isinstance(total_score, int)`，决定是否执行当前分支。
        # 判断 `if not isinstance(total_score, int)`，决定是否执行当前分支。
        # 判断 `if not isinstance(total_score, int)`，决定是否执行当前分支。
        if not isinstance(total_score, int):
            # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            total_score = find_int(r"总分[:：]?\s*(\d+)") or find_int(r"(\d+)\s*分") or 100

        # 把 `duration` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        duration = parsed.get('duration')
        # 确认 `isinstance(duration, int)` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `parse_exam_intent`。
        # 判断 `if not isinstance(duration, int)`，决定是否执行当前分支。
        # 判断 `if not isinstance(duration, int)`，决定是否执行当前分支。
        # 判断 `if not isinstance(duration, int)`，决定是否执行当前分支。
        if not isinstance(duration, int):
            # 把 `duration` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            duration = find_int(r"时长[:：]?\s*(\d+)\s*分钟") or find_int(r"(\d+)\s*分钟") or 90

        # 把 `qc` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        qc = parsed.get('question_counts') or {}
        # 封装“完成 `qc val` 对应的业务处理”流程，供接口层或其他业务服务调用。
        # 定义 `qc_val()`，实现该函数对应的接口或业务流程。
        def qc_val(key: str, patterns: List[str]) -> int:
            # 把 `v` 字段写入当前请求、响应或数据对象，供 `qc_val` 后续步骤使用。
            v = qc.get(key)
            # 确认 `v` 的数据类型符合后续字段读取要求，避免解析外部响应时发生类型错误。
            # 判断 `if isinstance(v, int) and v >= 0`，决定是否执行当前分支。
            # 判断 `if isinstance(v, int) and v >= 0`，决定是否执行当前分支。
            # 判断 `if isinstance(v, int) and v >= 0`，决定是否执行当前分支。
            if isinstance(v, int) and v >= 0:
                # 把“完成 `qc val` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return v
            # 逐项处理“完成 `qc val` 对应的业务处理”所需的数据集合，构造最终列表或查找目标记录。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for p in patterns:
                # 把 `res` 字段写入当前请求、响应或数据对象，供 `qc_val` 后续步骤使用。
                res = find_int(p)
                # 确认 `res` 的数据类型符合后续字段读取要求，避免解析外部响应时发生类型错误。
                # 判断 `if isinstance(res, int)`，决定是否执行当前分支。
                # 判断 `if isinstance(res, int)`，决定是否执行当前分支。
                # 判断 `if isinstance(res, int)`，决定是否执行当前分支。
                if isinstance(res, int):
                    # 把“完成 `qc val` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return res
            # 把“完成 `qc val` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return 0

        # 把 `single_choice` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        single_choice = (
            # 调用 `qc_val` 执行 `parse_exam_intent` 的当前步骤，并将产生的数据交给后续流程。
            # 调用 `qc_val()`，完成当前函数调用并取得其返回结果。
            qc_val('single_choice', [r"单选题\s*(\d+)", r"单选\s*(\d+)"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 调用 `extract_question_count()`，解析或转换当前内容。
            or extract_question_count(["单选题", "单选", "选择题"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 执行表达式 `or 0`，完成本行明确的数据运算或控制操作。
            or 0
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        # 把 `multiple_choice` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        multiple_choice = (
            # 调用 `qc_val` 执行 `parse_exam_intent` 的当前步骤，并将产生的数据交给后续流程。
            # 调用 `qc_val()`，完成当前函数调用并取得其返回结果。
            qc_val('multiple_choice', [r"多选题\s*(\d+)", r"多选\s*(\d+)"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 调用 `extract_question_count()`，解析或转换当前内容。
            or extract_question_count(["多选题", "多选"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 执行表达式 `or 0`，完成本行明确的数据运算或控制操作。
            or 0
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        # 把 `short_answer` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        short_answer = (
            # 调用 `qc_val` 执行 `parse_exam_intent` 的当前步骤，并将产生的数据交给后续流程。
            # 调用 `qc_val()`，完成当前函数调用并取得其返回结果。
            qc_val('short_answer', [r"简答题\s*(\d+)", r"简答\s*(\d+)"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 调用 `extract_question_count()`，解析或转换当前内容。
            or extract_question_count(["简答题", "简答", "问答题", "问答"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 执行表达式 `or 0`，完成本行明确的数据运算或控制操作。
            or 0
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )
        # 把 `fill_blank` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        fill_blank = (
            # 调用 `qc_val` 执行 `parse_exam_intent` 的当前步骤，并将产生的数据交给后续流程。
            # 调用 `qc_val()`，完成当前函数调用并取得其返回结果。
            qc_val('fill_blank', [r"填空题\s*(\d+)", r"填空\s*(\d+)"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 调用 `extract_question_count()`，解析或转换当前内容。
            or extract_question_count(["填空题", "填空"])
            # 使用布尔运算连接多个判断条件，整体结果决定当前分支是否执行。
            # 执行表达式 `or 0`，完成本行明确的数据运算或控制操作。
            or 0
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 把 `title_match` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        title_match = re.search(r"(试卷名称|标题)[:：]?\s*([^\n]+)", text)
        # 把 `subject_match` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        subject_match = re.search(r"(科目|主题)[:：]?\s*([^\n]+)", text)
        # 把 `explicit_title` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        explicit_title = extract_named_title(text)
        # 把 `title` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        title = explicit_title or parsed.get('title') or (title_match and title_match.group(2).strip())
        # 把 `subject` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        subject = parsed.get('subject') or (subject_match and subject_match.group(2).strip()) or extract_subject(text)
        # 确认 `subject and title` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `parse_exam_intent`。
        # 判断 `if not subject and title`，决定是否执行当前分支。
        # 判断 `if not subject and title`，决定是否执行当前分支。
        # 判断 `if not subject and title`，决定是否执行当前分支。
        if not subject and title:
            # 把 `subject` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            subject = title

        # 把 `special_requirements` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
        special_requirements = parsed.get('special_requirements')
        # 确认 `special_requirements` 是否缺失；缺失时及时走错误或兜底路径，避免无效数据继续进入 `parse_exam_intent`。
        # 判断 `if not special_requirements`，决定是否执行当前分支。
        # 判断 `if not special_requirements`，决定是否执行当前分支。
        # 判断 `if not special_requirements`，决定是否执行当前分支。
        if not special_requirements:
            # 把 `m` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            m = re.search(r"(要求|注意事项|其他)[:：]?\s*(.+)", text)
            # 把 `special_requirements` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            special_requirements = m.group(2).strip() if m else ""

        # 保存当前服务调用结果，供接口返回或继续处理。
        # 将 `ExamIntentParseResponse(` 的结果保存到 `result`，供后续流程使用。
        result = ExamIntentParseResponse(
            # 把 `title` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            title=title,
            # 把 `subject` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            subject=subject,
            # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            total_score=total_score,
            # 把 `difficulty` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            difficulty=difficulty,
            # 把 `duration` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            duration=duration,
            # 把 `question_counts` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            question_counts={
                # 向下游请求或返回结果写入 `single_choice` 业务字段。
                # 设置 `single_choice` 字段，值来自 `single_choice`。
                'single_choice': single_choice,
                # 向下游请求或返回结果写入 `multiple_choice` 业务字段。
                # 设置 `multiple_choice` 字段，值来自 `multiple_choice`。
                'multiple_choice': multiple_choice,
                # 向下游请求或返回结果写入 `short_answer` 业务字段。
                # 设置 `short_answer` 字段，值来自 `short_answer`。
                'short_answer': short_answer,
                # 向下游请求或返回结果写入 `fill_blank` 业务字段。
                # 设置 `fill_blank` 字段，值来自 `fill_blank`。
                'fill_blank': fill_blank
            # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            },
            # 把 `special_requirements` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            special_requirements=special_requirements
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

        # 自动选择最匹配的知识库文档，填充 knowledge_files
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 把 `selector` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            selector = KBSelectionService(db)
            # 把 `question` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            question = "".join([
                # 把当前字段纳入 `parse_exam_intent` 的请求或响应结构，确保后续步骤获得完整业务数据。
                # 执行表达式 `subject or "",`，完成本行明确的数据运算或控制操作。
                subject or "",
                # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                " ",
                # 把当前字段纳入 `parse_exam_intent` 的请求或响应结构，确保后续步骤获得完整业务数据。
                # 执行表达式 `title or "",`，完成本行明确的数据运算或控制操作。
                title or "",
                # 向 `parse_exam_intent` 使用的 AI 提示词补充业务背景、输入内容或输出格式要求。
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                " ",
                # 把当前字段纳入 `parse_exam_intent` 的请求或响应结构，确保后续步骤获得完整业务数据。
                # 执行表达式 `special_requirements or "",`，完成本行明确的数据运算或控制操作。
                special_requirements or "",
            # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 调用 `strip()`，完成当前函数调用并取得其返回结果。
            ]).strip() or (request.text if hasattr(request, 'text') else '')
            # 调用 `selector.select_kb_for_question` 执行“完成 `parse exam intent` 对应的业务处理”所需的下游操作，并保存返回结果供下一步使用。
            # 将 `await selector.select_kb_for_question(` 的结果保存到 `selection`，供后续流程使用。
            selection = await selector.select_kb_for_question(
                # 把 `question` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
                question=question,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                # 将 `current_user.id` 的结果保存到 `user_id`，供后续流程使用。
                user_id=current_user.id,
                # 把 `max_candidates` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
                max_candidates=200,
            # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
            # 把 `knowledge_files` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
            knowledge_files: List[KnowledgeFileInfo] = []
            # 确认已经得到有效选择结果，再读取其中的业务 ID 和匹配信息。
            # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
            # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
            # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
            if selection and selection.get("document_id"):
                # 把当前岗位条件追加到提示词片段中，使 Dify 获得更完整的生成要求。
                # 调用 `knowledge_files.append()`，保存或创建当前数据。
                knowledge_files.append(
                    # 调用 `KnowledgeFileInfo` 执行 `parse_exam_intent` 的当前步骤，并将产生的数据交给后续流程。
                    # 调用 `KnowledgeFileInfo()`，完成当前函数调用并取得其返回结果。
                    KnowledgeFileInfo(
                        # 把 `id` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
                        id=str(selection.get("document_id")),
                        # 把 `fileName` 字段写入当前请求、响应或数据对象，供 `parse_exam_intent` 后续步骤使用。
                        fileName=selection.get("filename")
                    # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                    # 结束上方多行参数或数据结构。
                    )
                # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
                # 结束上方多行参数或数据结构。
                )
            # 为 `parse_exam_intent` 补充当前业务所需的数据，供下一步下游调用、校验或响应使用。
            # 将 `knowledge_files` 的结果保存到 `result.knowledge_files`，供后续流程使用。
            # 将 `knowledge_files` 的结果保存到 `result.knowledge_files`，供后续流程使用。
            # 将 `knowledge_files` 的结果保存到 `result.knowledge_files`，供后续流程使用。
            result.knowledge_files = knowledge_files
        # 捕获“完成 `parse exam intent` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as se:
            # 记录“完成 `parse exam intent` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
            # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
            logger.warning(f"知识库自动选择失败: {se}")

        # 把“完成 `parse exam intent` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return result
    # 捕获“完成 `parse exam intent` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“完成 `parse exam intent` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"试卷意图解析失败: {str(e)}")
        # 终止“完成 `parse exam intent` 对应的业务处理”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"试卷意图解析失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"试卷意图解析失败: {str(e)}"
        # 完成“完成 `parse exam intent` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )

# 生成试卷
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
# 注册 FastAPI 路由，并声明请求方法、路径或响应配置。
@router.post("/papers/generate")
# 封装“完成 `generate exam` 对应的业务处理”流程，供接口层或其他业务服务调用。
# 定义 `generate_exam()`，实现该函数对应的接口或业务流程。
async def generate_exam(
    # 接收并使用前端已经校验的业务请求数据
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamGenerateRequest`。
    # 声明 `request` 参数（当前步骤使用的数据），类型为 `ExamGenerateRequest`。
    request: ExamGenerateRequest,
    # 取得当前登录用户，用于权限校验和数据隔离
    # 声明 `current_user` 参数（当前登录用户），类型为 `UserSchema`，默认值为 `Depends(get_current_user`。
    current_user: UserSchema = Depends(get_current_user),
    # 取得当前请求的异步数据库会话，用于读取或保存本地业务数据
    # 声明 `db` 参数（异步数据库会话），类型为 `AsyncSession`，默认值为 `Depends(get_db`。
    db: AsyncSession = Depends(get_db)
# 完成“完成 `generate exam` 对应的业务处理”所需参数的定义，下面开始执行具体业务步骤。
# 结束上方多行参数或数据结构。
) -> Any:
    """
    基于文档内容生成试卷
    type=5
    """
    # 执行 远程服务调用 主流程，并在失败时进入下方异常处理，避免返回半成品数据。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    # 开始执行可能失败的步骤，由后续异常分支统一处理。
    try:
        # 从 `app.services.exam_service` 导入 `ExamService`，供本文件调用。
        from app.services.exam_service import ExamService

        # 初始化试卷服务
        # 将 `ExamService(db)` 的结果保存到 `exam_service`，供后续流程使用。
        exam_service = ExamService(db)

        # 根据前端的流式开关选择实时输出或一次性返回。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        # 判断 `if request.stream`，决定是否执行当前分支。
        if request.stream:
            # 流式响应
            # 将 `await exam_service.generate_exam(` 的结果保存到 `stream_generator`，供后续流程使用。
            stream_generator = await exam_service.generate_exam(
                # 把 `title` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                title=request.title,
                # 把 `subject` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                subject=request.subject,
                # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                total_score=request.total_score,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                # 将 `current_user.id` 的结果保存到 `user_id`，供后续流程使用。
                user_id=current_user.id,
                # 把 `description` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                description=request.description,
                # 把 `difficulty` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                difficulty=request.difficulty,
                # 把 `duration` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                duration=request.duration,
                # 把 `question_types` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                question_types=request.question_types,
                # 把 `question_counts` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                question_counts=request.question_counts,
                # 把 `knowledge_files` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                knowledge_files=request.knowledge_files,
                # 把 `special_requirements` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                special_requirements=request.special_requirements,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                conversation_id=request.conversation_id,
                # 告诉下游是否需要边生成边返回。
                # 将 `True` 的结果保存到 `stream`，供后续流程使用。
                stream=True
            # 完成“完成 `generate exam` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )

            # 把“完成 `generate exam` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return StreamingResponse(
                # 引入或传递 `stream_generator`，供“完成 `generate exam` 对应的业务处理”的后续步骤使用。
                # 执行表达式 `stream_generator,`，完成本行明确的数据运算或控制操作。
                stream_generator,
                # 声明流式响应类型，使浏览器按照连续数据流读取。
                # 将 `"text/plain"` 的结果保存到 `media_type`，供后续流程使用。
                media_type="text/plain",
                # 保存下游 HTTP 请求所需的鉴权和内容类型。
                # 将 `{"Cache-Control": "no-cache", "Connection": "keep-alive"}` 的结果保存到 `headers`，供后续流程使用。
                headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
            # 完成“完成 `generate exam` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
        # 上方没有得到预期字段或匹配结果时启用兼容/默认方案，保证 `generate_exam` 能继续返回可处理结果。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 同步响应
            # 将 `await exam_service.generate_exam(` 的结果保存到 `result`，供后续流程使用。
            result = await exam_service.generate_exam(
                # 把 `title` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                title=request.title,
                # 把 `subject` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                subject=request.subject,
                # 把 `total_score` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                total_score=request.total_score,
                # 携带当前用户 ID，让下游服务只操作该用户的数据。
                # 将 `current_user.id` 的结果保存到 `user_id`，供后续流程使用。
                user_id=current_user.id,
                # 把 `description` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                description=request.description,
                # 把 `difficulty` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                difficulty=request.difficulty,
                # 把 `duration` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                duration=request.duration,
                # 把 `question_types` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                question_types=request.question_types,
                # 把 `question_counts` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                question_counts=request.question_counts,
                # 把 `knowledge_files` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                knowledge_files=request.knowledge_files,
                # 把 `special_requirements` 字段写入当前请求、响应或数据对象，供 `generate_exam` 后续步骤使用。
                special_requirements=request.special_requirements,
                # 把可选对话 ID 传给 Dify；有值时延续上下文，无值时创建独立任务。
                # 将 `request.conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                conversation_id=request.conversation_id,
                # 告诉下游是否需要边生成边返回。
                # 将 `False` 的结果保存到 `stream`，供后续流程使用。
                stream=False
            # 完成“完成 `generate exam` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
            # 结束上方多行参数或数据结构。
            )
            # 把“完成 `generate exam` 对应的业务处理”得到的最终业务结果返回给接口层或前端。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return result

    # 捕获“完成 `generate exam` 对应的业务处理”中的失败，记录原因或转换成上层能够识别的业务错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    # 捕获指定异常，记录或转换为上层能够识别的错误。
    except Exception as e:
        # 记录“完成 `generate exam` 对应的业务处理”的关键状态或失败原因，方便排查下游服务问题。
        # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
        logger.error(f"Error generating exam: {str(e)}")
        # 终止“完成 `generate exam` 对应的业务处理”并向前端返回明确的 HTTP 状态和失败原因。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        # 抛出异常，中止当前失败流程。
        raise HTTPException(
            # 设置返回给前端的 HTTP 状态码，使前端能够区分失败类型。
            # 将 `status.HTTP_500_INTERNAL_SERVER_ERROR` 的结果保存到 `status_code`，供后续流程使用。
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # 把具体失败原因写入 HTTP 响应，方便前端提示和开发排查。
            # 将 `f"生成试卷失败: {str(e)}"` 的结果保存到 `detail`，供后续流程使用。
            detail=f"生成试卷失败: {str(e)}"
        # 完成“完成 `generate exam` 对应的业务处理”所需的数据封装或下游调用，准备处理其结果。
        # 结束上方多行参数或数据结构。
        )    

# 把 POST /papers/submit 请求绑定到 submit_exam()，用于接收并评分一份已完成的试卷。
@router.post("/papers/submit")
async def submit_exam(
    # FastAPI 根据 ExamSubmitRequest 校验请求体，并解析出试卷 ID、考生信息、答案和试卷正文。
    request: ExamSubmitRequest,
    # Depends(get_db) 为本次请求注入 AsyncSession，供服务层查询试卷并保存考试结果。
    db: AsyncSession = Depends(get_db)
):
    """
    接收前端提交的整份答卷并交给 ExamService 自动评分。

    本接口只负责参数接收、数据库会话注入和异常转换；选择题答案比对、简答题调用
    Dify 评分、总分汇总以及考试结果落库均由 ExamService.submit_exam() 完成。
    """
    try:
        # 延迟导入试卷业务服务，避免模块加载时产生不必要的循环依赖。
        from app.services.exam_service import ExamService

        # 将本次请求的数据库会话交给服务对象，使评分流程可以读取 exams/questions 并写入 exam_results。
        exam_service = ExamService(db)

        # 调用真正的考试提交与自动评分流程，并等待异步处理完成。
        result = await exam_service.submit_exam(
            # 指定本次作答对应的试卷，服务层据此读取试卷及标准答案。
            exam_id=request.exam_id,
            # 记录参加考试的人员姓名。
            student_name=request.student_name,
            # 记录考生所属部门，作为考试结果的人员信息。
            department=request.department,
            # 传入各题的考生答案，供选择题直接比对、简答题交给 Dify 评分。
            answers=request.answers,
            # 传入前端当前显示的完整试卷内容，供服务层识别题目、标准答案和分值。
            exam_content=request.exam_content
        )

        # 把逐题得分、总分和服务层生成的结果直接返回给前端。
        return result

    except Exception as e:
        # 记录数据库、答案解析或 Dify 评分过程中抛出的原始异常，便于后端排查。
        logger.error(f"Error submitting exam: {str(e)}")
        # 将未处理异常统一转换为 HTTP 500，前端可通过 detail 显示提交失败原因。
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"提交考试失败: {str(e)}"
        )
