"""
* 文件作用
  * 这是智能培训的核心业务服务，使用 SQLAlchemy AsyncSession 管理 exams、questions 和 exam_results 表，并组合 EnhancedDocumentService、KBSelectionService 与 DifyService。它负责读取知识库正文、分配题目分值、构造结构化出题 Prompt、以 workflow_type=5 生成试卷、解析并保存题目，以及在提交考试时直接判定选择题、以 workflow_type=6 逐题评分简答题并保存完整结果。
* 类说明
  * `ExamService`：智能培训业务服务类，持有 AsyncSession 和 DifyService，集中实现试卷 CRUD、知识文档读取、AI 生成、题目解析、分值分配、自动评分、结果查询和 CSV 导出。
* 函数与方法说明
  * `ExamService.__init__()`：保存 AsyncSession 并创建 DifyService，使同一服务实例的查询、生成和评分操作共享数据库会话与 Dify 配置。
  * `ExamService.get_exam_list()`：试卷列表功能，使用 skip/limit 分页并按可选关键词过滤 exams 表，返回列表和总数量。
  * `ExamService.save_exam()`：保存试卷功能，把前端解析后的试卷配置和 questions 数组写入 exams、questions 表；同一事务提交，失败时回滚，保证试卷与题目一致。
  * `ExamService.get_exam_detail()`：按 exam_id 查询试卷，并使用 selectinload 一次加载关联 questions，按 order_index 返回完整试卷详情。
  * `ExamService.update_exam()`：按 exam_id 更新试卷字段，并重新处理需要变更的题目数据；成功后提交事务，目标不存在时返回对应失败结果。
  * `ExamService.delete_exam()`：删除指定试卷；Exam.questions 配置了 delete-orphan 级联，因此删除 exams 记录时会一并删除其 questions。
  * `ExamService.get_exam_for_share()`：无需当前用户身份即可按 ID 读取允许分享的试卷及题目，供公开答题页面展示。
  * `ExamService.get_exam_results()`：分页查询 exam_results，可按关键词、试卷 ID 和部门过滤，并计算每次考试的得分率。
  * `ExamService.get_exam_result()`：读取单次考试结果详情，返回考生、总分、实际得分、提交时间和 exam_data 中的逐题评分明细。
  * `ExamService.generate_exam()`：读取用户所选 documents.extracted_content，按单选、多选、简答 2:3:5 权重计算整数分值，构造题型数量与输出格式严格受限的 Prompt，再以 workflow_type=5 调用 Dify；流式模式逐块返回并以 [DONE] 结束。
  * `ExamService.generate_exam.fmt_scores_line()`：把某一题型的逐题分值数组转换成 Prompt 文本，例如“单选题每题分值（按题序）：5, 5, 6”；没有该题型时返回空字符串。
  * `ExamService.generate_exam.generate_stream()`：试卷生成的内部异步生成器，调用 DifyService.call_workflow_stream(workflow_type=5) 并把每个内容分片包装成 SSE data 行，结束后发送 data: [DONE]。
  * `ExamService._question_count_from_config()`：兼容 single_choice、multiple_choice、short_answer 及其简写字段，累加各题型数量得到总题数，用于计算参考文档题和面试方案导向题的 8:2 配比。
  * `ExamService._choice_question_kind()`：把中文或英文的单选、多选题型名称进行 Unicode 和大小写标准化，统一返回 single、multiple；非选择题返回 None。
  * `ExamService._valid_choice_option_ids()`：读取选择题 options 中已有的选项 ID；缺少 ID 时按位置生成 A、B、C，并统一转换为大写集合，用于过滤考生答案中的无效选项。
  * `ExamService._normalize_choice_answer()`：把考生答案和标准答案统一为合法的大写选项 ID 集合，兼容字符串、列表和不同分隔符，为选择题比较消除格式差异。
  * `ExamService._grade_choice_answer()`：对单选和多选答案直接评分：单选完全一致得满分；多选按正确选项命中和错误选项情况计算正确、部分正确或错误结果；简答题返回 None，交给 Dify。
  * `ExamService._parse_exam_content()`：解析大模型生成的试卷文本：用 *** 分隔题目、用 | 分隔题干/题型/选项/答案/分值/解析，并把选项和分值转换为前端及数据库需要的结构化字段。
  * `ExamService._allocate_question_scores()`：先计算加权题量，再用 // 得到基础整数单位分、用 % 得到余数；基础分按 2:3:5 乘权重，余数按简答题、多选题、单选题顺序从各题型末题向前每题加 1，循环直到总分完全分配。
  * `ExamService.submit_exam()`：从 exams 和 questions 读取标准答案，选择题调用 _grade_choice_answer() 本地判分，简答题逐道以 workflow_type=6 调用 Dify 并把得分限制在 0 到本题满分；汇总后把答案、逐题评分和总成绩写入 exam_results.exam_data。
  * `ExamService.export_exam_result_to_csv()`：把指定 exam_result 的考生信息、总成绩和逐题结果整理为 CSV 字符串，供接口作为附件下载。
"""

# 从 `typing` 导入 `Any, Optional, Dict, List`，供本文件调用。
from typing import Any, Optional, Dict, List
# 导入 `ast` 模块，供后续代码使用。
import ast
# 导入 `json` 模块，供后续代码使用。
import json
# 导入 `math` 模块，供后续代码使用。
import math
# 导入 `re` 模块，供后续代码使用。
import re
# 导入 `unicodedata` 模块，供后续代码使用。
import unicodedata
# 导入 `uuid` 模块，供后续代码使用。
import uuid
# 从 `datetime` 导入 `datetime`，供本文件调用。
from datetime import datetime
# 从 `sqlalchemy` 导入 `select, func, or_`，供本文件调用。
from sqlalchemy import select, func, or_
# 从 `sqlalchemy.ext.asyncio` 导入 `AsyncSession`，供本文件调用。
from sqlalchemy.ext.asyncio import AsyncSession
# 从 `sqlalchemy.orm` 导入 `selectinload`，供本文件调用。
from sqlalchemy.orm import selectinload

# 从 `app.models.exam` 导入 `Exam, Question`，供本文件调用。
from app.models.exam import Exam, Question
# 从 `app.models.exam_result` 导入 `ExamResult`，供本文件调用。
from app.models.exam_result import ExamResult
# 从 `app.services.dify_service` 导入 `DifyService`，供本文件调用。
from app.services.dify_service import DifyService
# 从 `app.services.enhanced_document_service` 导入 `EnhancedDocumentService`，供本文件调用。
from app.services.enhanced_document_service import EnhancedDocumentService
# 从 `app.core.logging` 导入 `logger`，供本文件调用。
from app.core.logging import logger
# 从 `app.services.kb_selection_service` 导入 `KBSelectionService`，供本文件调用。
from app.services.kb_selection_service import KBSelectionService

# 定义 `ExamService` 类，封装该类对应的数据字段和业务方法。
class ExamService:
    """试卷服务类"""

    # 定义 `__init__()`，实现该函数对应的接口或业务流程。
    def __init__(self, db: AsyncSession):
        # 将 `db` 的结果保存到 `self.db`，供后续流程使用。
        # 将 `db` 的结果保存到 `self.db`，供后续流程使用。
        # 将 `db` 的结果保存到 `self.db`，供后续流程使用。
        # 将 `db` 的结果保存到 `self.db`，供后续流程使用。
        self.db = db
        # 将 `DifyService()` 的结果保存到 `self.dify_service`，供后续流程使用。
        # 将 `DifyService()` 的结果保存到 `self.dify_service`，供后续流程使用。
        # 将 `DifyService()` 的结果保存到 `self.dify_service`，供后续流程使用。
        # 将 `DifyService()` 的结果保存到 `self.dify_service`，供后续流程使用。
        self.dify_service = DifyService()

    # 定义 `get_exam_list()`，实现该函数对应的接口或业务流程。
    async def get_exam_list(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `skip` 参数（分页跳过数量），类型为 `int`，默认值为 `0`。
        skip: int = 0,
        # 声明 `limit` 参数（分页返回上限），类型为 `int`，默认值为 `20`。
        limit: int = 20,
        # 声明 `search` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        search: Optional[str] = None
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        获取试卷列表

        Args:
            skip: 跳过的记录数
            limit: 每页记录数
            search: 搜索关键词

        Returns:
            包含试卷列表和分页信息的字典
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 构建查询
            # 将 `select(Exam).options(selectinload(Exam.questions))` 的结果保存到 `query`，供后续流程使用。
            query = select(Exam).options(selectinload(Exam.questions))

            # 应用搜索过滤
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            if search:
                # 将 `or_(` 的结果保存到 `search_filter`，供后续流程使用。
                search_filter = or_(
                    # 调用 `Exam.title.ilike()`，完成当前函数调用并取得其返回结果。
                    Exam.title.ilike(f"%{search}%"),
                    # 调用 `Exam.subject.ilike()`，完成当前函数调用并取得其返回结果。
                    Exam.subject.ilike(f"%{search}%"),
                    # 调用 `Exam.description.ilike()`，完成当前函数调用并取得其返回结果。
                    Exam.description.ilike(f"%{search}%")
                # 结束上方多行参数或数据结构。
                )
                # 将 `query.where(search_filter)` 的结果保存到 `query`，供后续流程使用。
                query = query.where(search_filter)

            # 获取总数
            # 将 `select(func.count(Exam.id))` 的结果保存到 `count_query`，供后续流程使用。
            count_query = select(func.count(Exam.id))
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            if search:
                # 将 `count_query.where(search_filter)` 的结果保存到 `count_query`，供后续流程使用。
                count_query = count_query.where(search_filter)

            # 将 `await self.db.execute(count_query)` 的结果保存到 `total_result`，供后续流程使用。
            total_result = await self.db.execute(count_query)
            # 将 `total_result.scalar()` 的结果保存到 `total`，供后续流程使用。
            total = total_result.scalar()

            # 应用分页和排序
            # 将 `query.order_by(Exam.created_at.desc()).offset(skip).limit(limit)` 的结果保存到 `query`，供后续流程使用。
            query = query.order_by(Exam.created_at.desc()).offset(skip).limit(limit)

            # 将 `await self.db.execute(query)` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(query)
            # 将 `result.scalars().all()` 的结果保存到 `exams`，供后续流程使用。
            exams = result.scalars().all()

            # 转换为响应格式
            # 将 `[]` 的结果保存到 `exam_list`，供后续流程使用。
            exam_list = []
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for exam in exams:
                # 将 `{` 的结果保存到 `exam_dict`，供后续流程使用。
                exam_dict = {
                    # 设置 `id` 字段，值来自 `exam.id`。
                    "id": exam.id,
                    # 设置 `title` 字段，值来自 `exam.title`。
                    "title": exam.title,
                    # 设置 `subject` 字段，值来自 `exam.subject`。
                    "subject": exam.subject,
                    # 设置 `description` 字段，值来自 `exam.description`。
                    "description": exam.description,
                    # 设置 `difficulty` 字段，值来自 `exam.difficulty`。
                    "difficulty": exam.difficulty,
                    # 设置 `duration` 字段，值来自 `exam.duration`。
                    "duration": exam.duration,
                    # 设置 `total_score` 字段，值来自 `exam.total_score`。
                    "total_score": exam.total_score,
                    # 设置 `question_types` 字段，值来自 `exam.question_types or []`。
                    "question_types": exam.question_types or [],
                    # 设置 `question_counts` 字段，值来自 `exam.question_counts or {}`。
                    "question_counts": exam.question_counts or {},
                    # 设置 `knowledge_files` 字段，值来自 `exam.knowledge_files or []`。
                    "knowledge_files": exam.knowledge_files or [],
                    # 设置 `special_requirements` 字段，值来自 `exam.special_requirements or ""`。
                    "special_requirements": exam.special_requirements or "",
                    # 设置 `questionCount` 字段，值来自 `len(exam.questions) if exam.questions else self._question_count_from_config(exam.question_counts)`。
                    "questionCount": len(exam.questions) if exam.questions else self._question_count_from_config(exam.question_counts),
                    # 设置 `created_at` 字段，值来自 `exam.created_at.isoformat() if exam.created_at else None`。
                    "created_at": exam.created_at.isoformat() if exam.created_at else None,
                    # 设置 `updated_at` 字段，值来自 `exam.updated_at.isoformat() if exam.updated_at else None`。
                    "updated_at": exam.updated_at.isoformat() if exam.updated_at else None
                # 结束上方多行参数或数据结构。
                }
                # 调用 `exam_list.append()`，保存或创建当前数据。
                exam_list.append(exam_dict)

            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {
                # 设置 `items` 字段，值来自 `exam_list`。
                "items": exam_list,
                # 设置 `total` 字段，值来自 `total`。
                "total": total,
                # 设置 `skip` 字段，值来自 `skip`。
                "skip": skip,
                # 设置 `limit` 字段，值来自 `limit`。
                "limit": limit
            # 结束上方多行参数或数据结构。
            }

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取试卷列表时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `save_exam()`，实现该函数对应的接口或业务流程。
    async def save_exam(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        exam_data: Dict[str, Any],
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        user_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        保存试卷到数据库

        Args:
            exam_data: 试卷数据
            user_id: 用户ID

        Returns:
            保存的试卷信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            if not exam_data.get('questions') and exam_data.get('content'):
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                exam_data['questions'] = self._parse_exam_content(exam_data.get('content'))

            # 创建试卷实例
            # 将 `Exam(` 的结果保存到 `exam`，供后续流程使用。
            exam = Exam(
                # 将 `exam_data['title']` 的结果保存到 `title`，供后续流程使用。
                title=exam_data['title'],
                # 将 `exam_data['subject']` 的结果保存到 `subject`，供后续流程使用。
                subject=exam_data['subject'],
                # 将 `exam_data.get('description')` 的结果保存到 `description`，供后续流程使用。
                description=exam_data.get('description'),
                # 将 `exam_data.get('difficulty')` 的结果保存到 `difficulty`，供后续流程使用。
                difficulty=exam_data.get('difficulty'),
                # 将 `exam_data.get('duration')` 的结果保存到 `duration`，供后续流程使用。
                duration=exam_data.get('duration'),
                # 将 `exam_data['total_score']` 的结果保存到 `total_score`，供后续流程使用。
                total_score=exam_data['total_score'],
                # 将 `exam_data.get('question_types')` 的结果保存到 `question_types`，供后续流程使用。
                question_types=exam_data.get('question_types'),
                # 将 `exam_data.get('question_counts')` 的结果保存到 `question_counts`，供后续流程使用。
                question_counts=exam_data.get('question_counts'),
                # 将 `[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]` 的结果保存到 `knowledge_files`，供后续流程使用。
                knowledge_files=[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])],
                # 将 `exam_data.get('special_requirements')` 的结果保存到 `special_requirements`，供后续流程使用。
                special_requirements=exam_data.get('special_requirements'),
                # 将 `exam_data.get('content')` 的结果保存到 `content`，供后续流程使用。
                content=exam_data.get('content'),
                # 将 `user_id` 的结果保存到 `created_by`，供后续流程使用。
                created_by=user_id,
                # 将 `user_id` 的结果保存到 `updated_by`，供后续流程使用。
                updated_by=user_id
            # 结束上方多行参数或数据结构。
            )

            # 保存到数据库
            # 调用 `self.db.add()`，保存或创建当前数据。
            self.db.add(exam)
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            await self.db.refresh(exam)

            # 保存结构化试题数据
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            if exam_data.get('questions'):
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for question_data in exam_data['questions']:
                    # 将 `Question(` 的结果保存到 `question`，供后续流程使用。
                    question = Question(
                        # 将 `exam.id` 的结果保存到 `exam_id`，供后续流程使用。
                        exam_id=exam.id,
                        # 将 `question_data['type']` 的结果保存到 `question_type`，供后续流程使用。
                        question_type=question_data['type'],
                        # 将 `question_data['text']` 的结果保存到 `question_text`，供后续流程使用。
                        question_text=question_data['text'],
                        # 将 `question_data.get('options')` 的结果保存到 `options`，供后续流程使用。
                        options=question_data.get('options'),
                        # 将 `question_data.get('correct_answers')` 的结果保存到 `correct_answer`，供后续流程使用。
                        correct_answer=question_data.get('correct_answers'),
                        # 将 `question_data['score']` 的结果保存到 `score`，供后续流程使用。
                        score=question_data['score'],
                        # 将 `question_data['number']` 的结果保存到 `order_index`，供后续流程使用。
                        order_index=question_data['number'],
                        # 将 `question_data.get('explanation')` 的结果保存到 `explanation`，供后续流程使用。
                        explanation=question_data.get('explanation'),
                        # 将 `user_id` 的结果保存到 `created_by`，供后续流程使用。
                        created_by=user_id,
                        # 将 `user_id` 的结果保存到 `updated_by`，供后续流程使用。
                        updated_by=user_id
                    # 结束上方多行参数或数据结构。
                    )
                    # 调用 `self.db.add()`，保存或创建当前数据。
                    self.db.add(question)

                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                await self.db.commit()

            # 返回保存的试卷信息
            # 将 `{` 的结果保存到 `saved_exam`，供后续流程使用。
            saved_exam = {
                # 设置 `id` 字段，值来自 `str(exam.id)`。
                "id": str(exam.id),
                # 设置 `title` 字段，值来自 `exam.title`。
                "title": exam.title,
                # 设置 `subject` 字段，值来自 `exam.subject`。
                "subject": exam.subject,
                # 设置 `description` 字段，值来自 `exam.description`。
                "description": exam.description,
                # 设置 `difficulty` 字段，值来自 `exam.difficulty`。
                "difficulty": exam.difficulty,
                # 设置 `duration` 字段，值来自 `exam.duration`。
                "duration": exam.duration,
                # 设置 `total_score` 字段，值来自 `exam.total_score`。
                "total_score": exam.total_score,
                # 设置 `question_types` 字段，值来自 `exam.question_types`。
                "question_types": exam.question_types,
                # 设置 `question_counts` 字段，值来自 `exam.question_counts`。
                "question_counts": exam.question_counts,
                # 设置 `knowledge_files` 字段，值来自 `exam.knowledge_files`。
                "knowledge_files": exam.knowledge_files,
                # 设置 `special_requirements` 字段，值来自 `exam.special_requirements`。
                "special_requirements": exam.special_requirements,
                # 设置 `created_at` 字段，值来自 `exam.created_at.isoformat() + "Z"`。
                "created_at": exam.created_at.isoformat() + "Z",
                # 设置 `updated_at` 字段，值来自 `exam.updated_at.isoformat() + "Z"`。
                "updated_at": exam.updated_at.isoformat() + "Z",
                # 设置 `created_by` 字段，值来自 `str(exam.created_by)`。
                "created_by": str(exam.created_by)
            # 结束上方多行参数或数据结构。
            }

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"试卷成功保存到数据库: {exam.id}")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return saved_exam

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"保存试卷到数据库时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `get_exam_detail()`，实现该函数对应的接口或业务流程。
    async def get_exam_detail(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        paper_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        获取试卷详情

        Args:
            paper_id: 试卷ID

        Returns:
            试卷详情信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 查询试卷及其关联的试题
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(Exam)
                # 调用 `options()`，完成当前函数调用并取得其返回结果。
                .options(selectinload(Exam.questions))
                # 调用 `where()`，完成当前函数调用并取得其返回结果。
                .where(Exam.id == paper_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam`，供后续流程使用。
            exam = result.scalar_one_or_none()

            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            if not exam:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("试卷不存在")

            # 构建试题数据
            # 将 `[]` 的结果保存到 `questions`，供后续流程使用。
            questions = []
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for question in sorted(exam.questions, key=lambda x: x.order_index):
                # 调用 `questions.append()`，保存或创建当前数据。
                questions.append({
                    # 设置 `id` 字段，值来自 `f"q_{question.order_index}"`。
                    "id": f"q_{question.order_index}",
                    # 设置 `number` 字段，值来自 `question.order_index`。
                    "number": question.order_index,
                    # 设置 `text` 字段，值来自 `question.question_text`。
                    "text": question.question_text,
                    # 设置 `type` 字段，值来自 `question.question_type`。
                    "type": question.question_type,
                    # 设置 `score` 字段，值来自 `question.score`。
                    "score": question.score,
                    # 设置 `correct_answers` 字段，值来自 `question.correct_answer`。
                    "correct_answers": question.correct_answer,
                    # 设置 `explanation` 字段，值来自 `question.explanation`。
                    "explanation": question.explanation,
                    # 设置 `options` 字段，值来自 `question.options or []`。
                    "options": question.options or []
                # 结束上方多行参数或数据结构。
                })

            # 返回试卷详情
            # 将 `{` 的结果保存到 `exam_detail`，供后续流程使用。
            exam_detail = {
                # 设置 `id` 字段，值来自 `str(exam.id)`。
                "id": str(exam.id),
                # 设置 `title` 字段，值来自 `exam.title`。
                "title": exam.title,
                # 设置 `subject` 字段，值来自 `exam.subject`。
                "subject": exam.subject,
                # 设置 `description` 字段，值来自 `exam.description`。
                "description": exam.description,
                # 设置 `difficulty` 字段，值来自 `exam.difficulty`。
                "difficulty": exam.difficulty,
                # 设置 `duration` 字段，值来自 `exam.duration`。
                "duration": exam.duration,
                # 设置 `total_score` 字段，值来自 `exam.total_score`。
                "total_score": exam.total_score,
                # 设置 `question_types` 字段，值来自 `exam.question_types`。
                "question_types": exam.question_types,
                # 设置 `question_counts` 字段，值来自 `exam.question_counts`。
                "question_counts": exam.question_counts,
                # 设置 `knowledge_files` 字段，值来自 `exam.knowledge_files`。
                "knowledge_files": exam.knowledge_files,
                # 设置 `special_requirements` 字段，值来自 `exam.special_requirements`。
                "special_requirements": exam.special_requirements,
                # 设置 `content` 字段，值来自 `exam.content`。
                "content": exam.content,
                # 设置 `questions` 字段，值来自 `questions`。
                "questions": questions,
                # 设置 `created_at` 字段，值来自 `exam.created_at.isoformat() + "Z"`。
                "created_at": exam.created_at.isoformat() + "Z",
                # 设置 `updated_at` 字段，值来自 `exam.updated_at.isoformat() + "Z"`。
                "updated_at": exam.updated_at.isoformat() + "Z",
                # 设置 `created_by` 字段，值来自 `str(exam.created_by)`。
                "created_by": str(exam.created_by)
            # 结束上方多行参数或数据结构。
            }

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"成功获取试卷详情: {exam.id}")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return exam_detail

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取试卷详情时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `update_exam()`，实现该函数对应的接口或业务流程。
    async def update_exam(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        paper_id: str,
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        # 声明 `exam_data` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        exam_data: Dict[str, Any],
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        user_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        更新试卷

        Args:
            paper_id: 试卷ID
            exam_data: 试卷数据
            user_id: 用户ID

        Returns:
            更新后的试卷信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            # 判断 `if not exam_data.get('questions') and exam_data.get('content')`，决定是否执行当前分支。
            if not exam_data.get('questions') and exam_data.get('content'):
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                # 将 `self._parse_exam_content(exam_data.get('content'))` 的结果保存到 `exam_data['questions']`，供后续流程使用。
                exam_data['questions'] = self._parse_exam_content(exam_data.get('content'))

            # 查找试卷及其关联的试题
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(Exam)
                # 调用 `options()`，完成当前函数调用并取得其返回结果。
                .options(selectinload(Exam.questions))
                # 调用 `where()`，完成当前函数调用并取得其返回结果。
                .where(Exam.id == paper_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam`，供后续流程使用。
            exam = result.scalar_one_or_none()

            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            if not exam:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("试卷不存在")

            # 更新试卷字段
            # 将 `exam_data['title']` 的结果保存到 `exam.title`，供后续流程使用。
            # 将 `exam_data['title']` 的结果保存到 `exam.title`，供后续流程使用。
            # 将 `exam_data['title']` 的结果保存到 `exam.title`，供后续流程使用。
            exam.title = exam_data['title']
            # 将 `exam_data['subject']` 的结果保存到 `exam.subject`，供后续流程使用。
            # 将 `exam_data['subject']` 的结果保存到 `exam.subject`，供后续流程使用。
            # 将 `exam_data['subject']` 的结果保存到 `exam.subject`，供后续流程使用。
            # 将 `exam_data['subject']` 的结果保存到 `exam.subject`，供后续流程使用。
            exam.subject = exam_data['subject']
            # 将 `exam_data.get('description')` 的结果保存到 `exam.description`，供后续流程使用。
            # 将 `exam_data.get('description')` 的结果保存到 `exam.description`，供后续流程使用。
            # 将 `exam_data.get('description')` 的结果保存到 `exam.description`，供后续流程使用。
            # 将 `exam_data.get('description')` 的结果保存到 `exam.description`，供后续流程使用。
            exam.description = exam_data.get('description')
            # 将 `exam_data.get('difficulty')` 的结果保存到 `exam.difficulty`，供后续流程使用。
            # 将 `exam_data.get('difficulty')` 的结果保存到 `exam.difficulty`，供后续流程使用。
            # 将 `exam_data.get('difficulty')` 的结果保存到 `exam.difficulty`，供后续流程使用。
            # 将 `exam_data.get('difficulty')` 的结果保存到 `exam.difficulty`，供后续流程使用。
            exam.difficulty = exam_data.get('difficulty')
            # 将 `exam_data.get('duration')` 的结果保存到 `exam.duration`，供后续流程使用。
            # 将 `exam_data.get('duration')` 的结果保存到 `exam.duration`，供后续流程使用。
            # 将 `exam_data.get('duration')` 的结果保存到 `exam.duration`，供后续流程使用。
            # 将 `exam_data.get('duration')` 的结果保存到 `exam.duration`，供后续流程使用。
            exam.duration = exam_data.get('duration')
            # 将 `exam_data['total_score']` 的结果保存到 `exam.total_score`，供后续流程使用。
            # 将 `exam_data['total_score']` 的结果保存到 `exam.total_score`，供后续流程使用。
            # 将 `exam_data['total_score']` 的结果保存到 `exam.total_score`，供后续流程使用。
            # 将 `exam_data['total_score']` 的结果保存到 `exam.total_score`，供后续流程使用。
            exam.total_score = exam_data['total_score']
            # 将 `exam_data.get('question_types')` 的结果保存到 `exam.question_types`，供后续流程使用。
            # 将 `exam_data.get('question_types')` 的结果保存到 `exam.question_types`，供后续流程使用。
            # 将 `exam_data.get('question_types')` 的结果保存到 `exam.question_types`，供后续流程使用。
            # 将 `exam_data.get('question_types')` 的结果保存到 `exam.question_types`，供后续流程使用。
            exam.question_types = exam_data.get('question_types')
            # 将 `exam_data.get('question_counts')` 的结果保存到 `exam.question_counts`，供后续流程使用。
            # 将 `exam_data.get('question_counts')` 的结果保存到 `exam.question_counts`，供后续流程使用。
            # 将 `exam_data.get('question_counts')` 的结果保存到 `exam.question_counts`，供后续流程使用。
            # 将 `exam_data.get('question_counts')` 的结果保存到 `exam.question_counts`，供后续流程使用。
            exam.question_counts = exam_data.get('question_counts')
            # 将 `[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]` 的结果保存到 `exam.knowledge_files`，供后续流程使用。
            # 将 `[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]` 的结果保存到 `exam.knowledge_files`，供后续流程使用。
            # 将 `[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]` 的结果保存到 `exam.knowledge_files`，供后续流程使用。
            # 将 `[{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]` 的结果保存到 `exam.knowledge_files`，供后续流程使用。
            exam.knowledge_files = [{"id": kf["id"], "fileName": kf["fileName"]} for kf in exam_data.get('knowledge_files', [])]
            # 将 `exam_data.get('special_requirements')` 的结果保存到 `exam.special_requirements`，供后续流程使用。
            # 将 `exam_data.get('special_requirements')` 的结果保存到 `exam.special_requirements`，供后续流程使用。
            # 将 `exam_data.get('special_requirements')` 的结果保存到 `exam.special_requirements`，供后续流程使用。
            # 将 `exam_data.get('special_requirements')` 的结果保存到 `exam.special_requirements`，供后续流程使用。
            exam.special_requirements = exam_data.get('special_requirements')
            # 将 `exam_data.get('content')` 的结果保存到 `exam.content`，供后续流程使用。
            # 将 `exam_data.get('content')` 的结果保存到 `exam.content`，供后续流程使用。
            # 将 `exam_data.get('content')` 的结果保存到 `exam.content`，供后续流程使用。
            # 将 `exam_data.get('content')` 的结果保存到 `exam.content`，供后续流程使用。
            exam.content = exam_data.get('content')
            # 将 `user_id` 的结果保存到 `exam.updated_by`，供后续流程使用。
            # 将 `user_id` 的结果保存到 `exam.updated_by`，供后续流程使用。
            # 将 `user_id` 的结果保存到 `exam.updated_by`，供后续流程使用。
            # 将 `user_id` 的结果保存到 `exam.updated_by`，供后续流程使用。
            exam.updated_by = user_id

            # 提交更改
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.refresh()` 完成，取得数据库或外部服务处理结果。
            await self.db.refresh(exam)

            # 更新试题数据
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            # 判断 `if exam_data.get('questions')`，决定是否执行当前分支。
            if exam_data.get('questions'):
                # 清空现有试题（利用cascade特性自动删除）
                # 确保关系已加载后再清空
                # 判断 `if exam.questions`，决定是否执行当前分支。
                # 判断 `if exam.questions`，决定是否执行当前分支。
                # 判断 `if exam.questions`，决定是否执行当前分支。
                if exam.questions:
                    # 调用 `exam.questions.clear()`，完成当前函数调用并取得其返回结果。
                    exam.questions.clear()
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                await self.db.commit()

                # 添加新试题
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for question_data in exam_data['questions']:
                    # 将 `Question(` 的结果保存到 `question`，供后续流程使用。
                    question = Question(
                        # 将 `exam.id` 的结果保存到 `exam_id`，供后续流程使用。
                        exam_id=exam.id,
                        # 将 `question_data['type']` 的结果保存到 `question_type`，供后续流程使用。
                        question_type=question_data['type'],
                        # 将 `question_data['text']` 的结果保存到 `question_text`，供后续流程使用。
                        question_text=question_data['text'],
                        # 将 `question_data.get('options')` 的结果保存到 `options`，供后续流程使用。
                        options=question_data.get('options'),
                        # 将 `question_data.get('correct_answers')` 的结果保存到 `correct_answer`，供后续流程使用。
                        correct_answer=question_data.get('correct_answers'),
                        # 将 `question_data['score']` 的结果保存到 `score`，供后续流程使用。
                        score=question_data['score'],
                        # 将 `question_data['number']` 的结果保存到 `order_index`，供后续流程使用。
                        order_index=question_data['number'],
                        # 将 `question_data.get('explanation')` 的结果保存到 `explanation`，供后续流程使用。
                        explanation=question_data.get('explanation'),
                        # 将 `user_id` 的结果保存到 `created_by`，供后续流程使用。
                        created_by=user_id,
                        # 将 `user_id` 的结果保存到 `updated_by`，供后续流程使用。
                        updated_by=user_id
                    # 结束上方多行参数或数据结构。
                    )
                    # 调用 `self.db.add()`，保存或创建当前数据。
                    self.db.add(question)

                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
                await self.db.commit()

            # 返回更新后的试卷信息
            # 将 `{` 的结果保存到 `updated_exam`，供后续流程使用。
            updated_exam = {
                # 设置 `id` 字段，值来自 `str(exam.id)`。
                "id": str(exam.id),
                # 设置 `title` 字段，值来自 `exam.title`。
                "title": exam.title,
                # 设置 `subject` 字段，值来自 `exam.subject`。
                "subject": exam.subject,
                # 设置 `description` 字段，值来自 `exam.description`。
                "description": exam.description,
                # 设置 `difficulty` 字段，值来自 `exam.difficulty`。
                "difficulty": exam.difficulty,
                # 设置 `duration` 字段，值来自 `exam.duration`。
                "duration": exam.duration,
                # 设置 `total_score` 字段，值来自 `exam.total_score`。
                "total_score": exam.total_score,
                # 设置 `question_types` 字段，值来自 `exam.question_types`。
                "question_types": exam.question_types,
                # 设置 `question_counts` 字段，值来自 `exam.question_counts`。
                "question_counts": exam.question_counts,
                # 设置 `knowledge_files` 字段，值来自 `exam.knowledge_files`。
                "knowledge_files": exam.knowledge_files,
                # 设置 `special_requirements` 字段，值来自 `exam.special_requirements`。
                "special_requirements": exam.special_requirements,
                # 设置 `created_at` 字段，值来自 `exam.created_at.isoformat() + "Z"`。
                "created_at": exam.created_at.isoformat() + "Z",
                # 设置 `updated_at` 字段，值来自 `exam.updated_at.isoformat() + "Z"`。
                "updated_at": exam.updated_at.isoformat() + "Z",
                # 设置 `created_by` 字段，值来自 `str(exam.created_by)`。
                "created_by": str(exam.created_by)
            # 结束上方多行参数或数据结构。
            }

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"试卷更新成功: {paper_id}")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return updated_exam

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"更新试卷时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `delete_exam()`，实现该函数对应的接口或业务流程。
    async def delete_exam(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        paper_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        删除试卷

        Args:
            paper_id: 试卷ID

        Returns:
            删除结果信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 查找试卷
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(Exam).where(Exam.id == paper_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam`，供后续流程使用。
            exam = result.scalar_one_or_none()

            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            if not exam:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("试卷不存在")

            # 删除试卷
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.delete()` 完成，取得数据库或外部服务处理结果。
            await self.db.delete(exam)
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"试卷删除成功: {paper_id}")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {"message": "试卷删除成功", "paper_id": paper_id}

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.rollback()` 完成，取得数据库或外部服务处理结果。
            await self.db.rollback()
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"删除试卷时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `get_exam_for_share()`，实现该函数对应的接口或业务流程。
    async def get_exam_for_share(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `paper_id` 参数（当前步骤使用的数据），类型为 `str`。
        paper_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        获取用于分享的试卷信息（无需认证）

        Args:
            paper_id: 试卷ID

        Returns:
            试卷信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 查找试卷及其关联的试题
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(Exam)
                # 调用 `options()`，完成当前函数调用并取得其返回结果。
                .options(selectinload(Exam.questions))
                # 调用 `where()`，完成当前函数调用并取得其返回结果。
                .where(Exam.id == paper_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam`，供后续流程使用。
            exam = result.scalar_one_or_none()

            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            if not exam:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("试卷不存在")

            # 构建试题列表
            # 将 `[]` 的结果保存到 `questions`，供后续流程使用。
            questions = []
            # 判断 `if exam.questions`，决定是否执行当前分支。
            # 判断 `if exam.questions`，决定是否执行当前分支。
            # 判断 `if exam.questions`，决定是否执行当前分支。
            # 判断 `if exam.questions`，决定是否执行当前分支。
            if exam.questions:
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for question in exam.questions:
                    # 将 `{` 的结果保存到 `question_data`，供后续流程使用。
                    question_data = {
                        # 设置 `id` 字段，值来自 `question.id`。
                        "id": question.id,
                        # 设置 `question_text` 字段，值来自 `question.question_text`。
                        "question_text": question.question_text,
                        # 设置 `question_type` 字段，值来自 `question.question_type`。
                        "question_type": question.question_type,
                        # 设置 `score` 字段，值来自 `question.score`。
                        "score": question.score,
                        # 设置 `options` 字段，值来自 `question.options if question.options else []`。
                        "options": question.options if question.options else []
                    # 结束上方多行参数或数据结构。
                    }
                    # 调用 `questions.append()`，保存或创建当前数据。
                    questions.append(question_data)

            # 返回试卷信息
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {
                # 设置 `id` 字段，值来自 `exam.id`。
                "id": exam.id,
                # 设置 `title` 字段，值来自 `exam.title`。
                "title": exam.title,
                # 设置 `subject` 字段，值来自 `exam.subject`。
                "subject": exam.subject,
                # 设置 `description` 字段，值来自 `exam.description`。
                "description": exam.description,
                # 设置 `difficulty` 字段，值来自 `exam.difficulty`。
                "difficulty": exam.difficulty,
                # 设置 `duration` 字段，值来自 `exam.duration`。
                "duration": exam.duration,
                # 设置 `total_score` 字段，值来自 `exam.total_score`。
                "total_score": exam.total_score,
                # 设置 `content` 字段，值来自 `exam.content`。
                "content": exam.content,
                # 设置 `questions` 字段，值来自 `questions`。
                "questions": questions,
                # 设置 `created_at` 字段，值来自 `exam.created_at.isoformat() if exam.created_at else None`。
                "created_at": exam.created_at.isoformat() if exam.created_at else None
            # 结束上方多行参数或数据结构。
            }

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取分享试卷时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `get_exam_results()`，实现该函数对应的接口或业务流程。
    async def get_exam_results(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `page` 参数（当前步骤使用的数据），类型为 `int`，默认值为 `1`。
        page: int = 1,
        # 声明 `page_size` 参数（当前步骤使用的数据），类型为 `int`，默认值为 `20`。
        page_size: int = 20,
        # 声明 `search` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        search: Optional[str] = None,
        # 声明 `exam_id` 参数（试卷唯一标识），类型为 `Optional[str]`，默认值为 `None`。
        exam_id: Optional[str] = None,
        # 声明 `department` 参数（考生部门），类型为 `Optional[str]`，默认值为 `None`。
        department: Optional[str] = None
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        获取考试结果列表，支持分页和筛选

        Args:
            page: 页码
            page_size: 每页数量
            search: 搜索关键词
            exam_id: 考试ID筛选
            department: 部门筛选

        Returns:
            考试结果列表和分页信息
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 构建查询条件
            # 将 `select(ExamResult)` 的结果保存到 `query`，供后续流程使用。
            query = select(ExamResult)

            # 添加搜索条件
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            if search:
                # 将 `or_(` 的结果保存到 `search_filter`，供后续流程使用。
                search_filter = or_(
                    # 调用 `ExamResult.student_name.ilike()`，完成当前函数调用并取得其返回结果。
                    ExamResult.student_name.ilike(f"%{search}%"),
                    # 调用 `ExamResult.exam_name.ilike()`，完成当前函数调用并取得其返回结果。
                    ExamResult.exam_name.ilike(f"%{search}%")
                # 结束上方多行参数或数据结构。
                )
                # 将 `query.where(search_filter)` 的结果保存到 `query`，供后续流程使用。
                query = query.where(search_filter)

            # 添加考试ID筛选
            # 判断 `if exam_id`，决定是否执行当前分支。
            # 判断 `if exam_id`，决定是否执行当前分支。
            # 判断 `if exam_id`，决定是否执行当前分支。
            if exam_id:
                # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
                query = query.where(ExamResult.exam_id == exam_id)

            # 添加部门筛选
            # 判断 `if department`，决定是否执行当前分支。
            # 判断 `if department`，决定是否执行当前分支。
            # 判断 `if department`，决定是否执行当前分支。
            if department:
                # 调用 `query.where()`，完成当前函数调用并取得其返回结果。
                query = query.where(ExamResult.department == department)

            # 计算总数
            # 将 `select(func.count(ExamResult.id))` 的结果保存到 `count_query`，供后续流程使用。
            count_query = select(func.count(ExamResult.id))
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            # 判断 `if search`，决定是否执行当前分支。
            if search:
                # 将 `count_query.where(search_filter)` 的结果保存到 `count_query`，供后续流程使用。
                count_query = count_query.where(search_filter)
            # 判断 `if exam_id`，决定是否执行当前分支。
            # 判断 `if exam_id`，决定是否执行当前分支。
            # 判断 `if exam_id`，决定是否执行当前分支。
            # 判断 `if exam_id`，决定是否执行当前分支。
            if exam_id:
                # 调用 `count_query.where()`，完成当前函数调用并取得其返回结果。
                count_query = count_query.where(ExamResult.exam_id == exam_id)
            # 判断 `if department`，决定是否执行当前分支。
            # 判断 `if department`，决定是否执行当前分支。
            # 判断 `if department`，决定是否执行当前分支。
            # 判断 `if department`，决定是否执行当前分支。
            if department:
                # 调用 `count_query.where()`，完成当前函数调用并取得其返回结果。
                count_query = count_query.where(ExamResult.department == department)

            # 将 `await self.db.execute(count_query)` 的结果保存到 `total_result`，供后续流程使用。
            total_result = await self.db.execute(count_query)
            # 将 `total_result.scalar()` 的结果保存到 `total`，供后续流程使用。
            total = total_result.scalar()

            # 应用分页
            # 将 `(page - 1) * page_size` 的结果保存到 `offset`，供后续流程使用。
            offset = (page - 1) * page_size
            # 将 `query.order_by(ExamResult.submit_time.desc()).offset(offset).limit(page_size)` 的结果保存到 `query`，供后续流程使用。
            query = query.order_by(ExamResult.submit_time.desc()).offset(offset).limit(page_size)

            # 将 `await self.db.execute(query)` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(query)
            # 将 `result.scalars().all()` 的结果保存到 `exam_results`，供后续流程使用。
            exam_results = result.scalars().all()

            # 构建响应数据
            # 将 `[]` 的结果保存到 `results_list`，供后续流程使用。
            results_list = []
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for exam_result in exam_results:
                # 调用 `results_list.append()`，保存或创建当前数据。
                results_list.append({
                    # 设置 `id` 字段，值来自 `str(exam_result.id)`。
                    "id": str(exam_result.id),
                    # 设置 `exam_id` 字段，值来自 `str(exam_result.exam_id) if exam_result.exam_id else None`。
                    "exam_id": str(exam_result.exam_id) if exam_result.exam_id else None,
                    # 设置 `exam_name` 字段，值来自 `exam_result.exam_name`。
                    "exam_name": exam_result.exam_name,
                    # 设置 `student_name` 字段，值来自 `exam_result.student_name`。
                    "student_name": exam_result.student_name,
                    # 设置 `department` 字段，值来自 `exam_result.department`。
                    "department": exam_result.department,
                    # 设置 `total_possible_score` 字段，值来自 `exam_result.total_possible_score`。
                    "total_possible_score": exam_result.total_possible_score,
                    # 设置 `total_actual_score` 字段，值来自 `exam_result.total_actual_score`。
                    "total_actual_score": exam_result.total_actual_score,
                    # 设置 `score_percentage` 字段，值来自 `round((exam_result.total_actual_score / exam_result.total_possible_score) * 100, 2) if exam_result.total_possible_score > 0 else 0`。
                    "score_percentage": round((exam_result.total_actual_score / exam_result.total_possible_score) * 100, 2) if exam_result.total_possible_score > 0 else 0,
                    # 设置 `submit_time` 字段，值来自 `exam_result.submit_time.isoformat() + "Z" if exam_result.submit_time else None`。
                    "submit_time": exam_result.submit_time.isoformat() + "Z" if exam_result.submit_time else None,
                    # 设置 `status` 字段，值来自 `exam_result.status`。
                    "status": exam_result.status
                # 结束上方多行参数或数据结构。
                })
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"成功获取考试结果列表，第 {page} 页，共 {total} 条结果")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {
                # 设置 `items` 字段，值来自 `results_list`。
                "items": results_list,
                # 设置 `total` 字段，值来自 `total`。
                "total": total,
                # 设置 `page` 字段，值来自 `page`。
                "page": page,
                # 设置 `page_size` 字段，值来自 `page_size`。
                "page_size": page_size
            # 结束上方多行参数或数据结构。
            }

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取考试结果时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `get_exam_result()`，实现该函数对应的接口或业务流程。
    async def get_exam_result(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        result_id: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        获取考试结果详情

        Args:
            result_id: 考试结果ID

        Returns:
            考试结果详情
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 查找考试结果
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(ExamResult).where(ExamResult.id == result_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam_result`，供后续流程使用。
            exam_result = result.scalar_one_or_none()

            # 判断 `if not exam_result`，决定是否执行当前分支。
            # 判断 `if not exam_result`，决定是否执行当前分支。
            # 判断 `if not exam_result`，决定是否执行当前分支。
            # 判断 `if not exam_result`，决定是否执行当前分支。
            if not exam_result:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("考试结果不存在")
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"成功获取考试结果详情: {result_id}")
            # 返回考试结果详情
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {
                # 设置 `id` 字段，值来自 `str(exam_result.id)`。
                "id": str(exam_result.id),
                # 设置 `exam_id` 字段，值来自 `str(exam_result.exam_id) if exam_result.exam_id else None`。
                "exam_id": str(exam_result.exam_id) if exam_result.exam_id else None,
                # 设置 `exam_name` 字段，值来自 `exam_result.exam_name`。
                "exam_name": exam_result.exam_name,
                # 设置 `student_name` 字段，值来自 `exam_result.student_name`。
                "student_name": exam_result.student_name,
                # 设置 `department` 字段，值来自 `exam_result.department`。
                "department": exam_result.department,
                # 设置 `total_possible_score` 字段，值来自 `exam_result.total_possible_score`。
                "total_possible_score": exam_result.total_possible_score,
                # 设置 `total_actual_score` 字段，值来自 `exam_result.total_actual_score`。
                "total_actual_score": exam_result.total_actual_score,
                # 设置 `score_percentage` 字段，值来自 `round((exam_result.total_actual_score / exam_result.total_possible_score) * 100, 2) if exam_result.total_possible_score > 0 else 0`。
                "score_percentage": round((exam_result.total_actual_score / exam_result.total_possible_score) * 100, 2) if exam_result.total_possible_score > 0 else 0,
                # 设置 `exam_data` 字段，值来自 `exam_result.exam_data`。
                "exam_data": exam_result.exam_data,
                # 设置 `submit_time` 字段，值来自 `exam_result.submit_time.isoformat() + "Z" if exam_result.submit_time else None`。
                "submit_time": exam_result.submit_time.isoformat() + "Z" if exam_result.submit_time else None,
                # 设置 `status` 字段，值来自 `exam_result.status`。
                "status": exam_result.status
            # 结束上方多行参数或数据结构。
            }

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"获取考试结果详情时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `generate_exam()`，实现该函数对应的接口或业务流程。
    async def generate_exam(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `title` 参数（试卷标题），类型为 `str`。
        # 声明 `title` 参数（试卷标题），类型为 `str`。
        # 声明 `title` 参数（试卷标题），类型为 `str`。
        # 声明 `title` 参数（试卷标题），类型为 `str`。
        title: str,
        # 声明 `subject` 参数（考试科目），类型为 `str`。
        # 声明 `subject` 参数（考试科目），类型为 `str`。
        # 声明 `subject` 参数（考试科目），类型为 `str`。
        # 声明 `subject` 参数（考试科目），类型为 `str`。
        subject: str,
        # 声明 `total_score` 参数（试卷总分），类型为 `int`。
        # 声明 `total_score` 参数（试卷总分），类型为 `int`。
        # 声明 `total_score` 参数（试卷总分），类型为 `int`。
        # 声明 `total_score` 参数（试卷总分），类型为 `int`。
        total_score: int,
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `user_id` 参数（当前步骤使用的数据），类型为 `str`。
        user_id: str,
        # 声明 `description` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        description: Optional[str] = None,
        # 声明 `difficulty` 参数（试卷难度），类型为 `Optional[str]`，默认值为 `None`。
        difficulty: Optional[str] = None,
        # 声明 `duration` 参数（考试时长（分钟）），类型为 `Optional[int]`，默认值为 `None`。
        duration: Optional[int] = None,
        # 声明 `question_types` 参数（当前步骤使用的数据），类型为 `Optional[List[str]]`，默认值为 `None`。
        question_types: Optional[List[str]] = None,
        # 声明 `question_counts` 参数（各题型数量），类型为 `Optional[Dict[str`。
        question_counts: Optional[Dict[str, int]] = None,
        # 声明 `knowledge_files` 参数（选中的知识库文档），类型为 `Optional[List[Dict[str`。
        knowledge_files: Optional[List[Dict[str, Any]]] = None,
        # 声明 `special_requirements` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        special_requirements: Optional[str] = None,
        # 声明 `conversation_id` 参数（当前步骤使用的数据），类型为 `Optional[str]`，默认值为 `None`。
        conversation_id: Optional[str] = None,
        # 声明 `stream` 参数（当前步骤使用的数据），类型为 `bool`，默认值为 `True`。
        stream: bool = True
    # 结束上方多行参数或数据结构。
    ) -> Any:
        """
        生成试卷

        Args:
            title: 试卷标题
            subject: 科目
            total_score: 总分
            user_id: 用户ID
            description: 试卷描述
            difficulty: 难度等级
            duration: 考试时长
            question_types: 题型列表
            question_counts: 题目数量配置
            knowledge_files: 知识库文件
            special_requirements: 特殊要求
            conversation_id: 对话ID
            stream: 是否流式输出

        Returns:
            试卷生成结果
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 初始化文档服务
            # 将 `EnhancedDocumentService(self.db)` 的结果保存到 `document_service`，供后续流程使用。
            document_service = EnhancedDocumentService(self.db)

            # 若未提供知识库文档，自动选择最匹配文档
            # 判断 `if not knowledge_files`，决定是否执行当前分支。
            # 判断 `if not knowledge_files`，决定是否执行当前分支。
            # 判断 `if not knowledge_files`，决定是否执行当前分支。
            if not knowledge_files:
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                try:
                    # 将 `KBSelectionService(self.db)` 的结果保存到 `selector`，供后续流程使用。
                    selector = KBSelectionService(self.db)
                    # 将 `" ".join([` 的结果保存到 `selection_question`，供后续流程使用。
                    selection_question = " ".join([
                        # 执行表达式 `subject or "",`，完成本行明确的数据运算或控制操作。
                        subject or "",
                        # 执行表达式 `title or "",`，完成本行明确的数据运算或控制操作。
                        title or "",
                        # 执行表达式 `description or "",`，完成本行明确的数据运算或控制操作。
                        description or "",
                        # 执行表达式 `special_requirements or "",`，完成本行明确的数据运算或控制操作。
                        special_requirements or "",
                    # 调用 `strip()`，完成当前函数调用并取得其返回结果。
                    ]).strip()
                    # 将 `await selector.select_kb_for_question(` 的结果保存到 `selection`，供后续流程使用。
                    selection = await selector.select_kb_for_question(
                        # 将 `selection_question` 的结果保存到 `question`，供后续流程使用。
                        question=selection_question,
                        # 将 `user_id` 的结果保存到 `user_id`，供后续流程使用。
                        user_id=user_id,
                        # 将 `200` 的结果保存到 `max_candidates`，供后续流程使用。
                        max_candidates=200,
                    # 结束上方多行参数或数据结构。
                    )
                    # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
                    # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
                    # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
                    # 判断 `if selection and selection.get("document_id")`，决定是否执行当前分支。
                    if selection and selection.get("document_id"):
                        # 将 `[str(selection.get("document_id"))]` 的结果保存到 `knowledge_files`，供后续流程使用。
                        knowledge_files = [str(selection.get("document_id"))]
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                except Exception as se:
                    # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                    logger.warning(f"知识库自动选择失败: {se}")

            # 读取文档内容
            # 将 `[]` 的结果保存到 `file_contents`，供后续流程使用。
            file_contents = []
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            if knowledge_files:
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for file_info in knowledge_files:
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    try:
                        # 兼容不同的文件信息格式
                        # 将 `file_info.get('id') if isinstance(file_info, dict) else file_info` 的结果保存到 `file_id`，供后续流程使用。
                        file_id = file_info.get('id') if isinstance(file_info, dict) else file_info
                        # 获取文档信息
                        # 将 `await document_service.get_by_id(file_id, user_id)` 的结果保存到 `document`，供后续流程使用。
                        document = await document_service.get_by_id(file_id, user_id)
                        # 判断 `if document`，决定是否执行当前分支。
                        # 判断 `if document`，决定是否执行当前分支。
                        # 判断 `if document`，决定是否执行当前分支。
                        # 判断 `if document`，决定是否执行当前分支。
                        if document:
                            # 判断 `if document.extracted_content`，决定是否执行当前分支。
                            # 判断 `if document.extracted_content`，决定是否执行当前分支。
                            # 判断 `if document.extracted_content`，决定是否执行当前分支。
                            # 判断 `if document.extracted_content`，决定是否执行当前分支。
                            if document.extracted_content:
                                # 调用 `file_contents.append()`，保存或创建当前数据。
                                file_contents.append({
                                    # 设置 `filename` 字段，值来自 `document.filename`。
                                    "filename": document.filename,
                                    # 设置 `content` 字段，值来自 `document.extracted_content`。
                                    "content": document.extracted_content
                                # 结束上方多行参数或数据结构。
                                })
                            # 前面的条件不成立时执行当前备用分支。
                            # 前面的条件不成立时执行当前备用分支。
                            # 前面的条件不成立时执行当前备用分支。
                            # 前面的条件不成立时执行当前备用分支。
                            else:
                                # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                                logger.warning(f"文档 {file_id} 内容为空")
                        # 前面的条件不成立时执行当前备用分支。
                        # 前面的条件不成立时执行当前备用分支。
                        # 前面的条件不成立时执行当前备用分支。
                        # 前面的条件不成立时执行当前备用分支。
                        else:
                            # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                            logger.warning(f"未找到文档 {file_id}")
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    except Exception as e:
                        # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                        logger.warning(f"读取文档 {file_id} 失败: {e}")
                        # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                        continue

            # 添加日志记录文档内容状态
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"已处理 {len(file_contents)} 个文档，总共请求: {len(knowledge_files) if knowledge_files else 0}")
            # 判断 `if not file_contents and knowledge_files`，决定是否执行当前分支。
            # 判断 `if not file_contents and knowledge_files`，决定是否执行当前分支。
            # 判断 `if not file_contents and knowledge_files`，决定是否执行当前分支。
            # 判断 `if not file_contents and knowledge_files`，决定是否执行当前分支。
            if not file_contents and knowledge_files:
                # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                logger.warning("未提取到文档内容，文档可能为空或不可读")

            # 合并文档内容
            # 将 `""` 的结果保存到 `combined_content`，供后续流程使用。
            combined_content = ""
            # 判断 `if file_contents`，决定是否执行当前分支。
            # 判断 `if file_contents`，决定是否执行当前分支。
            # 判断 `if file_contents`，决定是否执行当前分支。
            # 判断 `if file_contents`，决定是否执行当前分支。
            if file_contents:
                # 将 `[]` 的结果保存到 `content_parts`，供后续流程使用。
                content_parts = []
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for file_info in file_contents:
                    # 调用 `content_parts.append()`，保存或创建当前数据。
                    content_parts.append(f"=== {file_info['filename']} ===\n{file_info['content']}")
                # 将 `"\n\n".join(content_parts)` 的结果保存到 `combined_content`，供后续流程使用。
                combined_content = "\n\n".join(content_parts)
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 如果没有文档内容，记录警告日志
                # 调用 `logger.warning()`，完成当前函数调用并取得其返回结果。
                logger.warning("没有可用的文档内容用于生成试卷")
                # 将 `"未提供文档内容"` 的结果保存到 `combined_content`，供后续流程使用。
                combined_content = "未提供文档内容"

            # 分值分配算法（按 2:3:5 权重，并将剩余分数依次分配给最后几题）
            # 将 `self._allocate_question_scores(total_score, question_counts or {})` 的结果保存到 `question_scores`，供后续流程使用。
            question_scores = self._allocate_question_scores(total_score, question_counts or {})

            # 构建试卷生成查询
            # 将 `bool(` 的结果保存到 `has_interview_plan_context`，供后续流程使用。
            has_interview_plan_context = bool(
                # 执行表达式 `special_requirements`，完成本行明确的数据运算或控制操作。
                special_requirements
                # 调用 `and()`，完成当前函数调用并取得其返回结果。
                and ("当前面试方案" in special_requirements or "面试方案" in special_requirements)
            # 结束上方多行参数或数据结构。
            )
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            if has_interview_plan_context:
                # 将 `self._question_count_from_config(question_counts)` 的结果保存到 `total_question_count`，供后续流程使用。
                total_question_count = self._question_count_from_config(question_counts)
                # 将 `max(1, round(total_question_count * 0.8)) if total_question_count else 0` 的结果保存到 `document_question_count`，供后续流程使用。
                document_question_count = max(1, round(total_question_count * 0.8)) if total_question_count else 0
                # 将 `max(1, total_question_count - document_question_count) if total_question_count > 1 else 0` 的结果保存到 `interview_question_count`，供后续流程使用。
                interview_question_count = max(1, total_question_count - document_question_count) if total_question_count > 1 else 0
                # 判断 `if document_question_count + interview_question_count > total_question_count and total_question_count`，决定是否执行当前分支。
                # 判断 `if document_question_count + interview_question_count > total_question_count and total_question_count`，决定是否执行当前分支。
                # 判断 `if document_question_count + interview_question_count > total_question_count and total_question_count`，决定是否执行当前分支。
                # 判断 `if document_question_count + interview_question_count > total_question_count and total_question_count`，决定是否执行当前分支。
                if document_question_count + interview_question_count > total_question_count and total_question_count:
                    # 将 `max(1, total_question_count - interview_question_count)` 的结果保存到 `document_question_count`，供后续流程使用。
                    document_question_count = max(1, total_question_count - interview_question_count)
                # 将 `[` 的结果保存到 `query_parts`，供后续流程使用。
                query_parts = [
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"请基于下方【参考文档】生成一份{subject}笔试试卷，并参考【面试方案】调整出题重点。",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "题目来源配比必须接近 8:2：约 80% 题目来自【参考文档】知识点，约 20% 题目来自【面试方案】中的能力验证点、风险点或追问方向。",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "如果总题数较少，至少生成 1 道面试方案导向题，其余题目来自参考文档。",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "参考文档题：题干、选项、参考答案和解析必须主要依据【参考文档】。",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "面试方案导向题：可以围绕面试方案中的能力短板、风险点、追问方向设计情境题/简答题，但仍应尽量结合参考文档中的业务或知识背景。",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "禁止把面试流程、面试安排、候选人评价原文直接改写成试题。",
                # 结束上方多行参数或数据结构。
                ]
                # 判断 `if total_question_count`，决定是否执行当前分支。
                # 判断 `if total_question_count`，决定是否执行当前分支。
                # 判断 `if total_question_count`，决定是否执行当前分支。
                # 判断 `if total_question_count`，决定是否执行当前分支。
                if total_question_count:
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append(
                        # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                        f"本次共 {total_question_count} 题，请生成约 {document_question_count} 道参考文档题、"
                        # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                        f"{interview_question_count} 道面试方案导向题。"
                    # 结束上方多行参数或数据结构。
                    )
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 将 `[f"请基于以下文档内容生成一份{subject}试卷"]` 的结果保存到 `query_parts`，供后续流程使用。
                query_parts = [f"请基于以下文档内容生成一份{subject}试卷"]

            # 调用 `query_parts.extend()`，完成当前函数调用并取得其返回结果。
            query_parts.extend([
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "请严格按以下结构化格式输出，每题之间用 *** 分隔，每题只占一条记录：",
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "题干|题型|选项|正确答案|分值|解析",
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "题型只能使用中文：单选题、多选题、简答题。不要输出 single_choice、multiple_choice、short_answer。",
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "单选题/多选题的选项用中文分号；分隔，例如：选项A；选项B；选项C；选项D。选项文本中不要再带 A.、B.、C.、D. 前缀。",
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "正确答案只填写选项字母或简答题要点，例如：A、AC、略。",
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "不要输出 Markdown 标题、编号列表或额外说明。",
            # 结束上方多行参数或数据结构。
            ])

            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            if title:
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"试卷标题：{title}")

            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            if description:
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"试卷描述：{description}")

            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            if question_types:
                # 将 `{` 的结果保存到 `type_labels`，供后续流程使用。
                type_labels = {
                    # 设置 `single_choice` 字段，值来自 `"单选题"`。
                    "single_choice": "单选题",
                    # 设置 `multiple_choice` 字段，值来自 `"多选题"`。
                    "multiple_choice": "多选题",
                    # 设置 `short_answer` 字段，值来自 `"简答题"`。
                    "short_answer": "简答题",
                    # 设置 `single` 字段，值来自 `"单选题"`。
                    "single": "单选题",
                    # 设置 `multiple` 字段，值来自 `"多选题"`。
                    "multiple": "多选题",
                    # 设置 `short` 字段，值来自 `"简答题"`。
                    "short": "简答题",
                # 结束上方多行参数或数据结构。
                }
                # 将 `"、".join(type_labels.get(item, item) for item in question_types)` 的结果保存到 `types_str`，供后续流程使用。
                types_str = "、".join(type_labels.get(item, item) for item in question_types)
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"题目类型：{types_str}")

            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            if question_counts:
                # 将 `[]` 的结果保存到 `counts_parts`，供后续流程使用。
                counts_parts = []
                # 将 `0` 的结果保存到 `expected_total_questions`，供后续流程使用。
                expected_total_questions = 0
                # 将 `{` 的结果保存到 `type_labels`，供后续流程使用。
                type_labels = {
                    # 设置 `single_choice` 字段，值来自 `"单选题"`。
                    "single_choice": "单选题",
                    # 设置 `multiple_choice` 字段，值来自 `"多选题"`。
                    "multiple_choice": "多选题",
                    # 设置 `short_answer` 字段，值来自 `"简答题"`。
                    "short_answer": "简答题",
                    # 设置 `single` 字段，值来自 `"单选题"`。
                    "single": "单选题",
                    # 设置 `multiple` 字段，值来自 `"多选题"`。
                    "multiple": "多选题",
                    # 设置 `short` 字段，值来自 `"简答题"`。
                    "short": "简答题",
                # 结束上方多行参数或数据结构。
                }
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for q_type, count in question_counts.items():
                    # 判断 `if count > 0`，决定是否执行当前分支。
                    # 判断 `if count > 0`，决定是否执行当前分支。
                    # 判断 `if count > 0`，决定是否执行当前分支。
                    # 判断 `if count > 0`，决定是否执行当前分支。
                    if count > 0:
                        # 调用 `int()`，完成当前函数调用并取得其返回结果。
                        expected_total_questions += int(count)
                        # 调用 `counts_parts.append()`，保存或创建当前数据。
                        counts_parts.append(f"{type_labels.get(q_type, q_type)}：{count}题")
                # 判断 `if counts_parts`，决定是否执行当前分支。
                # 判断 `if counts_parts`，决定是否执行当前分支。
                # 判断 `if counts_parts`，决定是否执行当前分支。
                # 判断 `if counts_parts`，决定是否执行当前分支。
                if counts_parts:
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append(f"题目数量：{', '.join(counts_parts)}")
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append(
                        # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                        f"必须严格生成合计 {expected_total_questions} 题，且每种题型数量必须与上面的配置完全一致；"
                        # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                        "不得少题、多题或合并题目。"
                    # 结束上方多行参数或数据结构。
                    )

            # 判断 `if total_score`，决定是否执行当前分支。
            # 判断 `if total_score`，决定是否执行当前分支。
            # 判断 `if total_score`，决定是否执行当前分支。
            # 判断 `if total_score`，决定是否执行当前分支。
            if total_score:
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"试卷总分：{total_score}")

            # 在提示词中明确每题分值设置，要求严格遵循
            # 仅在存在题量时附加说明
            # 将 `question_counts or {}` 的结果保存到 `counts_for_prompt`，供后续流程使用。
            counts_for_prompt = question_counts or {}
            # 判断 `if any(counts_for_prompt.get(t, 0) > 0 for t in ["single_choice", "multiple_choice", "short_answer"])`，决定是否执行当前分支。
            # 判断 `if any(counts_for_prompt.get(t, 0) > 0 for t in ["single_choice", "multiple_choice", "short_answer"])`，决定是否执行当前分支。
            # 判断 `if any(counts_for_prompt.get(t, 0) > 0 for t in ["single_choice", "multiple_choice", "short_answer"])`，决定是否执行当前分支。
            # 判断 `if any(counts_for_prompt.get(t, 0) > 0 for t in ["single_choice", "multiple_choice", "short_answer"])`，决定是否执行当前分支。
            if any(counts_for_prompt.get(t, 0) > 0 for t in ["single_choice", "multiple_choice", "short_answer"]):
                # 定义 `fmt_scores_line()`，实现该函数对应的接口或业务流程。
                def fmt_scores_line(t_key: str, t_name: str) -> str:
                    # 将 `question_scores.get(t_key, [])` 的结果保存到 `arr`，供后续流程使用。
                    arr = question_scores.get(t_key, [])
                    # 判断 `if not arr`，决定是否执行当前分支。
                    # 判断 `if not arr`，决定是否执行当前分支。
                    # 判断 `if not arr`，决定是否执行当前分支。
                    # 判断 `if not arr`，决定是否执行当前分支。
                    if not arr:
                        # 结束函数并把结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        # 结束函数并把结果返回给调用方。
                        return ""
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return f"{t_name}每题分值（按题序）：{', '.join(str(x) for x in arr)}"

                # 将 `[` 的结果保存到 `scores_lines`，供后续流程使用。
                scores_lines = [
                    # 调用 `fmt_scores_line()`，完成当前函数调用并取得其返回结果。
                    fmt_scores_line("single_choice", "单选题"),
                    # 调用 `fmt_scores_line()`，完成当前函数调用并取得其返回结果。
                    fmt_scores_line("multiple_choice", "多选题"),
                    # 调用 `fmt_scores_line()`，完成当前函数调用并取得其返回结果。
                    fmt_scores_line("short_answer", "简答题"),
                # 结束上方多行参数或数据结构。
                ]
                # 将 `[s for s in scores_lines if s]` 的结果保存到 `scores_lines`，供后续流程使用。
                scores_lines = [s for s in scores_lines if s]
                # 判断 `if scores_lines`，决定是否执行当前分支。
                # 判断 `if scores_lines`，决定是否执行当前分支。
                # 判断 `if scores_lines`，决定是否执行当前分支。
                # 判断 `if scores_lines`，决定是否执行当前分支。
                if scores_lines:
                    # 调用 `query_parts.append()`，保存或创建当前数据。
                    query_parts.append("请严格按照以下每题分值设置生成试卷：")
                    # 调用 `query_parts.extend()`，完成当前函数调用并取得其返回结果。
                    query_parts.extend(scores_lines)

            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            if special_requirements:
                # 调用 `query_parts.append()`，保存或创建当前数据。
                query_parts.append(f"特殊要求：{special_requirements}")

            # 将 `"\n".join(query_parts)` 的结果保存到 `query`，供后续流程使用。
            query = "\n".join(query_parts)
            # 调用 `print()`，完成当前函数调用并取得其返回结果。
            print('试卷要求：', query)

            # 将 `combined_content` 的结果保存到 `workflow_file_content`，供后续流程使用。
            workflow_file_content = combined_content
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            if has_interview_plan_context:
                # 将 `(` 的结果保存到 `workflow_file_content`，供后续流程使用。
                workflow_file_content = (
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "【参考文档】\n"
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"{combined_content}\n\n"
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "【面试方案】\n"
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"{special_requirements[:2500]}"
                # 结束上方多行参数或数据结构。
                )

            # 准备额外输入参数
            # 将 `{` 的结果保存到 `additional_inputs`，供后续流程使用。
            additional_inputs = {
                # 设置 `fileContent` 字段，值来自 `workflow_file_content`。
                "fileContent": workflow_file_content,
                # 设置 `total_score` 字段，值来自 `total_score`。
                "total_score": total_score,
                # 向工作流传递结构化的分值设置，便于遵守
                # 设置 `question_scores` 字段，值来自 `question_scores`。
                "question_scores": question_scores,
            # 结束上方多行参数或数据结构。
            }

            # 添加调试日志
            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"向Dify工作流发送文件内容，内容长度: {len(workflow_file_content)}")
            # 判断 `if len(workflow_file_content) < 100:  # 如果内容很短，记录具体内容（前100个字符）`，决定是否执行当前分支。
            # 判断 `if len(workflow_file_content) < 100:  # 如果内容很短，记录具体内容（前100个字符）`，决定是否执行当前分支。
            # 判断 `if len(workflow_file_content) < 100:  # 如果内容很短，记录具体内容（前100个字符）`，决定是否执行当前分支。
            # 判断 `if len(workflow_file_content) < 100:  # 如果内容很短，记录具体内容（前100个字符）`，决定是否执行当前分支。
            if len(workflow_file_content) < 100:  # 如果内容很短，记录具体内容（前100个字符）
                # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
                logger.info(f"文件内容预览: {workflow_file_content[:100]}")

            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            # 判断 `if title`，决定是否执行当前分支。
            if title:
                # 将 `title` 的结果保存到 `additional_inputs["title"]`，供后续流程使用。
                # 将 `title` 的结果保存到 `additional_inputs["title"]`，供后续流程使用。
                # 将 `title` 的结果保存到 `additional_inputs["title"]`，供后续流程使用。
                # 将 `title` 的结果保存到 `additional_inputs["title"]`，供后续流程使用。
                additional_inputs["title"] = title
            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            # 判断 `if description`，决定是否执行当前分支。
            if description:
                # 将 `description` 的结果保存到 `additional_inputs["description"]`，供后续流程使用。
                # 将 `description` 的结果保存到 `additional_inputs["description"]`，供后续流程使用。
                # 将 `description` 的结果保存到 `additional_inputs["description"]`，供后续流程使用。
                # 将 `description` 的结果保存到 `additional_inputs["description"]`，供后续流程使用。
                additional_inputs["description"] = description
            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            # 判断 `if question_types`，决定是否执行当前分支。
            if question_types:
                # 将 `question_types` 的结果保存到 `additional_inputs["question_types"]`，供后续流程使用。
                # 将 `question_types` 的结果保存到 `additional_inputs["question_types"]`，供后续流程使用。
                # 将 `question_types` 的结果保存到 `additional_inputs["question_types"]`，供后续流程使用。
                # 将 `question_types` 的结果保存到 `additional_inputs["question_types"]`，供后续流程使用。
                additional_inputs["question_types"] = question_types
            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            # 判断 `if question_counts`，决定是否执行当前分支。
            if question_counts:
                # 将 `question_counts` 的结果保存到 `additional_inputs["question_counts"]`，供后续流程使用。
                # 将 `question_counts` 的结果保存到 `additional_inputs["question_counts"]`，供后续流程使用。
                # 将 `question_counts` 的结果保存到 `additional_inputs["question_counts"]`，供后续流程使用。
                # 将 `question_counts` 的结果保存到 `additional_inputs["question_counts"]`，供后续流程使用。
                additional_inputs["question_counts"] = question_counts
            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            # 判断 `if special_requirements`，决定是否执行当前分支。
            if special_requirements:
                # 将 `special_requirements` 的结果保存到 `additional_inputs["special_requirements"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["special_requirements"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["special_requirements"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["special_requirements"]`，供后续流程使用。
                additional_inputs["special_requirements"] = special_requirements
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            # 判断 `if has_interview_plan_context`，决定是否执行当前分支。
            if has_interview_plan_context:
                # 将 `special_requirements` 的结果保存到 `additional_inputs["interview_plan_context"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["interview_plan_context"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["interview_plan_context"]`，供后续流程使用。
                # 将 `special_requirements` 的结果保存到 `additional_inputs["interview_plan_context"]`，供后续流程使用。
                additional_inputs["interview_plan_context"] = special_requirements
                # 将 `combined_content` 的结果保存到 `additional_inputs["reference_document_content"]`，供后续流程使用。
                # 将 `combined_content` 的结果保存到 `additional_inputs["reference_document_content"]`，供后续流程使用。
                # 将 `combined_content` 的结果保存到 `additional_inputs["reference_document_content"]`，供后续流程使用。
                # 将 `combined_content` 的结果保存到 `additional_inputs["reference_document_content"]`，供后续流程使用。
                additional_inputs["reference_document_content"] = combined_content
                # 将 `"document_grounded_interview_plan_guided"` 的结果保存到 `additional_inputs["generation_strategy"]`，供后续流程使用。
                # 将 `"document_grounded_interview_plan_guided"` 的结果保存到 `additional_inputs["generation_strategy"]`，供后续流程使用。
                # 将 `"document_grounded_interview_plan_guided"` 的结果保存到 `additional_inputs["generation_strategy"]`，供后续流程使用。
                # 将 `"document_grounded_interview_plan_guided"` 的结果保存到 `additional_inputs["generation_strategy"]`，供后续流程使用。
                additional_inputs["generation_strategy"] = "document_grounded_interview_plan_guided"

            # 附加自动选择的元信息，便于工作流提示词使用
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            # 判断 `if knowledge_files`，决定是否执行当前分支。
            if knowledge_files:
                # 将 `{` 的结果保存到 `additional_inputs["kb_selection"]`，供后续流程使用。
                # 将 `{` 的结果保存到 `additional_inputs["kb_selection"]`，供后续流程使用。
                # 将 `{` 的结果保存到 `additional_inputs["kb_selection"]`，供后续流程使用。
                # 将 `{` 的结果保存到 `additional_inputs["kb_selection"]`，供后续流程使用。
                additional_inputs["kb_selection"] = {
                    # 设置 `selected_document_id` 字段，值来自 `knowledge_files[0]`。
                    "selected_document_id": knowledge_files[0],
                    # 设置 `selected_document_filename` 字段，值来自 `None`。
                    "selected_document_filename": None,
                    # 设置 `selected_kb_id` 字段，值来自 `selection.get("knowledge_base_id") if 'selection' in locals() and selection else None`。
                    "selected_kb_id": selection.get("knowledge_base_id") if 'selection' in locals() and selection else None,
                    # 设置 `kb_selection_confidence` 字段，值来自 `selection.get("confidence", 0.0) if 'selection' in locals() and selection else 0.0`。
                    "kb_selection_confidence": selection.get("confidence", 0.0) if 'selection' in locals() and selection else 0.0,
                # 结束上方多行参数或数据结构。
                }
            
            # 判断 `if stream`，决定是否执行当前分支。
            # 判断 `if stream`，决定是否执行当前分支。
            # 判断 `if stream`，决定是否执行当前分支。
            # 判断 `if stream`，决定是否执行当前分支。
            if stream:
                # 流式响应
                # 定义 `generate_stream()`，实现该函数对应的接口或业务流程。
                async def generate_stream():
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    async for chunk in self.dify_service.call_workflow_stream(
                        # 将 `5,  # 使用类型5进行试卷生成` 的结果保存到 `workflow_type`，供后续流程使用。
                        workflow_type=5,  # 使用类型5进行试卷生成
                        # 将 `query` 的结果保存到 `query`，供后续流程使用。
                        query=query,
                        # conversation_id=conversation_id,
                        # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                        additional_inputs=additional_inputs
                    # 结束上方多行参数或数据结构。
                    ):
                        # 产出当前流式数据片段，并保留生成器状态。
                        # 产出当前流式数据片段，并保留生成器状态。
                        # 产出当前流式数据片段，并保留生成器状态。
                        # 产出当前流式数据片段，并保留生成器状态。
                        yield f"data: {chunk}\n\n"
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    # 产出当前流式数据片段，并保留生成器状态。
                    yield "data: [DONE]\n\n"

                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return generate_stream()
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            # 前面的条件不成立时执行当前备用分支。
            else:
                # 同步响应
                # 将 `await self.dify_service.call_workflow_sync(` 的结果保存到 `result`，供后续流程使用。
                result = await self.dify_service.call_workflow_sync(
                    # 将 `5` 的结果保存到 `workflow_type`，供后续流程使用。
                    workflow_type=5,
                    # 将 `query` 的结果保存到 `query`，供后续流程使用。
                    query=query,
                    # 将 `conversation_id` 的结果保存到 `conversation_id`，供后续流程使用。
                    conversation_id=conversation_id,
                    # 将 `additional_inputs` 的结果保存到 `additional_inputs`，供后续流程使用。
                    additional_inputs=additional_inputs
                # 结束上方多行参数或数据结构。
                )
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
            logger.error(f"生成试卷时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `_question_count_from_config()`，实现该函数对应的接口或业务流程。
    def _question_count_from_config(self, question_counts: Optional[Dict[str, int]]) -> int:
        # 判断 `if not question_counts`，决定是否执行当前分支。
        # 判断 `if not question_counts`，决定是否执行当前分支。
        # 判断 `if not question_counts`，决定是否执行当前分支。
        # 判断 `if not question_counts`，决定是否执行当前分支。
        if not question_counts:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return 0
        # 将 `["single_choice", "multiple_choice", "short_answer", "single", "multiple", "short"]` 的结果保存到 `keys`，供后续流程使用。
        keys = ["single_choice", "multiple_choice", "short_answer", "single", "multiple", "short"]
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return sum(int(question_counts.get(key) or 0) for key in keys)

    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    @staticmethod
    # 定义 `_choice_question_kind()`，实现该函数对应的接口或业务流程。
    def _choice_question_kind(question_type: Any) -> Optional[str]:
        # 将 `unicodedata.normalize(` 的结果保存到 `normalized_type`，供后续流程使用。
        normalized_type = unicodedata.normalize(
            # 调用 `str()`，完成当前函数调用并取得其返回结果。
            "NFKC", str(question_type or "")
        # 调用 `strip()`，完成当前函数调用并取得其返回结果。
        ).strip().lower()
        # 将 `re.sub(r"[\s-]+", "_", normalized_type)` 的结果保存到 `normalized_type`，供后续流程使用。
        normalized_type = re.sub(r"[\s-]+", "_", normalized_type)

        # 判断 `if normalized_type in {"单选题", "单选", "单项选择题", "single_choice", "single"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"单选题", "单选", "单项选择题", "single_choice", "single"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"单选题", "单选", "单项选择题", "single_choice", "single"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"单选题", "单选", "单项选择题", "single_choice", "single"}`，决定是否执行当前分支。
        if normalized_type in {"单选题", "单选", "单项选择题", "single_choice", "single"}:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return "single"
        # 判断 `if normalized_type in {"多选题", "多选", "多项选择题", "multiple_choice", "multiple"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"多选题", "多选", "多项选择题", "multiple_choice", "multiple"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"多选题", "多选", "多项选择题", "multiple_choice", "multiple"}`，决定是否执行当前分支。
        # 判断 `if normalized_type in {"多选题", "多选", "多项选择题", "multiple_choice", "multiple"}`，决定是否执行当前分支。
        if normalized_type in {"多选题", "多选", "多项选择题", "multiple_choice", "multiple"}:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return "multiple"
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return None

    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    @staticmethod
    # 定义 `_valid_choice_option_ids()`，实现该函数对应的接口或业务流程。
    def _valid_choice_option_ids(options: Any) -> set[str]:
        # 判断 `if not isinstance(options, (list, tuple))`，决定是否执行当前分支。
        # 判断 `if not isinstance(options, (list, tuple))`，决定是否执行当前分支。
        # 判断 `if not isinstance(options, (list, tuple))`，决定是否执行当前分支。
        # 判断 `if not isinstance(options, (list, tuple))`，决定是否执行当前分支。
        if not isinstance(options, (list, tuple)):
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return set()

        # 将 `set()` 的结果保存到 `option_ids`，供后续流程使用。
        option_ids = set()
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for index, option in enumerate(options):
            # 将 `option.get("id") if isinstance(option, dict) else None` 的结果保存到 `raw_option_id`，供后续流程使用。
            raw_option_id = option.get("id") if isinstance(option, dict) else None
            # 判断 `if raw_option_id is None or str(raw_option_id).strip() == ""`，决定是否执行当前分支。
            # 判断 `if raw_option_id is None or str(raw_option_id).strip() == ""`，决定是否执行当前分支。
            # 判断 `if raw_option_id is None or str(raw_option_id).strip() == ""`，决定是否执行当前分支。
            # 判断 `if raw_option_id is None or str(raw_option_id).strip() == ""`，决定是否执行当前分支。
            if raw_option_id is None or str(raw_option_id).strip() == "":
                # 将 `chr(65 + index)` 的结果保存到 `raw_option_id`，供后续流程使用。
                raw_option_id = chr(65 + index)

            # 将 `unicodedata.normalize(` 的结果保存到 `option_id`，供后续流程使用。
            option_id = unicodedata.normalize(
                # 调用 `str()`，完成当前函数调用并取得其返回结果。
                "NFKC", str(raw_option_id)
            # 调用 `strip()`，完成当前函数调用并取得其返回结果。
            ).strip().upper()
            # 判断 `if option_id`，决定是否执行当前分支。
            # 判断 `if option_id`，决定是否执行当前分支。
            # 判断 `if option_id`，决定是否执行当前分支。
            # 判断 `if option_id`，决定是否执行当前分支。
            if option_id:
                # 调用 `option_ids.add()`，保存或创建当前数据。
                option_ids.add(option_id)

        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return option_ids

    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    @classmethod
    # 定义 `_normalize_choice_answer()`，实现该函数对应的接口或业务流程。
    def _normalize_choice_answer(
        # 执行表达式 `cls,`，完成本行明确的数据运算或控制操作。
        cls,
        # 声明 `answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `answer` 参数（当前步骤使用的数据），类型为 `Any`。
        answer: Any,
        # 声明 `valid_option_ids` 参数（当前步骤使用的数据），类型为 `set[str]`。
        # 声明 `valid_option_ids` 参数（当前步骤使用的数据），类型为 `set[str]`。
        # 声明 `valid_option_ids` 参数（当前步骤使用的数据），类型为 `set[str]`。
        # 声明 `valid_option_ids` 参数（当前步骤使用的数据），类型为 `set[str]`。
        valid_option_ids: set[str]
    # 结束上方多行参数或数据结构。
    ) -> Optional[frozenset[str]]:
        """将选择题答案规范化为选项 ID 集合。

        返回 None 表示答案格式非法；空集合表示未作答。
        """
        # 判断 `if answer is None`，决定是否执行当前分支。
        # 判断 `if answer is None`，决定是否执行当前分支。
        # 判断 `if answer is None`，决定是否执行当前分支。
        # 判断 `if answer is None`，决定是否执行当前分支。
        if answer is None:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return frozenset()

        # 判断 `if isinstance(answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
        # 判断 `if isinstance(answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
        # 判断 `if isinstance(answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
        # 判断 `if isinstance(answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
        if isinstance(answer, (list, tuple, set, frozenset)):
            # 将 `set()` 的结果保存到 `normalized_items`，供后续流程使用。
            normalized_items = set()
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for item in answer:
                # 将 `cls._normalize_choice_answer(item, valid_option_ids)` 的结果保存到 `normalized_item`，供后续流程使用。
                normalized_item = cls._normalize_choice_answer(item, valid_option_ids)
                # 判断 `if normalized_item is None`，决定是否执行当前分支。
                # 判断 `if normalized_item is None`，决定是否执行当前分支。
                # 判断 `if normalized_item is None`，决定是否执行当前分支。
                # 判断 `if normalized_item is None`，决定是否执行当前分支。
                if normalized_item is None:
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    # 结束函数并把结果返回给调用方。
                    return None
                # 调用 `normalized_items.update()`，更新当前数据。
                normalized_items.update(normalized_item)
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return frozenset(normalized_items)

        # 判断 `if not isinstance(answer, (str, int))`，决定是否执行当前分支。
        # 判断 `if not isinstance(answer, (str, int))`，决定是否执行当前分支。
        # 判断 `if not isinstance(answer, (str, int))`，决定是否执行当前分支。
        # 判断 `if not isinstance(answer, (str, int))`，决定是否执行当前分支。
        if not isinstance(answer, (str, int)):
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 将 `unicodedata.normalize("NFKC", str(answer)).strip().upper()` 的结果保存到 `answer_text`，供后续流程使用。
        answer_text = unicodedata.normalize("NFKC", str(answer)).strip().upper()
        # 判断 `if not answer_text or answer_text == "未作答"`，决定是否执行当前分支。
        # 判断 `if not answer_text or answer_text == "未作答"`，决定是否执行当前分支。
        # 判断 `if not answer_text or answer_text == "未作答"`，决定是否执行当前分支。
        # 判断 `if not answer_text or answer_text == "未作答"`，决定是否执行当前分支。
        if not answer_text or answer_text == "未作答":
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return frozenset()

        # 判断 `if answer_text in valid_option_ids`，决定是否执行当前分支。
        # 判断 `if answer_text in valid_option_ids`，决定是否执行当前分支。
        # 判断 `if answer_text in valid_option_ids`，决定是否执行当前分支。
        # 判断 `if answer_text in valid_option_ids`，决定是否执行当前分支。
        if answer_text in valid_option_ids:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return frozenset({answer_text})

        # 判断 `if answer_text[0] in "[({" and answer_text[-1] in "])}"`，决定是否执行当前分支。
        # 判断 `if answer_text[0] in "[({" and answer_text[-1] in "])}"`，决定是否执行当前分支。
        # 判断 `if answer_text[0] in "[({" and answer_text[-1] in "])}"`，决定是否执行当前分支。
        # 判断 `if answer_text[0] in "[({" and answer_text[-1] in "])}"`，决定是否执行当前分支。
        if answer_text[0] in "[({" and answer_text[-1] in "])}":
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            # 开始执行可能失败的步骤，由后续异常分支统一处理。
            try:
                # 将 `ast.literal_eval(answer_text)` 的结果保存到 `parsed_answer`，供后续流程使用。
                parsed_answer = ast.literal_eval(answer_text)
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            # 捕获指定异常，记录或转换为上层能够识别的错误。
            except (ValueError, SyntaxError):
                # 将 `None` 的结果保存到 `parsed_answer`，供后续流程使用。
                parsed_answer = None
            # 判断 `if isinstance(parsed_answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
            # 判断 `if isinstance(parsed_answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
            # 判断 `if isinstance(parsed_answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
            # 判断 `if isinstance(parsed_answer, (list, tuple, set, frozenset))`，决定是否执行当前分支。
            if isinstance(parsed_answer, (list, tuple, set, frozenset)):
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return cls._normalize_choice_answer(parsed_answer, valid_option_ids)

        # 将 `[` 的结果保存到 `answer_parts`，供后续流程使用。
        answer_parts = [
            # 调用 `part.strip()`，完成当前函数调用并取得其返回结果。
            part.strip("[](){}'\".。")
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for part in re.split(r"(?:AND|和|及)|[,\s、，;；/|+&]+", answer_text)
            # 判断 `if part.strip("[](){}'\".。")`，决定是否执行当前分支。
            # 判断 `if part.strip("[](){}'\".。")`，决定是否执行当前分支。
            # 判断 `if part.strip("[](){}'\".。")`，决定是否执行当前分支。
            # 判断 `if part.strip("[](){}'\".。")`，决定是否执行当前分支。
            if part.strip("[](){}'\".。")
        # 结束上方多行参数或数据结构。
        ]
        # 判断 `if len(answer_parts) > 1`，决定是否执行当前分支。
        # 判断 `if len(answer_parts) > 1`，决定是否执行当前分支。
        # 判断 `if len(answer_parts) > 1`，决定是否执行当前分支。
        # 判断 `if len(answer_parts) > 1`，决定是否执行当前分支。
        if len(answer_parts) > 1:
            # 判断 `if all(part in valid_option_ids for part in answer_parts)`，决定是否执行当前分支。
            # 判断 `if all(part in valid_option_ids for part in answer_parts)`，决定是否执行当前分支。
            # 判断 `if all(part in valid_option_ids for part in answer_parts)`，决定是否执行当前分支。
            # 判断 `if all(part in valid_option_ids for part in answer_parts)`，决定是否执行当前分支。
            if all(part in valid_option_ids for part in answer_parts):
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                # 结束函数并把结果返回给调用方。
                return frozenset(answer_parts)
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 兼容多选标准答案的紧凑写法，例如 AC。
        # 判断 `if (`，决定是否执行当前分支。
        # 判断 `if (`，决定是否执行当前分支。
        # 判断 `if (`，决定是否执行当前分支。
        if (
            # 执行表达式 `answer_parts`，完成本行明确的数据运算或控制操作。
            answer_parts
            # 调用 `all()`，完成当前函数调用并取得其返回结果。
            and all(len(option_id) == 1 for option_id in valid_option_ids)
            # 调用 `all()`，完成当前函数调用并取得其返回结果。
            and all(char in valid_option_ids for char in answer_parts[0])
        # 结束上方多行参数或数据结构。
        ):
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return frozenset(answer_parts[0])

        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return None

    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    # 应用装饰器，为下方类或函数增加框架行为。
    @classmethod
    # 定义 `_grade_choice_answer()`，实现该函数对应的接口或业务流程。
    def _grade_choice_answer(
        # 执行表达式 `cls,`，完成本行明确的数据运算或控制操作。
        cls,
        # 声明 `question_type` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `question_type` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `question_type` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `question_type` 参数（当前步骤使用的数据），类型为 `Any`。
        question_type: Any,
        # 声明 `student_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `student_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `student_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `student_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        student_answer: Any,
        # 声明 `correct_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `correct_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `correct_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `correct_answer` 参数（当前步骤使用的数据），类型为 `Any`。
        correct_answer: Any,
        # 声明 `options` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `options` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `options` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `options` 参数（当前步骤使用的数据），类型为 `Any`。
        options: Any,
        # 声明 `max_score` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `max_score` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `max_score` 参数（当前步骤使用的数据），类型为 `Any`。
        # 声明 `max_score` 参数（当前步骤使用的数据），类型为 `Any`。
        max_score: Any
    # 结束上方多行参数或数据结构。
    ) -> Optional[Dict[str, Any]]:

        # 识别单选题、多选题及其中英文名称： _choice_question_kind
        question_kind = cls._choice_question_kind(question_type)
        # 判断 `if question_kind is None`，决定是否执行当前分支。
        # 判断 `if question_kind is None`，决定是否执行当前分支。
        # 判断 `if question_kind is None`，决定是否执行当前分支。
        # 判断 `if question_kind is None`，决定是否执行当前分支。
        if question_kind is None:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return None

        # 获取题目的有效选项ID：_valid_choice_option_ids
        # 将 `cls._valid_choice_option_ids(options)` 的结果保存到 `valid_option_ids`，供后续流程使用。
        valid_option_ids = cls._valid_choice_option_ids(options)
        # 判断 `if not valid_option_ids`，决定是否执行当前分支。
        # 判断 `if not valid_option_ids`，决定是否执行当前分支。
        # 判断 `if not valid_option_ids`，决定是否执行当前分支。
        # 判断 `if not valid_option_ids`，决定是否执行当前分支。
        if not valid_option_ids:
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise ValueError("选择题缺少有效选项，无法进行自动评分")

        # 统一处理 AC、A,C、A、C、数组、大小写等答案格式。
        # 将 `cls._normalize_choice_answer(` 的结果保存到 `normalized_correct`，供后续流程使用。
        normalized_correct = cls._normalize_choice_answer(
            # 执行表达式 `correct_answer, valid_option_ids`，完成本行明确的数据运算或控制操作。
            correct_answer, valid_option_ids
        # 结束上方多行参数或数据结构。
        )
        # 判断 `if not normalized_correct`，决定是否执行当前分支。
        # 判断 `if not normalized_correct`，决定是否执行当前分支。
        # 判断 `if not normalized_correct`，决定是否执行当前分支。
        # 判断 `if not normalized_correct`，决定是否执行当前分支。
        if not normalized_correct:
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise ValueError("选择题标准答案为空或格式无效")
        # 判断 `if question_kind == "single" and len(normalized_correct) != 1`，决定是否执行当前分支。
        # 判断 `if question_kind == "single" and len(normalized_correct) != 1`，决定是否执行当前分支。
        # 判断 `if question_kind == "single" and len(normalized_correct) != 1`，决定是否执行当前分支。
        # 判断 `if question_kind == "single" and len(normalized_correct) != 1`，决定是否执行当前分支。
        if question_kind == "single" and len(normalized_correct) != 1:
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            # 抛出异常，中止当前失败流程。
            raise ValueError("单选题的标准答案必须且只能包含一个选项")

        # 将 `cls._normalize_choice_answer(` 的结果保存到 `normalized_student`，供后续流程使用。
        normalized_student = cls._normalize_choice_answer(
            # 执行表达式 `student_answer, valid_option_ids`，完成本行明确的数据运算或控制操作。
            student_answer, valid_option_ids
        # 结束上方多行参数或数据结构。
        )

        # 单选或多选 完全匹配：normalized_student == normalized_correct 满分。
        # 将 `(` 的结果保存到 `is_correct`，供后续流程使用。
        is_correct = (
            # 执行表达式 `normalized_student is not None`，完成本行明确的数据运算或控制操作。
            normalized_student is not None
            # 调用 `bool()`，完成当前函数调用并取得其返回结果。
            and bool(normalized_student)
            # 执行表达式 `and normalized_student == normalized_correct`，完成本行明确的数据运算或控制操作。
            and normalized_student == normalized_correct
            # 调用 `and()`，完成当前函数调用并取得其返回结果。
            and (question_kind != "single" or len(normalized_student) == 1)
        # 结束上方多行参数或数据结构。
        )
        # 多选题中 normalized_student < normalized_correct：非空真子集，即漏选，得一半分
        # 将 `(` 的结果保存到 `is_partially_correct`，供后续流程使用。
        is_partially_correct = (
            # 执行表达式 `question_kind == "multiple"`，完成本行明确的数据运算或控制操作。
            question_kind == "multiple"
            # 执行表达式 `and normalized_student is not None`，完成本行明确的数据运算或控制操作。
            and normalized_student is not None
            # 调用 `bool()`，完成当前函数调用并取得其返回结果。
            and bool(normalized_student)
            # 执行表达式 `and normalized_student < normalized_correct`，完成本行明确的数据运算或控制操作。
            and normalized_student < normalized_correct
        # 结束上方多行参数或数据结构。
        )

        # 多选、非法选项、未作答：0 分。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 将 `float(max_score)` 的结果保存到 `numeric_max_score`，供后续流程使用。
            numeric_max_score = float(max_score)
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except (TypeError, ValueError):
            # 将 `0.0` 的结果保存到 `numeric_max_score`，供后续流程使用。
            numeric_max_score = 0.0
        # 判断 `if not math.isfinite(numeric_max_score) or numeric_max_score < 0`，决定是否执行当前分支。
        # 判断 `if not math.isfinite(numeric_max_score) or numeric_max_score < 0`，决定是否执行当前分支。
        # 判断 `if not math.isfinite(numeric_max_score) or numeric_max_score < 0`，决定是否执行当前分支。
        # 判断 `if not math.isfinite(numeric_max_score) or numeric_max_score < 0`，决定是否执行当前分支。
        if not math.isfinite(numeric_max_score) or numeric_max_score < 0:
            # 将 `0.0` 的结果保存到 `numeric_max_score`，供后续流程使用。
            numeric_max_score = 0.0

        # 判断 `if is_correct`，决定是否执行当前分支。
        # 判断 `if is_correct`，决定是否执行当前分支。
        # 判断 `if is_correct`，决定是否执行当前分支。
        # 判断 `if is_correct`，决定是否执行当前分支。
        if is_correct:
            # 将 `numeric_max_score` 的结果保存到 `score`，供后续流程使用。
            score = numeric_max_score
            # 将 `"correct"` 的结果保存到 `grading_status`，供后续流程使用。
            grading_status = "correct"
        # 判断 `elif is_partially_correct`，决定是否执行当前分支。
        # 判断 `elif is_partially_correct`，决定是否执行当前分支。
        # 判断 `elif is_partially_correct`，决定是否执行当前分支。
        # 判断 `elif is_partially_correct`，决定是否执行当前分支。
        elif is_partially_correct:
            # 将 `numeric_max_score / 2` 的结果保存到 `score`，供后续流程使用。
            score = numeric_max_score / 2
            # 将 `"partial"` 的结果保存到 `grading_status`，供后续流程使用。
            grading_status = "partial"
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        # 前面的条件不成立时执行当前备用分支。
        else:
            # 将 `0.0` 的结果保存到 `score`，供后续流程使用。
            score = 0.0
            # 将 `"incorrect"` 的结果保存到 `grading_status`，供后续流程使用。
            grading_status = "incorrect"

        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return {
            # 设置 `score` 字段，值来自 `score`。
            "score": score,
            # 设置 `is_correct` 字段，值来自 `is_correct`。
            "is_correct": is_correct,
            # 设置 `is_partially_correct` 字段，值来自 `is_partially_correct`。
            "is_partially_correct": is_partially_correct,
            # 设置 `grading_status` 字段，值来自 `grading_status`。
            "grading_status": grading_status,
            # 设置 `normalized_correct_answer` 字段，值来自 `sorted(normalized_correct)`。
            "normalized_correct_answer": sorted(normalized_correct),
            # 设置 `normalized_student_answer` 字段，值来自 `(`。
            "normalized_student_answer": (
                # 调用 `sorted()`，完成当前函数调用并取得其返回结果。
                sorted(normalized_student)
                # 判断 `if normalized_student is not None`，决定是否执行当前分支。
                # 判断 `if normalized_student is not None`，决定是否执行当前分支。
                # 判断 `if normalized_student is not None`，决定是否执行当前分支。
                # 判断 `if normalized_student is not None`，决定是否执行当前分支。
                if normalized_student is not None
                # 执行表达式 `else None`，完成本行明确的数据运算或控制操作。
                else None
            # 结束上方多行参数或数据结构。
            ),
            # 设置 `student_answer_valid` 字段，值来自 `normalized_student is not None`。
            "student_answer_valid": normalized_student is not None,
        # 结束上方多行参数或数据结构。
        }

    # 定义 `_parse_exam_content()`，实现该函数对应的接口或业务流程。
    def _parse_exam_content(self, content: Optional[str]) -> List[Dict[str, Any]]:
        # 判断 `if not content or not isinstance(content, str)`，决定是否执行当前分支。
        # 判断 `if not content or not isinstance(content, str)`，决定是否执行当前分支。
        # 判断 `if not content or not isinstance(content, str)`，决定是否执行当前分支。
        # 判断 `if not content or not isinstance(content, str)`，决定是否执行当前分支。
        if not content or not isinstance(content, str):
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return []

        # 声明 `questions` 参数（当前步骤使用的数据），类型为 `List[Dict[str`。
        questions: List[Dict[str, Any]] = []
        # 将 `[block.strip() for block in content.split("***") if block.strip()]` 的结果保存到 `blocks`，供后续流程使用。
        blocks = [block.strip() for block in content.split("***") if block.strip()]
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for index, block in enumerate(blocks, start=1):
            # 将 `[part.strip() for part in block.split("|")]` 的结果保存到 `parts`，供后续流程使用。
            parts = [part.strip() for part in block.split("|")]
            # 判断 `if len(parts) != 6`，决定是否执行当前分支。
            # 判断 `if len(parts) != 6`，决定是否执行当前分支。
            # 判断 `if len(parts) != 6`，决定是否执行当前分支。
            # 判断 `if len(parts) != 6`，决定是否执行当前分支。
            if len(parts) != 6:
                # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                continue

            # 执行表达式 `question_text, question_type, options_text, correct_answers, score, explanation = parts`，完成本行明确的数据运算或控制操作。
            question_text, question_type, options_text, correct_answers, score, explanation = parts
            # 将 `[]` 的结果保存到 `options`，供后续流程使用。
            options = []
            # 判断 `if question_type in {"单选题", "多选题", "单选", "多选"} and options_text`，决定是否执行当前分支。
            # 判断 `if question_type in {"单选题", "多选题", "单选", "多选"} and options_text`，决定是否执行当前分支。
            # 判断 `if question_type in {"单选题", "多选题", "单选", "多选"} and options_text`，决定是否执行当前分支。
            # 判断 `if question_type in {"单选题", "多选题", "单选", "多选"} and options_text`，决定是否执行当前分支。
            if question_type in {"单选题", "多选题", "单选", "多选"} and options_text:
                # 将 `[` 的结果保存到 `options`，供后续流程使用。
                options = [
                    # 调用 `chr()`，完成当前函数调用并取得其返回结果。
                    {"id": chr(65 + option_index), "text": option.strip()}
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    # 遍历当前集合或异步数据流，逐项执行处理。
                    for option_index, option in enumerate(re.split(r"[;；]", options_text))
                    # 判断 `if option.strip()`，决定是否执行当前分支。
                    # 判断 `if option.strip()`，决定是否执行当前分支。
                    # 判断 `if option.strip()`，决定是否执行当前分支。
                    # 判断 `if option.strip()`，决定是否执行当前分支。
                    if option.strip()
                # 结束上方多行参数或数据结构。
                ]

            # 将 `re.search(r"\d+", str(score))` 的结果保存到 `score_match`，供后续流程使用。
            score_match = re.search(r"\d+", str(score))
            # 调用 `questions.append()`，保存或创建当前数据。
            questions.append({
                # 设置 `id` 字段，值来自 `f"q_{index}"`。
                "id": f"q_{index}",
                # 设置 `number` 字段，值来自 `index`。
                "number": index,
                # 设置 `text` 字段，值来自 `question_text`。
                "text": question_text,
                # 设置 `type` 字段，值来自 `question_type`。
                "type": question_type,
                # 设置 `options` 字段，值来自 `options`。
                "options": options,
                # 设置 `correct_answers` 字段，值来自 `correct_answers`。
                "correct_answers": correct_answers,
                # 设置 `score` 字段，值来自 `int(score_match.group(0)) if score_match else 0`。
                "score": int(score_match.group(0)) if score_match else 0,
                # 设置 `explanation` 字段，值来自 `explanation`。
                "explanation": explanation,
            # 结束上方多行参数或数据结构。
            })

        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return questions

    # 定义 `_allocate_question_scores()`，实现该函数对应的接口或业务流程。
    def _allocate_question_scores(self, total_score: int, counts: Dict[str, int]) -> Dict[str, List[int]]:
        """
        分值分配算法（按 2:3:5 权重，并将剩余分数依次分配给最后几题）

        Args:
            total_score: 总分
            counts: 题目数量配置

        Returns:
            各题型分值分配结果
        """
        # 仅支持三种题型：单选题、多选题、简答题
        # 将 `{` 的结果保存到 `weights`，供后续流程使用。
        weights = {
            # 设置 `single_choice` 字段，值来自 `2`。
            "single_choice": 2,
            # 设置 `multiple_choice` 字段，值来自 `3`。
            "multiple_choice": 3,
            # 设置 `short_answer` 字段，值来自 `5`。
            "short_answer": 5,
        # 结束上方多行参数或数据结构。
        }

        # 初始化每题分值列表
        # 声明 `scores` 参数（当前步骤使用的数据），类型为 `Dict[str`。
        scores: Dict[str, List[int]] = {
            # 声明 `t` 参数（当前步骤使用的数据），类型为 `[0] * max(0`。
            # 声明 `t` 参数（当前步骤使用的数据），类型为 `[0] * max(0`。
            # 声明 `t` 参数（当前步骤使用的数据），类型为 `[0] * max(0`。
            # 声明 `t` 参数（当前步骤使用的数据），类型为 `[0] * max(0`。
            t: [0] * max(0, counts.get(t, 0)) for t in ["single_choice", "multiple_choice", "short_answer"]
        # 结束上方多行参数或数据结构。
        }
        '''示例返回格式：
        {
        "single_choice": [0, 0, 0, 0, 0],  # 5个单选题，初始分值都是0
        "multiple_choice": [0, 0, 0],       # 3个多选题，初始分值都是0  
        "short_answer": [0, 0]              # 2个简答题，初始分值都是0
        }'''

        # 计算加权题量总和
        # 将 `0` 的结果保存到 `weighted_total`，供后续流程使用。
        weighted_total = 0
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for t, w in weights.items():
            # 调用 `counts.get()`，获取所需数据。
            weighted_total += counts.get(t, 0) * w

        # 判断 `if weighted_total <= 0 or total_score <= 0`，决定是否执行当前分支。
        # 判断 `if weighted_total <= 0 or total_score <= 0`，决定是否执行当前分支。
        # 判断 `if weighted_total <= 0 or total_score <= 0`，决定是否执行当前分支。
        # 判断 `if weighted_total <= 0 or total_score <= 0`，决定是否执行当前分支。
        if weighted_total <= 0 or total_score <= 0:
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return scores

        # 基础单位分和剩余分
        # 将 `total_score // weighted_total` 的结果保存到 `base_unit`，供后续流程使用。
        base_unit = total_score // weighted_total
        # 将 `total_score % weighted_total` 的结果保存到 `remainder`，供后续流程使用。
        remainder = total_score % weighted_total

        # 先按权重为每题分配基础分值
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        # 遍历当前集合或异步数据流，逐项执行处理。
        for t, w in weights.items():
            # 将 `counts.get(t, 0)` 的结果保存到 `c`，供后续流程使用。
            c = counts.get(t, 0)
            # 判断 `if c > 0`，决定是否执行当前分支。
            # 判断 `if c > 0`，决定是否执行当前分支。
            # 判断 `if c > 0`，决定是否执行当前分支。
            # 判断 `if c > 0`，决定是否执行当前分支。
            if c > 0:
                # 将 `w * base_unit` 的结果保存到 `base_per_question`，供后续流程使用。
                base_per_question = w * base_unit
                # 将 `[base_per_question] * c` 的结果保存到 `scores[t]`，供后续流程使用。
                # 将 `[base_per_question] * c` 的结果保存到 `scores[t]`，供后续流程使用。
                # 将 `[base_per_question] * c` 的结果保存到 `scores[t]`，供后续流程使用。
                # 将 `[base_per_question] * c` 的结果保存到 `scores[t]`，供后续流程使用。
                scores[t] = [base_per_question] * c

        # 将剩余分数依次分配给"最后几题"：先简答题末尾，后多选题末尾，再单选题末尾
        # 将 `["short_answer", "multiple_choice", "single_choice"]` 的结果保存到 `distribution_order`，供后续流程使用。
        distribution_order = ["short_answer", "multiple_choice", "single_choice"]
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        # 在条件成立时循环执行，直到达到结束条件。
        while remainder > 0:
            # 将 `False` 的结果保存到 `allocated_this_round`，供后续流程使用。
            allocated_this_round = False
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for t in distribution_order:
                # 将 `scores.get(t, [])` 的结果保存到 `arr`，供后续流程使用。
                arr = scores.get(t, [])
                # 从末尾开始为每题加 1 分，直到该类型题目遍历完或剩余分数为 0
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                # 遍历当前集合或异步数据流，逐项执行处理。
                for i in range(len(arr) - 1, -1, -1):
                    # 判断 `if remainder <= 0`，决定是否执行当前分支。
                    # 判断 `if remainder <= 0`，决定是否执行当前分支。
                    # 判断 `if remainder <= 0`，决定是否执行当前分支。
                    # 判断 `if remainder <= 0`，决定是否执行当前分支。
                    if remainder <= 0:
                        # 执行表达式 `break`，完成本行明确的数据运算或控制操作。
                        break
                    # 执行表达式 `arr[i] += 1`，完成本行明确的数据运算或控制操作。
                    arr[i] += 1
                    # 执行表达式 `remainder -= 1`，完成本行明确的数据运算或控制操作。
                    remainder -= 1
                    # 将 `True` 的结果保存到 `allocated_this_round`，供后续流程使用。
                    allocated_this_round = True
            # 判断 `if not allocated_this_round`，决定是否执行当前分支。
            # 判断 `if not allocated_this_round`，决定是否执行当前分支。
            # 判断 `if not allocated_this_round`，决定是否执行当前分支。
            # 判断 `if not allocated_this_round`，决定是否执行当前分支。
            if not allocated_this_round:
                # 没有任何题目可分配（例如题量为 0），避免死循环
                # 执行表达式 `break`，完成本行明确的数据运算或控制操作。
                break

        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        # 结束函数并把结果返回给调用方。
        return scores
        '''示例返回格式：
        {
            "single_choice": [6, 6, 6, 6, 7],      # 单选题分值列表
            "multiple_choice": [9, 9, 10],          # 多选题分值列表  
            "short_answer": [15, 16]                # 简答题分值列表
        }'''

    # 定义 `submit_exam()`，实现该函数对应的接口或业务流程。
    async def submit_exam(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `exam_id` 参数（试卷唯一标识），类型为 `str`。
        # 声明 `exam_id` 参数（试卷唯一标识），类型为 `str`。
        # 声明 `exam_id` 参数（试卷唯一标识），类型为 `str`。
        # 声明 `exam_id` 参数（试卷唯一标识），类型为 `str`。
        exam_id: str,
        # 声明 `student_name` 参数（考生姓名），类型为 `str`。
        # 声明 `student_name` 参数（考生姓名），类型为 `str`。
        # 声明 `student_name` 参数（考生姓名），类型为 `str`。
        # 声明 `student_name` 参数（考生姓名），类型为 `str`。
        student_name: str,
        # 声明 `department` 参数（考生部门），类型为 `str`。
        # 声明 `department` 参数（考生部门），类型为 `str`。
        # 声明 `department` 参数（考生部门），类型为 `str`。
        # 声明 `department` 参数（考生部门），类型为 `str`。
        department: str,
        # 声明 `answers` 参数（考生答案），类型为 `Dict[str`。
        # 声明 `answers` 参数（考生答案），类型为 `Dict[str`。
        # 声明 `answers` 参数（考生答案），类型为 `Dict[str`。
        # 声明 `answers` 参数（考生答案），类型为 `Dict[str`。
        answers: Dict[str, Any],
        # 声明 `exam_content` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `exam_content` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `exam_content` 参数（当前步骤使用的数据），类型为 `str`。
        # 声明 `exam_content` 参数（当前步骤使用的数据），类型为 `str`。
        exam_content: str
    # 结束上方多行参数或数据结构。
    ) -> Dict[str, Any]:
        """
        提交考试答案并进行自动评分

        Args:
            exam_id: 试卷ID
            student_name: 考生姓名
            department: 考生部门
            answers: 学生答案
            exam_content: 试卷内容

        Returns:
            考试结果
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 校验 exam_content 是合法 JSON
            # 调用 `json.loads()`，完成当前函数调用并取得其返回结果。
            json.loads(exam_content)

            # 从数据库获取完整的试卷信息（包括标准答案和解析）
            # 将 `await self.db.execute(` 的结果保存到 `result`，供后续流程使用。
            result = await self.db.execute(
                # 调用 `select()`，获取所需数据。
                select(Exam)
                # 调用 `options()`，完成当前函数调用并取得其返回结果。
                .options(selectinload(Exam.questions))
                # 调用 `where()`，完成当前函数调用并取得其返回结果。
                .where(Exam.id == exam_id)
            # 结束上方多行参数或数据结构。
            )
            # 将 `result.scalar_one_or_none()` 的结果保存到 `exam`，供后续流程使用。
            exam = result.scalar_one_or_none()

            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            # 判断 `if not exam`，决定是否执行当前分支。
            if not exam:
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                # 抛出异常，中止当前失败流程。
                raise ValueError("试卷不存在")

            # 构建完整的试题信息（包含标准答案和解析）
            # 将 `[]` 的结果保存到 `questions_with_answers`，供后续流程使用。
            questions_with_answers = []
            # 将 `json.loads(answers) if isinstance(answers, str) else answers` 的结果保存到 `student_answers`，供后续流程使用。
            student_answers = json.loads(answers) if isinstance(answers, str) else answers

            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for question in sorted(exam.questions, key=lambda x: x.order_index):
                # 使用题目的实际ID来匹配答案
                # 将 `str(question.id)` 的结果保存到 `question_id`，供后续流程使用。
                question_id = str(question.id)
                # 将 `student_answers.get(question_id, "未作答")` 的结果保存到 `student_answer`，供后续流程使用。
                student_answer = student_answers.get(question_id, "未作答")

                # 将 `{` 的结果保存到 `question_info`，供后续流程使用。
                question_info = {
                    # 设置 `题目编号` 字段，值来自 `question.order_index`。
                    "题目编号": question.order_index,
                    # 设置 `题目类型` 字段，值来自 `question.question_type`。
                    "题目类型": question.question_type,
                    # 设置 `题目内容` 字段，值来自 `question.question_text`。
                    "题目内容": question.question_text,
                    # 设置 `选项` 字段，值来自 `question.options or []`。
                    "选项": question.options or [],
                    # 设置 `标准答案` 字段，值来自 `question.correct_answer`。
                    "标准答案": question.correct_answer,
                    # 设置 `解析` 字段，值来自 `question.explanation`。
                    "解析": question.explanation,
                    # 设置 `分值` 字段，值来自 `question.score`。
                    "分值": question.score,
                    # 设置 `考生答案` 字段，值来自 `student_answer`。
                    "考生答案": student_answer
                # 结束上方多行参数或数据结构。
                }
                # 调用 `questions_with_answers.append()`，保存或创建当前数据。
                questions_with_answers.append(question_info)

            # 选择题直接比较答案，其他题型逐题调用 Dify 评分
            # 将 `[]` 的结果保存到 `questions_with_scores`，供后续流程使用。
            questions_with_scores = []
            # 将 `[]` 的结果保存到 `per_question_results`，供后续流程使用。
            per_question_results = []
            # 将 `0.0` 的结果保存到 `total_score`，供后续流程使用。
            total_score = 0.0

            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for idx, q_info in enumerate(questions_with_answers, 1):

                # 实际判分 _grade_choice_answer
                # 将 `self._grade_choice_answer(` 的结果保存到 `choice_result`，供后续流程使用。
                choice_result = self._grade_choice_answer(
                    # 将 `q_info["题目类型"]` 的结果保存到 `question_type`，供后续流程使用。
                    question_type=q_info["题目类型"],
                    # 将 `q_info["考生答案"]` 的结果保存到 `student_answer`，供后续流程使用。
                    student_answer=q_info["考生答案"],
                    # 将 `q_info["标准答案"]` 的结果保存到 `correct_answer`，供后续流程使用。
                    correct_answer=q_info["标准答案"],
                    # 将 `q_info["选项"]` 的结果保存到 `options`，供后续流程使用。
                    options=q_info["选项"],
                    # 将 `q_info["分值"]` 的结果保存到 `max_score`，供后续流程使用。
                    max_score=q_info["分值"],
                # 结束上方多行参数或数据结构。
                )
                # 判断 `if choice_result is not None`，决定是否执行当前分支。
                # 判断 `if choice_result is not None`，决定是否执行当前分支。
                # 判断 `if choice_result is not None`，决定是否执行当前分支。
                # 判断 `if choice_result is not None`，决定是否执行当前分支。
                if choice_result is not None:
                    # 将 `choice_result["score"]` 的结果保存到 `per_score`，供后续流程使用。
                    per_score = choice_result["score"]
                    # 执行表达式 `total_score += per_score`，完成本行明确的数据运算或控制操作。
                    total_score += per_score
                    # 调用 `per_question_results.append()`，保存或创建当前数据。
                    per_question_results.append({
                        # 设置 `题目编号` 字段，值来自 `q_info["题目编号"]`。
                        "题目编号": q_info["题目编号"],
                        # 设置 `scoring_method` 字段，值来自 `"direct_answer_comparison"`。
                        "scoring_method": "direct_answer_comparison",
                        # 设置 `parsed_score` 字段，值来自 `per_score`。
                        "parsed_score": per_score,
                        # 开始构造多行调用参数或数据结构。
                        **{
                            # 声明 `key` 参数（当前步骤使用的数据），类型为 `value`。
                            # 声明 `key` 参数（当前步骤使用的数据），类型为 `value`。
                            # 声明 `key` 参数（当前步骤使用的数据），类型为 `value`。
                            # 声明 `key` 参数（当前步骤使用的数据），类型为 `value`。
                            key: value
                            # 遍历当前集合或异步数据流，逐项执行处理。
                            # 遍历当前集合或异步数据流，逐项执行处理。
                            # 遍历当前集合或异步数据流，逐项执行处理。
                            # 遍历当前集合或异步数据流，逐项执行处理。
                            for key, value in choice_result.items()
                            # 判断 `if key != "score"`，决定是否执行当前分支。
                            # 判断 `if key != "score"`，决定是否执行当前分支。
                            # 判断 `if key != "score"`，决定是否执行当前分支。
                            # 判断 `if key != "score"`，决定是否执行当前分支。
                            if key != "score"
                        # 结束上方多行参数或数据结构。
                        },
                    # 结束上方多行参数或数据结构。
                    })

                    # 将 `q_info.copy()` 的结果保存到 `question_with_score`，供后续流程使用。
                    question_with_score = q_info.copy()
                    # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                    # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                    # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                    # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                    question_with_score["实际得分"] = per_score
                    # 调用 `questions_with_scores.append()`，保存或创建当前数据。
                    questions_with_scores.append(question_with_score)
                    # 执行表达式 `continue`，完成本行明确的数据运算或控制操作。
                    continue

                # 为每道题构造精简评分提示，减少上下文噪声
                # 将 `[` 的结果保存到 `per_query_parts`，供后续流程使用。
                per_query_parts = [
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"请对以下试题进行评分，仅返回该题的得分(数字即可)：",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"题目类型：{q_info['题目类型']}",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"题目内容：{q_info['题目内容']}",
                # 结束上方多行参数或数据结构。
                ]
                # 判断 `if q_info.get('选项')`，决定是否执行当前分支。
                # 判断 `if q_info.get('选项')`，决定是否执行当前分支。
                # 判断 `if q_info.get('选项')`，决定是否执行当前分支。
                # 判断 `if q_info.get('选项')`，决定是否执行当前分支。
                if q_info.get('选项'):
                    # 调用 `per_query_parts.append()`，保存或创建当前数据。
                    per_query_parts.append(f"选项：{q_info['选项']}")
                # 调用 `per_query_parts.extend()`，完成当前函数调用并取得其返回结果。
                per_query_parts.extend([
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"标准答案：{q_info['标准答案']}",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"解析：{q_info['解析']}",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"分值（满分）：{q_info['分值']}",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    f"考生答案：{q_info['考生答案']}",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "",
                    # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                    "请严格返回纯数字（可为小数），不要包含其他文字、单位或标点。如果用户没有作答，必须判0分，否则会收到惩罚！"
                # 结束上方多行参数或数据结构。
                ])
                # 将 `"\n".join([p for p in per_query_parts if p])` 的结果保存到 `per_query`，供后续流程使用。
                per_query = "\n".join([p for p in per_query_parts if p])

                # 逐题调用工作流进行评分
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                # 开始执行可能失败的步骤，由后续异常分支统一处理。
                try:
                    # 将 `await self.dify_service.call_workflow_sync(` 的结果保存到 `per_result`，供后续流程使用。
                    per_result = await self.dify_service.call_workflow_sync(
                        # 将 `6` 的结果保存到 `workflow_type`，供后续流程使用。
                        workflow_type=6,
                        # 将 `per_query` 的结果保存到 `query`，供后续流程使用。
                        query=per_query,
                        # 将 `{` 的结果保存到 `additional_inputs`，供后续流程使用。
                        additional_inputs={
                            # 设置 `type` 字段，值来自 `6`。
                            "type": 6,
                            # 设置 `max_score` 字段，值来自 `q_info["分值"]`。
                            "max_score": q_info["分值"],
                            # 设置 `question_type` 字段，值来自 `q_info["题目类型"]`。
                            "question_type": q_info["题目类型"],
                            # 设置 `question_index` 字段，值来自 `idx`。
                            "question_index": idx
                        # 结束上方多行参数或数据结构。
                        }
                    # 结束上方多行参数或数据结构。
                    )
                    # 解析返回的数值得分
                    # 将 `per_result.get("answer", "0")` 的结果保存到 `raw_answer`，供后续流程使用。
                    raw_answer = per_result.get("answer", "0")
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    try:
                        # 将 `float(str(raw_answer).strip())` 的结果保存到 `per_score`，供后续流程使用。
                        per_score = float(str(raw_answer).strip())
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    except ValueError:
                        # 将 `0.0` 的结果保存到 `per_score`，供后续流程使用。
                        per_score = 0.0
                    # 边界保护：0 到满分
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    # 开始执行可能失败的步骤，由后续异常分支统一处理。
                    try:
                        # 将 `float(q_info["分值"]) if q_info.get("分值") is not None else None` 的结果保存到 `max_score`，供后续流程使用。
                        max_score = float(q_info["分值"]) if q_info.get("分值") is not None else None
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    # 捕获指定异常，记录或转换为上层能够识别的错误。
                    except Exception:
                        # 将 `None` 的结果保存到 `max_score`，供后续流程使用。
                        max_score = None
                    # 判断 `if max_score is not None`，决定是否执行当前分支。
                    # 判断 `if max_score is not None`，决定是否执行当前分支。
                    # 判断 `if max_score is not None`，决定是否执行当前分支。
                    # 判断 `if max_score is not None`，决定是否执行当前分支。
                    if max_score is not None:
                        # 将 `max(0.0, min(per_score, max_score))` 的结果保存到 `per_score`，供后续流程使用。
                        per_score = max(0.0, min(per_score, max_score))
                    # 执行表达式 `total_score += per_score`，完成本行明确的数据运算或控制操作。
                    total_score += per_score

                    # 调用 `per_question_results.append()`，保存或创建当前数据。
                    per_question_results.append({
                        # 设置 `题目编号` 字段，值来自 `q_info["题目编号"]`。
                        "题目编号": q_info["题目编号"],
                        # 设置 `scoring_method` 字段，值来自 `"dify"`。
                        "scoring_method": "dify",
                        # 设置 `raw_answer` 字段，值来自 `raw_answer`。
                        "raw_answer": raw_answer,
                        # 设置 `parsed_score` 字段，值来自 `per_score`。
                        "parsed_score": per_score
                    # 结束上方多行参数或数据结构。
                    })
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                # 捕获指定异常，记录或转换为上层能够识别的错误。
                except Exception as e:
                    # 失败容错：给0分并记录错误
                    # 将 `0.0` 的结果保存到 `per_score`，供后续流程使用。
                    per_score = 0.0
                    # 调用 `per_question_results.append()`，保存或创建当前数据。
                    per_question_results.append({
                        # 设置 `题目编号` 字段，值来自 `q_info["题目编号"]`。
                        "题目编号": q_info["题目编号"],
                        # 设置 `scoring_method` 字段，值来自 `"dify"`。
                        "scoring_method": "dify",
                        # 设置 `error` 字段，值来自 `str(e)`。
                        "error": str(e),
                        # 设置 `parsed_score` 字段，值来自 `per_score`。
                        "parsed_score": per_score
                    # 结束上方多行参数或数据结构。
                    })

                # 汇总到题目结构
                # 将 `q_info.copy()` 的结果保存到 `question_with_score`，供后续流程使用。
                question_with_score = q_info.copy()
                # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                # 将 `per_score` 的结果保存到 `question_with_score["实际得分"]`，供后续流程使用。
                question_with_score["实际得分"] = per_score
                # 调用 `questions_with_scores.append()`，保存或创建当前数据。
                questions_with_scores.append(question_with_score)

            # 构建完整的考试数据JSON
            # 将 `{` 的结果保存到 `exam_data`，供后续流程使用。
            exam_data = {
                # 设置 `exam_info` 字段，值来自 `{`。
                "exam_info": {
                    # 设置 `exam_id` 字段，值来自 `exam_id`。
                    "exam_id": exam_id,
                    # 设置 `title` 字段，值来自 `exam.title`。
                    "title": exam.title,
                    # 设置 `description` 字段，值来自 `exam.description`。
                    "description": exam.description,
                    # 设置 `total_score` 字段，值来自 `exam.total_score`。
                    "total_score": exam.total_score,
                    # 设置 `time_limit` 字段，值来自 `exam.duration`。
                    "time_limit": exam.duration,
                    # 设置 `instructions` 字段，值来自 `exam.description or "请认真答题，注意时间限制。"`。
                    "instructions": exam.description or "请认真答题，注意时间限制。"
                # 结束上方多行参数或数据结构。
                },
                # 设置 `questions` 字段，值来自 `questions_with_scores`。
                "questions": questions_with_scores,
                # 设置 `student_answers` 字段，值来自 `student_answers`。
                "student_answers": student_answers,
                # 设置 `scoring_result` 字段，值来自 `{`。
                "scoring_result": {
                    # 设置 `mode` 字段，值来自 `"per_question"`。
                    "mode": "per_question",
                    # 设置 `results` 字段，值来自 `per_question_results`。
                    "results": per_question_results
                # 结束上方多行参数或数据结构。
                },
                # 设置 `submit_time` 字段，值来自 `datetime.utcnow().isoformat() + "Z"`。
                "submit_time": datetime.utcnow().isoformat() + "Z",
                # 设置 `total_actual_score` 字段，值来自 `total_score`。
                "total_actual_score": total_score,
                # 设置 `score_percentage` 字段，值来自 `round((total_score / exam.total_score) * 100, 2) if exam.total_score > 0 else 0`。
                "score_percentage": round((total_score / exam.total_score) * 100, 2) if exam.total_score > 0 else 0
            # 结束上方多行参数或数据结构。
            }

            # 创建考试结果记录
            # 将 `ExamResult(` 的结果保存到 `exam_result`，供后续流程使用。
            exam_result = ExamResult(
                # 将 `uuid.uuid4()` 的结果保存到 `id`，供后续流程使用。
                id=uuid.uuid4(),
                # 将 `exam.id` 的结果保存到 `exam_id`，供后续流程使用。
                exam_id=exam.id,
                # 将 `exam.title` 的结果保存到 `exam_name`，供后续流程使用。
                exam_name=exam.title,
                # 将 `student_name` 的结果保存到 `student_name`，供后续流程使用。
                student_name=student_name,
                # 将 `department` 的结果保存到 `department`，供后续流程使用。
                department=department,
                # 将 `exam.total_score` 的结果保存到 `total_possible_score`，供后续流程使用。
                total_possible_score=exam.total_score,
                # 将 `total_score` 的结果保存到 `total_actual_score`，供后续流程使用。
                total_actual_score=total_score,
                # 将 `exam_data` 的结果保存到 `exam_data`，供后续流程使用。
                exam_data=exam_data,
                # 将 `datetime.utcnow()` 的结果保存到 `submit_time`，供后续流程使用。
                submit_time=datetime.utcnow(),
                # 将 `"completed"` 的结果保存到 `status`，供后续流程使用。
                status="completed"
            # 结束上方多行参数或数据结构。
            )

            # 调用 `self.db.add()`，保存或创建当前数据。
            self.db.add(exam_result)

            # 提交数据库事务
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            # 异步等待 `self.db.commit()` 完成，取得数据库或外部服务处理结果。
            await self.db.commit()

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"考试提交并保存成功，考生: {student_name}, 考试结果ID: {exam_result.id}")

            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return {
                # 设置 `message` 字段，值来自 `"考试提交成功"`。
                "message": "考试提交成功",
                # 设置 `exam_result_id` 字段，值来自 `str(exam_result.id)`。
                "exam_result_id": str(exam_result.id),
                # 设置 `student_name` 字段，值来自 `student_name`。
                "student_name": student_name,
                # 设置 `department` 字段，值来自 `department`。
                "department": department,
                # 设置 `exam_title` 字段，值来自 `exam.title`。
                "exam_title": exam.title,
                # 设置 `total_possible_score` 字段，值来自 `exam.total_score`。
                "total_possible_score": exam.total_score,
                # 设置 `total_actual_score` 字段，值来自 `total_score`。
                "total_actual_score": total_score,
                # 设置 `score_percentage` 字段，值来自 `round((total_score / exam.total_score) * 100, 2) if exam.total_score > 0 else 0`。
                "score_percentage": round((total_score / exam.total_score) * 100, 2) if exam.total_score > 0 else 0,
                # 设置 `questions` 字段，值来自 `questions_with_scores`。
                "questions": questions_with_scores
            # 结束上方多行参数或数据结构。
            }

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"提交考试时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise

    # 定义 `export_exam_result_to_csv()`，实现该函数对应的接口或业务流程。
    async def export_exam_result_to_csv(
        # 执行表达式 `self,`，完成本行明确的数据运算或控制操作。
        self,
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        # 声明 `result_id` 参数（考试结果唯一标识），类型为 `str`。
        result_id: str
    # 结束上方多行参数或数据结构。
    ) -> str:
        """
        导出考试结果为CSV格式

        Args:
            result_id: 考试结果ID

        Returns:
            CSV格式的字符串
        """
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        # 开始执行可能失败的步骤，由后续异常分支统一处理。
        try:
            # 获取考试结果详情
            # 将 `await self.get_exam_result(result_id)` 的结果保存到 `exam_result`，供后续流程使用。
            exam_result = await self.get_exam_result(result_id)

            # 解析题目数据
            # 将 `exam_result.get("exam_data", {})` 的结果保存到 `exam_data`，供后续流程使用。
            exam_data = exam_result.get("exam_data", {})
            # 判断 `if isinstance(exam_data, str)`，决定是否执行当前分支。
            # 判断 `if isinstance(exam_data, str)`，决定是否执行当前分支。
            # 判断 `if isinstance(exam_data, str)`，决定是否执行当前分支。
            # 判断 `if isinstance(exam_data, str)`，决定是否执行当前分支。
            if isinstance(exam_data, str):
                # 将 `json.loads(exam_data)` 的结果保存到 `exam_data`，供后续流程使用。
                exam_data = json.loads(exam_data)

            # 将 `exam_data.get("questions", [])` 的结果保存到 `questions`，供后续流程使用。
            questions = exam_data.get("questions", [])

            # 构建CSV内容
            # 导入 `csv` 模块，供后续代码使用。
            import csv
            # 导入 `io` 模块，供后续代码使用。
            import io

            # 将 `io.StringIO()` 的结果保存到 `output`，供后续流程使用。
            output = io.StringIO()
            # 将 `csv.writer(output)` 的结果保存到 `writer`，供后续流程使用。
            writer = csv.writer(output)

            # 写入表头
            # 调用 `writer.writerow()`，完成当前函数调用并取得其返回结果。
            writer.writerow([
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "考生姓名", "所属部门", "考试名称", "总分", "得分", "得分率(%)", "提交时间"
            # 结束上方多行参数或数据结构。
            ])

            # 写入概要信息
            # 调用 `writer.writerow()`，完成当前函数调用并取得其返回结果。
            writer.writerow([
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("student_name", ""),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("department", ""),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("exam_name", ""),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("total_possible_score", 0),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("total_actual_score", 0),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("score_percentage", 0),
                # 调用 `exam_result.get()`，获取所需数据。
                exam_result.get("submit_time", "")
            # 结束上方多行参数或数据结构。
            ])

            # 空行
            # 调用 `writer.writerow()`，完成当前函数调用并取得其返回结果。
            writer.writerow([])

            # 写入题目详情表头
            # 调用 `writer.writerow()`，完成当前函数调用并取得其返回结果。
            writer.writerow([
                # 补充当前 Prompt、日志、响应文本或多行字符串内容。
                "题号", "题目类型", "题目内容", "分值", "考生答案", "标准答案", "实际得分", "解析"
            # 结束上方多行参数或数据结构。
            ])

            # 写入题目详情
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            # 遍历当前集合或异步数据流，逐项执行处理。
            for i, question in enumerate(questions, 1):
                # 调用 `writer.writerow()`，完成当前函数调用并取得其返回结果。
                writer.writerow([
                    # 执行表达式 `i,`，完成本行明确的数据运算或控制操作。
                    i,
                    # 调用 `question.get()`，获取所需数据。
                    question.get("题目类型", ""),
                    # 调用 `question.get()`，获取所需数据。
                    question.get("题目内容", "")[:50] + "..." if len(question.get("题目内容", "")) > 50 else question.get("题目内容", ""),
                    # 调用 `question.get()`，获取所需数据。
                    question.get("分值", 0),
                    # 调用 `str()`，完成当前函数调用并取得其返回结果。
                    str(question.get("考生答案", ""))[:30] + "..." if len(str(question.get("考生答案", ""))) > 30 else str(question.get("考生答案", "")),
                    # 调用 `str()`，完成当前函数调用并取得其返回结果。
                    str(question.get("标准答案", ""))[:30] + "..." if len(str(question.get("标准答案", ""))) > 30 else str(question.get("标准答案", "")),
                    # 调用 `question.get()`，获取所需数据。
                    question.get("实际得分", 0),
                    # 调用 `question.get()`，获取所需数据。
                    question.get("解析", "")[:50] + "..." if len(question.get("解析", "")) > 50 else question.get("解析", "")
                # 结束上方多行参数或数据结构。
                ])

            # 将 `output.getvalue()` 的结果保存到 `csv_content`，供后续流程使用。
            csv_content = output.getvalue()
            # 调用 `output.close()`，完成当前函数调用并取得其返回结果。
            output.close()

            # 调用 `logger.info()`，完成当前函数调用并取得其返回结果。
            logger.info(f"成功导出考试结果为CSV: {result_id}")
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            # 结束函数并把结果返回给调用方。
            return csv_content

        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        # 捕获指定异常，记录或转换为上层能够识别的错误。
        except Exception as e:
            # 调用 `logger.error()`，完成当前函数调用并取得其返回结果。
            logger.error(f"导出考试结果为CSV时出错: {str(e)}")
            # 执行表达式 `raise`，完成本行明确的数据运算或控制操作。
            raise
