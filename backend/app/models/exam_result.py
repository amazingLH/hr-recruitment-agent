"""
* 文件作用
  * 这是考试结果的 SQLAlchemy ORM 模型，ExamResult 映射 exam_results 表，保存试卷 ID、考生、部门、满分、实际得分、提交时间和状态。exam_data JSON 保存完整试卷信息、考生答案、逐题实际得分、评分方式和错误记录，便于结果详情展示与 CSV 导出。
* 类说明
  * `ExamResult`：exam_results 表 ORM 类，表示一次考试提交及其总分和完整 JSON 评分明细。
* 函数与方法说明
  * 本文件没有定义函数，主要声明数据库模型字段与表关系。
"""

# 从 `sqlalchemy` 导入 `Column, String, Integer, Text, JSON, ForeignKey, Float, DateTime`，供本文件调用。
from sqlalchemy import Column, String, Integer, Text, JSON, ForeignKey, Float, DateTime
# 从 `sqlalchemy.dialects.postgresql` 导入 `UUID`，供本文件调用。
from sqlalchemy.dialects.postgresql import UUID
# 从 `sqlalchemy.orm` 导入 `relationship`，供本文件调用。
from sqlalchemy.orm import relationship
# 从 `app.models.base` 导入 `BaseModel`，供本文件调用。
from app.models.base import BaseModel


# 定义 `ExamResult` 类，封装该类对应的数据字段和业务方法。
class ExamResult(BaseModel):
    """考试结果模型"""
    
    # 将 `"exam_results"` 的结果保存到 `__tablename__`，供后续流程使用。
    __tablename__ = "exam_results"
    
    # 基本元数据
    # 定义 `exam_id` 字段，用于保存考试ID；数据库类型为 `UUID(as_uuid=True)`，不能为空。
    exam_id = Column(UUID(as_uuid=True), nullable=False, comment="考试ID")
    # 定义 `exam_name` 字段，用于保存考试名称；数据库类型为 `String(200)`，不能为空。
    exam_name = Column(String(200), nullable=False, comment="考试名称")
    # 定义 `student_name` 字段，用于保存考生姓名；数据库类型为 `String(100)`，不能为空。
    student_name = Column(String(100), nullable=False, comment="考生姓名")
    # 定义 `department` 字段，用于保存考生部门；数据库类型为 `String(100)`，允许为空。
    department = Column(String(100), nullable=True, comment="考生部门")
    # 定义 `total_possible_score` 字段，用于保存试卷总分；数据库类型为 `Float`，不能为空。
    total_possible_score = Column(Float, nullable=False, comment="试卷总分")
    # 定义 `total_actual_score` 字段，用于保存实际得分；数据库类型为 `Float`，不能为空。
    total_actual_score = Column(Float, nullable=False, comment="实际得分")
    
    # 完整考试数据（JSON格式，包含试卷、答案、评分等所有信息）
    # 定义 `exam_data` 字段，用于保存完整考试数据JSON，包含试卷内容、学生答案、评分结果等；数据库类型为 `JSON`，不能为空。
    exam_data = Column(JSON, nullable=False, comment="完整考试数据JSON，包含试卷内容、学生答案、评分结果等")
    
    # 提交时间
    # 定义 `submit_time` 字段，用于保存提交时间；数据库类型为 `DateTime`，允许为空。
    submit_time = Column(DateTime, nullable=True, comment="提交时间")
    
    # 状态信息
    # 定义 `status` 字段，用于保存考试状态；数据库类型为 `String(20)`，不能为空，默认值为 `"completed"`。
    status = Column(String(20), nullable=False, default="completed", comment="考试状态")
