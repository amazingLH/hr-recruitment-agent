"""
* 文件作用
  * 这是试卷和题目的 SQLAlchemy ORM 模型。Exam 映射 exams 表，保存标题、科目、难度、时长、总分、题型配置、知识库文件和原始生成内容；Question 映射 questions 表，保存题型、题干、选项、标准答案、分值、顺序和解析。两者通过 Question.exam_id 建立一对多关系，并使用 delete-orphan 级联删除试卷下的题目。
* 类说明
  * `Exam`：exams 表 ORM 类，表示一份试卷及其生成配置，并通过 questions 关系包含多道 Question。
  * `Question`：questions 表 ORM 类，表示一道题，通过 exam_id 外键归属于一份 Exam。
* 函数与方法说明
  * 本文件没有定义函数，主要声明数据库模型字段与表关系。
"""

# 从 `sqlalchemy` 导入 `Column, String, Integer, Text, JSON, ForeignKey, Float`，供本文件调用。
from sqlalchemy import Column, String, Integer, Text, JSON, ForeignKey, Float
# 从 `sqlalchemy.dialects.postgresql` 导入 `UUID`，供本文件调用。
from sqlalchemy.dialects.postgresql import UUID
# 从 `sqlalchemy.orm` 导入 `relationship`，供本文件调用。
from sqlalchemy.orm import relationship
# 从 `app.models.base` 导入 `BaseModel`，供本文件调用。
from app.models.base import BaseModel


# 定义 `Exam` 类，封装该类对应的数据字段和业务方法。
class Exam(BaseModel):
    """ 试卷模型 """

    # 将 `"exams"` 的结果保存到 `__tablename__`，供后续流程使用。
    __tablename__ = "exams"

    # 定义 `title` 字段，用于保存试卷标题；数据库类型为 `String(255)`，不能为空。
    title = Column(String(255), nullable=False, comment="试卷标题")
    # 定义 `subject` 字段，用于保存科目；数据库类型为 `String(100)`，不能为空。
    subject = Column(String(100), nullable=False, comment="科目")
    # 定义 `description` 字段，用于保存试卷描述；数据库类型为 `Text`，允许为空。
    description = Column(Text, nullable=True, comment="试卷描述")
    # 定义 `difficulty` 字段，用于保存难度等级；数据库类型为 `String(50)`，不能为空。
    difficulty = Column(String(50), nullable=False, comment="难度等级")
    # 定义 `duration` 字段，用于保存考试时长(分钟)；数据库类型为 `Integer`，不能为空。
    duration = Column(Integer, nullable=False, comment="考试时长(分钟)")
    # 定义 `total_score` 字段，用于保存总分；数据库类型为 `Float`，不能为空。
    total_score = Column(Float, nullable=False, comment="总分")

    # 试卷配置信息
    # 定义 `question_types` 字段，用于保存题型配置；数据库类型为 `JSON`，允许为空。
    question_types = Column(JSON, nullable=True, comment="题型配置")
    # 定义 `question_counts` 字段，用于保存题目数量配置；数据库类型为 `JSON`，允许为空。
    question_counts = Column(JSON, nullable=True, comment="题目数量配置")
    # 定义 `knowledge_files` 字段，用于保存知识库文件；数据库类型为 `JSON`，允许为空。
    knowledge_files = Column(JSON, nullable=True, comment="知识库文件")
    # 定义 `special_requirements` 字段，用于保存特殊要求；数据库类型为 `Text`，允许为空。
    special_requirements = Column(Text, nullable=True, comment="特殊要求")
    # 定义 `content` 字段，用于保存原始试卷内容；数据库类型为 `Text`，允许为空。
    content = Column(Text, nullable=True, comment="原始试卷内容")

    # 关联关系: 表示一个试卷 可以包含多道题
    # 定义 ORM 关系 `questions`，用于关联 `Question` 对象及配置级联行为。
    questions = relationship("Question", back_populates="exam", cascade="all, delete-orphan")
    '''
        cascade="all, delete-orphan"： 级联删除关系，意思是删除试卷时，会自动删除关联的题目。
         - 保存试卷时：自动保存关联的题目
         - 删除试卷时：自动删除所有关联题目
         - 移除题目时：如果题目不再属于任何试卷，自动删除该题目
    '''


# 定义 `Question` 类，封装该类对应的数据字段和业务方法。
class Question(BaseModel):
    """试题模型"""

    # 将 `"questions"` 的结果保存到 `__tablename__`，供后续流程使用。
    __tablename__ = "questions"

    # 定义 `question_type` 字段，用于保存题型；数据库类型为 `String(50)`，不能为空。
    question_type = Column(String(50), nullable=False, comment="题型")
    # 定义 `question_text` 字段，用于保存题目内容；数据库类型为 `Text`，不能为空。
    question_text = Column(Text, nullable=False, comment="题目内容")
    # 定义 `options` 字段，用于保存选择题选项；数据库类型为 `JSON`，允许为空。
    options = Column(JSON, nullable=True, comment="选择题选项")
    # 定义 `correct_answer` 字段，用于保存正确答案；数据库类型为 `Text`，允许为空。
    correct_answer = Column(Text, nullable=True, comment="正确答案")
    # 定义 `score` 字段，用于保存分值；数据库类型为 `Float`，不能为空。
    score = Column(Float, nullable=False, comment="分值")
    # 定义 `order_index` 字段，用于保存题目顺序；数据库类型为 `Integer`，不能为空。
    order_index = Column(Integer, nullable=False, comment="题目顺序")
    # 定义 `explanation` 字段，用于保存题目解释；数据库类型为 `Text`，允许为空。
    explanation = Column(Text, nullable=True, comment="题目解释")

    # 关联关系: 表示题目所属试卷
    # 定义 `exam_id` 字段，用于保存所属试卷ID；数据库类型为 `UUID(as_uuid=True)`，不能为空。
    exam_id = Column(UUID(as_uuid=True), ForeignKey("exams.id"), nullable=False, comment="所属试卷ID")
    # 声明 SQLAlchemy 模型关系及级联规则。
    # 定义 ORM 关系 `exam`，用于关联 `Exam` 对象及配置级联行为。
    exam = relationship("Exam", back_populates="questions")
