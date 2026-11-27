"""
v1端点的主API路由
"""
from fastapi import APIRouter

from app.api.v1.endpoints import auth, users, conversations, documents, knowledge_base, chat, knowledge_assistant, stats, hr_workflows, scoring_criteria, job_description, resume_evaluation, interview_plan, email_configs, exam_management, intent_router, agent

'''
路由的作用：路由相当于后端接口的“导航表”，FastAPI 根据请求方式和 URL，找到并执行对应的处理函数。
    1、例如：
        @router.post("/route")
        async def route_by_intent():
        ...
        表示收到 POST /route 请求时，执行 route_by_intent()。
    2、运行时的请求顺序：    
        前端发送 POST /api/v1/intent/route
        → FastAPI 根据请求方式和 URL 查找匹配的路由
        → 执行 Depends(get_current_user) 验证用户
        → 执行 Depends(get_db) 获取数据库会话
        → 执行 route_by_intent()
        → 调用 IntentService
        → 返回 JSON 给前端
'''

# 创建一个 FastAPI 路由聚合器，用于统一收集并管理项目中的所有 v1 业务路由
api_router = APIRouter()

# include_router()= 把子路由中的所有接口注册到上一级路由。（详细解释：include_router() 是 FastAPI 提供的路由注册方法，作用是：把一个子路由对象中的所有接口，统一注册到上一级路由中。）
# intent_router.router= 取得 intent_router.py 文件中的 router 路由对象
# 挂载认证模块路由；统一增加 /auth 前缀，并在 Swagger 文档中归入 authentication 分组
api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])

# 挂载用户管理路由；统一增加 /users 前缀，并在接口文档中归入 users 分组
api_router.include_router(users.router, prefix="/users", tags=["users"])

# 挂载会话管理路由；负责会话创建、查询、更新等功能
api_router.include_router(conversations.router, prefix="/conversations", tags=["conversations"])

# 挂载文档管理路由；负责文档上传、查询、删除和状态管理等功能
api_router.include_router(documents.router, prefix="/documents", tags=["documents"])

# 挂载知识库管理路由；负责知识库及知识库文档的管理
api_router.include_router(knowledge_base.router, prefix="/knowledge-base", tags=["knowledge-base"])

# 挂载聊天路由；负责接收聊天消息及返回模型回答
api_router.include_router(chat.router, prefix="/chat", tags=["chat"])

# 挂载知识助手路由；负责结合知识库完成检索增强问答
api_router.include_router(knowledge_assistant.router, prefix="/knowledge-assistant", tags=["knowledge-assistant"])

# 挂载统计数据路由；负责工作台统计、趋势数据和活动记录等功能
api_router.include_router(stats.router, prefix="/stats", tags=["stats"])

# 挂载 HR 工作流路由；负责组织和调用招聘、培训等 HR 业务流程
api_router.include_router(hr_workflows.router, prefix="/hr-workflows", tags=["hr-workflows"])

# 挂载职位描述路由；负责职位信息管理和 JD 生成
api_router.include_router(job_description.router, prefix="/job-descriptions", tags=["job-descriptions"])

# 挂载评分标准路由；负责评分维度、权重和评分规则的管理
api_router.include_router(scoring_criteria.router, prefix="/scoring-criteria", tags=["scoring-criteria"])

# 挂载简历评估路由；负责简历解析、岗位匹配和候选人评分
api_router.include_router(resume_evaluation.router, prefix="/resume-evaluation", tags=["resume-evaluation"])

# 挂载面试计划路由；负责创建、查询和管理面试计划
api_router.include_router(interview_plan.router, prefix="/interview-plans", tags=["interview-plans"])

# 挂载邮箱配置路由；负责邮箱账号、服务器配置及邮件处理规则管理
api_router.include_router(email_configs.router, prefix="/email-configs", tags=["email-configs"])

# 挂载考试管理路由；负责试卷、题目、考试发布和考试结果管理
api_router.include_router(exam_management.router, prefix="/exam-management", tags=["exam-management"])

# 挂载意图识别路由；负责识别用户意图并决定后续业务处理方向
'''
下面这段代码的解释
    把 intent_router.py 中 router 包含的全部接口
    → 注册到总路由 api_router
    → 统一添加 /intent 前缀
    → 在接口文档中归入 intent 分组
'''
api_router.include_router(intent_router.router, prefix="/intent", tags=["intent"])

# 挂载智能 Agent 路由；负责接收 Agent 请求并调度模型、工具和业务能力
api_router.include_router(agent.router, prefix="/agent", tags=["agent"])


@api_router.get("/health")
async def health_check():
    """健康检查端点"""
    return {"status": "healthy", "message": "HR Agent API正在运行"}
