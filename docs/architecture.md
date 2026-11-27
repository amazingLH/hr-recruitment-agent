# 系统架构与模块实现

## 应用分层

[main.py](../backend/main.py)在生命周期中初始化数据库并启动邮件调度器。[API路由](../backend/app/api/v1/api.py)划分账号、会话、文档、知识库、Agent、业务生成与培训接口。服务层组合模型、Dify和远程资源客户端，ORM保存业务数据，Vue界面负责表单与产物展示。

FastAPI接口异步不表示整条链路所有操作都没有阻塞；文档解析、同步向量库调用及外部SDK要分别分析。当前没有把每次聊天统一投递到Redis/Celery任务队列。

## Agent工具与决策

[AgentService](../backend/app/services/agent_service.py)先规范附件，构建当前用户的会话记忆，再处理已确认请求；其余请求优先用明确业务规则匹配，规则不足时进入模型决策。

[核心控制器](../backend/app/services/_agent_service_core.py)维护工具白名单。每个`AgentToolSpec`包括名称、意图、路由、说明和前置条件。模型返回`thought`摘要、`action`、`intent`、`tool`、`observation`和置信度等结构，程序只接受允许的意图并转换为[ReActDecision](../backend/app/services/_agent_service_types.py)。动作分为普通对话、工具调用和缺条件追问。

这里采用ReAct式决策和分步骤反馈，不是允许模型无限自由循环；`thought`仅用于简短决策摘要，不是完整内部推理记录。业务执行由明确服务分支控制，模型JSON也不直接等于OpenAI原生工具调用协议。

## 业务链路与资源关联

岗位生成会先解析结构化条件，再调用Dify生成岗位文本，保存岗位资源后生成评分标准。产物元数据记录已保存ID；后续编辑从当前用户资源和会话产物中定位对象，有多个候选时先要求选择。

候选处理先要求材料和岗位ID，评价后保存结果，再用结果ID和岗位上下文生成沟通方案。测验根据选定知识文件组织题目配置。前后续接依赖真实资源ID，不能仅依据页面标题或模型随口提及的对象。

[RemoteServiceClient](../backend/app/services/remote_service_client.py)连接独立HR资源服务；[DifyService](../backend/app/services/dify_service.py)负责生成，两者不是同一个服务。Dify成功输出文字不代表远端资源已保存，界面应结合产物metadata判断保存状态。

## 记忆、编辑与删除

会话记忆从历史消息中的结构化产物、近期岗位与评价资源提取上下文，用于理解“刚才那个”“改一下规则”等指代。它不是让模型长期无界保存所有聊天，也不是额外的向量记忆Agent。

[编辑链路](../backend/app/services/_agent_service_editing.py)定位待修改对象并解析修改要求；[资源链路](../backend/app/services/_agent_service_resources.py)查找可删除对象、匹配条件并处理确认。名称匹配不能代替用户归属和资源ID检查。删除、外部写入与邮件分别有自己的确认逻辑，不能认为一个`auto_execute`字段覆盖全部安全边界。

## 本地Skill与邮件

[agent_skills.py](../backend/app/services/agent_skills.py)扫描`backend/skills`的可信目录，解析`SKILL.md`与`skill.json`，按意图和阶段动态加载Python脚本。清单定义默认阶段、前置条件、路由和确认动作。

邮件Skill的阶段为`draft`、`send`，确认动作为`send_email`。草稿阶段整理正文和发送请求，返回`requires_confirmation`；确认后才调发送阶段。真实投递由配置的邮件服务完成。

这套机制是应用内本地代码扩展，不是远端MCP服务器或任意插件沙箱。新增Skill应由维护者审查脚本和权限；清单内容本身不能隔离恶意Python代码。人工确认也不能单独保证SMTP重试不会重复投递，仍需持久化幂等记录。

## 文档处理

[EnhancedDocumentService](../backend/app/services/enhanced_document_service.py)按用户、知识库与SHA-256查重，保存原文件及文档元数据，再执行解析与向量化。上传成功和处理完成是两个不同状态，失败需保留错误并允许重试。

分块先尝试模型语义分割点，再按段落或句子拆过长块；超过1500字符时强制拆分，短于200字符的块与相邻块合并，合并后不超过1500。文档块保留文档、知识库等元数据，以便回查来源。切块长度是字符约束，不是固定Token数量。

## RAG链路

[RAGService](../backend/app/services/rag_service.py)先判断问题是否需要知识库。需要时进行查询增强和关键词提取，再执行pgvector向量检索与PostgreSQL全文匹配。英文/数字可使用`tsvector`及前缀查询，中文补充关键词`ILIKE`匹配；检索范围受集合条件约束。

两路结果分数归一化后加权融合，默认向量权重`0.7`、文本权重`0.3`，阈值`0.2`，默认上下文数量10；可选Rerank调整顺序。融合是分数加权，不是RRF名次融合。去重后把正文与来源放入提示词，流式生成答案并返回引用材料。

没有命中时的通用聊天或空来源回答应与有证据回答区分；检索相关不等于来源完全支持。更换Embedding模型或维度必须迁移对应索引，不能混用不同向量空间。

## 测验生成和评分

[ExamService](../backend/app/services/exam_service.py)读取选定文档，按单选、多选、简答等配置构造生成请求，解析输出并保存题目。当前分值组织包含`2:3:5`权重规则及整数分配处理。

提交时客观题根据标准答案本地判定，简答题逐题请求Dify评分，并把得分限制在零与该题满分之间，最后保存逐题结果和总分。大模型评分是辅助结果，需要结合题目、标准答案和评分依据核对。

## 前后端数据契约

[AgentChatResponse](../backend/app/schemas/agent.py)包含消息、意图、页面路由、`steps`、`artifacts`、后续建议、`requires_confirmation`与缺失字段。界面根据产物类型显示表单、候选选择、业务卡片和草稿。

SSE通过`data:`发送计划、进度、文本增量和最终结果，最后输出`[DONE]`。HTTP流建立成功不代表业务完成，前端应以最终事件和产物保存状态更新界面，取消连接也不等于已回滚外部操作。
