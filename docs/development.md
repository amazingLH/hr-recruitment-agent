# 开发、接口与验证

## 代码阅读顺序

先看[Agent请求与产物协议](../backend/app/schemas/agent.py)、[Agent接口](../backend/app/api/v1/endpoints/agent.py)，再看[AgentService](../backend/app/services/agent_service.py)以及三个拆分文件：[_core](../backend/app/services/_agent_service_core.py)、[_resources](../backend/app/services/_agent_service_resources.py)、[_editing](../backend/app/services/_agent_service_editing.py)。

知识链路读[文档处理](../backend/app/services/enhanced_document_service.py)、[RAG](../backend/app/services/rag_service.py)和[重排](../backend/app/services/rerank_service.py)；技能链路读[运行时](../backend/app/services/agent_skills.py)、[邮件清单](../backend/skills/hr-agent-email/skill.json)与[阶段脚本](../backend/skills/hr-agent-email/scripts/email_workflow.py)。

## Agent接口

API统一前缀为`/api/v1`，业务接口需要有效登录身份。

| 路径 | 形式 | 用途 |
| --- | --- | --- |
| `/agent/chat` | POST JSON | 返回完整Agent响应 |
| `/agent/chat/stream` | POST JSON＋SSE | 统一流式入口 |
| `/agent/stream` | POST JSON＋SSE | 已确认条件的岗位生成 |
| `/agent/resume-screen/stream` | POST multipart＋SSE | 候选材料处理，带岗位ID和文件 |
| `/agent/interview-plan/stream` | POST multipart＋SSE | 根据已评价资源生成沟通方案 |
| `/agent/exam/stream` | POST JSON＋SSE | 已确认配置的测验生成 |

通用请求字段包括`message`、`conversation_id`、`auto_execute`、`confirmed_requirements`和附件元信息。附件元信息不等于文件已经上传；需要实际字节的流程使用multipart接口。

```json
{
  "message": "生成一个Python开发岗位的岗位说明",
  "conversation_id": null,
  "auto_execute": true,
  "attachments": []
}
```

返回结构包含`steps`、`artifacts`、`requires_confirmation`及`missing_fields`。缺条件时前端应补表单后续接，不应把提醒文案作为生成产物保存。

## SSE契约

Agent分支可能发送`plan`、`progress`、`delta`、`final`或`error`，由`type`区分；RAG分支使用自己的`start/chunk/end/error`结构并在结束返回来源。不同接口的事件格式不能直接共用一个未经分支判断的解析器。

SSE建立成功仅代表连接可用，最终产物需从`final`或结束事件核对。流式文本、工具进度和资源保存结果分开处理，避免重复拼接导致内容重影。

## 验证矩阵

| 模块 | 应覆盖的输入 | 检查结果 |
| --- | --- | --- |
| 路由 | 普通对话、明确任务、模糊任务、规则与模型冲突 | 普通聊天不误调用工具，意图属于白名单 |
| 岗位生成 | 完整条件、缺字段、确认后执行 | 条件正确、产物完整、保存ID真实 |
| 编辑 | 最近资源、同名多资源、跨会话资源 | 正确定位或要求选择，不擅自修改 |
| 候选处理 | 单份、多份、空文件、缺岗位 | 前置条件、逐份结果及资源关联一致 |
| Skill | 草稿、取消、确认、缺配置、重复确认 | 草稿不发送，确认后才进入发送阶段 |
| 文档 | 重复文件、超长段落、中文、解析失败 | 去重范围正确，分块可溯源，状态可信 |
| RAG | 语义表达、专有名词、无答案、不同知识库 | 召回、融合、来源与归属正确 |
| 测验 | 不同题量、客观题、简答题、越界评分 | 题量分值合理、评分被限定到合法范围 |
| 权限 | 无令牌、其他用户资源ID | 后端拒绝越权，不依赖前端隐藏 |

外部Dify、HR服务和SMTP分别记录请求结果。排查时先分清“生成失败”“保存失败”“投递失败”，不要仅依据最后一条聊天文本判断完成。

## 评测指标与统计口径

Agent回归覆盖150个测试用例，每个用例运行3轮，共450次执行。意图识别准确率为`95.56%`，人工确认判断准确率为`99.11%`，输出契约通过率为`99.33%`。三项分别衡量意图分类、确认决策和结构化业务输出，不能直接等同于端到端任务成功率。

知识检索覆盖120个知识问答样本，`Recall@5`为`94.17%`，RAGAS回答正确性平均得分为`0.91`。检索召回与生成质量分别统计；回答正确性是评分均值，不能解释为91%的问题完全答对。

每次执行记录输入、参考资源、模型与Dify版本、知识库版本、期望与实际意图、确认行为、输出契约检查和业务副作用。检索对照固定语料与问题，分别比较查询增强、全文召回、向量召回、融合和重排。指标汇总协议见[评测模块](../benchmarks/evaluation/README.md)。

## 扩展业务或Skill

新增业务工具需同步工具清单、允许意图、前置条件、执行分支、产物类型与界面组件。新增Skill需定义清单、阶段脚本和确认动作，并检查脚本加载是否只来自受信任目录。

新增外部写操作应先设计资源归属、确认版本与幂等记录；仅增加一个按钮或提示词约束，不能解决重复提交和超时后的重复执行。增加文档后台任务时，也需明确原文件、文档状态和向量索引失败后的补偿关系。
