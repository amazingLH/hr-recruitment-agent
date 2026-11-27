# 配置与本地运行

## 环境与依赖

使用Python 3.11。前端锁定Vite 7依赖，可使用Node.js 22.12及以上的22.x环境；不要只依据`package.json`较宽的Node下限判断实际构建兼容性。

基础服务是带vector扩展的PostgreSQL，应用后端示例端口`8001`，前端固定端口`3001`。模型问答需要兼容聊天和Embedding服务；业务生成需要Dify；岗位等资源的远程读写需要独立HR服务。仓库当前没有提供这两个外部服务的完整部署包。

## 安装与启动

```bash
python -m venv backend/.venv
# Windows PowerShell：backend/.venv/Scripts/Activate.ps1
# Linux/macOS：source backend/.venv/bin/activate
python -m pip install -r backend/requirements.txt
```

复制`backend/.env.example`为`backend/.env`。设置完成后在`backend`执行：

```bash
uvicorn main:app --host 127.0.0.1 --port 8001
```

另开终端进入`frontend`，运行`npm ci`、`npm run dev`并访问`http://localhost:3001`。[Vite代理](../frontend/vite.config.js)把`/api`转发到`http://localhost:8001`，修改后端端口时同步代理。

## 必需配置

| 变量 | 用途 |
| --- | --- |
| `SECRET_KEY` | JWT签名密钥，配置要求至少32字符 |
| `DATABASE_URL` | PostgreSQL连接串，服务层转换为异步驱动 |
| `LLM_API_KEY` / `LLM_BASE_URL` / `LLM_MODEL` | 需求解析、Agent决策及问答模型 |
| `EMBEDDING_API_KEY` / `EMBEDDING_BASE_URL` / `EMBEDDING_MODEL` | 文档向量服务 |
| `VECTOR_DIMENSION` | 必须与实际Embedding输出和数据库列一致，默认1536 |
| `QWEN_API_KEY` / `QWEN_MODEL` | 可选重排，默认模型`gte-rerank-v2` |
| `DIFY_BASE_URL` / `DIFY_API_KEY` | Dify应用地址及应用密钥 |
| `DIFY_USER_ID` | Dify请求的业务用户标识 |
| `HR_SERVICE_HOST` / `HR_SERVICE_PORT` / `HR_SERVICE_APIKEY` | 独立HR资源服务 |

配置由[Pydantic Settings](../backend/app/core/config.py)从`backend/.env`读取；进程环境同名变量会影响实际值。生成随机密钥：

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

默认`HR_SERVICE_PORT=8000`与应用`8001`是不同服务。不要把远端资源地址指回当前应用形成循环请求；外部资源服务的API需要与[客户端](../backend/app/services/remote_service_client.py)匹配。

## 数据库准备

先创建连接串指定数据库，并安装支持pgvector的PostgreSQL服务。应用启动会尝试执行`CREATE EXTENSION IF NOT EXISTS vector`及建立ORM表，数据库账号必须有相应权限；也可由管理员提前创建扩展。

业务表包括用户、会话、文档、知识库、邮件配置、测验与成绩等。向量存储使用PGVector相关表。`VECTOR_DIMENSION`变更不是自动迁移操作，应先检查列Schema与既有向量，按新索引重建流程处理。

## Dify应用契约

[DifyService](../backend/app/services/dify_service.py)通过`POST {DIFY_BASE_URL}/chat-messages`调用应用，使用Bearer应用密钥。主载荷为`inputs.type`、`query`、`response_mode`、`conversation_id`和`user`。因此应准备支持该接口和分支输入的Chatflow/聊天应用，而不是只创建一个使用`/workflows/run`的Workflow后直接填入其密钥。

| `inputs.type` | 业务用途 |
| --- | --- |
| `1` | 岗位说明生成 |
| `2` | 评分标准生成 |
| `3` | 候选材料评价 |
| `4` | 沟通方案生成 |
| `5` | 测验内容生成 |
| `6` | 简答题评分 |

类型需要和应用分支一致，输出也必须符合服务侧解析格式。当前公开仓库不包含可直接导入的完整Dify应用DSL；应按上述契约配置自己的应用。`DIFY_BASE_URL`包含服务API前缀时，确保最终路径只出现一次`/chat-messages`。

部分同步评分调用的源码说明仍使用Workflow措辞，实际HTTP请求请以[DifyService](../backend/app/services/dify_service.py)实现为准，不按注释推断接口类型。

## RAG可调配置

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `KB_QUERY_ENHANCE_ENABLED` | `true` | 查询增强 |
| `KB_QUERY_EXPANSION_MAX_TERMS` | `6` | 扩展术语上限 |
| `RAG_CONTENT_WEIGHT` / `RAG_TEXT_WEIGHT` | `0.7` / `0.3` | 归一化后的双路分数组合 |
| `RAG_MIN_SIMILARITY_SCORE` | `0.2` | 融合结果过滤 |
| `CONTEXT_LIMIT` | `10` | 上下文文档数量 |
| `RERANK_ENABLED` | `true` | 精排开关 |
| `RERANK_TOP_K` / `RERANK_FINAL_K` | `10` / `10` | 候选与结果数量 |
| `MAX_FILE_SIZE` | `10MB` | 上传大小限制 |
| `UPLOAD_DIR` | `uploads` | 原文件保存位置 |

没有重排凭据时，可明确关闭`RERANK_ENABLED`并检查日志，不应把回退后的结果当作精排已执行。上传文档后先检查处理完成与块数量，再开始问答。

## 邮件与权限

通过应用的邮件配置模块设置测试邮箱、服务器、端口和授权信息。邮件Skill先生成草稿，检查收件人、主题、正文及附件后再确认发送。仅在独立测试邮箱验证流程，检查真实投递记录，不把“已生成发送请求”当作已送达。

本地同源代理能避免跨域问题；直连API时，还需配置`BACKEND_CORS_ORIGINS`并保持认证头一致。账号和资源权限由后端校验，隐藏页面按钮不能代替接口授权。

## 故障排查

| 现象 | 优先检查 |
| --- | --- |
| Settings校验失败 | `SECRET_KEY`长度、环境变量类型和配置文件位置 |
| 数据库初始化失败 | 数据库是否存在、账号权限及vector扩展 |
| 页面正常但业务生成失败 | Dify应用类型、密钥、`inputs.type`与输出格式 |
| 生成文本但列表没有资源 | HR资源服务连接、API契约与保存日志 |
| 向量入库报错 | 模型输出维度和数据库列维度 |
| 问答不带来源 | 文档处理状态、知识库范围、检索结果和是否走普通聊天 |
| SSE无增量 | 代理缓冲、流式事件格式及后端错误事件 |
| 邮件重复或未送达 | 草稿/发送阶段、SMTP响应、重试与幂等记录 |
