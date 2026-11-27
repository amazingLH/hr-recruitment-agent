# HR Recruitment Agent

自然语言驱动招聘和培训业务。FastAPI 提供白名单工具和业务接口，Vue 展示结构化产物，PostgreSQL/pgvector 支撑知识库检索，Dify 承载生成流程。

## 本地运行

启用 PostgreSQL vector 扩展。招聘生成需自己的 Dify Chatflow 应用及资源服务配置。Chatflow 通过 /chat-messages 调用，inputs.type 对应岗位描述、评分标准、候选评价、方案、试卷和简答评分。邮件默认应使用隔离测试邮箱。

在 `backend` 创建 Python 3.11 虚拟环境。复制 `.env.example` 为 `.env` 后填写本地服务与密钥，按需导出为进程环境变量。签名密钥请在本地独立生成。

```sh
pip install -r requirements.txt
uvicorn main:app --host 127.0.0.1 --port 8001
```

在 `frontend` 安装依赖并启动开发服务。根据本机环境配置后端地址，后端示例端口为 8001。

```sh
npm ci
npm run dev
```
