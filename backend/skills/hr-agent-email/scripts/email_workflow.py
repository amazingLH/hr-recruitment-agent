"""
* 文件作用
  * 这是邮件 Skill 的阶段执行脚本。draft 阶段整理收件人、主题和正文并返回待确认草稿；send 阶段只在确认动作匹配且邮件字段完整时返回发送请求，由 Agent 外层调用 EmailSendService/SMTP 真正发送，从而把内容生成与有外部影响的发送操作分开。
* 类说明
  * 本文件没有定义业务类，使用模块级函数实现 FastAPI 接口或 Skill 阶段。
* 主要函数与方法说明
  * `_fallback_draft()`：当 LLM 草稿生成失败或没有可用结果时，根据收件人、主题和用户原始要求拼出最小可编辑邮件草稿。
  * `_build_send_request()`：从草稿文本及人工确认参数中整理 recipient_email、subject、body 和 action，形成前端确认表单的数据载荷。
  * `run_draft_phase()`：根据用户要求生成邮件草稿字段，返回 requires_confirmation，禁止在草稿阶段直接发送。
  * `run_send_phase()`：验证确认动作及邮件字段后生成 send_email 请求，交给外层邮件服务执行。
"""

# 导入 re，用于通过正则识别意图、字段、候选人和邮件地址。
import re


# 调用 `re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")` 完成当前解析、查询或对象构造，并把返回值保存到 `EMAIL_PATTERN`；它表示用于从用户消息中识别收件人地址的邮箱正则规则。
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


# 使用 `def` 普通函数 定义 `_fallback_draft()`。当 LLM 草稿生成失败或没有可用结果时，根据收件人、主题和用户原始要求拼出最小可编辑邮件草稿。
def _fallback_draft() -> str:
    # 从 `_fallback_draft()` 返回 `(`，并结束当前函数。
    return (
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "主题：【待补充岗位】面试/考试通知\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "候选人您好，\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "感谢您关注我们的【待补充岗位】机会。我们希望邀请您参与下一轮招聘环节。\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "时间：【待补充】\n地点/方式：【待补充】\n需准备材料：【待补充】\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "如时间不便，请回复可替代时间，我们会尽快协调。\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "祝好，\n【HR姓名/公司】"
    # 结束 `_fallback_draft` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )


# 使用 `def` 普通函数 定义 `_build_send_request()`。从草稿文本及人工确认参数中整理 recipient_email、subject、body 和 action，形成前端确认表单的数据载荷。
def _build_send_request(user_text: str, draft_text: str, confirmation_action: str) -> dict[str, str]:
    # 把 `recipient_email` 初始化为 `""`，表示当前尚无可用结果；后续分支成功后会写入邮件收件人地址。
    recipient_email = ""
    # 调用 `EMAIL_PATTERN.search(user_text or "")` 完成当前解析、查询或对象构造，并把返回值保存到 `email_match`；它表示邮箱正则表达式的匹配结果。
    email_match = EMAIL_PATTERN.search(user_text or "")
    # 使用 `if` 判断 `email_match`；条件成立时执行下面分支。
    if email_match:
        # 调用 `email_match.group(0)` 完成当前解析、查询或对象构造，并把返回值保存到 `recipient_email`；它表示邮件收件人地址。
        recipient_email = email_match.group(0)

    # 计算表达式 `"【待补充主题】"` 并把结果保存到 `subject`；该结果在后续流程中用作待发送邮件的主题。
    subject = "【待补充主题】"
    # 调用 `draft_text.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
    body = draft_text.strip()
    # 调用 `draft_text.splitlines()` 完成当前解析、查询或对象构造，并把返回值保存到 `lines`；它表示按换行拆开的文本行。
    lines = draft_text.splitlines()
    # 使用 `for` 循环 `index, line in enumerate(lines)`，逐项处理集合数据。
    for index, line in enumerate(lines):
        # 调用 `line.strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `stripped`；它表示去除首尾空白后的当前文本行，便于判断标题、正文或配置字段。
        stripped = line.strip()
        # 使用 `if` 判断 `stripped.startswith("主题：") or stripped.startswith("Subject:")`；条件成立时执行下面分支。
        if stripped.startswith("主题：") or stripped.startswith("Subject:"):
            # 调用 `stripped.split("：", 1)[-1].split(":", 1)[-1].strip() or subject` 完成当前解析、查询或对象构造，并把返回值保存到 `subject`；它表示待发送邮件的主题。
            subject = stripped.split("：", 1)[-1].split(":", 1)[-1].strip() or subject
            # 计算表达式 `"\n".join(lines[index + 1:]).strip() or body` 并把结果保存到 `body`；该结果在后续流程中用作待发送邮件的正文。
            body = "\n".join(lines[index + 1:]).strip() or body
            # 使用 `break` 结束当前循环，避免继续执行无效迭代。
            break

    # 从 `_build_send_request()` 返回 `{`，并结束当前函数。
    return {
        # 在字典中设置 `action` 字段为 `confirmation_action`，用于表示用户确认参数中的操作名称，用于决定继续 draft 还是 send 阶段。
        "action": confirmation_action,
        # 在字典中设置 `recipient_email` 字段为 `recipient_email`，用于表示邮件收件人地址。
        "recipient_email": recipient_email,
        # 在字典中设置 `subject` 字段为 `subject`，用于表示待发送邮件的主题。
        "subject": subject,
        # 在字典中设置 `body` 字段为 `body`，用于表示待发送邮件的正文。
        "body": body,
        # 在字典中设置 `draft_text` 字段为 `draft_text.strip()`，用于表示等待用户确认的完整邮件草稿。
        "draft_text": draft_text.strip(),
    # 结束 `_build_send_request` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    }


# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `run_draft_phase()`。根据用户要求生成邮件草稿字段，返回 requires_confirmation，禁止在草稿阶段直接发送。
async def run_draft_phase(context: dict) -> dict:
    # 调用 `context.get("llm_service")` 完成当前解析、查询或对象构造，并把返回值保存到 `llm_service`；它表示直接调用千问兼容接口的模型服务。
    llm_service = context.get("llm_service")
    # 调用 `context.get("message", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `message`；它表示用户本次输入的自然语言消息。
    message = context.get("message", "")
    # 调用 `context.get("memory_context", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `memory_context`；它表示从数据库历史消息整理出的短期会话记忆。
    memory_context = context.get("memory_context", "")
    # 调用 `context.get("skill_markdown", "")` 完成当前解析、查询或对象构造，并把返回值保存到 `skill_markdown`；它表示SKILL.md 的完整规则文本。
    skill_markdown = context.get("skill_markdown", "")
    # 调用 `context.get("confirmation_action", "send_email")` 完成当前解析、查询或对象构造，并把返回值保存到 `confirmation_action`；它表示触发高风险操作继续执行的人工确认动作。
    confirmation_action = context.get("confirmation_action", "send_email")
    # 调用 `str(context.get("draft_text") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `draft_override`；它表示用户确认参数中传入的已编辑邮件草稿；存在时应优先于重新生成。
    draft_override = str(context.get("draft_text") or "").strip()

    # 计算表达式 `f"历史上下文：\n{memory_context}\n" if memory_context else ""` 并把结果保存到 `memory_block`；该结果在后续流程中用作整理后准备加入模型提示词的会话历史文本块。
    memory_block = f"历史上下文：\n{memory_context}\n" if memory_context else ""
    # 计算表达式 `(` 并把结果保存到 `prompt`；该结果在后续流程中用作发送给 LLM 的完整提示词。
    prompt = (
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "你正在执行 hr-agent-email skill。请先遵循下面的 SKILL.md 工作流，再完成当前任务。\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        f"{skill_markdown}\n\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "当前执行阶段：draft phase。\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "你是专业 HR。请根据 skill 工作流生成一封中文候选人邮件草稿。\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "请优先按以下结构输出：\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "主题：...\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "正文：\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        "要求：包含主题、称呼、正文、下一步动作、署名占位；如果关键信息缺失，用【待补充】标注，不要编造具体时间地点。不要把具体的面试题目发给候选人\n"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        f"{memory_block}"
        # 把该字符串作为邮件草稿或邮件提示词的一部分；相邻字符串在圆括号中会被 Python 自动拼接。
        f"用户需求：{message}"
    # 结束 `run_draft_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    )

    # 使用 `if` 判断 `draft_override`；条件成立时执行下面分支。
    if draft_override:
        # 计算表达式 `draft_override` 并把结果保存到 `draft`；该结果在后续流程中用作准备展示给用户确认的邮件主题和正文草稿。
        draft = draft_override
    # 前面的 `if/elif` 条件均不成立时进入该备用分支。
    else:
        # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
        try:
            # 使用 `if` 判断 `llm_service is None`；条件成立时执行下面分支。
            if llm_service is None:
                # 导入 LLMService，用于普通模型调用、ReAct 和文本改写服务。
                from app.services.llm_service import LLMService
                # 调用 `LLMService()` 完成当前解析、查询或对象构造，并把返回值保存到 `llm_service`；它表示直接调用千问兼容接口的模型服务。
                llm_service = LLMService()
            # 计算表达式 `(await llm_service.generate_response(prompt)).strip()` 并把结果保存到 `draft`；该结果在后续流程中用作准备展示给用户确认的邮件主题和正文草稿。
            draft = (await llm_service.generate_response(prompt)).strip()
        # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
        except Exception:
            # 调用 `_fallback_draft()` 完成当前解析、查询或对象构造，并把返回值保存到 `draft`；它表示准备展示给用户确认的邮件主题和正文草稿。
            draft = _fallback_draft()

    # 调用 `_build_send_request(message, draft, confirmation_action)` 完成当前解析、查询或对象构造，并把返回值保存到 `send_request`；它表示邮件草稿确认卡片使用的结构化发送参数。
    send_request = _build_send_request(message, draft, confirmation_action)
    # 从 `run_draft_phase()` 返回 `{`，并结束当前函数。
    return {
        # 在字典中设置 `message` 字段为 `draft`，用于表示用户本次输入的自然语言消息。
        "message": draft,
        # 在字典中设置 `steps` 字段为 `[`，用于表示返回前端展示的 Agent 执行步骤。
        "steps": [
            # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
            {"id": "draft", "title": "生成邮件草稿", "status": "completed", "detail": "已生成可编辑邮件草稿。", "tool": "draft"},
            # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
            {"id": "confirm_send", "title": "等待人工确认", "status": "running", "detail": "请确认收件人、主题和正文后再发送。"},
            # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
            {"id": "send_email", "title": "发送邮件", "status": "pending", "detail": "确认后使用 backend/skills/hr-agent-email/config.txt 中配置的 SMTP 邮箱发出邮件。", "tool": "send"},
        # 结束 `run_draft_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ],
        # 在字典中设置 `artifacts` 字段为 `[`，用于表示本轮生成的 JD、评分、面试方案等业务产物。
        "artifacts": [
            # 构造一条 Agent 业务产物字典，保存产物类型、标题、正文和元数据，供前端选择对应卡片组件展示；语法上使用字典字面量组织结构化结果。
            {"type": "email_draft", "title": "邮件通知草稿", "content": draft, "metadata": {"skill": "hr-agent-email", "phase": "draft"}},
            # 构造一条 Agent 业务产物字典，保存产物类型、标题、正文和元数据，供前端选择对应卡片组件展示；语法上使用字典字面量组织结构化结果。
            {"type": "email_send_request", "title": "确认并发送邮件", "content": send_request, "metadata": {"skill": "hr-agent-email", "phase": "draft"}},
        # 结束 `run_draft_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ],
        # 在字典中设置 `suggestions` 字段为 `[]`，用于表示任务结束后提供给用户的下一步操作建议。
        "suggestions": [],
        # 在字典中设置 `requires_confirmation` 字段为 `True`，用于表示后端是否暂停执行并要求用户人工确认。
        "requires_confirmation": True,
        # 在字典中设置 `missing_fields` 字段为 `["recipient_email"] if not send_request.get("recipient_email") else []`，用于表示继续执行工具前仍缺少的字段名称列表。
        "missing_fields": ["recipient_email"] if not send_request.get("recipient_email") else [],
    # 结束 `run_draft_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    }


# 使用 `async def` 异步函数，可在内部使用 `await` 调用数据库或网络服务 定义 `run_send_phase()`。验证确认动作及邮件字段后生成 send_email 请求，交给外层邮件服务执行。
async def run_send_phase(context: dict) -> dict:
    # 调用 `context.get("confirmed_requirements") or {}` 完成当前解析、查询或对象构造，并把返回值保存到 `payload`；它表示本次 Skill 或接口准备交给下游服务的结构化业务数据。
    payload = context.get("confirmed_requirements") or {}
    # 调用 `context.get("email_service")` 完成当前解析、查询或对象构造，并把返回值保存到 `email_service`；它表示负责读取邮箱配置并通过 SMTP 真正发送邮件的服务对象。
    email_service = context.get("email_service")
    # 调用 `context.get("user_id")` 完成当前解析、查询或对象构造，并把返回值保存到 `user_id`；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
    user_id = context.get("user_id")
    # 调用 `str(payload.get("recipient_email") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `recipient_email`；它表示邮件收件人地址。
    recipient_email = str(payload.get("recipient_email") or "").strip()
    # 调用 `str(payload.get("subject") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `subject`；它表示待发送邮件的主题。
    subject = str(payload.get("subject") or "").strip()
    # 调用 `str(payload.get("body") or "").strip()` 完成当前解析、查询或对象构造，并把返回值保存到 `body`；它表示待发送邮件的正文。
    body = str(payload.get("body") or "").strip()

    # 把 `missing_fields` 初始化为 `[]`，表示当前尚无可用结果；后续分支成功后会写入继续执行工具前仍缺少的字段名称列表。
    missing_fields = []
    # 使用 `if` 判断 `not recipient_email`；条件成立时执行下面分支。
    if not recipient_email:
        # 调用列表的 `append()`，把 `missing_fields.append("recipient_email")` 对应的新元素追加到现有结果末尾。
        missing_fields.append("recipient_email")
    # 使用 `if` 判断 `not subject`；条件成立时执行下面分支。
    if not subject:
        # 调用列表的 `append()`，把 `missing_fields.append("subject")` 对应的新元素追加到现有结果末尾。
        missing_fields.append("subject")
    # 使用 `if` 判断 `not body`；条件成立时执行下面分支。
    if not body:
        # 调用列表的 `append()`，把 `missing_fields.append("body")` 对应的新元素追加到现有结果末尾。
        missing_fields.append("body")

    # 保护后续数据库或外部服务调用，使失败能够被记录、回滚或转换为明确响应。
    try:
        # 异步调用 `EmailSendService.send_agent_email()`，校验人工确认后的收件人、主题和正文，并通过 SMTP 真正发送邮件。
        result = await email_service.send_agent_email(
            # 使用关键字参数语法，把 `user_id` 传入 `user_id` 参数；它表示当前登录用户的唯一 ID，用于权限和数据隔离。
            user_id=user_id,
            # 使用关键字参数语法，把 `recipient_email` 传入 `recipient_email` 参数；它表示邮件收件人地址。
            recipient_email=recipient_email,
            # 使用关键字参数语法，把 `subject` 传入 `subject` 参数；它表示待发送邮件的主题。
            subject=subject,
            # 使用关键字参数语法，把 `body` 传入 `body` 参数；它表示待发送邮件的正文。
            body=body,
        # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        )
    # 捕获本分支可能出现的解析、数据库或外部服务异常并执行降级处理。
    except ValueError as exc:
        # 从 `run_send_phase()` 返回 `{`，并结束当前函数。
        return {
            # 在字典中设置 `message` 字段为 `str(exc)`，用于表示用户本次输入的自然语言消息。
            "message": str(exc),
            # 在字典中设置 `steps` 字段为 `[`，用于表示返回前端展示的 Agent 执行步骤。
            "steps": [
                # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
                {"id": "confirm_send", "title": "确认邮件内容", "status": "completed", "detail": "已收到人工确认。"},
                # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
                {"id": "send_email", "title": "发送邮件", "status": "failed", "detail": str(exc), "tool": "send"},
            # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 在字典中设置 `artifacts` 字段为 `[`，用于表示本轮生成的 JD、评分、面试方案等业务产物。
            "artifacts": [
                # 构造一条 Agent 业务产物字典，保存产物类型、标题、正文和元数据，供前端选择对应卡片组件展示；语法上使用字典字面量组织结构化结果。
                {"type": "email_send_request", "title": "确认并发送邮件", "content": payload, "metadata": {"skill": "hr-agent-email", "phase": "send"}}
            # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
            ],
            # 在字典中设置 `suggestions` 字段为 `["填写 backend/skills/hr-agent-email/config.txt", "修改收件人后重试", "重新生成邮件草稿"]`，用于表示任务结束后提供给用户的下一步操作建议。
            "suggestions": ["填写 backend/skills/hr-agent-email/config.txt", "修改收件人后重试", "重新生成邮件草稿"],
            # 在字典中设置 `requires_confirmation` 字段为 `True`，用于表示后端是否暂停执行并要求用户人工确认。
            "requires_confirmation": True,
            # 在字典中设置 `missing_fields` 字段为 `missing_fields`，用于表示继续执行工具前仍缺少的字段名称列表。
            "missing_fields": missing_fields,
        # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        }

    # 从 `run_send_phase()` 返回 `{`，并结束当前函数。
    return {
        # 在字典中设置 `message` 字段为 `f"邮件已提交给 SMTP 服务器，收件人：{result['recipient_email']}。如果后续收到退信，请以退信原因排查收件地址、域名策略或发件邮箱信誉。"`，用于表示用户本次输入的自然语言消息。
        "message": f"邮件已提交给 SMTP 服务器，收件人：{result['recipient_email']}。如果后续收到退信，请以退信原因排查收件地址、域名策略或发件邮箱信誉。",
        # 在字典中设置 `steps` 字段为 `[`，用于表示返回前端展示的 Agent 执行步骤。
        "steps": [
            # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
            {"id": "confirm_send", "title": "确认邮件内容", "status": "completed", "detail": "已收到人工确认。"},
            # 构造一条 Agent 执行步骤字典，写入步骤标识、标题、状态和详情，供前端按时间顺序展示当前进度；语法上使用字典字面量一次创建多个键值对。
            {"id": "send_email", "title": "提交邮件", "status": "completed", "detail": f"已通过 {result['sender_email']} 提交给 SMTP 服务器；最终投递结果以收件方服务器为准。", "tool": "send"},
        # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ],
        # 在字典中设置 `artifacts` 字段为 `[`，用于表示本轮生成的 JD、评分、面试方案等业务产物。
        "artifacts": [
            # 构造一条 Agent 业务产物字典，保存产物类型、标题、正文和元数据，供前端选择对应卡片组件展示；语法上使用字典字面量组织结构化结果。
            {"type": "email_send_result", "title": "邮件提交结果", "content": result, "metadata": {"skill": "hr-agent-email", "phase": "send"}}
        # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
        ],
        # 在字典中设置 `suggestions` 字段为 `["继续生成下一封邮件", "查看 backend/skills/hr-agent-email/config.txt"]`，用于表示任务结束后提供给用户的下一步操作建议。
        "suggestions": ["继续生成下一封邮件", "查看 backend/skills/hr-agent-email/config.txt"],
        # 在字典中设置 `requires_confirmation` 字段为 `False`，用于表示后端是否暂停执行并要求用户人工确认。
        "requires_confirmation": False,
        # 在字典中设置 `missing_fields` 字段为 `[]`，用于表示继续执行工具前仍缺少的字段名称列表。
        "missing_fields": [],
    # 结束 `run_send_phase` 中当前多行参数、集合或调用结构；这是 Python 的闭合语法。
    }
